import os
import json
import argparse
import asyncio
import logging
import re
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from io import BytesIO
from collections import deque

import httpx
from httpx import HTTPStatusError, ReadTimeout, Timeout
from PIL import Image

# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
# --- Константы ---
MAX_POSTED_RECORDS = 100
WATERMARK_SCALE = 0.45
HTTPX_TIMEOUT = Timeout(connect=10.0, read=60.0, write=10.0, pool=5.0)
MAX_RETRIES   = 3
RETRY_DELAY   = 5.0
DEFAULT_DELAY = 10.0

def escape_html(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")

def chunk_text(text: str, size: int = 4096) -> List[str]:
    paras = [p for p in text.replace('\r\n', '\n').split('\n\n') if p.strip()]
    chunks, curr = [], ""
    for p in paras:
        if len(p) > size:
            if curr: chunks.append(curr)
            parts, sub = [], ""
            for w in p.split(" "):
                if len(sub) + len(w) + 1 > size:
                    parts.append(sub)
                    sub = w
                else:
                    sub = (sub + " " + w).lstrip()
            if sub: parts.append(sub)
            chunks.extend(parts)
            curr = ""
        else:
            if not curr: curr = p
            elif len(curr) + 2 + len(p) <= size: curr += "\n\n" + p
            else:
                chunks.append(curr)
                curr = p
    if curr: chunks.append(curr)
    return chunks

def apply_watermark_to_image(img_path: Path, scale: float) -> bytes:
    try:
        base_img = Image.open(img_path).convert("RGBA")
        base_width, _ = base_img.size
        watermark_path = Path(__file__).parent / "watermark.png"
        if not watermark_path.exists():
            logging.warning("Watermark file not found. Skipping watermark for image.")
            img_byte_arr = BytesIO()
            base_img.convert("RGB").save(img_byte_arr, format='JPEG', quality=90)
            return img_byte_arr.getvalue()
        watermark_img = Image.open(watermark_path).convert("RGBA")
        wm_width, wm_height = watermark_img.size
        new_wm_width = int(base_width * scale)
        new_wm_height = int(wm_height * (new_wm_width / wm_width))
        filt = getattr(Image.Resampling, "LANCZOS", Image.LANCZOS)
        watermark_img = watermark_img.resize((new_wm_width, new_wm_height), resample=filt)
        overlay = Image.new("RGBA", base_img.size, (0, 0, 0, 0))
        padding = int(base_width * 0.02)
        position = (base_width - new_wm_width - padding, padding)
        overlay.paste(watermark_img, position, watermark_img)
        composite_img = Image.alpha_composite(base_img, overlay).convert("RGB")
        img_byte_arr = BytesIO()
        composite_img.save(img_byte_arr, format='JPEG', quality=90)
        return img_byte_arr.getvalue()
    except Exception as e:
        logging.error(f"Failed to apply watermark to image {img_path}: {e}")
        try:
            with open(img_path, 'rb') as f: return f.read()
        except Exception as e_orig:
            logging.error(f"Failed to read original image {img_path}: {e_orig}")
            return b""

def apply_watermark_to_video(video_path: Path, scale: float) -> Optional[Path]:
    logging.info(f"Applying watermark to video {video_path.name}...")
    watermark_path = Path(__file__).parent / "watermark.png"
    if not watermark_path.exists():
        logging.warning("Watermark file not found. Skipping video watermarking.")
        return video_path
    output_path = video_path.with_name(f"wm_{video_path.name}")
    command = [
        'ffmpeg', '-i', str(video_path), '-i', str(watermark_path),
        '-filter_complex', f'[1:v]scale=iw*{scale}:-1[ovr];[0:v][ovr]overlay=main_w-overlay_w-10:10',
        '-codec:a', 'copy', str(output_path), '-y'
    ]
    try:
        subprocess.run(command, check=True, capture_output=True, text=True)
        logging.info(f"Watermark applied to video. New file: {output_path.name}")
        return output_path
    except subprocess.CalledProcessError as e:
        logging.error(f"FFmpeg error for {video_path.name}:\n{e.stderr}")
        return None

async def _post_with_retry(client: httpx.AsyncClient, method: str, url: str, data: Dict[str, Any], files: Optional[Dict[str, Any]] = None) -> bool:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = await client.request(method, url, data=data, files=files, timeout=HTTPX_TIMEOUT)
            resp.raise_for_status()
            return True
        except HTTPStatusError as e:
            if e.response.status_code == 429:
                retry_after = int(e.response.json().get("parameters", {}).get("retry_after", RETRY_DELAY))
                logging.warning(f"🐢 Rate limited. Retrying after {retry_after} seconds...")
                await asyncio.sleep(retry_after)
            elif 400 <= e.response.status_code < 500:
                logging.error(f"❌ Client error {e.response.status_code}: {e.response.text}")
                return False
            else:
                logging.warning(f"⚠️ Server error {e.response.status_code}. Retry {attempt}/{MAX_RETRIES}...")
                await asyncio.sleep(RETRY_DELAY * attempt)
        except (ReadTimeout, httpx.RequestError) as e:
            logging.warning(f"⏱️ Network error: {e}. Retry {attempt}/{MAX_RETRIES}...")
            await asyncio.sleep(RETRY_DELAY * attempt)
    logging.error(f"☠️ Failed to send request to {url} after {MAX_RETRIES} attempts.")
    return False

async def send_media_group(client: httpx.AsyncClient, token: str, chat_id: str, media_paths: List[Path], media_type: str) -> bool:
    url = f"https://api.telegram.org/bot{token}/sendMediaGroup"
    media, files = [], {}
    for idx, file_path in enumerate(media_paths[:10]):
        key = f"file{idx}"
        if media_type == "photo":
            file_bytes = apply_watermark_to_image(file_path, scale=WATERMARK_SCALE)
            content_type = "image/jpeg"
        elif media_type == "video":
            file_bytes = file_path.read_bytes()
            content_type = "video/mp4"
        else:
            return False
        if file_bytes:
            files[key] = (file_path.name, file_bytes, content_type)
            media.append({"type": media_type, "media": f"attach://{key}"})
    if not media: return False
    data = {"chat_id": chat_id, "media": json.dumps(media)}
    return await _post_with_retry(client, "POST", url, data, files)

async def send_message(client: httpx.AsyncClient, token: str, chat_id: str, text: str, **kwargs) -> bool:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if kwargs.get("reply_markup"):
        data["reply_markup"] = json.dumps(kwargs["reply_markup"])
    return await _post_with_retry(client, "POST", url, data)

def validate_article(art: Dict[str, Any], article_dir: Path) -> Optional[Tuple[str, Path, List[Path], List[Path], str]]:
    aid, title = art.get("id"), art.get("title", "").strip()
    txt_name = Path(art.get("text_file", "")).name if art.get("text_file") else None
    if not title:
        logging.error("Invalid title for article in %s. Skipping.", article_dir)
        return None

    # Оригинальная логика поиска текстового файла
    text_path: Optional[Path] = None
    if txt_name and (article_dir / txt_name).is_file():
        text_path = article_dir / txt_name
    if not text_path:
        if (article_dir / "content.ru.txt").is_file(): text_path = article_dir / "content.ru.txt"
        elif (article_dir / "content.txt").is_file(): text_path = article_dir / "content.txt"
        elif candidates := list(article_dir.glob("*.txt")): text_path = candidates[0]
    if not text_path or not text_path.is_file():
        logging.error("No text file found for article in %s. Skipping.", article_dir)
        return None
    
    # Оригинальная логика поиска изображений
    valid_imgs: List[Path] = []
    for name in art.get("images", []):
        p = article_dir / Path(name).name
        if not p.is_file(): p = article_dir / "images" / Path(name).name
        if p.is_file(): valid_imgs.append(p)
    if not valid_imgs and (imgs_dir := article_dir / "images").is_dir():
        valid_imgs = [p for p in imgs_dir.iterdir() if p.suffix.lower() in (".jpg", ".jpeg", ".png")]
    
    # ДОБАВЛЕНО: Логика поиска видео
    valid_videos: List[Path] = []
    video_names = art.get("videos", [])
    for name in video_names:
        p = article_dir / "videos" / Path(name).name
        if p.is_file(): valid_videos.append(p)

    html_title = f"<b>{escape_html(title)}</b>"
    return html_title, text_path, valid_imgs, valid_videos, title

def load_posted_ids(state_file: Path) -> Set[str]:
    """Читает state-файл, возвращает set из ID в виде СТРОК."""
    if not state_file.is_file(): return set()
    try:
        data = json.loads(state_file.read_text(encoding="utf-8").strip())
        return {str(item) for item in data if str(item).isdigit()}
    except (json.JSONDecodeError, ValueError): return set()

def save_posted_ids(all_ids_to_save: Set[str], state_file: Path) -> None:
    """Сохраняет отсортированный список ID в файл состояния."""
    state_file.parent.mkdir(parents=True, exist_ok=True)
    # Оригинальная логика с deque
    temp_deque = deque(maxlen=MAX_POSTED_RECORDS)
    sorted_ids = sorted([int(i) for i in all_ids_to_save]) # Приводим к int для сортировки
    for aid in sorted_ids:
        temp_deque.append(aid)
    final_list_to_save = list(temp_deque)
    try:
        with state_file.open("w", encoding="utf-8") as f:
            json.dump(final_list_to_save, f, ensure_ascii=False, indent=2)
        logging.info(f"Saved {len(final_list_to_save)} IDs to state file (max {MAX_POSTED_RECORDS}).")
    except Exception as e:
        logging.error(f"Failed to save state file: {e}")

async def main(parsed_dir: str, state_path: str, limit: Optional[int]):
    token, chat_id = os.getenv("TELEGRAM_TOKEN"), os.getenv("TELEGRAM_CHANNEL")
    if not token or not chat_id:
        logging.error("TELEGRAM_TOKEN или TELEGRAM_CHANNEL переменные окружения должны быть установлены.")
        return

    parsed_root, state_file = Path(parsed_dir), Path(state_path)
    if not parsed_root.is_dir():
        logging.error("Директория %s не существует.", parsed_root)
        return

    posted_ids_old = load_posted_ids(state_file)
    logging.info("Загружено %d ранее опубликованных ID.", len(posted_ids_old))

    articles_to_post: List[Dict[str, Any]] = []
    for d in sorted(parsed_root.iterdir()):
        meta_file = d / "meta.json"
        if d.is_dir() and meta_file.is_file():
            try:
                art_meta = json.loads(meta_file.read_text(encoding="utf-8"))
                # Сравнение как строк
                article_id_str = str(art_meta.get("id"))
                if article_id_str and article_id_str not in posted_ids_old:
                    if validated_data := validate_article(art_meta, d):
                        html_title, text_path, image_paths, video_paths, original_title = validated_data
                        articles_to_post.append({
                            "id": article_id_str, "html_title": html_title, "text_path": text_path,
                            "image_paths": image_paths, "video_paths": video_paths,
                            "original_plain_title": original_title
                        })
            except Exception as e:
                logging.error("Ошибка при обработке статьи %s: %s.", d.name, e)

    articles_to_post.sort(key=lambda x: int(x["id"]))
    if not articles_to_post:
        logging.info("🔍 Нет новых статей для публикации.")
        return
    
    async with httpx.AsyncClient() as client:
        sent, new_ids = 0, set()
        for article in articles_to_post:
            if limit is not None and sent >= limit: break
            aid = article["id"]
            logging.info("Попытка публикации ID=%s", aid)
            processed_videos_to_delete = []
            try:
                if article["image_paths"]:
                    await send_media_group(client, token, chat_id, article["image_paths"], "photo")
                
                processed_videos = [apply_watermark_to_video(vp, WATERMARK_SCALE) for vp in article["video_paths"]]
                valid_processed_videos = [pv for pv in processed_videos if pv and pv.exists()]
                processed_videos_to_delete.extend(valid_processed_videos)

                if valid_processed_videos:
                    await send_media_group(client, token, chat_id, valid_processed_videos, "video")
                
                raw_text = article["text_path"].read_text(encoding="utf-8")
                cleaned_text = re.sub(rf"^{re.escape(article['original_plain_title'])}\s*", "", raw_text, flags=re.IGNORECASE)
                full_html_content = f"{article['html_title']}\n\n{escape_html(cleaned_text)}"
                chunks = chunk_text(full_html_content)

                for i, part in enumerate(chunks):
                    reply_markup = {"inline_keyboard": [[{"text": "Обмен валют", "url": "https://t.me/mister1dollar"}, {"text": "Отзывы", "url": "https://t.me/feedback1dollar"}]]} if i == len(chunks) - 1 else None
                    if not await send_message(client, token, chat_id, part, reply_markup=reply_markup):
                        raise Exception(f"Failed to send text chunk for ID={aid}")

                new_ids.add(aid)
                sent += 1
                logging.info("✅ Опубликовано ID=%s", aid)
            except Exception as e:
                logging.error("❌ Ошибка во время публикации статьи ID=%s: %s.", aid, e)
            finally:
                for pv in processed_videos_to_delete:
                    if "wm_" in pv.name: pv.unlink(missing_ok=True)
            await asyncio.sleep(float(os.getenv("POST_DELAY", DEFAULT_DELAY)))

    await client.aclose()
    all_ids_to_save = posted_ids_old.union(new_ids)
    save_posted_ids(all_ids_to_save, state_file)
    logging.info("📢 Завершено: отправлено %d статей.", sent)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Poster")
    parser.add_argument("--parsed-dir", type=str, default="articles", help="директория со статьями")
    parser.add_argument("--state-file", type=str, default="articles/posted.json", help="путь к state-файлу")
    parser.add_argument("-n", "--limit", type=int, default=None, help="максимальное число статей")
    args = parser.parse_args()
    asyncio.run(main(parsed_dir=args.parsed_dir, state_path=args.state_file, limit=args.limit))
