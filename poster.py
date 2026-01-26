import os
import json
import argparse
import asyncio
import logging
import re
import subprocess
import time
import shutil
import fcntl
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from io import BytesIO
import httpx
from httpx import HTTPStatusError, ReadTimeout, Timeout
from PIL import Image

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# --- КОНФИГУРАЦИЯ ---
MAX_POSTED_RECORDS = 100
WATERMARK_SCALE = 0.35
HTTPX_TIMEOUT = Timeout(connect=10.0, read=60.0, write=10.0, pool=5.0)
MAX_RETRIES   = 3
RETRY_DELAY   = 5.0
DEFAULT_DELAY = 10.0

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

def escape_html(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")

def chunk_text(text: str, size: int = 4096) -> List[str]:
    paras = [p for p in text.replace('\r\n', '\n').split('\n\n') if p.strip()]
    chunks, current_chunk = [], ""
    for p in paras:
        if len(p) > size:
            if current_chunk: chunks.append(current_chunk)
            parts, sub_part = [], ""
            for word in p.split():
                if len(sub_part) + len(word) + 1 > size:
                    parts.append(sub_part)
                    sub_part = word
                else:
                    sub_part = f"{sub_part} {word}".lstrip()
            if sub_part: parts.append(sub_part)
            chunks.extend(parts)
            current_chunk = ""
        else:
            if not current_chunk: current_chunk = p
            elif len(current_chunk) + len(p) + 2 <= size: current_chunk += f"\n\n{p}"
            else:
                chunks.append(current_chunk)
                current_chunk = p
    if current_chunk: chunks.append(current_chunk)
    return chunks

# --- БЛОК ОБРАБОТКИ МЕДИА ---

def apply_watermark(img_path: Path, scale: float) -> bytes:
    """Наложение водяного знака на фото (используется текущая логика PIL)"""
    try:
        base_img = Image.open(img_path).convert("RGBA")
        base_width, _ = base_img.size
        watermark_path = Path(__file__).parent / "watermark.png"
        if not watermark_path.exists():
            img_byte_arr = BytesIO()
            base_img.convert("RGB").save(img_byte_arr, format='JPEG', quality=90)
            return img_byte_arr.getvalue()
        
        watermark_img = Image.open(watermark_path).convert("RGBA")
        wm_width, wm_height = watermark_img.size
        new_wm_width = int(base_width * scale)
        new_wm_height = int(wm_height * (new_wm_width / wm_width))
        resample_filter = getattr(Image.Resampling, "LANCZOS", Image.LANCZOS)
        watermark_img = watermark_img.resize((new_wm_width, new_wm_height), resample=resample_filter)
        
        overlay = Image.new("RGBA", base_img.size, (0, 0, 0, 0))
        padding = 10 # Отступ сверху/справа
        position = (base_width - new_wm_width - padding, padding)
        overlay.paste(watermark_img, position, watermark_img)
        
        composite_img = Image.alpha_composite(base_img, overlay).convert("RGB")
        img_byte_arr = BytesIO()
        composite_img.save(img_byte_arr, format='JPEG', quality=90)
        return img_byte_arr.getvalue()
    except Exception as e:
        logging.error(f"Ошибка вотермарки фото {img_path}: {e}")
        return img_path.read_bytes() if img_path.exists() else b""

async def process_video_logic(video_url: str, watermark_path: str = "watermark.png") -> Optional[str]:
    """Скачивание видео 360p и наложение вотермарки через FFmpeg"""
    if not video_url: return None
    ts = int(time.time())
    raw_path, final_path = f"raw_{ts}.mp4", f"video_{ts}.mp4"
    
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            # 1. Запрос к Loader.to
            resp = await client.get("https://loader.to/ajax/download.php", params={"format": "360", "url": video_url})
            task_id = resp.json().get("id")
            
            # 2. Ожидание
            download_url = None
            for _ in range(25):
                await asyncio.sleep(3)
                status = (await client.get("https://loader.to/ajax/progress.php", params={"id": task_id})).json()
                if status.get("success") == 1:
                    download_url = status.get("download_url")
                    break
            
            if not download_url: return None

            # 3. Скачивание
            async with client.stream("GET", download_url) as r:
                with open(raw_path, 'wb') as f:
                    async for chunk in r.aiter_bytes(): f.write(chunk)

            # 4. FFmpeg вотермарка (35% ширины, правый верхний угол)
            cmd = [
                "ffmpeg", "-y", "-i", raw_path, "-i", watermark_path,
                "-filter_complex", f"[1:v][0:v]scale2ref=iw*{WATERMARK_SCALE}:-1[wm][vid];[vid][wm]overlay=W-w-10:10",
                "-c:v", "libx264", "-preset", "ultrafast", "-crf", "28", "-c:a", "copy", final_path
            ]
            process = await asyncio.create_subprocess_exec(*cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            await process.wait()

            if os.path.exists(raw_path): os.remove(raw_path)
            return final_path if os.path.exists(final_path) else None
        except Exception as e:
            logging.error(f"Ошибка обработки видео: {e}")
            if os.path.exists(raw_path): os.remove(raw_path)
            return None

# --- СЕТЕВОЙ БЛОК ---

async def _post_with_retry(client: httpx.AsyncClient, method: str, url: str, data: Dict[str, Any], files: Optional[Dict[str, Any]] = None) -> bool:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = await client.request(method, url, data=data, files=files, timeout=HTTPX_TIMEOUT)
            resp.raise_for_status()
            return True
        except HTTPStatusError as e:
            if e.response.status_code == 429:
                retry_after = int(e.response.json().get("parameters", {}).get("retry_after", RETRY_DELAY))
                await asyncio.sleep(retry_after)
            elif 400 <= e.response.status_code < 500: return False
            else: await asyncio.sleep(RETRY_DELAY * attempt)
        except Exception: await asyncio.sleep(RETRY_DELAY * attempt)
    return False

async def send_complex_media_group(client: httpx.AsyncClient, token: str, chat_id: str, images: List[Path], video_path: Optional[str], watermark_scale: float) -> bool:
    """Сборка и отправка медиа-групп по 10 объектов. Видео всегда последнее."""
    all_items = []
    files_to_send = {}
    
    # Сначала фото
    for idx, img_path in enumerate(images):
        image_bytes = apply_watermark(img_path, scale=watermark_scale)
        if image_bytes:
            key = f"photo_{idx}"
            files_to_send[key] = (img_path.name, image_bytes, "image/jpeg")
            all_items.append({"type": "photo", "media": f"attach://{key}"})
    
    # В самый конец - видео
    if video_path and os.path.exists(video_path):
        key = "video_main"
        with open(video_path, 'rb') as f:
            files_to_send[key] = ("video.mp4", f.read(), "video/mp4")
        all_items.append({"type": "video", "media": f"attach://{key}"})

    if not all_items: return False

    # Разбивка на группы по 10
    chunks = [all_items[i:i + 10] for i in range(0, len(all_media := all_items), 10)]
    url = f"https://api.telegram.org/bot{token}/sendMediaGroup"

    success = True
    for chunk in chunks:
        # Отбираем только те файлы, которые нужны для текущего чанка
        current_files = {}
        for item in chunk:
            key = item["media"].replace("attach://", "")
            if key in files_to_send:
                current_files[key] = files_to_send[key]
        
        data = {"chat_id": chat_id, "media": json.dumps(chunk)}
        if not await _post_with_retry(client, "POST", url, data, current_files):
            success = False
        await asyncio.sleep(1) # Пауза между группами
        
    return success

async def send_message(client: httpx.AsyncClient, token: str, chat_id: str, text: str, **kwargs) -> bool:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if kwargs.get("reply_markup"):
        data["reply_markup"] = json.dumps(kwargs["reply_markup"])
    return await _post_with_retry(client, "POST", url, data)

# --- ЛОГИКА СОСТОЯНИЯ ---

def load_posted_ids(state_file: Path) -> List[str]:
    if not state_file.is_file(): return []
    try:
        data = json.loads(state_file.read_text(encoding="utf-8"))
        if len(data) > MAX_POSTED_RECORDS: data = data[-MAX_POSTED_RECORDS:]
        return [str(item) for item in data if item is not None]
    except Exception: return []

def save_posted_ids(ids_to_save: List[str], state_file: Path) -> None:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    try:
        final_ids = [int(i) for i in ids_to_save]
        with state_file.open("w", encoding="utf-8") as f:
            json.dump(final_ids, f, ensure_ascii=False, indent=2)
    except Exception as e: logging.error(f"Ошибка сохранения состояния: {e}")

# --- MAIN ---

async def main(parsed_dir: str, state_path: str, limit: Optional[int], watermark_scale: float):
    token, chat_id = os.getenv("TELEGRAM_TOKEN"), os.getenv("TELEGRAM_CHANNEL")
    if not token or not chat_id: return

    parsed_root, state_file = Path(parsed_dir), Path(state_path)
    posted_ids_list = load_posted_ids(state_file)
    posted_ids_set = set(posted_ids_list)

    articles_to_post = []
    for d in sorted(parsed_root.iterdir()):
        meta_file = d / "meta.json"
        if d.is_dir() and meta_file.is_file():
            try:
                art_meta = json.loads(meta_file.read_text(encoding="utf-8"))
                aid = str(art_meta.get("id"))
                if aid and aid not in posted_ids_set:
                    # Валидация
                    title = art_meta.get("title", "").strip()
                    text_path = d / art_meta.get("text_file", "")
                    if not text_path.is_file(): continue
                    
                    img_dir = d / "images"
                    imgs = sorted([p for p in img_dir.iterdir() if p.suffix.lower() in {".jpg", ".jpeg", ".png"}]) if img_dir.is_dir() else []
                    
                    articles_to_post.append({
                        "id": aid, "title": title, "text_path": text_path, 
                        "image_paths": imgs, "video_url": art_meta.get("video_url")
                    })
            except Exception: continue

    articles_to_post.sort(key=lambda x: int(x["id"]))
    if not articles_to_post:
        save_posted_ids(posted_ids_list, state_file)
        return

    async with httpx.AsyncClient() as client:
        sent_count = 0
        final_posted_ids = list(posted_ids_list)
        
        for article in articles_to_post:
            if limit and sent_count >= limit: break
            
            logging.info(f"Публикуем ID={article['id']}...")
            processed_video = None
            try:
                # 1. Обработка видео (если есть)
                if article["video_url"]:
                    processed_video = await process_video_logic(article["video_url"])

                # 2. Отправка медиа-групп (Фото + Видео в конце)
                await send_complex_media_group(client, token, chat_id, article["image_paths"], processed_video, watermark_scale)

                # 3. Подготовка и отправка текста
                raw_text = article["text_path"].read_text(encoding="utf-8")
                cleaned_text = raw_text.lstrip()
                if cleaned_text.startswith(article["title"]):
                    cleaned_text = cleaned_text[len(article["title"]):].lstrip()

                full_html = f"<b>{escape_html(article['title'])}</b>\n\n{escape_html(cleaned_text)}"
                full_html = re.sub(r'\n{3,}', '\n\n', full_html).strip()
                chunks = chunk_text(full_html)

                for i, chunk in enumerate(chunks):
                    is_last = (i == len(chunks) - 1)
                    markup = {"inline_keyboard": [[{"text": "Обмен валют", "url": "https://t.me/mister1dollar"}, {"text": "Отзывы", "url": "https://t.me/feedback1dollar"}]]} if is_last else None
                    await send_message(client, token, chat_id, chunk, reply_markup=markup)

                final_posted_ids.append(article['id'])
                sent_count += 1
                logging.info(f"✅ Успешно: ID={article['id']}")

            except Exception as e:
                logging.error(f"❌ Ошибка ID={article['id']}: {e}")
            finally:
                if processed_video and os.path.exists(processed_video):
                    os.remove(processed_video)
            
            await asyncio.sleep(float(os.getenv("POST_DELAY", DEFAULT_DELAY)))

    if sent_count > 0:
        if len(final_posted_ids) > MAX_POSTED_RECORDS:
            final_posted_ids = final_posted_ids[-MAX_POSTED_RECORDS:]
        save_posted_ids(final_posted_ids, state_file)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--parsed-dir", type=str, default="articles")
    parser.add_argument("--state-file", type=str, default="articles/posted.json")
    parser.add_argument("-n", "--limit", type=int, default=None)
    parser.add_argument("--watermark-scale", type=float, default=WATERMARK_SCALE)
    args = parser.parse_args()
    asyncio.run(main(args.parsed_dir, args.state_file, args.limit, args.watermark_scale))
