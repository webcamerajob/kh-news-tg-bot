import os
import json
import argparse
import asyncio
import logging
import re
import requests  # Синхронные вызовы для Facebook
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from io import BytesIO
import httpx
from httpx import HTTPStatusError, ReadTimeout, Timeout
from PIL import Image
import subprocess

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# --- КОНСТАНТЫ ---
MAX_POSTED_RECORDS = 300
WATERMARK_SCALE = 0.35
HTTPX_TIMEOUT = Timeout(connect=10.0, read=60.0, write=10.0, pool=5.0)
MAX_RETRIES     = 3
RETRY_DELAY     = 5.0
DEFAULT_DELAY = 10.0

# --- НАСТРОЙКИ FACEBOOK ---
FB_PAGE_ID = os.getenv("FB_PAGE_ID")
FB_PAGE_ACCESS_TOKEN = os.getenv("FB_PAGE_ACCESS_TOKEN")

def extract_video_thumb(video_path: Path) -> Optional[bytes]:
    """Извлекает кадр из видео для использования как thumbnail в Telegram."""
    try:
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tmp:
            tmp_path = tmp.name

        cmd = [
            "ffmpeg", "-y", "-i", str(video_path),
            "-ss", "00:00:01",
            "-vframes", "1",
            "-vf", "scale='min(320,iw)':-2",
            "-q:v", "5",
            tmp_path
        ]
        r = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

        if r.returncode != 0 or not Path(tmp_path).exists():
            return None

        data = Path(tmp_path).read_bytes()
        Path(tmp_path).unlink(missing_ok=True)

        if len(data) > 200 * 1024:
            logging.warning(f"⚠️ Thumb {video_path.name} > 200KB, пропускаем")
            return None
        return data
    except Exception as e:
        logging.error(f"❌ Ошибка извлечения thumb для {video_path.name}: {e}")
        return None

def apply_watermark(img_path: Path, scale: float) -> bytes:
    try:
        base_img = Image.open(img_path).convert("RGBA")
        base_width, _ = base_img.size
        watermark_path = Path(__file__).parent / "watermark.png"
        if not watermark_path.exists():
            logging.warning(f"⚠️ watermark.png не найден. Отправка оригинала {img_path.name}")
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
        padding = int(base_width * 0.02)
        position = (base_width - new_wm_width - padding, padding)
        overlay.paste(watermark_img, position, watermark_img)

        composite_img = Image.alpha_composite(base_img, overlay).convert("RGB")
        img_byte_arr = BytesIO()
        composite_img.save(img_byte_arr, format='JPEG', quality=90)
        return img_byte_arr.getvalue()
    except Exception as e:
        logging.error(f"❌ Ошибка водяного знака {img_path}: {e}")
        try:
            with open(img_path, 'rb') as f: return f.read()
        except: return b""

def post_to_facebook(text, media_files=None, watermark_scale=WATERMARK_SCALE):
    if not FB_PAGE_ACCESS_TOKEN or not FB_PAGE_ID:
        logging.warning("⚠️ Данные для Facebook не заполнены. Пропуск.")
        return

    full_message = text
    video_files = [f for f in (media_files or []) if f.suffix.lower() in ['.mp4', '.mov', '.m4v']]
    image_files = [f for f in (media_files or []) if f.suffix.lower() in ['.jpg', '.png', '.jpeg', '.webp']]
    posted_something = False

    # --- 1. ФОТО (ГАЛЕРЕЯ) ---
    if image_files:
        logging.info(f"📤 FB: Подготовка галереи из {len(image_files)} фото...")
        media_fbid_list = []

        for img_path in image_files:
            img_bytes = apply_watermark(img_path, watermark_scale)
            if not img_bytes: continue

            url_upload = f"https://graph.facebook.com/v19.0/{FB_PAGE_ID}/photos"
            payload = {"access_token": FB_PAGE_ACCESS_TOKEN, "published": "false"}
            files = {'source': ('image.jpg', img_bytes, 'image/jpeg')}

            try:
                up_r = requests.post(url_upload, data=payload, files=files, timeout=60)
                if up_r.status_code == 200:
                    photo_id = up_r.json().get('id')
                    media_fbid_list.append({"media_fbid": photo_id})
                else:
                    logging.warning(f"⚠️ FB Photo Upload Fail {img_path.name}: {up_r.text}")
            except Exception as e:
                logging.error(f"⚠️ FB Photo Exception {img_path.name}: {e}")

        if media_fbid_list:
            url_feed = f"https://graph.facebook.com/v19.0/{FB_PAGE_ID}/feed"
            payload = {
                "access_token": FB_PAGE_ACCESS_TOKEN,
                "message": full_message,
                "attached_media": json.dumps(media_fbid_list)
            }
            try:
                feed_r = requests.post(url_feed, data=payload, timeout=60)
                if feed_r.status_code == 200:
                    logging.info(f"✅ FB Photo Gallery Success: ID={feed_r.json().get('id')}")
                    posted_something = True
                else:
                    logging.error(f"❌ FB Gallery Error: {feed_r.text}")
            except Exception as e:
                 logging.error(f"❌ FB Gallery Exception: {e}")

    # --- 2. ВИДЕО ---
    if video_files:
        for vid in video_files:
            logging.info(f"📤 FB: Видео -> {vid.name}")
            url = f"https://graph.facebook.com/v19.0/{FB_PAGE_ID}/videos"
            payload = {"access_token": FB_PAGE_ACCESS_TOKEN, "description": full_message}
            try:
                with open(vid, 'rb') as f:
                    r = requests.post(url, data=payload, files={'source': f}, timeout=120)

                if r.status_code == 200:
                    logging.info(f"✅ FB Video Success: ID={r.json().get('id')}")
                    posted_something = True
                else:
                    logging.error(f"❌ FB Video Error: {r.text}")
            except Exception as e:
                logging.error(f"❌ FB Video Exception: {e}")

    # --- 3. ТОЛЬКО ТЕКСТ ---
    if not posted_something and not image_files and not video_files:
        logging.info("📤 FB: Только текст...")
        url = f"https://graph.facebook.com/v19.0/{FB_PAGE_ID}/feed"
        try:
            r = requests.post(url, data={"access_token": FB_PAGE_ACCESS_TOKEN, "message": full_message})
            if r.status_code == 200:
                logging.info(f"✅ FB Text Success: ID={r.json().get('id')}")
            else:
                logging.error(f"❌ FB Text Error: {r.text}")
        except Exception as e:
            logging.error(f"❌ FB Text Exception: {e}")

async def _post_with_retry(client: httpx.AsyncClient, method: str, url: str, data: Dict[str, Any], files: Optional[Dict[str, Any]] = None) -> bool:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = await client.request(method, url, data=data, files=files, timeout=HTTPX_TIMEOUT)
            resp.raise_for_status()
            return True
        except HTTPStatusError as e:
            if e.response.status_code == 429:
                retry_after = int(e.response.json().get("parameters", {}).get("retry_after", RETRY_DELAY))
                logging.warning(f"🐢 Rate limit. Ждем {retry_after} сек...")
                await asyncio.sleep(retry_after)
            elif 400 <= e.response.status_code < 500:
                logging.error(f"❌ Ошибка клиента {e.response.status_code}: {e.response.text}")
                return False
            else:
                logging.warning(f"⚠️ Ошибка сервера {e.response.status_code}. Попытка {attempt}/{MAX_RETRIES}")
                await asyncio.sleep(RETRY_DELAY * attempt)
        except Exception as e:
            logging.warning(f"⏱️ Сетевая ошибка: {e}. Попытка {attempt}/{MAX_RETRIES}")
            await asyncio.sleep(RETRY_DELAY * attempt)
    return False

async def send_media_group(client: httpx.AsyncClient, token: str, chat_id: str, media_files: List[Path], watermark_scale: float, silent: bool = True) -> bool:
    if not media_files: return False
    url = f"https://api.telegram.org/bot{token}/sendMediaGroup"
    overall_success = True

    for i in range(0, len(media_files), 10):
        chunk = media_files[i : i + 10]
        media_array = []
        files_to_send = {}

        logging.info(f"📦 Отправка пачки медиа {i//10 + 1} (файлов: {len(chunk)})")

        for idx, f_path in enumerate(chunk):
            f_key = f"media_{idx}"
            ext = f_path.suffix.lower()

            if ext in ['.mp4', '.mov', '.m4v']:
                m_type, m_mime = "video", "video/mp4"
                if f_path.stat().st_size > 49 * 1024 * 1024:
                    logging.warning(f"⚠️ Файл {f_path.name} > 49MB, пропускаем в Telegram")
                    continue
                m_bytes = f_path.read_bytes()
            else:
                m_type, m_mime = "photo", "image/jpeg"
                m_bytes = apply_watermark(f_path, watermark_scale)

            if not m_bytes: continue
            files_to_send[f_key] = (f_path.name, m_bytes, m_mime)
            media_item = {"type": m_type, "media": f"attach://{f_key}"}

            if m_type == "video":
                thumb_bytes = extract_video_thumb(f_path)
                if thumb_bytes:
                    thumb_key = f"thumb_{idx}"
                    files_to_send[thumb_key] = (f"{f_path.stem}_thumb.jpg", thumb_bytes, "image/jpeg")
                    media_item["thumbnail"] = f"attach://{thumb_key}"

            media_array.append(media_item)

        if not media_array: continue

        data = {
            "chat_id": chat_id, 
            "media": json.dumps(media_array),
            "disable_notification": silent
        }

        if not await _post_with_retry(client, "POST", url, data, files_to_send):
            overall_success = False
            logging.error(f"❌ Ошибка при отправке пачки {i//10 + 1}")

        if i + 10 < len(media_files):
            await asyncio.sleep(2)

    return overall_success

async def send_rich_accordion_message(client: httpx.AsyncClient, token: str, chat_id: str, title: str, text: str, silent: bool = False, **kwargs) -> bool:
    """
    Отправляет пост через Bot API 10.1+ (sendRichMessage) в виде чистого аккордеона (details):
    - Заголовок (H1) и первый абзац видны сразу.
    - Остальной текст убран в чистый сворачиваемый блок без полоски слева.
    """
    url = f"https://api.telegram.org/bot{token}/sendRichMessage"
    
    paras = [p.strip() for p in text.replace('\r\n', '\n').split('\n\n') if p.strip()]
    
    blocks = [
        {
            "type": "heading",
            "level": 1,
            "text": title
        }
    ]
    
    if paras:
        # Первый абзац отображается сразу
        blocks.append({
            "type": "paragraph",
            "text": paras[0]
        })
        
        # Остальные абзацы помещаем в чистый аккордеон (details)
        if len(paras) > 1:
            rest_text = "\n\n".join(paras[1:])
            blocks.append({
                "type": "details",
                "summary": "📖 Читать полностью",
                "blocks": [
                    {
                        "type": "paragraph",
                        "text": rest_text
                    }
                ]
            })

    payload = {
        "chat_id": chat_id,
        "rich_message": {
            "blocks": blocks
        },
        "disable_notification": silent
    }
    
    if kwargs.get("reply_markup"):
        payload["reply_markup"] = json.dumps(kwargs["reply_markup"])
        
    return await _post_with_retry(client, "POST", url, payload)

def validate_article(art: Dict[str, Any], article_dir: Path) -> Optional[Tuple[str, Path, List[Path], str]]:
    aid, title, text_fn = art.get("id"), art.get("title", "").strip(), art.get("text_file")
    if not all([aid, title, text_fn]): return None
    tp = article_dir / text_fn
    if not tp.is_file(): return None
    v_imgs = [article_dir / "images" / img for img in art.get("images", []) if (article_dir / "images" / img).is_file()]
    return title, tp, v_imgs, title

def load_posted_ids(state_file: Path) -> List[str]:
    if not state_file.is_file(): return []
    try:
        data = json.loads(state_file.read_text(encoding="utf-8"))
        return [str(i) for i in data[-MAX_POSTED_RECORDS:]] if isinstance(data, list) else []
    except Exception as e:
        logging.warning(f"Не удалось загрузить историю: {e}")
        return []

async def main(parsed_dir: str, state_file: str, limit: Optional[int], watermark_scale: float):
    token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHANNEL")
    chat_id2 = os.getenv("TELEGRAM_CHANNEL_2")
    chat_ids = [c for c in (chat_id, chat_id2) if c]
    if not token or not chat_ids:
        logging.error("🚨 Переменные окружения не установлены!")
        return

    parsed_root = Path(parsed_dir)
    state_file_path = Path(state_file)

    posted_ids_list = load_posted_ids(state_file_path)
    posted_ids_set = set(posted_ids_list)

    to_post = []
    if not parsed_root.exists():
        logging.error(f"📂 Директория {parsed_dir} не найдена!")
        return

    for d in sorted(parsed_root.iterdir()):
        meta_f = d / "meta.json"
        if d.is_dir() and meta_f.is_file():
            try:
                m = json.loads(meta_f.read_text(encoding="utf-8"))
                aid = str(m.get("id"))
                if aid and aid != 'None' and aid not in posted_ids_set:
                    if v := validate_article(m, d):
                        to_post.append({
                            "id": aid, 
                            "title": v[0], 
                            "text_path": v[1], 
                            "image_paths": v[2], 
                            "original_title": v[3]
                        })
            except: continue

    to_post.sort(key=lambda x: int(x["id"]))
    if not to_post:
        logging.info("🔍 Нет новых статей для публикации.")
        return

    if limit:
        to_post = to_post[:limit]

    total_articles = len(to_post)
    logging.info(f"🆕 Найдено статей для публикации: {total_articles}")

    async with httpx.AsyncClient() as client:
        sent = 0
        for idx, art in enumerate(to_post):
            logging.info(f"📤 ID={art['id']} ({idx + 1}/{total_articles})")

            is_last_article = (idx == total_articles - 1)
            try:
                # --- TELEGRAM: MEDIA ---
                if art["image_paths"]:
                    for cid in chat_ids:
                        await send_media_group(client, token, cid, art["image_paths"], watermark_scale, silent=True)

                # Читаем текст из файла
                txt = art["text_path"].read_text(encoding="utf-8").lstrip()
                if txt.startswith(art["original_title"]):
                    txt = txt[len(art["original_title"]):].lstrip()

                markup = {"inline_keyboard": [[
                    {"text": "Обмен валют", "url": "https://t.me/mister1dollar"},
                    {"text": "Отзывы", "url": "https://t.me/feedback1dollar"}
                ]]} if is_last_article else None

                # Отправка через Rich Message с чистым аккордеоном (details)
                for cid in chat_ids:
                    await send_rich_accordion_message(
                        client=client,
                        token=token,
                        chat_id=cid,
                        title=art['title'],
                        text=txt,
                        reply_markup=markup,
                        silent=not is_last_article
                    )

                # --- FACEBOOK: POSTING ---
                try:
                    fb_full_text = f"{art['original_title']}\n\n{txt}"
                    post_to_facebook(
                        text=fb_full_text,
                        media_files=art["image_paths"],
                        watermark_scale=watermark_scale
                    )
                except Exception as fb_e:
                    logging.error(f"❌ FB Error: {fb_e}")

                # --- MARKER ---
                if art['id'] not in posted_ids_list:
                    posted_ids_list.append(art['id'])
                sent += 1

                posted_ids_list = posted_ids_list[-MAX_POSTED_RECORDS:]
                state_file_path.write_text(json.dumps([int(i) for i in posted_ids_list], indent=2))
                logging.info(f"✅ Успешно опубликовано: ID={art['id']}")

            except Exception as e:
                logging.error(f"❌ Критическая ошибка при отправке ID={art['id']}: {e}")

            if not is_last_article:
                await asyncio.sleep(float(os.getenv("POST_DELAY", DEFAULT_DELAY)))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--parsed-dir", default="articles")
    parser.add_argument("--state-file", default="articles/posted.json")
    parser.add_argument("-n", "--limit", type=int, default=None)
    parser.add_argument("--watermark-scale", type=float, default=WATERMARK_SCALE)
    asyncio.run(main(**vars(parser.parse_args())))
