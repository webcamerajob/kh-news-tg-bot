import os
import json
import argparse
import asyncio
import logging
import re
import requests  # Добавлено для Facebook
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from io import BytesIO
import httpx
from httpx import HTTPStatusError, ReadTimeout, Timeout
from PIL import Image

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

def post_to_facebook(text, link, media_files=None):
    """
    Публикует пост в Facebook.
    Приоритет: Видео -> Фото -> Ссылка.
    """
    if not FB_PAGE_ACCESS_TOKEN or not FB_PAGE_ID:
        logging.warning("⚠️ Данные для Facebook не заполнены. Пропуск.")
        return

    # Базовый URL
    url = f"https://graph.facebook.com/v19.0/{FB_PAGE_ID}/feed"
    payload = {
        "access_token": FB_PAGE_ACCESS_TOKEN, 
        "message": f"{text}\n\n{link}"
    }
    files = {}

    try:
        # Ищем видео или фото в скачанных файлах
        video_file = next((f for f in (media_files or []) if str(f).endswith('.mp4')), None)
        image_file = next((f for f in (media_files or []) if str(f).endswith(('.jpg', '.png', '.jpeg'))), None)

        if video_file:
            logging.info(f"📤 FB: Грузим видео {video_file.name}...")
            url = f"https://graph.facebook.com/v19.0/{FB_PAGE_ID}/videos"
            # У видео поле называется 'description', а не 'message'
            payload = {
                "access_token": FB_PAGE_ACCESS_TOKEN, 
                "description": f"{text}\n\nИсточник: {link}"
            }
            files = {'source': open(video_file, 'rb')}
        
        elif image_file:
            logging.info(f"📤 FB: Грузим фото {image_file.name}...")
            url = f"https://graph.facebook.com/v19.0/{FB_PAGE_ID}/photos"
            files = {'source': open(image_file, 'rb')}
        
        else:
            logging.info("📤 FB: Грузим только ссылку...")
            payload["link"] = link

        # Отправка запроса с таймаутом 60 сек (видео может грузиться долго)
        r = requests.post(url, data=payload, files=files, timeout=60)
        
        if files:
            files['source'].close()

        if r.status_code == 200:
            logging.info(f"✅ Facebook Success: ID={r.json().get('id')}")
        else:
            logging.error(f"❌ Facebook Error: {r.status_code} - {r.text}")

    except Exception as e:
        logging.error(f"❌ Facebook Exception: {e}")

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
    
    # Делим общий список файлов на пачки по 10 штук
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
                m_bytes = f_path.read_bytes()
            else:
                m_type, m_mime = "photo", "image/jpeg"
                m_bytes = apply_watermark(f_path, watermark_scale)

            if not m_bytes: continue
            
            files_to_send[f_key] = (f_path.name, m_bytes, m_mime)
            media_array.append({"type": m_type, "media": f"attach://{f_key}"})
        
        if not media_array: continue

        data = {
            "chat_id": chat_id, 
            "media": json.dumps(media_array),
            "disable_notification": silent
        }
        
        # Если хотя бы одна пачка упала, помечаем общий успех как False
        if not await _post_with_retry(client, "POST", url, data, files_to_send):
            overall_success = False
            logging.error(f"❌ Ошибка при отправке пачки {i//10 + 1}")
        
        # Если есть еще пачки для этого же поста, ждем немного
        if i + 10 < len(media_files):
            await asyncio.sleep(2)
            
    return overall_success

async def send_message(client: httpx.AsyncClient, token: str, chat_id: str, text: str, silent: bool = False, **kwargs) -> bool:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = {
        "chat_id": chat_id, 
        "text": text, 
        "parse_mode": "HTML", 
        "disable_web_page_preview": True,
        "disable_notification": silent
    }
    if kwargs.get("reply_markup"):
        data["reply_markup"] = json.dumps(kwargs["reply_markup"])
    return await _post_with_retry(client, "POST", url, data)

def validate_article(art: Dict[str, Any], article_dir: Path) -> Optional[Tuple[str, Path, List[Path], str]]:
    aid, title, text_fn = art.get("id"), art.get("title", "").strip(), art.get("text_file")
    if not all([aid, title, text_fn]): return None
    tp = article_dir / text_fn
    if not tp.is_file(): return None
    # Здесь мы собираем и картинки, и видео, так как они все лежат в images
    v_imgs = [article_dir / "images" / img for img in art.get("images", []) if (article_dir / "images" / img).is_file()]
    return f"<b>{escape_html(title)}</b>", tp, v_imgs, title

def load_posted_ids(state_file: Path) -> List[str]:
    if not state_file.is_file(): return []
    try:
        data = json.loads(state_file.read_text(encoding="utf-8"))
        return [str(i) for i in data[-MAX_POSTED_RECORDS:]] if isinstance(data, list) else []
    except Exception as e:
        logging.warning(f"Не удалось загрузить историю: {e}")
        return []

async def main(parsed_dir: str, state_file: str, limit: Optional[int], watermark_scale: float):
    token, chat_id = os.getenv("TELEGRAM_TOKEN"), os.getenv("TELEGRAM_CHANNEL")
    if not token or not chat_id:
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
                        # ДОБАВЛЕНО: передаем ссылку ("link") из метаданных в структуру
                        to_post.append({
                            "id": aid, 
                            "html_title": v[0], 
                            "text_path": v[1], 
                            "image_paths": v[2], 
                            "original_title": v[3],
                            "link": m.get("link") # <-- Ссылка для Facebook
                        })
            except: continue

    to_post.sort(key=lambda x: int(x["id"]))
    if not to_post:
        logging.info("🔍 Нет новых статей для публикации.")
        return

        # Обрезаем список заранее, чтобы знать точное количество
    if limit:
        to_post = to_post[:limit]

    total_articles = len(to_post)
    logging.info(f"🆕 Найдено статей для публикации: {total_articles}")

    async with httpx.AsyncClient() as client:
        sent = 0
        for idx, art in enumerate(to_post):
            logging.info(f"📤 ID={art['id']} ({idx + 1}/{total_articles})")
            
            # Проверяем, является ли эта статья последней в текущем пакете
            is_last_article = (idx == total_articles - 1)
            try:
                # --- TELEGRAM: MEDIA ---
                if art["image_paths"]:
                    await send_media_group(client, token, chat_id, art["image_paths"], watermark_scale, silent=True)
                
                # --- TELEGRAM: TEXT ---
                txt = art["text_path"].read_text(encoding="utf-8").lstrip()
                if txt.startswith(art["original_title"]):
                    txt = txt[len(art["original_title"]):].lstrip()
                
                full_html = f"{art['html_title']}\n\n{escape_html(txt)}"
                chunks = chunk_text(re.sub(r'\n{3,}', '\n\n', full_html).strip())
                
                for i, c in enumerate(chunks):
                    is_last_chunk = (i == len(chunks) - 1)

                    should_be_silent = not (is_last_article and is_last_chunk)

                    markup = {"inline_keyboard": [[
                        {"text": "Обмен валют", "url": "https://t.me/mister1dollar"},
                        {"text": "Отзывы", "url": "https://t.me/feedback1dollar"}
                    ]]} if is_last_chunk else None
                    
                    await send_message(client, token, chat_id, c, reply_markup=markup, silent=should_be_silent)
                
                # --- FACEBOOK: POSTING ---
                # Отправляем в FB после успешной отправки в Telegram
                try:
                    fb_text = art['original_title'] # Используем чистый заголовок
                    fb_link = art.get('link', '')   # Ссылка из метаданных
                    post_to_facebook(fb_text, fb_link, art["image_paths"])
                except Exception as fb_e:
                    logging.error(f"❌ FB Error for ID={art['id']}: {fb_e}")
                # -------------------------

                if art['id'] not in posted_ids_list:
                    posted_ids_list.append(art['id'])
                sent += 1
                
                posted_ids_list = posted_ids_list[-MAX_POSTED_RECORDS:]
                state_file_path.write_text(json.dumps([int(i) for i in posted_ids_list], indent=2))
                logging.info(f"✅ Успешно опубликовано: ID={art['id']}")

            except Exception as e:
                logging.error(f"❌ Критическая ошибка при отправке ID={art['id']}: {e}")
            
            # Не ждем после самой последней статьи
            if not is_last_article:
                await asyncio.sleep(float(os.getenv("POST_DELAY", DEFAULT_DELAY)))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--parsed-dir", default="articles")
    parser.add_argument("--state-file", default="articles/posted.json")
    parser.add_argument("-n", "--limit", type=int, default=None)
    parser.add_argument("--watermark-scale", type=float, default=WATERMARK_SCALE)
    asyncio.run(main(**vars(parser.parse_args())))
