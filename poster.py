import os
import json
import argparse
import asyncio
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from io import BytesIO
import httpx
from httpx import HTTPStatusError, ReadTimeout, Timeout
from PIL import Image

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

MAX_POSTED_RECORDS = 300
WATERMARK_SCALE = 0.35
HTTPX_TIMEOUT = Timeout(connect=10.0, read=60.0, write=10.0, pool=5.0)
MAX_RETRIES   = 3
RETRY_DELAY   = 5.0
DEFAULT_DELAY = 10.0

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

async def send_media_group(client: httpx.AsyncClient, token: str, chat_id: str, images: List[Path], watermark_scale: float) -> bool:
    if not images: return False
    url = f"https://api.telegram.org/bot{token}/sendMediaGroup"
    success = True
    
    for i in range(0, len(images), 10):
        chunk = images[i : i + 10]
        logging.info(f"🖼️ Отправка пачки фото {i//10 + 1} (кол-во: {len(chunk)})")
        media, files = [], {}
        for idx, img_path in enumerate(chunk):
            image_bytes = apply_watermark(img_path, scale=watermark_scale)
            if image_bytes:
                key = f"photo{idx}"
                files[key] = (img_path.name, image_bytes, "image/jpeg")
                media.append({"type": "photo", "media": f"attach://{key}"})
        
        if not media: continue
        data = {"chat_id": chat_id, "media": json.dumps(media)}
        if not await _post_with_retry(client, "POST", url, data, files):
            success = False
            logging.error(f"❌ Не удалось отправить пачку фото {i//10 + 1}")
        
        if len(images) > 10: await asyncio.sleep(2)
    return success

async def send_message(client: httpx.AsyncClient, token: str, chat_id: str, text: str, **kwargs) -> bool:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if kwargs.get("reply_markup"):
        data["reply_markup"] = json.dumps(kwargs["reply_markup"])
    return await _post_with_retry(client, "POST", url, data)

def validate_article(art: Dict[str, Any], article_dir: Path) -> Optional[Tuple[str, Path, List[Path], str]]:
    aid, title, text_fn = art.get("id"), art.get("title", "").strip(), art.get("text_file")
    if not all([aid, title, text_fn]): return None
    tp = article_dir / text_fn
    if not tp.is_file(): return None
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

async def main(parsed_dir: str, state_path: str, limit: Optional[int], watermark_scale: float):
    token, chat_id = os.getenv("TELEGRAM_TOKEN"), os.getenv("TELEGRAM_CHANNEL")
    if not token or not chat_id:
        logging.error("🚨 Переменные окружения не установлены!")
        return

    parsed_root, state_file = Path(parsed_dir), Path(state_path)
    posted_ids_list = load_posted_ids(state_file)
    posted_ids_set = set(posted_ids_list)
    
    to_post = []
    for d in sorted(parsed_root.iterdir()):
        meta_f = d / "meta.json"
        if d.is_dir() and meta_f.is_file():
            try:
                m = json.loads(meta_f.read_text(encoding="utf-8"))
                aid = str(m.get("id"))
                if aid and aid != 'None' and aid not in posted_ids_set:
                    if v := validate_article(m, d):
                        to_post.append({"id": aid, "html_title": v[0], "text_path": v[1], "image_paths": v[2], "original_title": v[3]})
            except: continue

    to_post.sort(key=lambda x: int(x["id"]))
    if not to_post:
        logging.info("🔍 Нет новых статей для публикации.")
        return

    logging.info(f"🆕 Найдено статей для публикации: {len(to_post)}")

    async with httpx.AsyncClient() as client:
        sent = 0
        for art in to_post:
            if limit and sent >= limit: break
            logging.info(f"📤 Публикация ID={art['id']}...")
            try:
                if art["image_paths"]:
                    await send_media_group(client, token, chat_id, art["image_paths"], watermark_scale)
                
                txt = art["text_path"].read_text(encoding="utf-8").lstrip()
                if txt.startswith(art["original_title"]):
                    txt = txt[len(art["original_title"]):].lstrip()
                
                full_html = f"{art['html_title']}\n\n{escape_html(txt)}"
                chunks = chunk_text(re.sub(r'\n{3,}', '\n\n', full_html).strip())
                
                for i, c in enumerate(chunks):
                    markup = {"inline_keyboard": [[
                        {"text": "Обмен валют", "url": "https://t.me/mister1dollar"},
                        {"text": "Отзывы", "url": "https://t.me/feedback1dollar"}
                    ]]} if i == len(chunks)-1 else None
                    await send_message(client, token, chat_id, c, reply_markup=markup)

                if art['id'] not in posted_ids_list:
                    posted_ids_list.append(art['id'])
                sent += 1
                
                # Обрезка и сохранение стейта
                posted_ids_list = posted_ids_list[-MAX_POSTED_RECORDS:]
                state_file.write_text(json.dumps([int(i) for i in posted_ids_list], indent=2))
                logging.info(f"✅ Успешно опубликовано: ID={art['id']}")

            except Exception as e:
                logging.error(f"❌ Критическая ошибка при отправке ID={art['id']}: {e}")
            
            await asyncio.sleep(float(os.getenv("POST_DELAY", DEFAULT_DELAY)))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--parsed-dir", default="articles")
    parser.add_argument("--state-file", default="articles/posted.json")
    parser.add_argument("-n", "--limit", type=int, default=None)
    parser.add_argument("--watermark-scale", type=float, default=WATERMARK_SCALE)
    asyncio.run(main(**vars(parser.parse_args())))
