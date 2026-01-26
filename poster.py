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
    """Наложение водяного знака на фото с подробным логированием"""
    try:
        base_img = Image.open(img_path).convert("RGBA")
        base_width, _ = base_img.size
        watermark_path = Path(__file__).parent / "watermark.png"
        
        if not watermark_path.exists():
            logging.warning(f"⚠️ Файл вотермарки не найден. {img_path.name} будет отправлен без неё.")
            img_byte_arr = BytesIO()
            base_img.convert("RGB").save(img_byte_arr, format='JPEG', quality=90)
            return img_byte_arr.getvalue()

        watermark_img = Image.open(watermark_path).convert("RGBA")
        wm_width, wm_height = watermark_img.size
        
        # Расчет размеров: 35% от ширины оригинала
        new_wm_width = int(base_width * scale)
        new_wm_height = int(wm_height * (new_wm_width / wm_width))
        
        resample_filter = getattr(Image.Resampling, "LANCZOS", Image.LANCZOS)
        watermark_img = watermark_img.resize((new_wm_width, new_wm_height), resample=resample_filter)
        
        # Позиция: правый верхний угол
        overlay = Image.new("RGBA", base_img.size, (0, 0, 0, 0))
        padding = 10 
        position = (base_width - new_wm_width - padding, padding)
        
        overlay.paste(watermark_img, position, watermark_img)
        composite_img = Image.alpha_composite(base_img, overlay).convert("RGB")
        
        img_byte_arr = BytesIO()
        composite_img.save(img_byte_arr, format='JPEG', quality=90)
        
        logging.info(f"🎨 Вотермарка наложена на фото: {img_path.name}")
        return img_byte_arr.getvalue()
        
    except Exception as e:
        logging.error(f"❌ Ошибка вотермарки для {img_path.name}: {e}")
        return img_path.read_bytes() if img_path.exists() else b""

async def process_video_logic(video_url: str, watermark_path: str = "watermark.png") -> Optional[str]:
    """Скачивание видео 360p и наложение вотермарки с выводом всех этапов в лог"""
    if not video_url: return None
    ts = int(time.time())
    raw_path, final_path = f"raw_{ts}.mp4", f"video_{ts}.mp4"
    
    logging.info(f"🎬 Начало обработки видео: {video_url}")
    
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            # 1. Запрос к Loader.to
            resp = await client.get("https://loader.to/ajax/download.php", params={"format": "360", "url": video_url})
            task_id = resp.json().get("id")
            logging.info(f"⏳ Задача Loader.to создана. ID: {task_id}")
            
            # 2. Ожидание готовности
            download_url = None
            for attempt in range(25):
                await asyncio.sleep(3)
                status_resp = await client.get("https://loader.to/ajax/progress.php", params={"id": task_id})
                status = status_resp.json()
                
                prog_text = status.get('text', 'обработка')
                logging.info(f"   [{attempt+1}/25] Статус видео: {prog_text}")
                
                if status.get("success") == 1:
                    download_url = status.get("download_url")
                    break
            
            if not download_url:
                logging.error("❌ Loader.to не отдал ссылку за отведенное время.")
                return None

            # 3. Скачивание
            logging.info(f"⬇️ Скачивание временного файла {raw_path}...")
            async with client.stream("GET", download_url) as r:
                with open(raw_path, 'wb') as f:
                    async for chunk in r.aiter_bytes(): f.write(chunk)

            # 4. FFmpeg вотермарка
            logging.info("⚙️ Запуск FFmpeg рендеринга (360p + вотермарка 35%)...")
            cmd = [
                "ffmpeg", "-y", "-i", raw_path, "-i", watermark_path,
                "-filter_complex", f"[1:v][0:v]scale2ref=iw*{WATERMARK_SCALE}:-1[wm][vid];[vid][wm]overlay=W-w-10:10",
                "-c:v", "libx264", "-preset", "ultrafast", "-crf", "28", "-c:a", "copy", final_path
            ]
            
            process = await asyncio.create_subprocess_exec(
                *cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            stdout, stderr = await process.communicate()

            if process.returncode != 0:
                logging.error(f"❌ FFmpeg завершился с ошибкой: {stderr.decode()}")
                return None

            if os.path.exists(raw_path): os.remove(raw_path)
            logging.info(f"✅ Видео успешно обработано: {final_path}")
            return final_path
            
        except Exception as e:
            logging.error(f"❌ Критическая ошибка видео: {e}")
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
    """Сборка и отправка медиа-групп. Видео ВСЕГДА идет последним объектом."""
    all_items = []
    files_to_send = {}
    
    # Подготовка фото
    logging.info(f"📦 Подготовка {len(images)} фото для альбома...")
    for idx, img_path in enumerate(images):
        image_bytes = apply_watermark(img_path, scale=watermark_scale)
        if image_bytes:
            key = f"photo_{idx}"
            files_to_send[key] = (img_path.name, image_bytes, "image/jpeg")
            all_items.append({"type": "photo", "media": f"attach://{key}"})
    
    # Видео в самый конец
    if video_path and os.path.exists(video_path):
        logging.info(f"📦 Добавление видео в конец очереди: {video_path}")
        key = "video_main"
        with open(video_path, 'rb') as f:
            files_to_send[key] = ("video.mp4", f.read(), "video/mp4")
        all_items.append({"type": "video", "media": f"attach://{key}"})

    if not all_items:
        logging.warning("⚠️ Нет медиа-файлов для отправки.")
        return False

    # Разбивка на чанки (по 10 объектов)
    total_items = len(all_items)
    chunks = [all_media_slice := all_items[i:i + 10] for i in range(0, total_items, 10)]
    url = f"https://api.telegram.org/bot{token}/sendMediaGroup"

    logging.info(f"📤 Всего объектов: {total_items}. Будет отправлено {len(chunks)} медиа-групп.")

    success = True
    for i, chunk in enumerate(chunks):
        current_files = {}
        for item in chunk:
            key = item["media"].replace("attach://", "")
            if key in files_to_send:
                current_files[key] = files_to_send[key]
        
        data = {"chat_id": chat_id, "media": json.dumps(chunk)}
        
        logging.info(f"   🚀 Отправка группы {i+1}/{len(chunks)}...")
        if not await _post_with_retry(client, "POST", url, data, current_files):
            logging.error(f"   ❌ Ошибка при отправке группы {i+1}")
            success = False
        
        await asyncio.sleep(1.5) # Защита от флуда
        
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
    parsed_root, state_file = Path(parsed_dir), Path(state_path)
    
    # 1. ЗАГРУЗКА ИСТОРИИ (Строгий контроль типов)
    posted_ids = []
    if state_file.is_file():
        try:
            raw_data = json.loads(state_file.read_text())
            # Принудительно всё в строки, убираем дубли
            posted_ids = [str(x) for x in raw_data if x is not None]
        except Exception as e:
            logging.error(f"❌ Ошибка чтения истории {state_path}: {e}")
            
    posted_set = set(posted_ids)
    logging.info(f"📜 Загружена история: {len(posted_set)} объектов. (Файл: {state_path})")

    # 2. ПОИСК ПАПОК (Проверка путей)
    if not parsed_root.exists():
        logging.error(f"❌ Директория с контентом '{parsed_dir}' НЕ НАЙДЕНА!")
        return

    # Получаем все подпапки
    all_folders = [d for d in parsed_root.iterdir() if d.is_dir()]
    logging.info(f"📂 Всего папок в '{parsed_dir}': {len(all_folders)}")

    articles_to_post = []
    for d in sorted(all_folders, key=lambda x: x.name):
        meta_file = d / "meta.json"
        
        if not meta_file.is_file():
            logging.info(f"  🔍 Папка {d.name}: пропуск (нет meta.json)")
            continue
            
        try:
            meta = json.loads(meta_file.read_text(encoding="utf-8"))
            aid = str(meta.get("id"))
            
            # ГЛАВНАЯ ПРОВЕРКА
            if aid in posted_set:
                logging.info(f"  🔍 ID {aid}: пропуск (уже есть в истории)")
                continue

            # Проверка наличия текста
            text_file = meta.get("text_file", "")
            text_path = d / text_file
            if not text_path.is_file():
                logging.warning(f"  🔍 ID {aid}: пропуск (файл текста {text_file} не найден)")
                continue
            
            # Собираем фото
            img_dir = d / "images"
            imgs = sorted([p for p in img_dir.iterdir() if p.suffix.lower() in {".jpg", ".jpeg", ".png"}]) if img_dir.is_dir() else []
            
            articles_to_post.append({
                "id": aid, 
                "title": meta.get("title", "Без названия"), 
                "text_path": text_path, 
                "image_paths": imgs, 
                "video_url": meta.get("video_url")
            })
            logging.info(f"  ⭐️ ID {aid}: ДОБАВЛЕН В ОЧЕРЕДЬ ({len(imgs)} фото, видео: {'да' if meta.get('video_url') else 'нет'})")

        except Exception as e:
            logging.error(f"  ❌ Ошибка чтения метаданных в {d.name}: {e}")

    # 3. ПУБЛИКАЦИЯ
    if not articles_to_post:
        logging.info("🔍 Новых статей для отправки не нашлось.")
        return

    logging.info(f"🚀 Начинаем публикацию {len(articles_to_post)} статей...")

    async with httpx.AsyncClient() as client:
        sent_count = 0
        for article in articles_to_post:
            if limit and sent_count >= limit:
                logging.info(f"🛑 Достигнут лимит {limit} ст.")
                break
            
            logging.info(f"▶️ Публикуем {article['id']}...")
            processed_video = None
            try:
                # Видео
                if article["video_url"]:
                    processed_video = await process_video_logic(article["video_url"])

                # Медиа (фото + видео в конце)
                # Вызываем твою функцию send_complex_media_group
                media_success = await send_complex_media_group(
                    client, token, chat_id, 
                    article["image_paths"], 
                    processed_video, 
                    watermark_scale
                )

                # Текст (всегда шлем отдельно или как подпись, если медиа не ушло)
                raw_text = article["text_path"].read_text(encoding="utf-8")
                # Убираем заголовок из начала текста, если он там есть
                clean_body = raw_text
                if article['title'] in raw_text[:200]:
                    clean_body = raw_text.replace(article['title'], '', 1).strip()

                full_html = f"<b>{escape_html(article['title'])}</b>\n\n{escape_html(clean_body)}"
                chunks = chunk_text(full_html)

                for i, chunk in enumerate(chunks):
                    # Кнопки только к последнему куску
                    markup = None
                    if i == len(chunks) - 1:
                        markup = {"inline_keyboard": [[{"text": "Обмен", "url": "https://t.me/mister1dollar"}]]}
                    
                    await send_message(client, token, chat_id, chunk, reply_markup=markup)

                # Записываем успех
                posted_ids.append(article['id'])
                sent_count += 1
                logging.info(f"✅ ID {article['id']} отправлен.")

            except Exception as e:
                logging.error(f"❌ Провал на ID {article['id']}: {e}")
            finally:
                if processed_video and os.path.exists(processed_video):
                    os.remove(processed_video)
            
            await asyncio.sleep(float(os.getenv("POST_DELAY", DEFAULT_DELAY)))

    # 4. СОХРАНЕНИЕ СОСТОЯНИЯ
    if sent_count > 0:
        # Оставляем только свежие записи
        new_history = [int(i) for i in posted_ids[-MAX_POSTED_RECORDS:]]
        state_file.write_text(json.dumps(new_history, indent=2))
        logging.info(f"💾 История обновлена: {state_path}")

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
