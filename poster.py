#!/usr/bin/env python3
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
from PIL import Image, ImageFile

# Настройка обработки изображений
ImageFile.LOAD_TRUNCATED_IMAGES = True

# Конфигурация логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("poster.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Константы
HTTPX_TIMEOUT = Timeout(connect=10.0, read=60.0, write=10.0, pool=5.0)
MAX_RETRIES = 3
RETRY_DELAY = 5.0
DEFAULT_DELAY = 5.0

def escape_markdown(text: str) -> str:
    """Экранирование спецсимволов MarkdownV2"""
    markdown_chars = r'_*[]()~`>#+-=|{}.!'
    return re.sub(f'([{re.escape(markdown_chars)}])', r'\\\1', text)

def chunk_text(text: str, size: int = 4096) -> List[str]:
    """Разделение текста на части с сохранением абзацев"""
    paragraphs = [p.strip() for p in text.split('\n\n') if p.strip()]
    chunks = []
    current_chunk = ""
    
    for para in paragraphs:
        if len(para) > size:
            # Обработка слишком длинных абзацев
            words = para.split()
            current_line = ""
            for word in words:
                if len(current_line) + len(word) + 1 > size:
                    chunks.append(current_line)
                    current_line = word
                else:
                    current_line = f"{current_line} {word}".strip()
            if current_line:
                chunks.append(current_line)
        elif len(current_chunk) + len(para) + 2 <= size:
            current_chunk = f"{current_chunk}\n\n{para}".strip() if current_chunk else para
        else:
            chunks.append(current_chunk)
            current_chunk = para
    
    if current_chunk:
        chunks.append(current_chunk)
    
    return chunks

def apply_watermark(image_path: Path, watermark_path: Path, scale: float = 0.25) -> bytes:
    """Применение водяного знака к изображению"""
    try:
        with Image.open(image_path) as base:
            if base.mode != 'RGBA':
                base = base.convert('RGBA')
            
            with Image.open(watermark_path) as wm:
                wm = wm.convert('RGBA')
                
                # Масштабирование водяного знака
                wm_width = int(base.width * scale)
                wm_ratio = wm_width / wm.width
                wm_height = int(wm.height * wm_ratio)
                wm = wm.resize((wm_width, wm_height), Image.LANCZOS)
                
                # Позиционирование
                position = (base.width - wm.width - 10, base.height - wm.height - 10)
                
                # Наложение
                base.paste(wm, position, wm)
                
                # Сохранение в буфер
                buf = BytesIO()
                base.save(buf, format='PNG')
                return buf.getvalue()
                
    except Exception as e:
        logger.error(f"Error applying watermark to {image_path}: {e}")
        raise

async def send_with_retry(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    data: Optional[Dict] = None,
    files: Optional[Dict] = None
) -> bool:
    """Отправка запроса с повторными попытками"""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = await client.request(method, url, data=data, files=files, timeout=HTTPX_TIMEOUT)
            response.raise_for_status()
            return True
        except ReadTimeout:
            logger.warning(f"Timeout {attempt}/{MAX_RETRIES} for {url}")
        except HTTPStatusError as e:
            if 400 <= e.response.status_code < 500:
                logger.error(f"Client error {e.response.status_code}: {e.response.text}")
                return False
            logger.warning(f"Server error {e.response.status_code}, retrying {attempt}/{MAX_RETRIES}")
        
        await asyncio.sleep(RETRY_DELAY * attempt)
    
    logger.error(f"Failed after {MAX_RETRIES} attempts for {url}")
    return False

async def send_media_group(
    client: httpx.AsyncClient,
    token: str,
    chat_id: str,
    images: List[Path],
    caption: str
) -> bool:
    """Отправка группы медиа"""
    url = f"https://api.telegram.org/bot{token}/sendMediaGroup"
    media = []
    files = {}
    
    for idx, img_path in enumerate(images):
        try:
            # Применение водяного знака
            watermark_path = Path("watermark.png")
            if not watermark_path.exists():
                logger.error("Watermark file not found!")
                return False
                
            image_data = apply_watermark(img_path, watermark_path)
            key = f"photo{idx}"
            files[key] = (img_path.name, image_data, "image/png")
            
            media_item = {
                "type": "photo",
                "media": f"attach://{key}"
            }
            
            if idx == 0:
                media_item["caption"] = escape_markdown(caption)
                media_item["parse_mode"] = "MarkdownV2"
                
            media.append(media_item)
        except Exception as e:
            logger.error(f"Error processing image {img_path}: {e}")
            return False
    
    payload = {
        "chat_id": chat_id,
        "media": json.dumps(media, ensure_ascii=False)
    }
    
    return await send_with_retry(client, "POST", url, data=payload, files=files)

async def send_message(
    client: httpx.AsyncClient,
    token: str,
    chat_id: str,
    text: str
) -> bool:
    """Отправка текстового сообщения"""
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": escape_markdown(text),
        "parse_mode": "MarkdownV2"
    }
    return await send_with_retry(client, "POST", url, data=payload)

def validate_article(article_dir: Path) -> Optional[Tuple[str, Path, List[Path]]:
    """Валидация статьи"""
    meta_path = article_dir / "meta.json"
    if not meta_path.exists():
        logger.error(f"Meta.json not found in {article_dir}")
        return None
    
    try:
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
    except json.JSONDecodeError as e:
        logger.error(f"Error loading meta.json in {article_dir}: {e}")
        return None
    
    # Проверка обязательных полей
    required_fields = ["id", "title", "text_file", "images"]
    if any(field not in meta for field in required_fields):
        logger.error(f"Missing required fields in {meta_path}")
        return None
    
    # Проверка файла текста
    text_path = Path(meta["text_file"])
    if not text_path.is_absolute():
        text_path = article_dir / text_path
    
    if not text_path.exists() or not text_path.is_file():
        logger.error(f"Text file not found: {text_path}")
        return None
    
    # Проверка изображений
    valid_images = []
    for img_path in meta["images"]:
        img = Path(img_path)
        if not img.is_absolute():
            img = article_dir / img
            
        if img.exists() and img.is_file() and img.suffix.lower() in [".jpg", ".jpeg", ".png"]:
            valid_images.append(img)
        else:
            logger.warning(f"Invalid image: {img}")
    
    if not valid_images:
        logger.error(f"No valid images found for article {meta['id']}")
        return None
    
    # Формирование заголовка
    title = meta["title"]
    if len(title) > 1024:
        title = title[:1020] + "..."
    
    return escape_markdown(title), text_path, valid_images

async def process_article(client, token, chat_id, article_dir, delay):
    """Обработка одной статьи"""
    try:
        validated = validate_article(article_dir)
        if not validated:
            return None
            
        caption, text_path, images = validated
        
        # Отправка медиагруппы
        if not await send_media_group(client, token, chat_id, images, caption):
            logger.error(f"Failed to send media group for {article_dir.name}")
            return None
        
        # Отправка текста
        try:
            text_content = text_path.read_text(encoding="utf-8")
            chunks = chunk_text(text_content)
            
            for chunk in chunks[1:]:
                await send_message(client, token, chat_id, chunk)
                await asyncio.sleep(1)  # Небольшая задержка между сообщениями
        except Exception as e:
            logger.error(f"Error sending text for {article_dir.name}: {e}")
        
        # Помечаем статью как опубликованную
        meta_path = article_dir / "meta.json"
        with open(meta_path, "r+", encoding="utf-8") as f:
            meta = json.load(f)
            meta["posted"] = True
            f.seek(0)
            json.dump(meta, f, ensure_ascii=False, indent=2)
            f.truncate()
        
        logger.info(f"Successfully posted article {article_dir.name}")
        return meta["id"]
        
    except Exception as e:
        logger.exception(f"Error processing article {article_dir.name}")
        return None

async def main(parsed_dir: str, state_path: str, limit: Optional[int]):
    token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHANNEL")
    
    if not token or not chat_id:
        logger.error("TELEGRAM_TOKEN or TELEGRAM_CHANNEL not set")
        return
    
    delay = float(os.getenv("POST_DELAY", DEFAULT_DELAY))
    parsed_root = Path(parsed_dir)
    
    if not parsed_root.is_dir():
        logger.error(f"Directory not found: {parsed_root}")
        return
    
    state_file = Path(state_path)
    posted_ids = set()
    
    async with httpx.AsyncClient(timeout=HTTPX_TIMEOUT) as client:
        tasks = []
        article_dirs = sorted([d for d in parsed_root.iterdir() if d.is_dir()])
        
        for article_dir in article_dirs:
            if limit and len(tasks) >= limit:
                break
                
            # Пропускаем уже опубликованные статьи
            meta_path = article_dir / "meta.json"
            if meta_path.exists():
                try:
                    with open(meta_path, "r", encoding="utf-8") as f:
                        meta = json.load(f)
                    if meta.get("posted", False):
                        logger.info(f"Skipping already posted: {article_dir.name}")
                        continue
                except json.JSONDecodeError:
                    pass
                    
            tasks.append(process_article(client, token, chat_id, article_dir, delay))
        
        # Обработка статей с ограничением
        results = await asyncio.gather(*tasks)
        posted_ids = {aid for aid in results if aid is not None}
    
    # Сохранение ID опубликованных статей
    if posted_ids:
        all_ids = set()
        if state_file.exists():
            try:
                with open(state_file, "r", encoding="utf-8") as f:
                    all_ids = set(json.load(f))
            except json.JSONDecodeError:
                pass
        
        all_ids.update(posted_ids)
        with open(state_file, "w", encoding="utf-8") as f:
            json.dump(list(all_ids), f, indent=2)
        
        logger.info(f"Saved {len(posted_ids)} new posted IDs")
    
    logger.info(f"Posted {len(posted_ids)} articles")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Telegram article poster")
    parser.add_argument("--parsed-dir", default="articles", help="Parsed articles directory")
    parser.add_argument("--state-file", default="articles/posted.json", help="Posted state file")
    parser.add_argument("-n", "--limit", type=int, default=None, help="Max articles to post")
    
    args = parser.parse_args()
    asyncio.run(main(args.parsed_dir, args.state_file, args.limit))
