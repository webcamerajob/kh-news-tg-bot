#!/usr/bin/env python3
import os
import json
import argparse
import logging
import re
import hashlib
import time
import random
import sys
from pathlib import Path
from urllib.parse import urlparse
from datetime import datetime

import feedparser
import requests
from bs4 import BeautifulSoup

# Конфигурация логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("parser.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Константы
BAD_PATTERNS = [
    r"synopsis\s*:\s*",
    r"\(video inside\)",
    r"\bkhmer times\b"
]
BAD_RE = re.compile("|".join(BAD_PATTERNS), flags=re.IGNORECASE)
MAX_RETRIES = 5
BASE_DELAY = 3.0
OUTPUT_DIR = Path("articles")
CATALOG_PATH = OUTPUT_DIR / "catalog.json"

# Список User-Agents для ротации
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1"
]

def load_posted_ids(state_file: Path) -> set:
    try:
        if not state_file.exists():
            return set()
        
        with open(state_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            return {int(item["id"]) for item in data if isinstance(item, dict) and "id" in item}
    except (json.JSONDecodeError, IOError, TypeError) as e:
        logger.warning(f"Error loading posted IDs: {e}")
        return set()

def fetch_rss_feed(feed_url: str) -> list:
    """Получение RSS-ленты с случайными User-Agent"""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            headers = {"User-Agent": random.choice(USER_AGENTS)}
            response = requests.get(feed_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            # Парсим RSS
            feed = feedparser.parse(response.content)
            
            if not feed.entries:
                logger.warning(f"No entries found in RSS feed (attempt {attempt})")
                continue
                
            return feed.entries
        except Exception as e:
            wait_time = BASE_DELAY * 2 ** (attempt - 1)
            logger.warning(f"Error fetching RSS (attempt {attempt}): {e}, retry in {wait_time}s")
            time.sleep(wait_time)
    
    raise RuntimeError(f"Failed to fetch RSS feed after {MAX_RETRIES} attempts")

def fetch_full_article(url: str) -> dict:
    """Получение полного контента статьи"""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            headers = {"User-Agent": random.choice(USER_AGENTS)}
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            return {
                "content": response.text,
                "status": "success"
            }
        except Exception as e:
            wait_time = BASE_DELAY * 2 ** (attempt - 1)
            logger.warning(f"Error fetching article (attempt {attempt}): {e}, retry in {wait_time}s")
            time.sleep(wait_time)
    
    return {"status": "failed"}

def extract_images(soup: BeautifulSoup, base_url: str) -> list:
    """Извлечение изображений из контента"""
    images = []
    base_domain = urlparse(base_url).netloc
    
    for img in soup.find_all("img"):
        src = img.get("src") or img.get("data-src") or img.get("data-lazy-src")
        if not src:
            continue
        
        # Нормализация URL
        if src.startswith("//"):
            src = f"https:{src}"
        elif src.startswith("/"):
            src = f"https://{base_domain}{src}"
        elif not src.startswith("http"):
            src = f"https://{base_domain}/{src.lstrip('/')}"
            
        images.append(src)
    
    return images

def save_image(src_url: str, folder: Path) -> str:
    folder.mkdir(parents=True, exist_ok=True)
    filename = src_url.split('/')[-1].split('?')[0]
    dest = folder / filename
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            headers = {"User-Agent": random.choice(USER_AGENTS)}
            response = requests.get(src_url, headers=headers, timeout=30)
            response.raise_for_status()
            dest.write_bytes(response.content)
            return str(dest)
        except Exception as e:
            wait_time = BASE_DELAY * 2 ** (attempt - 1)
            logger.warning(f"Error saving image {filename} (attempt {attempt}): {e}, retry in {wait_time}s")
            time.sleep(wait_time)
    
    logger.error(f"Failed to save image: {src_url}")
    return None

def parse_and_save(entry: dict, base_url: str) -> dict:
    """Парсинг и сохранение статьи из RSS"""
    try:
        # Создаем уникальный ID на основе ссылки
        article_id = hashlib.md5(entry.link.encode()).hexdigest()
        article_id = int(article_id[:8], 16)  # Преобразуем в числовой ID
        
        # Создаем директорию для статьи
        slug = entry.link.split("/")[-2] if entry.link.endswith("/") else entry.link.split("/")[-1]
        slug = re.sub(r'\W+', '', slug)[:50]  # Очищаем от спецсимволов
        art_dir = OUTPUT_DIR / f"{article_id}_{slug}"
        art_dir.mkdir(parents=True, exist_ok=True)
        meta_path = art_dir / "meta.json"
        
        # Проверяем существующую статью
        content_hash = hashlib.md5(entry.link.encode()).hexdigest()
        if meta_path.exists():
            try:
                existing_meta = json.loads(meta_path.read_text(encoding="utf-8"))
                if existing_meta.get("hash") == content_hash:
                    logger.info(f"Skipping existing article: {entry.title}")
                    return existing_meta
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.warning(f"Error reading existing meta: {e}")
        
        # Получаем полный контент статьи
        article_content = fetch_full_article(entry.link)
        if article_content["status"] != "success":
            logger.error(f"Failed to fetch full content for: {entry.title}")
            return None
        
        # Парсим контент
        soup = BeautifulSoup(article_content["content"], "html.parser")
        
        # Извлекаем основной контент
        content_div = soup.find("div", class_="entry-content")
        if not content_div:
            content_div = soup.find("article")
        
        # Извлекаем текст
        paragraphs = []
        if content_div:
            for p in content_div.find_all("p"):
                text = p.get_text(strip=True)
                if text and not re.search(r'^\s*$', text):
                    paragraphs.append(text)
        
        # Если не нашли контент, используем описание из RSS
        if not paragraphs:
            if hasattr(entry, "summary"):
                paragraphs = [entry.summary]
            else:
                logger.warning(f"No content found for article: {entry.title}")
                return None
        
        raw_text = "\n\n".join(paragraphs)
        raw_text = BAD_RE.sub("", raw_text)
        raw_text = re.sub(r'\s+', ' ', raw_text)
        raw_text = f"**{entry.title}**\n\n{raw_text}"
        
        # Сохранение текста
        text_path = art_dir / "content.txt"
        text_path.write_text(raw_text, encoding="utf-8")
        
        # Обработка изображений
        img_dir = art_dir / "images"
        img_dir.mkdir(exist_ok=True)
        images = []
        
        # Извлечение URL изображений
        img_urls = extract_images(soup, base_url)
        
        # Загрузка изображений
        for url in img_urls[:10]:  # Ограничим 10 изображениями
            if path := save_image(url, img_dir):
                images.append(path)
        
        # Если нет изображений, используем медиа из RSS
        if not images and hasattr(entry, "media_content"):
            for media in entry.media_content:
                if media.get("type", "").startswith("image/"):
                    if path := save_image(media["url"], img_dir):
                        images.append(path)
        
        if not images and hasattr(entry, "enclosures"):
            for enc in entry.enclosures:
                if enc.get("type", "").startswith("image/"):
                    if path := save_image(enc["href"], img_dir):
                        images.append(path)
        
        if not images:
            logger.warning(f"No images for article: {entry.title}")
        
        # Создание метаданных
        published = datetime(*entry.published_parsed[:6]) if hasattr(entry, "published_parsed") else datetime.now()
        
        meta = {
            "id": article_id,
            "slug": slug,
            "date": published.isoformat(),
            "link": entry.link,
            "title": entry.title,
            "text_file": str(text_path),
            "images": images,
            "posted": False,
            "hash": content_hash
        }
        
        # Сохранение метаданных
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)
        
        return meta
        
    except Exception as e:
        logger.error(f"Error processing article {entry.title}: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description="RSS Article Parser")
    parser.add_argument("--rss-url", default="https://www.khmertimeskh.com/category/national/feed/", help="RSS feed URL")
    parser.add_argument("-n", "--limit", type=int, default=None, help="Max posts to parse")
    parser.add_argument("--posted-file", default="articles/posted.json", help="Posted articles state file")
    
    args = parser.parse_args()
    
    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        
        # Загрузка опубликованных ID
        posted_ids = load_posted_ids(Path(args.posted_file))
        logger.info(f"Loaded {len(posted_ids)} posted article IDs")
        
        # Получение RSS-ленты
        logger.info(f"Fetching RSS feed: {args.rss_url}")
        entries = fetch_rss_feed(args.rss_url)
        logger.info(f"Found {len(entries)} entries in RSS feed")
        
        # Обработка статей
        catalog = []
        processed = 0
        base_url = urlparse(args.rss_url).scheme + "://" + urlparse(args.rss_url).netloc
        
        for entry in entries:
            if args.limit and processed >= args.limit:
                break
                
            if meta := parse_and_save(entry, base_url):
                if meta["id"] in posted_ids:
                    logger.info(f"Skipping already posted article: {meta['title']}")
                    continue
                    
                catalog.append(meta)
                processed += 1
                logger.info(f"Processed article: {meta['title']}")
        
        logger.info(f"Processed {processed} new articles")
        
    except Exception as e:
        logger.exception("Fatal error in parser")
        sys.exit(1)

if __name__ == "__main__":
    main()
