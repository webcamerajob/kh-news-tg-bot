#!/usr/bin/env python3
import os
import json
import argparse
import logging
import re
import hashlib
import time
import fcntl
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys

# Настройка пути для импорта
sys.path.insert(0, str(Path(__file__).parent))

import cloudscraper
from requests.exceptions import ReadTimeout as ReqTimeout, RequestException
from bs4 import BeautifulSoup

os.environ["translators_default_region"] = "EN"
import translators as ts

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
SCRAPER_TIMEOUT = (10.0, 60.0)
MAX_RETRIES = 3
BASE_DELAY = 2.0
OUTPUT_DIR = Path("articles")
CATALOG_PATH = OUTPUT_DIR / "catalog.json"

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

def fetch_category_id(base_url: str, slug: str) -> int:
    scraper = cloudscraper.create_scraper()
    endpoint = f"{base_url}/wp-json/wp/v2/categories?slug={slug}"
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = scraper.get(endpoint, timeout=SCRAPER_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            
            if not data:
                raise ValueError(f"Category '{slug}' not found")
                
            return data[0]["id"]
        except (ReqTimeout, RequestException) as e:
            logger.warning(f"Timeout fetching category (attempt {attempt}): {e}")
            time.sleep(BASE_DELAY * 2 ** (attempt - 1))
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
            break
            
    raise RuntimeError(f"Failed to fetch category ID for slug: {slug}")

def fetch_posts(base_url: str, cat_id: int, per_page: int = 10) -> list:
    scraper = cloudscraper.create_scraper()
    endpoint = f"{base_url}/wp-json/wp/v2/posts?categories={cat_id}&per_page={per_page}&_embed"
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = scraper.get(endpoint, timeout=SCRAPER_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except (ReqTimeout, RequestException) as e:
            logger.warning(f"Timeout fetching posts (attempt {attempt}): {e}")
            time.sleep(BASE_DELAY * 2 ** (attempt - 1))
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
            break
            
    return []

def save_image(src_url: str, folder: Path) -> str:
    scraper = cloudscraper.create_scraper()
    folder.mkdir(parents=True, exist_ok=True)
    filename = src_url.split('/')[-1].split('?')[0]
    dest = folder / filename
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = scraper.get(src_url, timeout=SCRAPER_TIMEOUT)
            response.raise_for_status()
            dest.write_bytes(response.content)
            return str(dest)
        except (ReqTimeout, RequestException) as e:
            logger.warning(f"Error saving image {filename} (attempt {attempt}): {e}")
            time.sleep(BASE_DELAY * 2 ** (attempt - 1))
    
    raise RuntimeError(f"Failed to save image: {src_url}")

def translate_text(text: str, to_lang: str = "ru", provider: str = "yandex") -> str:
    if not text or not isinstance(text, str):
        return ""
    
    try:
        return ts.translate_text(text, translator=provider, from_language="en", to_language=to_lang)
    except Exception as e:
        logger.warning(f"Translation error [{provider} → {to_lang}]: {e}")
        return text

def parse_and_save(post: dict, translate_to: str, base_url: str) -> dict:
    post_id = post["id"]
    slug = post["slug"]
    art_dir = OUTPUT_DIR / f"{post_id}_{slug}"
    art_dir.mkdir(parents=True, exist_ok=True)
    meta_path = art_dir / "meta.json"
    
    # Проверка хеша контента
    content_hash = hashlib.sha256(post["content"]["rendered"].encode()).hexdigest()
    
    if meta_path.exists():
        try:
            existing_meta = json.loads(meta_path.read_text(encoding="utf-8"))
            if existing_meta.get("hash") == content_hash and existing_meta.get("translated_to") == translate_to:
                logger.info(f"Skipping unchanged article ID={post_id}")
                return existing_meta
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.warning(f"Error reading meta for ID={post_id}: {e}")
    
    # Парсинг контента
    try:
        soup = BeautifulSoup(post["content"]["rendered"], "html.parser")
        title = BeautifulSoup(post["title"]["rendered"], "html.parser").get_text(strip=True)
        
        # Перевод заголовка
        if translate_to:
            title = translate_text(title, to_lang=translate_to)
        
        # Извлечение текста
        paragraphs = [p.get_text(strip=True) for p in soup.find_all("p")]
        raw_text = "\n\n".join(paragraphs)
        raw_text = BAD_RE.sub("", raw_text)
        raw_text = re.sub(r"\s+", " ", raw_text)
        raw_text = f"**{title}**\n\n{raw_text}"
        
        # Сохранение текста
        text_path = art_dir / "content.txt"
        text_path.write_text(raw_text, encoding="utf-8")
        
        # Обработка изображений
        img_dir = art_dir / "images"
        img_dir.mkdir(exist_ok=True)
        images = []
        
        # Извлечение URL изображений
        img_urls = set()
        for img in soup.find_all("img"):
            for attr in ["src", "data-src", "data-lazy-src"]:
                if url := img.get(attr):
                    img_urls.add(url.split()[0])
        
        # Загрузка изображений
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(save_image, url, img_dir) for url in img_urls]
            for future in as_completed(futures):
                if path := future.result():
                    images.append(path)
        
        # Если нет изображений, пробуем featured media
        if not images and "_embedded" in post:
            media = post["_embedded"].get("wp:featuredmedia", [])
            if media and media[0].get("source_url"):
                if path := save_image(media[0]["source_url"], img_dir):
                    images.append(path)
        
        if not images:
            logger.warning(f"No images for article ID={post_id}")
            return None
        
        # Создание метаданных
        meta = {
            "id": post_id,
            "slug": slug,
            "date": post.get("date"),
            "link": post.get("link"),
            "title": title,
            "text_file": str(text_path),
            "images": images,
            "posted": False,
            "hash": content_hash,
            "translated_to": translate_to
        }
        
        # Перевод контента (если требуется)
        if translate_to:
            translated_paras = [translate_text(p, to_lang=translate_to) for p in paragraphs]
            translated_text = "\n\n".join(translated_paras)
            translated_path = art_dir / f"content.{translate_to}.txt"
            translated_path.write_text(f"**{title}**\n\n{translated_text}", encoding="utf-8")
            meta["translated_file"] = str(translated_path)
            meta["text_file"] = str(translated_path)
        
        # Сохранение метаданных
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)
        
        return meta
        
    except Exception as e:
        logger.error(f"Error processing article ID={post_id}: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description="Article parser with translation")
    parser.add_argument("--base-url", default="https://www.khmertimeskh.com", help="WordPress site URL")
    parser.add_argument("--slug", default="national", help="Category slug")
    parser.add_argument("-n", "--limit", type=int, default=None, help="Max posts to parse")
    parser.add_argument("-l", "--lang", default="ru", help="Translation language")
    parser.add_argument("--posted-file", default="articles/posted.json", help="Posted articles state file")
    
    args = parser.parse_args()
    
    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        
        # Загрузка опубликованных ID
        posted_ids = load_posted_ids(Path(args.posted_file))
        logger.info(f"Loaded {len(posted_ids)} posted article IDs")
        
        # Получение категории
        category_id = fetch_category_id(args.base_url, args.slug)
        logger.info(f"Fetching posts for category ID: {category_id}")
        
        # Получение постов
        posts = fetch_posts(args.base_url, category_id, per_page=args.limit or 50)
        logger.info(f"Found {len(posts)} posts to process")
        
        # Обработка постов
        catalog = []
        processed = 0
        
        for post in posts:
            post_id = post["id"]
            if post_id in posted_ids:
                logger.debug(f"Skipping posted article ID={post_id}")
                continue
                
            if meta := parse_and_save(post, args.lang, args.base_url):
                catalog.append(meta)
                processed += 1
                logger.info(f"Processed article ID={post_id}")
                
                if args.limit and processed >= args.limit:
                    break
        
        logger.info(f"Processed {processed} new articles")
        
    except Exception as e:
        logger.exception("Fatal error in parser")
        sys.exit(1)

if __name__ == "__main__":
    main()
