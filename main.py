import argparse
import logging
import json
import hashlib
import time
import re
import os
import shutil
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Set

os.environ["translators_default_region"] = "EN"
from bs4 import BeautifulSoup
import cloudscraper
from requests.exceptions import RequestException, Timeout as ReqTimeout
import translators as ts
import fcntl

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

OUTPUT_DIR = Path("articles")
CATALOG_PATH = OUTPUT_DIR / "catalog.json"
MAX_POSTED_RECORDS = 100
MAX_RETRIES = 3
BASE_DELAY = 1.0

SCRAPER = cloudscraper.create_scraper()
SCRAPER_TIMEOUT = (10.0, 60.0)

BAD_RE = re.compile(r"[\u200b-\u200f\uFEFF\u200E\u00A0]")

def load_posted_ids(state_file_path: Path) -> Set[str]:
    try:
        if state_file_path.exists():
            with open(state_file_path, 'r', encoding='utf-8') as f:
                fcntl.flock(f, fcntl.LOCK_SH)
                return {str(item) for item in json.load(f)}
        return set()
    except Exception as e:
        logging.warning(f"Could not load posted IDs from {state_file_path}: {e}. Assuming empty set.")
        return set()

def extract_img_url(img_tag: Any) -> Optional[str]:
    attributes_to_check = ["data-brsrcset", "data-breeze", "data-src", "data-lazy-src", "data-original", "srcset", "src"]
    for attr in attributes_to_check:
        if src_val := img_tag.get(attr):
            return src_val.split(',')[0].split()[0]
    return None

def fetch_category_id(base_url: str, slug: str) -> int:
    logging.info(f"Fetching category ID for {slug} from {base_url}...")
    endpoint = f"{base_url}/wp-json/wp/v2/categories?slug={slug}"
    # ... (код функции без изменений) ...
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = SCRAPER.get(endpoint, timeout=SCRAPER_TIMEOUT)
            r.raise_for_status()
            data = r.json()
            if not data: raise RuntimeError(f"Category '{slug}' not found")
            return data[0]["id"]
        except Exception as e:
            delay = BASE_DELAY * 2 ** (attempt - 1)
            logging.warning(f"Error fetching category (try {attempt}/{MAX_RETRIES}): {e}; retry in {delay:.1f}s")
            time.sleep(delay)
    raise RuntimeError("Failed fetching category id")


def fetch_posts(base_url: str, cat_id: int, per_page: int = 10) -> List[Dict[str, Any]]:
    # ... (код функции без изменений) ...
    logging.info(f"Fetching posts for category {cat_id}, per_page={per_page}...")
    endpoint = f"{base_url}/wp-json/wp/v2/posts?categories={cat_id}&per_page={per_page}&_embed"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = SCRAPER.get(endpoint, timeout=SCRAPER_TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            delay = BASE_DELAY * 2 ** (attempt - 1)
            logging.warning(f"Error fetching posts (try {attempt}/{MAX_RETRIES}): {e}; retry in {delay:.1f}s")
            time.sleep(delay)
    logging.error("Giving up fetching posts")
    return []

def save_image(src_url: str, folder: Path) -> Optional[str]:
    # ... (код функции без изменений) ...
    logging.info(f"Saving image from {src_url}...")
    folder.mkdir(parents=True, exist_ok=True)
    fn = src_url.rsplit('/', 1)[-1].split('?', 1)[0]
    dest = folder / fn
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = SCRAPER.get(src_url, timeout=SCRAPER_TIMEOUT)
            r.raise_for_status()
            dest.write_bytes(r.content)
            return str(dest)
        except Exception as e:
            delay = BASE_DELAY * 2 ** (attempt - 1)
            logging.warning(f"Error saving image {fn} (try {attempt}/{MAX_RETRIES}): {e}; retry in {delay:.1f}s")
            time.sleep(delay)
    logging.error(f"Failed saving image {fn} after {MAX_RETRIES} attempts")
    return None

# Функции load_catalog и save_catalog остаются без изменений
def load_catalog() -> List[Dict[str, Any]]:
    if not CATALOG_PATH.exists(): return []
    try:
        with open(CATALOG_PATH, "r", encoding="utf-8") as f:
            fcntl.flock(f, fcntl.LOCK_SH)
            return [item for item in json.load(f) if isinstance(item, dict) and "id" in item]
    except Exception as e:
        logging.error(f"Catalog read/decode error: {e}")
        return []

def save_catalog(catalog: List[Dict[str, Any]]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    minimal = [{"id": item["id"], "hash": item.get("hash", ""), "translated_to": item.get("translated_to", "")}
               for item in catalog if isinstance(item, dict) and "id" in item]
    try:
        with open(CATALOG_PATH, "w", encoding="utf-8") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            json.dump(minimal, f, ensure_ascii=False, indent=2)
    except IOError as e:
        logging.error(f"Failed to save catalog: {e}")

def translate_text(text: str, to_lang: str = "ru", provider: str = "yandex") -> Optional[str]:
    if not text or not isinstance(text, str): return ""
    logging.info(f"Translating text (provider: {provider}) to {to_lang}...")
    try:
        translated = ts.translate_text(text, translator=provider, from_language="en", to_language=to_lang)
        if isinstance(translated, str): return translated
        return None
    except Exception as e:
        logging.warning(f"Translation error: {e}")
        return None

def parse_and_save(post: Dict[str, Any], translate_to: str) -> Optional[Dict[str, Any]]:
    aid = str(post["id"])
    slug = post["slug"]
    link = post.get("link")
    if not link: return None

    art_dir = OUTPUT_DIR / f"{aid}_{slug}"
    art_dir.mkdir(parents=True, exist_ok=True)
    
    logging.info(f"Fetching full page HTML for article ID={aid} from {link}")
    try:
        page_response = SCRAPER.get(link, timeout=SCRAPER_TIMEOUT)
        page_response.raise_for_status()
        page_html = page_response.text
    except Exception as e:
        logging.error(f"Failed to fetch full page for ID={aid}: {e}")
        return None

    # --- ДИАГНОСТИКА ---
    (art_dir / "_debug_1_full_page.html").write_text(page_html, encoding="utf-8")
    logging.info(f"Saved _debug_1_full_page.html for ID={aid}")
    # --------------------

    soup = BeautifulSoup(page_html, "html.parser")
    
    article_content = soup.find("div", class_="entry-content")
    if not article_content:
        logging.warning(f"Could not find article content container for ID={aid}. Skipping.")
        return None
        
    # --- ДИАГНОСТИКА ---
    (art_dir / "_debug_2_article_content.html").write_text(str(article_content), encoding="utf-8")
    logging.info(f"Saved _debug_2_article_content.html for ID={aid}")
    # --------------------

    # Просто логируем, но не удаляем блок, чтобы увидеть его в debug-файле
    if article_content.find("ul", class_="rp4wp-posts-list"):
        logging.info("Found 'Related Posts' block.")
        
    # Ищем картинки в найденном контейнере
    img_tags = article_content.find_all("img", limit=15)
    if img_tags:
        logging.info(f"Found {len(img_tags)} img tags inside entry-content. First 5:")
        for img_tag in img_tags[:5]:
            logging.info(f"DEBUG_IMG_TAG: {img_tag}")
    else:
        logging.warning(f"No img tags found inside entry-content for ID={aid}.")
    
    # Мы не будем скачивать картинки в этом диагностическом запуске, просто вернём None
    logging.warning(f"DIAGNOSTIC RUN: Skipping actual processing for ID={aid}")
    return None # Завершаем здесь, чтобы не выполнять остальную логику

def main():
    parser = argparse.ArgumentParser(description="Parser")
    parser.add_argument("--base-url", type=str, required=True, help="WP site base URL")
    parser.add_argument("--slug", type=str, default="national", help="Category slug")
    parser.add_argument("-n", "--limit", type=int, default=10, help="Max posts to parse")
    parser.add_argument("-l", "--lang", type=str, default="ru", help="Translate to language code")
    parser.add_argument("--posted-state-file", type=str, default="articles/posted.json", help="State file path")
    args = parser.parse_args()

    try:
        cid = fetch_category_id(args.base_url, args.slug)
        posts = fetch_posts(args.base_url, cid, per_page=(args.limit or 10))
        posted_ids = load_posted_ids(Path(args.posted_state_file))
        
        for post in posts:
            if str(post["id"]) not in posted_ids:
                parse_and_save(post, args.lang)
        
        print("NEW_ARTICLES_STATUS:false") # Всегда false, так как мы ничего не публикуем

    except Exception as e:
        logging.exception("Fatal error in main:")
        exit(1)

if __name__ == "__main__":
    main()
