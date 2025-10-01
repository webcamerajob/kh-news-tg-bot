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

def cleanup_old_articles(posted_ids_path: Path, articles_dir: Path):
    """
    Удаляет папки со старыми статьями, которые были "вытеснены" из posted.json.
    """
    if not posted_ids_path.is_file() or not articles_dir.is_dir():
        return
    logging.info("Starting cleanup of old article directories...")
    try:
        with open(posted_ids_path, 'r', encoding='utf-8') as f:
            all_posted_ids = [str(item) for item in json.load(f)]
        if len(all_posted_ids) <= MAX_POSTED_RECORDS:
            logging.info("No old articles to clean up.")
            return
        ids_to_keep = set(all_posted_ids[-MAX_POSTED_RECORDS:])
        cleaned_count = 0
        for article_folder in articles_dir.iterdir():
            if article_folder.is_dir():
                dir_id = article_folder.name.split('_', 1)[0]
                if dir_id.isdigit() and dir_id not in ids_to_keep:
                    logging.warning(f"🧹 Cleaning up old article directory: {article_folder.name}")
                    shutil.rmtree(article_folder)
                    cleaned_count += 1
        if cleaned_count > 0:
            logging.info(f"Cleanup complete. Removed {cleaned_count} old article directories.")
    except Exception as e:
        logging.error(f"An error occurred during cleanup: {e}")

def load_posted_ids(state_file_path: Path) -> Set[str]:
    """Загружает множество ID из файла состояния."""
    try:
        if state_file_path.exists():
            with open(state_file_path, 'r', encoding='utf-8') as f:
                fcntl.flock(f, fcntl.LOCK_SH)
                return {str(item) for item in json.load(f)}
        return set()
    except (FileNotFoundError, json.JSONDecodeError, IOError) as e:
        logging.warning(f"Could not load posted IDs from {state_file_path}: {e}. Assuming empty set.")
        return set()

def extract_img_url(img_tag: Any) -> Optional[str]:
    for attr in ("data-src", "data-lazy-src", "data-srcset", "srcset", "src"):
        if val := img_tag.get(attr):
            return val.split()[0]
    return None

def fetch_category_id(base_url: str, slug: str) -> int:
    logging.info(f"Fetching category ID for {slug} from {base_url}...")
    endpoint = f"{base_url}/wp-json/wp/v2/categories?slug={slug}"
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

def translate_text(text: str, to_lang: str = "ru", provider: str = "yandex") -> str:
    if not text or not isinstance(text, str): return ""
    try:
        logging.info(f"Translating text (provider: {provider}) to {to_lang}...")
        return ts.translate_text(text, translator=provider, from_language="en", to_language=to_lang)
    except Exception as e:
        logging.warning(f"Translation error: {e}")
    return text

def parse_and_save(post: Dict[str, Any], translate_to: str, base_url: str) -> Optional[Dict[str, Any]]:
    aid = str(post["id"])
    slug = post["slug"]
    art_dir = OUTPUT_DIR / f"{aid}_{slug}"
    art_dir.mkdir(parents=True, exist_ok=True)
    meta_path = art_dir / "meta.json"

    current_hash = hashlib.sha256(post["content"]["rendered"].encode()).hexdigest()
    if meta_path.exists():
        try:
            existing_meta = json.loads(meta_path.read_text(encoding="utf-8"))
            if existing_meta.get("hash") == current_hash and existing_meta.get("translated_to", "") == translate_to:
                logging.info(f"Skipping unchanged article ID={aid}")
                return existing_meta
        except (json.JSONDecodeError, UnicodeDecodeError):
            logging.warning(f"Failed to read existing meta for ID={aid}. Reparsing.")

    orig_title = BeautifulSoup(post["title"]["rendered"], "html.parser").get_text(strip=True)
    title = translate_text(orig_title, to_lang=translate_to) if translate_to else orig_title

    soup = BeautifulSoup(post["content"]["rendered"], "html.parser")
    paras = [p.get_text(strip=True) for p in soup.find_all("p")]
    raw_text = "\n\n".join(paras)
    raw_text = BAD_RE.sub("", raw_text)

    img_dir = art_dir / "images"
    srcs = {extract_img_url(img) for img in soup.find_all("img")[:10] if extract_img_url(img)}
    images: List[str] = []
    if srcs:
        with ThreadPoolExecutor(max_workers=5) as ex:
            futures = {ex.submit(save_image, url, img_dir): url for url in srcs}
            for fut in as_completed(futures):
                if path := fut.result():
                    images.append(path)

    if not images and "_embedded" in post and (media := post["_embedded"].get("wp:featuredmedia")):
        if path := save_image(media[0]["source_url"], img_dir):
            images.append(path)

    if not images:
        logging.warning(f"No images for ID={aid}; skipping.")
        return None
    
    text_file_path = art_dir / "content.txt"
    meta = {
        "id": aid, "slug": slug, "date": post.get("date"), "link": post.get("link"),
        "title": title, "text_file": text_file_path.name,
        "images": sorted([Path(p).name for p in images]), "posted": False,
        "hash": current_hash, "translated_to": ""
    }
    text_file_path.write_text(raw_text, encoding="utf-8")

    if translate_to:
        trans_text = translate_text("\n\n".join(paras), to_lang=translate_to)
        trans_file_path = art_dir / f"content.{translate_to}.txt"
        trans_file_path.write_text(f"{title}\n\n\n{trans_text}", encoding="utf-8")
        meta.update({"translated_to": translate_to, "text_file": trans_file_path.name})

    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    return meta

def main():
    parser = argparse.ArgumentParser(description="Parser")
    parser.add_argument("--base-url", type=str, required=True, help="WP site base URL")
    parser.add_argument("--slug", type=str, default="national", help="Category slug")
    parser.add_argument("-n", "--limit", type=int, default=10, help="Max posts to parse")
    parser.add_argument("-l", "--lang", type=str, default="ru", help="Translate to language code")
    parser.add_argument("--posted-state-file", type=str, default="articles/posted.json", help="State file path")
    args = parser.parse_args()

    # Запускаем очистку старых папок в самом начале
    posted_ids_path = Path(args.posted_state_file)
    cleanup_old_articles(posted_ids_path, OUTPUT_DIR)

    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        cid = fetch_category_id(args.base_url, args.slug)
        posts = fetch_posts(args.base_url, cid, per_page=(args.limit or 10) * 3)

        catalog = load_catalog()
        posted_ids = load_posted_ids(posted_ids_path)
        
        new_articles_count = 0
        processed_articles_meta = []

        for post in posts:
            if str(post["id"]) not in posted_ids:
                # ИСПРАВЛЕНИЕ ЗДЕСЬ: добавлен args.base_url
                if meta := parse_and_save(post, args.lang, args.base_url):
                    processed_articles_meta.append(meta)
        
        if processed_articles_meta:
            existing_ids_in_catalog = {item['id'] for item in catalog}
            for meta in processed_articles_meta:
                if meta['id'] not in existing_ids_in_catalog:
                    new_articles_count += 1
                catalog = [item for item in catalog if item.get("id") != meta["id"]]
                catalog.append(meta)

            save_catalog(catalog)
            print("NEW_ARTICLES_STATUS:true")
        else:
            print("NEW_ARTICLES_STATUS:false")

    except Exception as e:
        logging.exception("Fatal error in main:")
        exit(1)

if __name__ == "__main__":
    main()
