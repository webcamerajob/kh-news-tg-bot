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

# ---Настройка окружения и импорты---
os.environ["translators_default_region"] = "EN"

from bs4 import BeautifulSoup
import cloudscraper
from requests.exceptions import RequestException, Timeout as ReqTimeout
import translators as ts
import fcntl

# ---Настройки логирования---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ---Константы---
OUTPUT_DIR = Path("articles")
CATALOG_PATH = OUTPUT_DIR / "catalog.json"
MAX_POSTED_RECORDS = 100
MAX_RETRIES = 3
BASE_DELAY = 1.0

# ---Инициализация скрейпера---
SCRAPER = cloudscraper.create_scraper()
SCRAPER_TIMEOUT = (10.0, 60.0)

BAD_RE = re.compile(r"[\u200b-\u200f\uFEFF\u200E\u00A0]")

# ---Вспомогательные функции---

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
        else:
            logging.info("Cleanup complete. No matching old directories found to remove.")

    except (json.JSONDecodeError, Exception) as e:
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
    # ... (код функции без изменений) ...
    for attr in ("data-src", "data-lazy-src", "data-srcset", "srcset", "src"):
        if val := img_tag.get(attr):
            return val.split()[0]
    return None

def fetch_category_id(base_url: str, slug: str) -> int:
    # ... (код функции без изменений) ...
    logging.info(f"Fetching category ID for {slug} from {base_url}...")
    endpoint = f"{base_url}/wp-json/wp/v2/categories?slug={slug}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = SCRAPER.get(endpoint, timeout=SCRAPER_TIMEOUT)
            r.raise_for_status()
            data = r.json()
            if not data: raise RuntimeError(f"Category '{slug}' not found")
            return data[0]["id"]
        except (ReqTimeout, RequestException) as e:
            delay = BASE_DELAY * 2 ** (attempt - 1)
            logging.warning(f"Timeout fetching category (try {attempt}/{MAX_RETRIES}): {e}; retry in {delay:.1f}s")
            time.sleep(delay)
        except json.JSONDecodeError as e:
            logging.error(f"JSON decode error for categories: {e}")
            break
    raise RuntimeError("Failed fetching category id")

def fetch_posts(base_url: str, cat_id: int, per_page: int = 10) -> List[Dict[str, Any]]:
    # ... (код функции без изменений) ...
    logging.info(f"Fetching posts for category {cat_id} from {base_url}, per_page={per_page}...")
    endpoint = f"{base_url}/wp-json/wp/v2/posts?categories={cat_id}&per_page={per_page}&_embed"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = SCRAPER.get(endpoint, timeout=SCRAPER_TIMEOUT)
            r.raise_for_status()
            return r.json()
        except (ReqTimeout, RequestException) as e:
            delay = BASE_DELAY * 2 ** (attempt - 1)
            logging.warning(f"Timeout fetching posts (try {attempt}/{MAX_RETRIES}): {e}; retry in {delay:.1f}s")
            time.sleep(delay)
        except json.JSONDecodeError as e:
            logging.error(f"JSON decode error for posts: {e}")
            break
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
        except (ReqTimeout, RequestException) as e:
            delay = BASE_DELAY * 2 ** (attempt - 1)
            logging.warning(f"Timeout saving image {fn} (try {attempt}/{MAX_RETRIES}): {e}; retry in {delay:.1f}s")
            time.sleep(delay)
    logging.error(f"Failed saving image {fn} after {MAX_RETRIES} attempts")
    return None

def load_catalog() -> List[Dict[str, Any]]:
    # ... (код функции без изменений) ...
    if not CATALOG_PATH.exists(): return []
    try:
        with open(CATALOG_PATH, "r", encoding="utf-8") as f:
            fcntl.flock(f, fcntl.LOCK_SH)
            data = json.load(f)
            return [item for item in data if isinstance(item, dict) and "id" in item]
    except (json.JSONDecodeError, UnicodeDecodeError, IOError) as e:
        logging.error(f"Catalog read/decode error: {e}")
        return []

def save_catalog(catalog: List[Dict[str, Any]]) -> None:
    # ... (код функции без изменений) ...
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    minimal = [
        {"id": item["id"], "hash": item.get("hash", ""), "translated_to": item.get("translated_to", "")}
        for item in catalog if isinstance(item, dict) and "id" in item
    ]
    try:
        with open(CATALOG_PATH, "w", encoding="utf-8") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            json.dump(minimal, f, ensure_ascii=False, indent=2)
    except IOError as e:
        logging.error(f"Failed to save catalog: {e}")

def load_stopwords(filepath: Path) -> Set[str]:
    # ... (код функции без изменений) ...
    if not filepath.exists():
        logging.info("Файл стоп-слов не найден, проверка не будет производиться.")
        return set()
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            stopwords = {line.strip().lower() for line in f if line.strip()}
            logging.info(f"Загружено {len(stopwords)} стоп-слов из {filepath.name}.")
            return stopwords
    except Exception as e:
        logging.error(f"Не удалось прочитать файл стоп-слов {filepath.name}: {e}")
        return set()

def translate_text(text: str, to_lang: str = "ru", provider: str = "yandex") -> str:
    # ... (код функции без изменений) ...
    if not text or not isinstance(text, str): return ""
    try:
        translated = ts.translate_text(text, translator=provider, from_language="en", to_language=to_lang)
        if isinstance(translated, str): return translated
        logging.warning(f"Translator returned non-str for text: {text[:50]}")
    except Exception as e:
        logging.warning(f"Translation error [{provider} -> {to_lang}]: {e}")
    return text

def translate_in_chunks(paragraphs: List[str], to_lang: str, provider: str = "yandex", chunk_size: int = 4500) -> List[str]:
    # ... (код функции без изменений) ...
    logging.info(f"Translating {len(paragraphs)} paragraphs in chunks to '{to_lang}'...")
    full_text = "\n\n".join(paragraphs)
    if len(full_text) <= chunk_size:
        translated_full_text = translate_text(full_text, to_lang=to_lang, provider=provider)
        return translated_full_text.split("\n\n")
    translated_paragraphs, current_chunk, current_len = [], [], 0
    for p in paragraphs:
        if current_len + len(p) + 2 > chunk_size and current_chunk:
            text_to_translate = "\n\n".join(current_chunk)
            translated_chunk_text = translate_text(text_to_translate, to_lang=to_lang, provider=provider)
            translated_paragraphs.extend(translated_chunk_text.split("\n\n"))
            current_chunk, current_len = [p], len(p)
        else:
            current_chunk.append(p)
            current_len += len(p) + 2
    if current_chunk:
        text_to_translate = "\n\n".join(current_chunk)
        translated_chunk_text = translate_text(text_to_translate, to_lang=to_lang, provider=provider)
        translated_paragraphs.extend(translated_chunk_text.split("\n\n"))
    return translated_paragraphs

def parse_and_save(post: Dict[str, Any], translate_to: str, base_url: str, stopwords: Set[str]) -> Optional[Dict[str, Any]]:
    # ... (код функции без изменений) ...
    orig_title = BeautifulSoup(post["title"]["rendered"], "html.parser").get_text(strip=True)
    if stopwords:
        for stop_phrase in stopwords:
            pattern = r'\b' + re.escape(stop_phrase) + r'\b'
            if re.search(pattern, orig_title, re.IGNORECASE):
                logging.warning(f"🚫 Статья ID={post['id']} пропущена из-за стоп-фразы в ЗАГОЛОВКЕ: '{stop_phrase}'.")
                return None
    aid, slug = str(post["id"]), post["slug"]
    art_dir = OUTPUT_DIR / f"{aid}_{slug}"
    art_dir.mkdir(parents=True, exist_ok=True)
    meta_path = art_dir / "meta.json"
    current_hash = hashlib.sha256(post["content"]["rendered"].encode()).hexdigest()
    if meta_path.exists():
        try:
            existing_meta = json.loads(meta_path.read_text(encoding="utf-8"))
            if existing_meta.get("hash") == current_hash and existing_meta.get("translated_to", "") == translate_to:
                logging.info(f"Skipping unchanged article ID={aid} (content and translation match local cache).")
                return existing_meta
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logging.warning(f"Failed to read existing meta for ID={aid}: {e}. Reparsing.")
    title = orig_title
    if translate_to:
        title = translate_text(orig_title, to_lang=translate_to, provider="yandex")
    soup = BeautifulSoup(post["content"]["rendered"], "html.parser")
    paras = [p.get_text(strip=True) for p in soup.find_all("p")]
    raw_text = "\n\n".join(paras)
    raw_text = BAD_RE.sub("", raw_text)
    img_dir = art_dir / "images"
    srcs = {extract_img_url(img) for img in soup.find_all("img")[:10] if extract_img_url(img)}
    images: List[str] = []
    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = {ex.submit(save_image, url, img_dir): url for url in srcs}
        for fut in as_completed(futures):
            if path := fut.result():
                images.append(path)
    if not images and "_embedded" in post:
        if media := post["_embedded"].get("wp:featuredmedia"):
            if path := save_image(media[0]["source_url"], img_dir):
                images.append(path)
    if not images:
        logging.warning(f"No images for ID={aid}; skipping article.")
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
        clean_paras = [BAD_RE.sub("", p) for p in paras if p]
        trans_paras = translate_in_chunks(clean_paras, to_lang=translate_to, provider="yandex")
        trans_txt = "\n\n".join(trans_paras)
        trans_file_path = art_dir / f"content.{translate_to}.txt"
        header_t = f"{title}\n\n\n"
        trans_file_path.write_text(header_t + trans_txt, encoding="utf-8")
        meta.update({
            "translated_to": translate_to, "translated_file": trans_file_path.name,
            "text_file": trans_file_path.name
        })
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    return meta

def main():
    """Основная функция запуска скрипта."""
    parser = argparse.ArgumentParser(description="Parser with translation")
    parser.add_argument("--base-url", type=str, default="https://www.khmertimeskh.com", help="WP site base URL")
    parser.add_argument("--slug", type=str, default="national", help="Category slug")
    parser.add_argument("-n", "--limit", type=int, default=10, help="Max posts to parse")
    parser.add_argument("-l", "--lang", type=str, default="ru", help="Translate to language code")
    parser.add_argument("--posted-state-file", type=str, default="articles/posted.json", help="Path to the state file")
    args = parser.parse_args()

    posted_ids_path = Path(args.posted_state_file)
    cleanup_old_articles(posted_ids_path, OUTPUT_DIR)

    stopwords_path = Path("stopwords.txt")
    stopwords = load_stopwords(stopwords_path)

    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        cid = fetch_category_id(args.base_url, args.slug)
        
        post_request_count = (args.limit or 10) * 3
        posts = fetch_posts(args.base_url, cid, per_page=post_request_count)

        catalog = load_catalog()
        existing_ids_in_catalog = {article["id"] for article in catalog}
        posted_ids_from_repo = load_posted_ids(Path(args.posted_state_file))
        logging.info(f"Loaded {len(posted_ids_from_repo)} posted IDs from {args.posted_state_file}.")

        new_articles_in_run, updated_articles_in_run, processed_count = 0, 0, 0
        
        for post in posts:
            if args.limit and processed_count >= args.limit:
                logging.info(f"Processing limit of {args.limit} articles reached.")
                break

            post_id = str(post["id"])
            if post_id in posted_ids_from_repo:
                continue

            if meta := parse_and_save(post, args.lang, args.base_url, stopwords):
                processed_count += 1
                if post_id not in existing_ids_in_catalog:
                    new_articles_in_run += 1
                else:
                    updated_articles_in_run += 1
                
                catalog = [item for item in catalog if item.get("id") != post_id]
                catalog.append(meta)

        if new_articles_in_run > 0 or updated_articles_in_run > 0:
            save_catalog(catalog)
            logging.info(f"Catalog saved. New: {new_articles_in_run}, Updated: {updated_articles_in_run}.")
            if new_articles_in_run > 0:
                 print("NEW_ARTICLES_STATUS:true")
            else:
                 print("NEW_ARTICLES_STATUS:false")
        else:
            logging.info("No new or updated articles found.")
            print("NEW_ARTICLES_STATUS:false")

    except Exception as e:
        logging.exception("Fatal error in main:")
        exit(1)

if __name__ == "__main__":
    main()
