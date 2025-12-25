import argparse
import logging
import json
import hashlib
import time
import re
import os
import shutil
import html
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
MAX_RETRIES = 3
BASE_DELAY = 1.0

# --- УСИЛЕНИЕ СКРЕЙПЕРА (ИСПРАВЛЕННАЯ ВЕРСИЯ) ---
DEFAULT_HEADERS = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9,ru;q=0.8',
    'accept-encoding': 'gzip, deflate, br',
    'upgrade-insecure-requests': '1',
}

# 1. Создаем объект скрейпера без заголовков
SCRAPER = cloudscraper.create_scraper(
    browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True},
    delay=10
)
# 2. Устанавливаем заголовки ПОСЛЕ создания объекта
SCRAPER.headers.update(DEFAULT_HEADERS)
SCRAPER_TIMEOUT = (15.0, 60.0)

BAD_RE = re.compile(r"[\u200b-\u200f\uFEFF\u200E\u00A0]")

def normalize_text(text: str) -> str:
    """Заменяет специальные типографские символы на их простые аналоги."""
    replacements = {'–': '-', '—': '-', '“': '"', '”': '"', '‘': "'", '’': "'"}
    for special, simple in replacements.items():
        text = text.replace(special, simple)
    return text

def sanitize_text(text: str) -> str:
    """Полная стерилизация текста от HTML-тегов и мусора редактора."""
    if not text: return ""
    text = html.unescape(text)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'mce_SELRES_[^ ]+', '', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()

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
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = SCRAPER.get(endpoint, timeout=SCRAPER_TIMEOUT)
            r.raise_for_status()
            
            content_type = r.headers.get('Content-Type', '')
            if 'application/json' not in content_type:
                raise RuntimeError(f"Expected JSON response, but got '{content_type}'. Body: {r.text[:300]}...")
            
            data = r.json()
            if not data:
                raise RuntimeError(f"Category '{slug}' not found (empty JSON response)")
            
            return data[0]["id"]
            
        except Exception as e:
            delay = BASE_DELAY * 2 ** (attempt - 1)
            response_text_snippet = ""
            if 'r' in locals() and hasattr(r, 'text') and r.text:
                response_text_snippet = f" | Response Body: {r.text[:300]}..."
            logging.warning(f"Error fetching category: {e}{response_text_snippet}; retry in {delay:.1f}s")
            time.sleep(delay)
            
    raise RuntimeError("Failed fetching category id")

def fetch_posts(base_url: str, cat_id: int, per_page: int = 10) -> List[Dict[str, Any]]:
    logging.info(f"Fetching posts for category {cat_id}...")
    endpoint = f"{base_url}/wp-json/wp/v2/posts?categories={cat_id}&per_page={per_page}&_embed"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = SCRAPER.get(endpoint, timeout=SCRAPER_TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            delay = BASE_DELAY * 2 ** (attempt - 1)
            time.sleep(delay)
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
            time.sleep(BASE_DELAY * 2 ** (attempt - 1))
    return None

def load_catalog() -> List[Dict[str, Any]]:
    if not CATALOG_PATH.exists(): return []
    try:
        with open(CATALOG_PATH, "r", encoding="utf-8") as f:
            fcntl.flock(f, fcntl.LOCK_SH)
            return [item for item in json.load(f) if isinstance(item, dict) and "id" in item]
    except Exception: return []

def save_catalog(catalog: List[Dict[str, Any]]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    minimal = [{"id": item["id"], "hash": item.get("hash", ""), "translated_to": item.get("translated_to", "")}
               for item in catalog if isinstance(item, dict) and "id" in item]
    try:
        with open(CATALOG_PATH, "w", encoding="utf-8") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            json.dump(minimal, f, ensure_ascii=False, indent=2)
    except IOError: pass

PROVIDER_LIMITS = {"google": 4800, "bing": 4500, "yandex": 4000}

def chunk_text_by_limit(text: str, limit: int) -> List[str]:
    chunks = []
    while text:
        if len(text) <= limit:
            chunks.append(text)
            break
        split_pos = text.rfind('\n\n', 0, limit)
        if split_pos == -1: split_pos = text.rfind('. ', 0, limit)
        if split_pos == -1: split_pos = text.rfind(' ', 0, limit)
        if split_pos == -1: split_pos = limit
        chunk_end = split_pos + (2 if text[split_pos:split_pos+2] == '\n\n' else 1)
        chunks.append(text[:chunk_end])
        text = text[chunk_end:].lstrip()
    return chunks

def translate_text(text: str, to_lang: str = "ru") -> Optional[str]:
    if not text: return ""
    providers = ["yandex", "google", "bing"]
    normalized_text = normalize_text(text)
    for provider in providers:
        limit = PROVIDER_LIMITS.get(provider, 3000)
        try:
            chunks = chunk_text_by_limit(normalized_text, limit)
            translated_chunks = []
            for i, chunk in enumerate(chunks):
                if i > 0: time.sleep(0.5)
                res = ts.translate_text(chunk, translator=provider, from_language="en", to_language=to_lang, timeout=45)
                if res and isinstance(res, str): 
                    translated_chunks.append(res)
                else: 
                    raise ValueError("Empty or invalid chunk translation")
            return "".join(translated_chunks)
        except Exception: 
            continue
    return None

def load_stopwords(file_path: Optional[Path]) -> List[str]:
    if not file_path or not file_path.exists(): return []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return [line.strip().lower() for line in f if line.strip()]
    except Exception: 
        return []

def parse_and_save(post: Dict[str, Any], translate_to: str, stopwords: List[str]) -> Optional[Dict[str, Any]]:
    aid = str(post["id"])
    slug = post["slug"]
    link = post.get("link")
    if not link: return None

    art_dir = OUTPUT_DIR / f"{aid}_{slug}"
    art_dir.mkdir(parents=True, exist_ok=True)
    meta_path = art_dir / "meta.json"

    raw_title = BeautifulSoup(post["title"]["rendered"], "html.parser").get_text(strip=True)
    orig_title = sanitize_text(raw_title)
    
    if stopwords:
        normalized_title = orig_title.lower()
        for phrase in stopwords:
            if phrase in normalized_title:
                logging.info(f"Stopword '{phrase}' found in ID={aid}. Skipping.")
                return None

    logging.info(f"Processing ID={aid}: {link}")
    try:
        page_response = SCRAPER.get(link, timeout=SCRAPER_TIMEOUT)
        page_response.raise_for_status()
        page_html = page_response.text
    except Exception as e:
        logging.error(f"Fetch error ID={aid}: {e}")
        return None

    current_hash = hashlib.sha256(page_html.encode()).hexdigest()
    if meta_path.exists():
        try:
            existing_meta = json.loads(meta_path.read_text(encoding="utf-8"))
            if existing_meta.get("hash") == current_hash and existing_meta.get("translated_to", "") == translate_to:
                logging.info(f"No changes for ID={aid}. Skipping.")
                return existing_meta
        except Exception: 
            pass

    title = translate_text(orig_title, to_lang=translate_to) if translate_to else orig_title
    if title:
        title = sanitize_text(title)
    else:
        logging.error(f"Title translation failed for ID={aid}")
        return None

    soup = BeautifulSoup(page_html, "html.parser")
    
    for junk in soup.find_all(["span", "div", "script", "style", "iframe"]):
        if junk.get("data-mce-type") or "mce_SELRES" in str(junk.get("class", "")):
            junk.decompose()
            
    article_content = soup.find("div", class_="entry-content")
    
    paras = []
    if article_content:
        for rel in article_content.find_all(["ul", "ol", "div"], class_=re.compile(r"rp4wp|related|ad-")):
            rel.decompose()
        for p in article_content.find_all("p"):
            p_text = p.get_text(strip=True)
            if p_text:
                clean_p = sanitize_text(p_text)
                if clean_p:
                    paras.append(clean_p)
    
    raw_text = "\n\n".join(paras)
    raw_text = BAD_RE.sub("", raw_text)

    img_dir = art_dir / "images"
    srcs = set()
    for link_tag in soup.find_all("a", class_="ci-lightbox", limit=10):
        if h := link_tag.get("href"): 
            srcs.add(h)
    if article_content:
        for img in article_content.find_all("img"):
            if u := extract_img_url(img): 
                srcs.add(u)

    images: List[str] = []
    if srcs:
        with ThreadPoolExecutor(max_workers=5) as ex:
            futures = {ex.submit(save_image, url, img_dir): url for url in list(srcs)[:10]}
            for fut in as_completed(futures):
                if path := fut.result(): 
                    images.append(path)

    if not images and "_embedded" in post and (media := post["_embedded"].get("wp_featuredmedia")):
        if isinstance(media, list) and (u := media[0].get("source_url")):
            if path := save_image(u, img_dir): 
                images.append(path)

    if not images:
        logging.warning(f"No images for ID={aid}. Skipping.")
        return None
    
    text_file_path = art_dir / "content.txt"
    meta = {
        "id": aid, "slug": slug, "date": post.get("date"), "link": link,
        "title": title, "text_file": text_file_path.name,
        "images": sorted([Path(p).name for p in images]), "posted": False,
        "hash": current_hash, "translated_to": ""
    }
    text_file_path.write_text(raw_text, encoding="utf-8")

    if translate_to:
        trans_text = translate_text(raw_text, to_lang=translate_to)
        if not trans_text:
             logging.error(f"Translation failed ID={aid}.")
             return None
        
        trans_text = sanitize_text(trans_text)
        
        trans_file_path = art_dir / f"content.{translate_to}.txt"
        final_translated_text = f"{title}\n\n{trans_text}"
        trans_file_path.write_text(final_translated_text, encoding="utf-8")
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
    parser.add_argument("--stopwords-file", type=str, help="Path to stopwords file")
    args = parser.parse_args()

    try:
        cid = fetch_category_id(args.base_url, args.slug)
        posts = fetch_posts(args.base_url, cid, per_page=(args.limit or 10) * 3)
        catalog = load_catalog()
        posted_ids = load_posted_ids(Path(args.posted_state_file))
        stopwords = load_stopwords(Path(args.stopwords_file) if args.stopwords_file else None)
        
        processed_articles_meta = []
        for post in posts:
            if str(post.get("id")) not in posted_ids:
                if meta := parse_and_save(post, args.lang, stopwords):
                    processed_articles_meta.append(meta)
        
        if processed_articles_meta:
            for meta in processed_articles_meta:
                catalog = [item for item in catalog if item.get("id") != meta.get("id")]
                catalog.append(meta)
            save_catalog(catalog)
            print("NEW_ARTICLES_STATUS:true")
        else:
            print("NEW_ARTICLES_STATUS:false")

    except Exception as e:
        logging.exception("Fatal error:")
        exit(1)

if __name__ == "__main__":
    main()
