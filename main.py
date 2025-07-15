#!/usr/bin/env python3
import os
import fcntl  # ADDED: для блокировки файла
import argparse
import logging
import time
import json
import re
import hashlib
from pathlib import Path
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
import cloudscraper
from requests.exceptions import RequestException, Timeout as ReqTimeout
import translators as ts

# ─── Настройки ────────────────────────────────────────────────────────────────
SCRAPER = cloudscraper.create_scraper()
SCRAPER_TIMEOUT = (10.0, 60.0)
MAX_RETRIES = 3
BASE_DELAY  = 2.0

OUTPUT_DIR   = Path("articles")
CATALOG_PATH = OUTPUT_DIR / "catalog.json"
bad_re = re.compile(r"

\[\s*.*?\]

")

# ─── Вспомогательные функции ──────────────────────────────────────────────────

def extract_img_url(post: dict) -> Optional[str]:
    images = post.get("images", [])
    for img in images:
        if "full" in img:
            return img["full"]
    return None

def fetch_category_id(base_url: str, slug: str) -> int:
    endpoint = f"{base_url}/ml-api/v2/categories"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = SCRAPER.get(endpoint, timeout=SCRAPER_TIMEOUT)
            r.raise_for_status()
            data = r.json().get("categories", [])
            for cat in data:
                if cat.get("slug") == slug:
                    return int(cat["cat_id"])
            raise RuntimeError(f"Category '{slug}' not found")
        except (ReqTimeout, RequestException) as e:
            delay = BASE_DELAY * 2 ** (attempt - 1)
            logging.warning("Timeout fetching category (try %s/%s): %s; retry in %.1fs", attempt, MAX_RETRIES, e, delay)
            time.sleep(delay)
        except json.JSONDecodeError as e:
            logging.error("JSON decode error for categories: %s", e)
            break
    raise RuntimeError("Failed fetching category id")

def fetch_posts(base_url: str, cat_id: int, per_page: int = 10) -> List[Dict[str, Any]]:
    endpoint = f"{base_url}/ml-api/v2/posts/lists?limit={per_page}&cat_id={cat_id}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = SCRAPER.get(endpoint, timeout=SCRAPER_TIMEOUT)
            r.raise_for_status()
            return r.json().get("posts", [])
        except (ReqTimeout, RequestException) as e:
            delay = BASE_DELAY * 2 ** (attempt - 1)
            logging.warning("Timeout fetching posts (try %s/%s): %s; retry in %.1fs", attempt, MAX_RETRIES, e, delay)
            time.sleep(delay)
        except json.JSONDecodeError as e:
            logging.error("JSON decode error for posts: %s", e)
            break
    logging.error("Giving up fetching posts")
    return []

def save_image(src_url: str, folder: Path) -> Optional[str]:
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
            logging.warning("Timeout saving image %s (try %s/%s): %s; retry in %.1fs", fn, attempt, MAX_RETRIES, e, delay)
            time.sleep(delay)
    logging.error("Failed saving image %s after %s attempts", fn, MAX_RETRIES)
    return None

def load_catalog() -> List[Dict[str, Any]]:
    if not CATALOG_PATH.exists():
        return []
    try:
        with open(CATALOG_PATH, "r", encoding="utf-8") as f:
            fcntl.flock(f, fcntl.LOCK_SH)
            data = json.load(f)
            return [item for item in data if isinstance(item, dict) and "post_id" in item]
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        logging.error("Catalog JSON decode error: %s", e)
        return []
    except IOError as e:
        logging.error("Catalog read error: %s", e)
        return []

def save_catalog(catalog: List[Dict[str, Any]]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    minimal = []
    for item in catalog:
        minimal.append({
            "post_id": item["post_id"],
            "hash": item["hash"],
            "translated_to": item.get("translated_to", "")
        })
    try:
        with open(CATALOG_PATH, "w", encoding="utf-8") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            json.dump(minimal, f, ensure_ascii=False, indent=2)
    except IOError as e:
        logging.error("Failed to save catalog: %s", e)

def translate_text(text: str, to_lang: str = "ru", provider: str = "yandex") -> str:
    if not text or not isinstance(text, str):
        return ""
    try:
        translated = ts.translate_text(text, translator=provider, from_language="en", to_language=to_lang)
        if isinstance(translated, str):
            return translated
        logging.warning("Translator returned non-str: %s", text[:50])
    except Exception as e:
        logging.warning("Translation error [%s → %s]: %s", provider, to_lang, e)
    return text

def parse_and_save(post: Dict[str, Any], translate_to: str, base_url: str) -> Optional[Dict[str, Any]]:
    aid = post["post_id"]
    slug = post["slug"]
    art_dir = OUTPUT_DIR / f"{aid}_{slug}"
    art_dir.mkdir(parents=True, exist_ok=True)

    meta_path = art_dir / "meta.json"
    current_hash = hashlib.sha256(post["content"].encode()).hexdigest()

    if meta_path.exists():
        try:
            existing_meta = json.loads(meta_path.read_text(encoding="utf-8"))
            if existing_meta.get("hash") == current_hash and existing_meta.get("translated_to", "") == translate_to:
                logging.info(f"Skipping unchanged article ID={aid}")
                return existing_meta
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logging.warning(f"Failed to read existing meta for ID={aid}: {e}")

    orig_title = BeautifulSoup(post["title"], "html.parser").get_text(strip=True)
    title = translate_text(orig_title, to_lang=translate_to) if translate_to else orig_title

    soup = BeautifulSoup(post["content"], "html.parser")
    paras = [p.get_text(strip=True) for p in soup.find_all("p")]
    raw_text = "\n\n".join(paras)
    raw_text = bad_re.sub("", raw_text)
    raw_text = re.sub(r"[ \t]+", " ", raw_text)
    raw_text = re.sub(r"\n{3,}", "\n\n", raw_text)
    raw_text = f"**{title}**\n\n{raw_text}"

    img_dir = art_dir / "images"
    images: List[str] = []
    img_url = extract_img_url(post)
    if img_url:
        path = save_image(img_url, img_dir)
        if path:
            images.append(path)

    if not images:
        logging.warning("No images for ID=%s; skipping", aid)
        return None

    meta = {
        "post_id": aid, "slug": slug,
        "date": post.get("date"), "link": post.get("permalink"),
        "title": title,
        "text_file": str(art_dir / "content.txt"),
        "images": images, "posted": False,
        "hash": current_hash
    }
    (art_dir / "content.txt").write_text(raw_text, encoding="utf-8")

    if translate_to:
        trans = [translate_text(p, to_lang=translate_to) for p in paras]
        txt_t = art_dir / f"content.{translate_to}.txt"
        trans_txt = "\n\n".join(trans)
        txt_t.write_text(f"{title}\n\n\n{trans_txt}", encoding="utf-8")
        meta.update({
            "translated_to": translate_to,
            "translated_paras": trans,
            "translated_file": str(txt_t),
            "text_file": str(txt_t)
        })

    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    return meta

# ─── Точка входа ──────────────────────────────────────────────────────────────

def main():
    """CHANGED: Полностью переработанная логика обработки"""
    parser = argparse.ArgumentParser(description="Parser with translation")
    parser.add_argument("--base-url", type=str,
                       default="https://www.khmertimeskh.com",
                       help="WP site base URL")
    parser.add_argument("--slug", type=str, default="national",
                       help="Category slug")
    parser.add_argument("-n", "--limit", type=int, default=None,
                       help="Max posts to parse")
    parser.add_argument("-l", "--lang", type=str, default="",
                       help="Translate to language code")
    args = parser.parse_args()

    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        cid = fetch_category_id(args.base_url, args.slug)
        posts = fetch_posts(args.base_url, cid, per_page=(args.limit or 10)) # количество статей за один проход

        catalog = load_catalog()
        existing_ids = {article["id"] for article in catalog}
        new_articles = 0

        for post in posts[:args.limit or len(posts)]:
            post_id = post["id"]
            if post_id in existing_ids:
                logging.debug(f"Skipping existing article ID={post_id}")
                continue

            if meta := parse_and_save(post, args.lang, args.base_url):
                catalog.append(meta)
                existing_ids.add(post_id)
                new_articles += 1
                logging.info(f"Processed new article ID={post_id}")

        if new_articles > 0:
            save_catalog(catalog)
            logging.info(f"Added {new_articles} new articles. Total: {len(catalog)}")
        else:
            logging.info("No new articles found")

    except Exception as e:
        logging.exception("Fatal error in main:")
        exit(1)

if __name__ == "__main__":
    main()
