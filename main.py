#!/usr/bin/env python3
import os
import json
import argparse
import logging
import re
import hashlib
import time
import fcntl
from typing import Any, Dict, List, Optional
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import cloudscraper
from requests.exceptions import ReadTimeout as ReqTimeout, RequestException
from deep_translator import GoogleTranslator
from bs4 import BeautifulSoup

# регулярка для ненужных фрагментов
bad_patterns = [
    r"synopsis\s*:\s*",
    r"\(video inside\)",
    r"\bkhmer times\b"
]
bad_re = re.compile("|".join(bad_patterns), flags=re.IGNORECASE)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

SCRAPER = cloudscraper.create_scraper()
SCRAPER_TIMEOUT = (10.0, 60.0)

MAX_RETRIES = 3
BASE_DELAY = 2.0

OUTPUT_DIR = Path("articles")
CATALOG_PATH = OUTPUT_DIR / "catalog.json"


def extract_img_url(img_tag):
    for attr in ("data-src", "data-lazy-src", "data-srcset", "srcset", "src"):
        val = img_tag.get(attr)
        if not val:
            continue
        return val.split()[0]
    return None


def fetch_category_id(base_url: str, slug: str) -> int:
    endpoint = f"{base_url}/wp-json/wp/v2/categories?slug={slug}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = SCRAPER.get(endpoint, timeout=SCRAPER_TIMEOUT)
            r.raise_for_status()
            data = r.json()
            if not data:
                raise RuntimeError(f"Category '{slug}' not found")
            return data[0]["id"]
        except (ReqTimeout, RequestException) as e:
            delay = BASE_DELAY * 2 ** (attempt - 1)
            logging.warning(
                "Timeout fetching category (try %s/%s): %s; retry in %.1fs",
                attempt, MAX_RETRIES, e, delay
            )
            time.sleep(delay)
        except json.JSONDecodeError as e:
            logging.error("JSON decode error for categories: %s", e)
            break
    raise RuntimeError("Failed fetching category id")


def fetch_posts(base_url: str, cat_id: int, per_page: int = 10) -> List[Dict[str, Any]]:
    endpoint = f"{base_url}/wp-json/wp/v2/posts?categories={cat_id}&per_page={per_page}&_embed"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = SCRAPER.get(endpoint, timeout=SCRAPER_TIMEOUT)
            r.raise_for_status()
            return r.json()
        except (ReqTimeout, RequestException) as e:
            delay = BASE_DELAY * 2 ** (attempt - 1)
            logging.warning(
                "Timeout fetching posts (try %s/%s): %s; retry in %.1fs",
                attempt, MAX_RETRIES, e, delay
            )
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
            logging.warning(
                "Timeout saving image %s (try %s/%s): %s; retry in %.1fs",
                fn, attempt, MAX_RETRIES, e, delay
            )
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
            return [item for item in data if isinstance(item, dict) and "id" in item]
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        logging.error("Catalog JSON decode error: %s", e)
        return []
    except IOError as e:
        logging.error("Catalog read error: %s", e)
        return []


def save_catalog(catalog: List[Dict[str, Any]]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    try:
        with open(CATALOG_PATH, "w", encoding="utf-8") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            json.dump(catalog, f, ensure_ascii=False, indent=2)
    except IOError as e:
        logging.error("Failed to save catalog: %s", e)


def chunk_text(
    text: str,
    size: int = 4096,
    preserve_formatting: bool = True,
    title: Optional[str] = None
) -> List[str]:
    title_md = f"**{title}**\n\n" if title else ""
    avail = size - len(title_md)
    if avail <= 0:
        raise ValueError("Title is longer than allowed chunk size")

    norm = text.replace('\r\n', '\n')
    paras = [p for p in norm.split('\n\n') if p.strip()]
    if not preserve_formatting:
        paras = [re.sub(r'\n+', ' ', p) for p in paras]

    def _split_long(p: str) -> List[str]:
        parts, sub = [], ""
        for w in p.split(" "):
            if len(sub) + len(w) + 1 > avail:
                parts.append(sub)
                sub = w
            else:
                sub = (sub + " " + w).lstrip()
        if sub:
            parts.append(sub)
        return parts

    chunks, curr = [], ""
    for p in paras:
        pieces = _split_long(p) if len(p) > avail else [p]
        for piece in pieces:
            if not curr:
                curr = piece
            elif len(curr) + 2 + len(piece) <= avail:
                curr += "\n\n" + piece
            else:
                chunks.append(curr)
                curr = piece
    if curr:
        chunks.append(curr)

    if title_md and chunks:
        chunks[0] = title_md + chunks[0]
    return chunks


def parse_and_save(post: Dict[str, Any], translate_to: str, base_url: str) -> Optional[Dict[str, Any]]:
    aid, slug = post["id"], post["slug"]
    art_dir = OUTPUT_DIR / f"{aid}_{slug}"
    art_dir.mkdir(parents=True, exist_ok=True)
    meta_path = art_dir / "meta.json"

    # проверка на неизменность
    if meta_path.exists():
        try:
            existing_meta = json.loads(meta_path.read_text(encoding="utf-8"))
            current_hash = hashlib.sha256(post["content"]["rendered"].encode()).hexdigest()
            if existing_meta.get("hash") == current_hash and existing_meta.get("translated_to", "") == translate_to:
                logging.info(f"Skipping unchanged article ID={aid}")
                return existing_meta
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logging.warning(f"Failed to read existing meta for ID={aid}: {e}")

    orig_title = BeautifulSoup(post["title"]["rendered"], "html.parser").get_text(strip=True)
    title = orig_title
    if translate_to:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                title = GoogleTranslator(source="auto", target=translate_to).translate(orig_title)
                break
            except Exception as e:
                delay = BASE_DELAY * 2 ** (attempt - 1)
                logging.warning(
                    "Translate title attempt %s failed: %s; retry in %.1fs",
                    attempt, e, delay
                )
                time.sleep(delay)

    soup = BeautifulSoup(post["content"]["rendered"], "html.parser")
    paras = [p.get_text(strip=True) for p in soup.find_all("p")]
    raw_text = "\n\n".join(bad_re.sub("", p) for p in paras)
    raw_text = re.sub(r"[ \t]+", " ", raw_text)
    raw_text = re.sub(r"\n{3,}", "\n\n", raw_text)

    img_dir = art_dir / "images"
    images, srcs = [], []
    for img in soup.find_all("img"):
        if url := extract_img_url(img):
            srcs.append(url)
    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = {ex.submit(save_image, url, img_dir): url for url in srcs}
        for fut in as_completed(futures):
            if path := fut.result():
                images.append(path)
    if not images and "_embedded" in post:
        media = post["_embedded"].get("wp:featuredmedia")
        if media and media[0].get("source_url"):
            if path := save_image(media[0]["source_url"], img_dir):
                images.append(path)
    if not images:
        logging.warning("No images for ID=%s; skipping", aid)
        return None

    # разбиваем текст на чанки, вставляем заголовок
    chunks = chunk_text(
        text=raw_text,
        size=4096,
        preserve_formatting=True,
        title=title
    )

    # сохраняем первый чанк в content.txt
    (art_dir / "content.txt").write_text(chunks[0], encoding="utf-8")

    meta = {
        "id": aid,
        "slug": slug,
        "date": post.get("date"),
        "link": post.get("link"),
        "title": title,
        "text_file": str(art_dir / "content.txt"),
        "images": images,
        "posted": False,
        "hash": hashlib.sha256(raw_text.encode()).hexdigest(),
        "chunks": chunks
    }

    if translate_to:
        h = meta["hash"]
        old = {}
        if meta_path.exists():
            try:
                old = json.loads(meta_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                pass
        if old.get("hash") != h or old.get("translated_to") != translate_to:
            clean_paras = [bad_re.sub("", p) for p in paras]
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    trans = [
                        GoogleTranslator(source="auto", target=translate_to).translate(p)
                        for p in clean_paras
                    ]
                    txt_t = art_dir / f"content.{translate_to}.txt"
                    txt_t.write_text("\n\n".join(trans), encoding="utf-8")
                    meta.update({
                        "translated_to": translate_to,
                        "translated_paras": trans,
                        "translated_file": str(txt_t),
                        "text_file": str(txt_t)
                    })
                    break
                except Exception as e:
                    delay = BASE_DELAY * 2 ** (attempt - 1)
                    logging.warning("Translate try %s failed: %s; retry in %.1fs", attempt, e, delay)
                    time.sleep(delay)
        else:
            logging.info("Using cached translation %s for ID=%s", translate_to, aid)

    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    return meta


def main():
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
        posts = fetch_posts(args.base_url, cid, per_page=(args.limit or 10))

        catalog = load_catalog()
        existing_ids = {article["id"] for article in catalog}
        new_articles = 0

        for post in posts[:args.limit or len(posts)]:
            pid = post["id"]
            if pid in existing_ids:
                logging.debug(f"Skipping existing article ID={pid}")
                continue

            if meta := parse_and_save(post, args.lang, args.base_url):
                catalog.append(meta)
                existing_ids.add(pid)
                new_articles += 1
                logging.info(f"Processed new article ID={pid}")

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
