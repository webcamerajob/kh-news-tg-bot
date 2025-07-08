#!/usr/bin/env python3
import os
import json
import argparse
import logging
import re
import hashlib
import time

from typing import Any, Dict, List, Optional
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import cloudscraper
from requests.exceptions import ReadTimeout as ReqTimeout, RequestException
from deep_translator import GoogleTranslator
from bs4 import BeautifulSoup

import re
# списком — все фразы/слова, которые нужно вырезать
bad_patterns = [
    r"synopsis\s*:\s*",    # «Synopsis»
    r"\(video inside\)",   # «(video inside)»
    r"\bkhmer  times\b"      # слово «khmer times»
]
# единое регулярное выражение с флагом IGNORECASE
bad_re = re.compile("|".join(bad_patterns), flags=re.IGNORECASE)

# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
# ──────────────────────────────────────────────────────────────────────────────

# cloudscraper для обхода Cloudflare
SCRAPER = cloudscraper.create_scraper()
SCRAPER_TIMEOUT = (10.0, 60.0)    # (connect_timeout, read_timeout) в секундах

MAX_RETRIES = 3
BASE_DELAY  = 2.0                 # базовый интервал для backoff (сек)

OUTPUT_DIR   = Path("articles")
CATALOG_PATH = OUTPUT_DIR / "catalog.json"
# ──────────────────────────────────────────────────────────────────────────────

def extract_img_url(img_tag):
    """
    Возвращает первый валидный URL картинки из тега <img>,
    проверяя атрибуты data-src, srcset и src.
    """
    for attr in ("data-src", "data-lazy-src", "data-srcset", "srcset", "src"):
        val = img_tag.get(attr)
        if not val:
            continue
        # если это srcset или data-srcset, берём первую пару URL
        parts = val.split()
        if parts:
            return parts[0]
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
        return json.loads(CATALOG_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        logging.error("Catalog JSON decode error: %s", e)
        return []


def save_catalog(catalog: List[Dict[str, Any]]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    CATALOG_PATH.write_text(
        json.dumps(catalog, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


def chunk_text(
    text: str,
    size: int = 4096,
    preserve_formatting: bool = True
) -> List[str]:
    norm = text.replace('\r\n', '\n')
    paras = [p for p in norm.split('\n\n') if p.strip()]
    if not preserve_formatting:
        paras = [re.sub(r'\n+', ' ', p) for p in paras]

    chunks, curr = [], ""
    def _split_long(p: str) -> List[str]:
        parts, sub = [], ""
        for w in p.split(" "):
            if len(sub) + len(w) + 1 > size:
                parts.append(sub); sub = w
            else:
                sub = (sub + " " + w).lstrip()
        if sub:
            parts.append(sub)
        return parts

    for p in paras:
        if len(p) > size:
            if curr:
                chunks.append(curr); curr = ""
            chunks.extend(_split_long(p))
        else:
            if not curr:
                curr = p
            elif len(curr) + 2 + len(p) <= size:
                curr += "\n\n" + p
            else:
                chunks.append(curr); curr = p

    if curr:
        chunks.append(curr)
    return chunks


def parse_and_save(
    post: Dict[str, Any],
    translate_to: str,
    base_url: str
) -> Optional[Dict[str, Any]]:
    aid, slug = post["id"], post["slug"]
    art_dir = OUTPUT_DIR / f"{aid}_{slug}"
    art_dir.mkdir(parents=True, exist_ok=True)

    # извлекаем оригинальный заголовок
    orig_title = BeautifulSoup(post["title"]["rendered"], "html.parser")\
                 .get_text(strip=True)
    title = orig_title

    # переводим заголовок, если задан язык
    if translate_to:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                title = GoogleTranslator(source="auto", target=translate_to)\
                        .translate(orig_title)
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
    raw_text = "\n\n".join(paras)
        # ── Вырезаем все запрещённые слова/фразы ─────────────────────────────
    # bad_re объявлен вверху модуля
    raw_text = bad_re.sub("", raw_text)
    # опционально: убираем лишние пробелы и пустые строки
    raw_text = re.sub(r"[ \t]+", " ", raw_text)
    raw_text = re.sub(r"\n{3,}", "\n\n", raw_text)
    # ─────────────────────────────────────────────────────────────────────

    # Parallel image downloading with lazy-loading support
    img_dir = art_dir / "images"
    images: List[str] = []

    srcs = []
    for img in soup.find_all("img"):
        url = extract_img_url(img)
        if url:
            srcs.append(url)

    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = {ex.submit(save_image, url, img_dir): url for url in srcs}
        for fut in as_completed(futures):
            if path := fut.result():
                images.append(path)

    # если после этого нет images, попробуем media embedded из API
    if not images and "_embedded" in post:
        media = post["_embedded"].get("wp:featuredmedia")
        if media and media[0].get("source_url"):
            path = save_image(media[0]["source_url"], img_dir)
            if path:
                images.append(path)

    if not images:
        logging.warning("No images for ID=%s; skipping", aid)
        return None

    meta = {
        "id": aid, "slug": slug,
        "date": post.get("date"), "link": post.get("link"),
        "title": title,
        "text_file": str(art_dir / "content.txt"),
        "images": images, "posted": False
    }
    (art_dir / "content.txt").write_text(raw_text, encoding="utf-8")

    # перевод
    if translate_to:
        h = hashlib.sha256(raw_text.encode()).hexdigest()
        old = {}
        if (art_dir / "meta.json").exists():
            old = json.loads((art_dir / "meta.json").read_text(encoding="utf-8"))
        if old.get("hash") != h or old.get("translated_to") != translate_to:
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    trans = [
                        GoogleTranslator(source="auto", target=translate_to).translate(p)
                        for p in paras
                    ]
                    txt_t = art_dir / f"content.{translate_to}.txt"
                    txt_t.write_text("\n\n".join(trans), encoding="utf-8")
                    meta.update({
                        "translated_to": translate_to,
                        "hash": h,
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

    with open(art_dir / "meta.json", "w", encoding="utf-8") as f:
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

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    cid   = fetch_category_id(args.base_url, args.slug)
    posts = fetch_posts(args.base_url, cid, per_page=(args.limit or 10))

    catalog = load_catalog()
    for post in posts[: args.limit or len(posts)]:
        if meta := parse_and_save(post, args.lang, args.base_url):
            catalog.append(meta)
            logging.info("Saved ID=%s", meta["id"])

    save_catalog(catalog)
    logging.info("Done: %d articles in catalog", len(catalog))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logging.exception("Unhandled exception in main:")
        exit(1)
