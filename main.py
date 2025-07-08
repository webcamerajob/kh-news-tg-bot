#!/usr/bin/env python3
import os
import json
import argparse
import logging
import re
import hashlib
import time
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
from io import BytesIO

import httpx
from httpx import Timeout, HTTPStatusError, ReadTimeout, TransportError
from deep_translator import GoogleTranslator
from bs4 import BeautifulSoup

import cloudscraper

# создаём скрапер, который умеет «решать» Cloudflare
SCRAPER = cloudscraper.create_scraper()

# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
# ──────────────────────────────────────────────────────────────────────────────

TIMEOUT      = Timeout(connect=10.0, read=60.0, write=10.0, pool=5.0)
MAX_RETRIES  = 3
RETRY_DELAY  = 5.0

OUTPUT_DIR   = Path("articles")
CATALOG_PATH = OUTPUT_DIR / "catalog.json"


def fetch_category_id(slug: str = "national") -> int:
    url = f"https://www.khmertimeskh.com/wp-json/wp/v2/categories?slug={slug}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = SCRAPER.get(url, timeout=TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            if not data:
                raise RuntimeError(f"Category “{slug}” not found")
            return data[0]["id"]
        except (ReadTimeout, TransportError):
            logging.warning("Timeout fetching category (attempt %s/%s)", attempt, MAX_RETRIES)
        except HTTPStatusError as e:
            logging.error("HTTP %s fetching category: %s", e.response.status_code, e.response.text)
            break
        time.sleep(RETRY_DELAY)
    raise RuntimeError("Failed fetching category id")


def fetch_posts(cat_id: int, per_page: int = 10) -> List[Dict[str, Any]]:
    url = (
        f"https://www.khmertimeskh.com/wp-json/wp/v2"
        f"/posts?categories={cat_id}&per_page={per_page}&_embed"
    )
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = SCRAPER.get(url, timeout=TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except (ReadTimeout, TransportError):
            logging.warning("Timeout fetching posts (attempt %s/%s)", attempt, MAX_RETRIES)
        except HTTPStatusError as e:
            logging.error("HTTP %s fetching posts: %s", e.response.status_code, e.response.text)
            break
        time.sleep(RETRY_DELAY)
    logging.error("Giving up fetching posts")
    return []


def save_image(src_url: str, folder: Path) -> Optional[str]:
    folder.mkdir(parents=True, exist_ok=True)
    fn = src_url.split("/")[-1].split("?")[0]
    dest = folder / fn
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = SCRAPER.get(src_url, timeout=TIMEOUT)
            resp.raise_for_status()
            dest.write_bytes(resp.content)
            return str(dest)
        except (ReadTimeout, TransportError):
            logging.warning("Timeout saving image %s (attempt %s/%s)", fn, attempt, MAX_RETRIES)
        except HTTPStatusError as e:
            logging.error("HTTP %s saving image %s", e.response.status_code, fn)
            break
        time.sleep(RETRY_DELAY)
    logging.error("Failed saving image %s", fn)
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
    paras = [p for p in norm.split("\n\n") if p.strip()]
    if not preserve_formatting:
        paras = [re.sub(r'\n+', ' ', p) for p in paras]

    chunks, curr = [], ""
    def split_long(p: str) -> List[str]:
        parts, sub = [], ""
        for w in p.split(" "):
            if len(sub) + len(w) + 1 > size:
                parts.append(sub)
                sub = w
            else:
                sub = (sub + " " + w).lstrip()
        if sub:
            parts.append(sub)
        return parts

    for p in paras:
        if len(p) > size:
            if curr:
                chunks.append(curr)
                curr = ""
            chunks.extend(split_long(p))
        else:
            if not curr:
                curr = p
            elif len(curr) + 2 + len(p) <= size:
                curr += "\n\n" + p
            else:
                chunks.append(curr)
                curr = p

    if curr:
        chunks.append(curr)
    return chunks


def parse_and_save(post: Dict[str, Any], translate_to: str) -> Dict[str, Any]:
    aid, slug = post["id"], post["slug"]
    art_dir = OUTPUT_DIR / f"{aid}_{slug}"
    art_dir.mkdir(parents=True, exist_ok=True)

    meta_path = art_dir / "meta.json"
    old_meta = {}
    if meta_path.exists():
        old_meta = json.loads(meta_path.read_text(encoding="utf-8"))

    meta: Dict[str, Any] = {
        "id": aid,
        "slug": slug,
        "date": post.get("date"),
        "link": post.get("link"),
        "title": BeautifulSoup(post["title"]["rendered"], "html.parser")
                 .get_text(strip=True),
        "posted": old_meta.get("posted", False)
    }

    # extract paragraphs
    soup = BeautifulSoup(post["content"]["rendered"], "html.parser")
    paras = [p.get_text(strip=True) for p in soup.find_all("p")]
    raw = "\n\n".join(paras)
    hash_raw = hashlib.sha256(raw.encode("utf-8")).hexdigest()

    translated = False
    if translate_to:
        if old_meta.get("hash") == hash_raw and old_meta.get("translated_to") == translate_to:
            translated = True
            paras = json.loads(old_meta.get("translated_paras", "[]"))
            logging.info("Using cached translation %s for ID=%s", translate_to, aid)
        else:
            for attempt in range(1, MAX_RETRIES+1):
                try:
                    paras = [
                        GoogleTranslator(source="auto", target=translate_to).translate(p)
                        for p in paras
                    ]
                    translated = True
                    break
                except Exception as e:
                    logging.warning("Translate attempt %s failed: %s", attempt, e)
                    time.sleep(2)

    txt_orig = art_dir / "content.txt"
    txt_orig.write_text(raw, encoding="utf-8")
    meta["text_file"] = str(txt_orig)

    if translated:
        meta["translated_to"] = translate_to
        meta["hash"] = hash_raw
        meta["translated_paras"] = json.dumps(paras, ensure_ascii=False)
        out = art_dir / f"content.{translate_to}.txt"
        out.write_text("\n\n".join(paras), encoding="utf-8")
        meta["translated_file"] = str(out)
        meta["text_file"] = str(out)
    else:
        meta["translated_to"] = False
        meta["hash"] = hash_raw

    # download images
    img_dir = art_dir / "images"
    images = []
    for img in soup.find_all("img"):
        src = img.get("src")
        if not src:
            continue
        path = save_image(src, img_dir)
        if path:
            images.append(path)
    meta["images"] = images

    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2),
                         encoding="utf-8")
    return meta


def main(limit: Optional[int], translate_to: str):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    cid = fetch_category_id("national")
    posts = fetch_posts(cid)

    catalog: List[Dict[str, Any]] = []
    subset = posts if limit is None else posts[:limit]
    for post in subset:
        meta = parse_and_save(post, translate_to)
        catalog.append(meta)
        logging.info("Parsed and saved ID=%s", meta["id"])

    save_catalog(catalog)
    logging.info("Catalog saved: %s (%d articles)", CATALOG_PATH, len(catalog))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parser with translation")
    parser.add_argument("-n", "--limit", type=int, default=None,
                        help="Max number of posts to parse")
    parser.add_argument("-l", "--lang", type=str, default="",
                        help="Translate content to this language code (e.g. 'ru')")
    args = parser.parse_args()
    main(limit=args.limit, translate_to=args.lang)
