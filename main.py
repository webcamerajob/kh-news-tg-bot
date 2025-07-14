#!/usr/bin/env python3
# coding: utf-8

import sys
import os
import json
import logging
import argparse
import re
import subprocess

from pathlib import Path
from typing import Optional, List, Dict, Any, Set

import cloudscraper
import translators as ts
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# обход Cloudflare
scraper = cloudscraper.create_scraper()

def setup_vpn():
    """
    Поднимает WireGuard из полного конфига в WG_CONFIG.
    Если переменной нет – завершаемся, иначе API недоступно.
    """
    cfg = os.getenv("WG_CONFIG")
    if not cfg:
        logging.error("WG_CONFIG not provided → cannot reach site")
        sys.exit(1)

    conf = Path("/tmp/wg0.conf")
    conf.write_text(cfg, encoding="utf-8")
    try:
        subprocess.run(["sudo", "wg-quick", "up", str(conf)], check=True)
        logging.info("WireGuard tunnel is up")
    except Exception as e:
        logging.error("WireGuard setup failed: %s", e)
        sys.exit(1)

# старт VPN до любых HTTP
setup_vpn()

def load_posted_ids(state_file: Path) -> Set[int]:
    """
    Читает articles/posted.json и возвращает set уже опубл.
    Поддерживает: отсутствующий, пустой, [1,2], [{"id":1},...]
    """
    if not state_file.is_file():
        logging.info("State file %s not found, starting fresh", state_file)
        return set()

    raw = state_file.read_text("utf-8").strip()
    if not raw:
        return set()

    try:
        arr = json.loads(raw)
    except Exception:
        logging.warning("State file not JSON: %s", state_file)
        return set()

    if not isinstance(arr, list):
        logging.warning("State file is not a list: %s", state_file)
        return set()

    out: Set[int] = set()
    for item in arr:
        if isinstance(item, dict) and "id" in item:
            try:
                out.add(int(item["id"]))
            except:
                pass
        elif isinstance(item, (int, str)) and str(item).isdigit():
            out.add(int(item))
    return out

def fetch_category_id(base_url: str, slug: str) -> int:
    """
    По WP REST API получает ID категории по её slug.
    """
    url = f"{base_url}/wp-json/wp/v2/categories?slug={slug}"
    resp = scraper.get(url, timeout=10)
    resp.raise_for_status()
    cats = resp.json()
    if not isinstance(cats, list) or not cats:
        raise ValueError(f"No category for slug={slug}")
    return int(cats[0]["id"])

def fetch_posts(
    base_url: str,
    category_id: int,
    per_page: int
) -> List[Dict[str, Any]]:
    """
    Берёт последние per_page постов категории.
    """
    url = f"{base_url}/wp-json/wp/v2/posts"
    params = {
        "categories": category_id,
        "per_page": per_page,
        "orderby": "date",
        "order": "desc"
    }
    resp = scraper.get(url, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()

def slugify(text: str) -> str:
    """
    lowercase + убрать не-алфавитные + пробелы→дефис.
    """
    s = text.lower()
    s = re.sub(r"[^\w\s-]", "", s)
    return re.sub(r"\s+", "-", s).strip("-")

def translate_text(text: str, lang: str) -> str:
    """
    Перевод через Yandex. Пустой lang → исходник.
    """
    if not lang:
        return text
    try:
        return ts.translate_text(text, to_language=lang, provider="yandex")
    except Exception as e:
        logging.warning("Translation failed: %s", e)
        return text

def parse_and_save(
    post: Dict[str, Any],
    lang: str,
    base_url: str,
    out_dir: Path
) -> Optional[Dict[str, Any]]:
    """
    Парсит один пост:
      - чистит HTML, переводит текст
      - сохраняет content.<lang>.txt
      - скачивает <img> → images/1.ext…
      - fallback: featured_media
      - пишет meta.json с полями id, title, text_file, images
    """
    pid = post.get("id")
    title = post.get("title", {}).get("rendered", "").strip()
    html  = post.get("content", {}).get("rendered", "")

    if not title or not html:
        logging.warning("Skipping %s: empty title/content", pid)
        return None

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n").strip()
    text = translate_text(text, lang)

    # папка для статьи
    name = f"{pid}_{slugify(title)}"
    d = out_dir / name
    d.mkdir(parents=True, exist_ok=True)

    # текст
    suf = lang or "raw"
    tf = d / f"content.{suf}.txt"
    tf.write_text(text, "utf-8")

    # картинки
    images_dir = d / "images"
    images_dir.mkdir(exist_ok=True)
    images: List[str] = []

    for idx, img in enumerate(soup.find_all("img"), start=1):
        src = img.get("src") or ""
        try:
            r = scraper.get(src, timeout=10)
            r.raise_for_status()
            ext = Path(src).suffix or ".jpg"
            fn  = images_dir / f"{idx}{ext}"
            fn.write_bytes(r.content)
            images.append(str(Path("images") / fn.name))
        except Exception as e:
            logging.warning("Img download failed for %s: %s", src, e)

    # featured_media fallback
    if not images and post.get("featured_media"):
        mid = post["featured_media"]
        mu  = f"{base_url}/wp-json/wp/v2/media/{mid}"
        try:
            m = scraper.get(mu, timeout=10); m.raise_for_status()
            url = m.json().get("source_url")
            if url:
                r = scraper.get(url, timeout=10); r.raise_for_status()
                ext = Path(url).suffix or ".jpg"
                fn  = images_dir / f"1{ext}"
                fn.write_bytes(r.content)
                images.append(str(Path("images") / fn.name))
        except Exception as e:
            logging.warning("Featured media fetch failed: %s", e)

    meta = {
        "id":        pid,
        "title":     title,
        "text_file": tf.name,
        "images":    images
    }
    (d / "meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    logging.info("Parsed %s → %s", pid, d)
    return meta

def main():
    p = argparse.ArgumentParser("Smart parser w/ VPN & CF bypass")
    p.add_argument("--state-file", required=True, help="articles/posted.json")
    p.add_argument("--output-dir", required=True, help="parser_output")
    p.add_argument("--base-url",    default="https://www.khmertimeskh.com")
    p.add_argument("--slug",        default="national")
    p.add_argument("--lang",        default="ru")
    p.add_argument("--limit",   type=int, default=10)
    args = p.parse_args()

    posted = load_posted_ids(Path(args.state_file))
    logging.info("Already posted IDs: %s", sorted(posted))

    cid = fetch_category_id(args.base_url, args.slug)
    logging.info("Category %r → ID %s", args.slug, cid)

    posts = fetch_posts(args.base_url, cid, per_page=args.limit)
    ids   = [p.get("id") for p in posts]
    logging.info("Fetched %d posts: %s", len(posts), ids)

    out_dir = Path(args.output_dir); out_dir.mkdir(exist_ok=True, parents=True)
    new_count = 0
    for post in posts:
        if post.get("id") in posted:
            continue
        if parse_and_save(post, args.lang, args.base_url, out_dir):
            new_count += 1

    logging.info("Total new parsed: %d", new_count)
    # единственное, что мы печатаем в stdout — число, без лишних логов
    print(new_count)

if __name__ == "__main__":
    main()
