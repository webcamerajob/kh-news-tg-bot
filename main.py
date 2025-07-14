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

# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# Обход Cloudflare
scraper = cloudscraper.create_scraper()
# ──────────────────────────────────────────────────────────────────────────────

def setup_vpn():
    """
    Поднимает WireGuard из полной конфигурации в переменной WG_CONFIG.
    """
    config = os.getenv("WG_CONFIG")
    if not config:
        logging.info("WG_CONFIG not provided, skipping VPN setup")
        return

    conf_path = Path("/tmp/wg0.conf")
    conf_path.write_text(config, encoding="utf-8")

    try:
        subprocess.run(["wg-quick", "up", str(conf_path)], check=True)
        logging.info("WireGuard interface is up")
    except Exception as e:
        logging.error("Failed to start WireGuard: %s", e)
        sys.exit(1)

# инициализируем VPN
setup_vpn()


def load_posted_ids(state_file: Path) -> Set[int]:
    """
    Читает articles/posted.json и возвращает множество уже размещенных ID.
    """
    if not state_file.is_file():
        logging.info("State file %s not found, starting fresh", state_file)
        return set()

    text = state_file.read_text(encoding="utf-8").strip()
    if not text:
        return set()

    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        logging.warning("State file is not valid JSON: %s", state_file)
        return set()

    if not isinstance(data, list):
        logging.warning("State file isn’t a list: %s", state_file)
        return set()

    ids: Set[int] = set()
    for item in data:
        if isinstance(item, dict) and "id" in item:
            try:
                ids.add(int(item["id"]))
            except:
                pass
        elif isinstance(item, (int, str)) and str(item).isdigit():
            ids.add(int(item))
    return ids


def fetch_category_id(base_url: str, slug: str) -> int:
    """
    Запрашивает WP REST API и возвращает ID категории по slug.
    """
    url = f"{base_url}/wp-json/wp/v2/categories?slug={slug}"
    resp = scraper.get(url, timeout=10)
    resp.raise_for_status()
    cats = resp.json()
    if not isinstance(cats, list) or not cats:
        raise ValueError(f"No category for slug={slug}")
    return int(cats[0].get("id"))


def fetch_posts(
    base_url: str,
    category_id: int,
    per_page: int = 10
) -> List[Dict[str, Any]]:
    """
    Возвращает список постов из указанной категории.
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
    Простое slugify: lowercase + alnum + dashes.
    """
    s = text.lower()
    s = re.sub(r"[^\w\s-]", "", s)
    return re.sub(r"\s+", "-", s).strip("-")


def translate_text(text: str, lang: str) -> str:
    """
    Переводит текст через Yandex. Если lang пуст, возвращает исходный текст.
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
    output_dir: Path
) -> Optional[Dict[str, Any]]:
    """
    Парсит один пост:
      - очищает и переводит текст;
      - сохраняет его в content.<lang>.txt;
      - скачивает <img> в images/;
      - fallback: featured_media;
      - записывает meta.json с локальными путями.
    """
    post_id = post.get("id")
    title_raw = post.get("title", {}).get("rendered", "").strip()
    content_html = post.get("content", {}).get("rendered", "")

    if not title_raw or not content_html:
        logging.warning("Empty title/content for post %s", post_id)
        return None

    soup = BeautifulSoup(content_html, "html.parser")
    text = soup.get_text(separator="\n").strip()
    text = translate_text(text, lang)

    dirname = f"{post_id}_{slugify(title_raw)}"
    article_dir = output_dir / dirname
    article_dir.mkdir(parents=True, exist_ok=True)

    suffix = lang or "raw"
    text_file = article_dir / f"content.{suffix}.txt"
    text_file.write_text(text, encoding="utf-8")

    images_dir = article_dir / "images"
    images_dir.mkdir(exist_ok=True)
    images: List[str] = []

    for idx, img_tag in enumerate(soup.find_all("img"), start=1):
        src = img_tag.get("src")
        if not src:
            continue
        try:
            r = scraper.get(src, timeout=10)
            r.raise_for_status()
            ext = Path(src).suffix or ".jpg"
            img_path = images_dir / f"{idx}{ext}"
            img_path.write_bytes(r.content)
            # путь относительный к article_dir
            images.append(str(Path("images") / img_path.name))
        except Exception as e:
            logging.warning("Failed to download image %s: %s", src, e)

    if not images and post.get("featured_media"):
        mid = post["featured_media"]
        m_url = f"{base_url}/wp-json/wp/v2/media/{mid}"
        try:
            m = scraper.get(m_url, timeout=10)
            m.raise_for_status()
            src = m.json().get("source_url")
            if src:
                r = scraper.get(src, timeout=10)
                r.raise_for_status()
                ext = Path(src).suffix or ".jpg"
                img_path = images_dir / f"1{ext}"
                img_path.write_bytes(r.content)
                images.append(str(Path("images") / img_path.name))
        except Exception as e:
            logging.warning("Failed featured image for %s: %s", post_id, e)

    meta = {
        "id":        post_id,
        "title":     title_raw,
        "text_file": text_file.name,
        "images":    images
    }
    (article_dir / "meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    logging.info("Parsed post %s → %s", post_id, article_dir)
    return meta


def main(
    state_file: str,
    output_dir: str,
    base_url: str,
    slug: str,
    lang: str,
    limit: Optional[int]
):
    try:
        out_dir = Path(output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        posted = load_posted_ids(Path(state_file))
        logging.info("Loaded %d posted IDs", len(posted))

        cid   = fetch_category_id(base_url, slug)
        posts = fetch_posts(base_url, cid, per_page=(limit or 10))

        new_count = 0
        for post in posts[: limit or len(posts)]:
            pid = post.get("id")
            if pid in posted:
                logging.debug("Skip posted %s", pid)
                continue
            if parse_and_save(post, lang, base_url, out_dir):
                new_count += 1

        if new_count:
            logging.info("Parsed %d new articles", new_count)
        else:
            logging.info("No new articles to parse")

        # GitHub Actions
        print(f"::set-output name=new_count::{new_count}")

    except Exception:
        logging.exception("Fatal error in parser")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Parser with VPN, CF-bypass & translate")
    parser.add_argument("--state-file", type=str, required=True,
        help="path to articles/posted.json")
    parser.add_argument("--output-dir", type=str, required=True,
        help="where to write parsed/{id}")
    parser.add_argument("--base-url", type=str,
        default="https://www.khmertimeskh.com",
        help="WP site base URL")
    parser.add_argument("--slug", type=str, default="national",
        help="category slug")
    parser.add_argument("--lang", type=str, default="ru",
        help="language for translation")
    parser.add_argument("--limit", type=int, default=None,
        help="max number of posts to fetch")
    args = parser.parse_args()

    main(
        state_file=args.state_file,
        output_dir=args.output_dir,
        base_url=args.base_url,
        slug=args.slug,
        lang=args.lang,
        limit=args.limit
    )
