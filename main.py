#!/usr/bin/env python3
# coding: utf-8

import os
import json
import logging
import argparse
from pathlib import Path
from typing import Optional, List, Dict, Any
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def load_posted_ids(state_path: Path) -> set[int]:
    if not state_path.is_file():
        logging.info("State file %s not found, starting fresh", state_path)
        return set()
    try:
        arr = json.loads(state_path.read_text(encoding="utf-8"))
        return {int(x) for x in arr}
    except Exception as e:
        logging.warning("Cannot read state-file %s: %s", state_path, e)
        return set()


def fetch_category_id(base_url: str, slug: str) -> int:
    url = f"{base_url}/wp-json/wp/v2/categories?slug={slug}"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    cats = resp.json()
    if not cats:
        raise ValueError(f"No category found for slug={slug}")
    return cats[0]["id"]


def fetch_posts(base_url: str, category_id: int, per_page: int = 10) -> List[Dict[str, Any]]:
    url = f"{base_url}/wp-json/wp/v2/posts"
    params = {
        "categories": category_id,
        "per_page": per_page,
        "orderby": "date",
        "order": "desc"
    }
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


def parse_and_save(post: Dict[str, Any], lang: str, base_url: str) -> Optional[Dict[str, Any]]:
    article_id = post["id"]
    title = post.get("title", {}).get("rendered", "").strip()
    if not title:
        return None

    # Простейшее преобразование — берём основной текст как content.txt
    content_html = post.get("content", {}).get("rendered", "")
    if not content_html:
        return None

    # Очистка от тегов — можно усложнить при необходимости
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(content_html, "html.parser")
    text = soup.get_text(separator="\n")

    dirname = f"{article_id}_{slugify(title)}"
    article_dir = Path(output_dir) / dirname
    article_dir.mkdir(parents=True, exist_ok=True)

    # Сохраняем текст
    text_file = article_dir / f"content.{lang or 'raw'}.txt"
    text_file.write_text(text.strip(), encoding="utf-8")

    # Заглушка: создаём папку images (можно парсить <img>)
    images_dir = article_dir / "images"
    images_dir.mkdir(exist_ok=True)

    # meta.json
    meta = {
        "id": article_id,
        "title": title,
        "text_file": text_file.name,
        "images": []
    }
    (article_dir / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    return meta


def slugify(text: str) -> str:
    import re
    text = text.lower()
    text = re.sub(r"[^\w\s-]", "", text)
    return re.sub(r"\s+", "-", text).strip("-")


def main(
    state_file: str,
    output_dir: str,
    base_url: str,
    slug: str,
    lang: str,
    limit: Optional[int]
):
    try:
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        posted_ids = load_posted_ids(Path(state_file))
        logging.info("Loaded %d posted IDs", len(posted_ids))

        cid = fetch_category_id(base_url, slug)
        posts = fetch_posts(base_url, cid, per_page=(limit or 10))

        catalog: List[Dict[str, Any]] = []
        catalog_path = Path("articles/catalog.json")
        if catalog_path.is_file():
            try:
                catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
            except Exception as e:
                logging.warning("Cannot load catalog: %s", e)

        new_articles = 0
        for post in posts[:limit or len(posts)]:
            post_id = post["id"]
            if post_id in posted_ids:
                logging.debug(f"Skipping already posted ID={post_id}")
                continue

            meta = parse_and_save(post, lang, base_url)
            if meta:
                catalog.append(meta)
                new_articles += 1
                logging.info(f"Processed new article ID={post_id}")

        if new_articles > 0:
            catalog_path.write_text(json.dumps(catalog, ensure_ascii=False, indent=2), encoding="utf-8")
            logging.info(f"Added {new_articles} new articles. Total: {len(catalog)}")
        else:
            logging.info("No new articles found")

        print(f"::set-output name=new_count::{new_articles}")

    except Exception as e:
        logging.exception("Fatal error in main:")
        exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Smart parser with dedupe")
    parser.add_argument("--state-file",  type=str, required=True, help="path to articles/posted.json")
    parser.add_argument("--output-dir",  type=str, required=True, help="where to write parsed/{id}")
    parser.add_argument("--base-url",    type=str, default="https://www.khmertimeskh.com", help="API base URL")
    parser.add_argument("--slug",        type=str, default="national", help="Category slug")
    parser.add_argument("--lang",        type=str, default="", help="Target language code")
    parser.add_argument("--limit",       type=int, default=None, help="Max number of articles to process")
    args = parser.parse_args()

    main(
        state_file=args.state_file,
        output_dir=args.output_dir,
        base_url=args.base_url,
        slug=args.slug,
        lang=args.lang,
        limit=args.limit
    )
