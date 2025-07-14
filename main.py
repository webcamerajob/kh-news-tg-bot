#!/usr/bin/env python3
# coding: utf-8

import os
import json
import logging
import argparse
from pathlib import Path
from typing import Optional, List, Dict, Any
from parser_utils import (
    fetch_category_id,
    fetch_posts,
    parse_and_save,
    load_posted_ids,
    save_catalog
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


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

        # 1) Загружаем ранее опубликованные ID
        posted_ids = load_posted_ids(Path(state_file))
        logging.info("Loaded %d posted IDs", len(posted_ids))

        # 2) Загружаем актуальный список постов
        cid = fetch_category_id(base_url, slug)
        posts = fetch_posts(base_url, cid, per_page=(limit or 10))

        # 3) Загружаем старый каталог
        catalog: List[Dict[str, Any]] = []
        catalog_path = Path("articles/catalog.json")
        if catalog_path.is_file():
            try:
                catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
            except Exception as e:
                logging.warning("Cannot load catalog: %s", e)

        # 4) Обрабатываем только новые статьи
        new_articles = 0
        for post in posts[: limit or len(posts)]:
            post_id = post["id"]
            if post_id in posted_ids:
                logging.debug(f"Skipping already posted ID={post_id}")
                continue

            meta = parse_and_save(post, lang, base_url)
            if meta:
                catalog.append(meta)
                new_articles += 1
                logging.info(f"Processed new article ID={post_id}")

        # 5) Сохраняем обновлённый каталог
        if new_articles > 0:
            catalog_path.write_text(
                json.dumps(catalog, ensure_ascii=False, indent=2),
                encoding="utf-8"
            )
            logging.info(f"Added {new_articles} new articles. Total: {len(catalog)}")
        else:
            logging.info("No new articles found")

        # 6) Выводим результат для GitHub Actions
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
