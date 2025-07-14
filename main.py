#!/usr/bin/env python3
# coding: utf-8

import os
import sys
import json
import logging
import argparse
import re

from pathlib import Path
from typing import Optional, List, Dict, Any, Set

import cloudscraper
import translators as ts
from bs4 import BeautifulSoup
from PIL import Image

# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# Cloudflare bypass
scraper = cloudscraper.create_scraper()

# Путь к водяной бумажке в корне репозитория
WATERMARK_PATH = Path(__file__).parent / "watermark.png"
# ──────────────────────────────────────────────────────────────────────────────

def load_posted_ids(state_path: Path) -> Set[int]:
    """
    Читает state-файл posted.json и возвращает множество уже опубликованных ID.
    """
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
    """
    Возвращает ID категории по slug через WP REST API.
    """
    url = f"{base_url}/wp-json/wp/v2/categories?slug={slug}"
    resp = scraper.get(url, timeout=10)
    resp.raise_for_status()
    cats = resp.json()
    if not cats:
        raise ValueError(f"No category found for slug={slug}")
    return cats[0]["id"]


def fetch_posts(
    base_url: str,
    category_id: int,
    per_page: int = 10
) -> List[Dict[str, Any]]:
    """
    Возвращает список постов из категории category_id.
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
    Простейший slugify: оставляет буквы, цифры, дефисы.
    """
    text = text.lower()
    text = re.sub(r"[^\w\s-]", "", text)
    return re.sub(r"\s+", "-", text).strip("-")


def translate_text(text: str, lang: str) -> str:
    """
    Переводит text на lang через провайдера Yandex. Если пустой lang — возвращает исходник.
    """
    if not lang:
        return text
    try:
        return ts.translate_text(text, to_language=lang, provider="yandex")
    except Exception as e:
        logging.warning("Translation failed: %s", e)
        return text


def apply_watermark(image_path: Path):
    """
    Накладывает картинку watermark.png из корня на image_path.
    """
    if not WATERMARK_PATH.is_file():
        logging.warning("Watermark file %s not found", WATERMARK_PATH)
        return

    try:
        base = Image.open(image_path).convert("RGBA")
        mark = Image.open(WATERMARK_PATH).convert("RGBA")

        # Масштабируем watermark до 30% ширины
        scale = base.width * 0.3 / mark.width
        w, h = int(mark.width * scale), int(mark.height * scale)
        mark = mark.resize((w, h), Image.ANTIALIAS)

        # Позиция: правый нижний угол с отступом 10px
        pos = (base.width - w - 10, base.height - h - 10)

        # Накладываем
        transparent = Image.new("RGBA", base.size)
        transparent.paste(mark, pos, mask=mark)
        watermarked = Image.alpha_composite(base, transparent)
        # Сохраняем обратно
        watermarked.convert("RGB").save(image_path)
    except Exception as e:
        logging.warning("Watermark failed for %s: %s", image_path, e)


def parse_and_save(
    post: Dict[str, Any],
    lang: str,
    base_url: str,
    output_dir: Path
) -> Optional[Dict[str, Any]]:
    """
    Парсит один пост:
      - очищает HTML-контент
      - переводит на lang
      - сохраняет текст и картинки с watermark
      - возвращает meta для catalog.json
    """
    post_id = post.get("id")
    title_raw = post.get("title", {}).get("rendered", "").strip()
    if not title_raw:
        logging.warning("Empty title for post %s", post_id)
        return None

    # Содержимое
    content_html = post.get("content", {}).get("rendered", "")
    if not content_html:
        logging.warning("Empty content for post %s", post_id)
        return None

    # Вычищаем HTML
    soup = BeautifulSoup(content_html, "html.parser")
    text = soup.get_text(separator="\n").strip()
    text = translate_text(text, lang)

    # Создаём папку статьи
    dirname = f"{post_id}_{slugify(title_raw)}"
    article_dir = output_dir / dirname
    article_dir.mkdir(parents=True, exist_ok=True)

    # Сохраняем текст
    lang_suffix = lang or "raw"
    text_file = article_dir / f"content.{lang_suffix}.txt"
    text_file.write_text(text, encoding="utf-8")

    # Скачиваем картинки из <img>
    images = []
    for idx, img_tag in enumerate(soup.find_all("img"), start=1):
        src = img_tag.get("src")
        if not src:
            continue
        try:
            resp = scraper.get(src, timeout=10)
            resp.raise_for_status()
            ext = Path(src).suffix or ".jpg"
            img_path = article_dir / f"{idx}{ext}"
            img_path.write_bytes(resp.content)
            apply_watermark(img_path)
            images.append(str(Path("images") / img_path.name))
        except Exception as e:
            logging.warning("Failed to fetch image %s: %s", src, e)

    # Если нет картинок внутри контента, пробуем featured_media
    if not images and post.get("featured_media"):
        # GET /media/{id}
        mid = post["featured_media"]
        url = f"{base_url}/wp-json/wp/v2/media/{mid}"
        try:
            m = scraper.get(url, timeout=10)
            m.raise_for_status()
            src = m.json().get("source_url")
            if src:
                resp = scraper.get(src, timeout=10)
                resp.raise_for_status()
                ext = Path(src).suffix or ".jpg"
                img_dir = article_dir / "images"
                img_dir.mkdir(exist_ok=True)
                img_path = img_dir / f"1{ext}"
                img_path.write_bytes(resp.content)
                apply_watermark(img_path)
                images.append(str(Path("images") / img_path.name))
        except Exception as e:
            logging.warning("Failed to fetch featured image: %s", e)

    # Список относительных путей для meta.json
    images_rel = images

    # Сохранение meta.json
    meta = {
        "id": post_id,
        "title": title_raw,
        "text_file": text_file.name,
        "images": images_rel
    }
    (article_dir / "meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    logging.info("Saved post %s → %s", post_id, article_dir)
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

        # 1) Фильтруем уже опубликованные по posted.json
        posted_ids = load_posted_ids(Path(state_file))
        logging.info("Loaded %d posted IDs", len(posted_ids))

        # 2) Получаем посты
        cid   = fetch_category_id(base_url, slug)
        posts = fetch_posts(base_url, cid, per_page=(limit or 10))

        # 3) Загружаем существующий catalog.json, если есть
        catalog_path = Path("articles/catalog.json")
        catalog: List[Dict[str, Any]] = []
        if catalog_path.is_file():
            try:
                catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
            except Exception as e:
                logging.warning("Cannot load catalog.json: %s", e)

        # 4) Парсим только новые
        new_count = 0
        for post in posts[: limit or len(posts)]:
            pid = post.get("id")
            if pid in posted_ids:
                logging.debug("Skip already posted %s", pid)
                continue
            if meta := parse_and_save(post, lang, base_url, out_dir):
                catalog.append(meta)
                new_count += 1

        # 5) Сохраняем catalog.json
        if new_count > 0:
            catalog_path.write_text(
                json.dumps(catalog, ensure_ascii=False, indent=2),
                encoding="utf-8"
            )
            logging.info("Added %d new articles (total %d)", new_count, len(catalog))
        else:
            logging.info("No new articles to parse")

        # 6) Вывод для GH Actions
        print(f"::set-output name=new_count::{new_count}")

    except Exception:
        logging.exception("Fatal error in main:")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Smart parser with watermark, VPN & translate")
    parser.add_argument(
        "--state-file", type=str, required=True,
        help="path to articles/posted.json"
    )
    parser.add_argument(
        "--output-dir", type=str, required=True,
        help="where to write parsed/{id}"
    )
    parser.add_argument(
        "--base-url", type=str,
        default="https://www.khmertimeskh.com",
        help="WP site base URL"
    )
    parser.add_argument(
        "--slug", type=str, default="national",
        help="Category slug"
    )
    parser.add_argument(
        "--lang", type=str, default="ru",
        help="language code for translation"
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="max number of posts to fetch"
    )
    args = parser.parse_args()

    main(
        state_file=args.state_file,
        output_dir=args.output_dir,
        base_url=args.base_url,
        slug=args.slug,
        lang=args.lang,
        limit=args.limit
    )
