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

# Cloudflare-bypass
scraper = cloudscraper.create_scraper()

def setup_vpn():
    """
    Поднимает WireGuard из полного конфига в WG_CONFIG.
    Если WG_CONFIG не задан — выходим, иначе API будет недоступно.
    """
    cfg = os.getenv("WG_CONFIG")
    if not cfg:
        logging.error("WG_CONFIG not provided → cannot reach site")
        sys.exit(1)

    conf_path = Path("/tmp/wg0.conf")
    conf_path.write_text(cfg, encoding="utf-8")

    try:
        subprocess.run(["sudo", "wg-quick", "up", str(conf_path)], check=True)
        logging.info("WireGuard is up")
    except Exception as e:
        logging.error("WireGuard setup failed: %s", e)
        sys.exit(1)

# запускаем VPN до любых HTTP-запросов
setup_vpn()

def load_posted_ids(state_file: Path) -> Set[int]:
    """
    Читает articles/posted.json → множество уже опубликованных ID.
    Поддерживает пустой/отсутствующий файл, список чисел [1,2,3] или [{"id":1},...].
    """
    if not state_file.is_file():
        logging.info("State file %s not found, starting fresh", state_file)
        return set()

    raw = state_file.read_text(encoding="utf-8").strip()
    if not raw:
        return set()

    try:
        arr = json.loads(raw)
    except json.JSONDecodeError:
        logging.warning("State file not valid JSON: %s", state_file)
        return set()

    if not isinstance(arr, list):
        logging.warning("State file is not a list: %s", state_file)
        return set()

    ids: Set[int] = set()
    for item in arr:
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
    Возвращает ID категории через WP REST API по её slug.
    """
    url = f"{base_url}/wp-json/wp/v2/categories?slug={slug}"
    resp = scraper.get(url, timeout=10)
    resp.raise_for_status()
    cats = resp.json()
    if not isinstance(cats, list) or not cats:
        raise ValueError(f"No category found for slug={slug}")
    return int(cats[0].get("id"))

def fetch_posts(
    base_url: str,
    category_id: int,
    per_page: int = 10
) -> List[Dict[str, Any]]:
    """
    Возвращает список последних постов.
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
    Простейший slugify: lowercase, удаление не-алфавитных, пробелы→дефисы.
    """
    s = text.lower()
    s = re.sub(r"[^\w\s-]", "", s)
    return re.sub(r"\s+", "-", s).strip("-")

def translate_text(text: str, lang: str) -> str:
    """
    Перевод через Yandex. Если lang пустой — возвращаем оригинал.
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
    Для одного поста:
      1) очистка HTML, перевод текста → content.<lang>.txt  
      2) скачивание <img> → images/1.jpg,2.png…  
      3) fallback на featured_media  
      4) запись meta.json с {"id","title","text_file","images"}  
    """
    pid = post.get("id")
    title_raw = post.get("title", {}).get("rendered", "").strip()
    html      = post.get("content", {}).get("rendered", "")

    if not title_raw or not html:
        logging.warning("Empty title/content for %s", pid)
        return None

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n").strip()
    text = translate_text(text, lang)

    # папка статьи
    folder = f"{pid}_{slugify(title_raw)}"
    d = output_dir / folder
    d.mkdir(parents=True, exist_ok=True)

    # сохраняем текст
    suf = lang or "raw"
    tf = d / f"content.{suf}.txt"
    tf.write_text(text, encoding="utf-8")

    # скачиваем картинки
    imgdir = d / "images"
    imgdir.mkdir(exist_ok=True)
    imgs: List[str] = []

    for i, tag in enumerate(soup.find_all("img"), start=1):
        src = tag.get("src") or ""
        try:
            r = scraper.get(src, timeout=10)
            r.raise_for_status()
            ext = Path(src).suffix or ".jpg"
            p = imgdir / f"{i}{ext}"
            p.write_bytes(r.content)
            imgs.append(str(Path("images") / p.name))
        except Exception as e:
            logging.warning("Img download failed: %s", e)

    # featured_media fallback
    if not imgs and post.get("featured_media"):
        mid = post["featured_media"]
        mu = f"{base_url}/wp-json/wp/v2/media/{mid}"
        try:
            m = scraper.get(mu, timeout=10)
            m.raise_for_status()
            url = m.json().get("source_url")
            if url:
                r = scraper.get(url, timeout=10)
                r.raise_for_status()
                ext = Path(url).suffix or ".jpg"
                p = imgdir / f"1{ext}"
                p.write_bytes(r.content)
                imgs.append(str(Path("images") / p.name))
        except Exception as e:
            logging.warning("Featured media failed: %s", e)

    meta = {
        "id":        pid,
        "title":     title_raw,
        "text_file": tf.name,
        "images":    imgs
    }
    (d / "meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    logging.info("Parsed %s → %s", pid, d)
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
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)

        posted = load_posted_ids(Path(state_file))
        logging.info("Loaded %d posted IDs", len(posted))

        cid  = fetch_category_id(base_url, slug)
        pts  = fetch_posts(base_url, cid, per_page=(limit or 10))

        nc = 0
        for post in pts[: limit or len(pts)]:
            pid = post.get("id")
            if pid in posted:
                continue
            if parse_and_save(post, lang, base_url, out):
                nc += 1

        logging.info("Parsed %d new articles", nc)
        # выводим ровно число — poster-job подхватит через GITHUB_OUTPUT
        print(nc)

    except Exception:
        logging.exception("Fatal error in parser")
        sys.exit(1)


if __name__ == "__main__":
    p = argparse.ArgumentParser("Parser w/ VPN, CF-bypass & translate")
    p.add_argument("--state-file", required=True, help="articles/posted.json")
    p.add_argument("--output-dir", required=True, help="where to write parsed/*")
    p.add_argument("--base-url",    default="https://www.khmertimeskh.com")
    p.add_argument("--slug",        default="national")
    p.add_argument("--lang",        default="ru")
    p.add_argument("--limit", type=int, default=None)
    args = p.parse_args()

    main(
        state_file=args.state_file,
        output_dir=args.output_dir,
        base_url=args.base_url,
        slug=args.slug,
        lang=args.lang,
        limit=args.limit
    )
