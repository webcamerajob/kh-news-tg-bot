#!/usr/bin/env python3
import os
import json
import argparse
import logging
import re
import hashlib
import time
import fcntl
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import cloudscraper
from requests.exceptions import ReadTimeout as ReqTimeout, RequestException
from deep_translator import GoogleTranslator
from bs4 import BeautifulSoup

# Конфигурация
MAX_TRANSLATE_LENGTH = 5000  # Максимальная длина текста для перевода за один запрос
bad_patterns = [
    r"synopsis\s*:\s*",
    r"\(video inside\)",
    r"\(VIDEO\)",
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

def extract_img_url(img_tag) -> Optional[str]:
    """Извлекает URL изображения из HTML-тега"""
    for attr in ("data-src", "data-lazy-src", "data-srcset", "srcset", "src"):
        if val := img_tag.get(attr):
            if parts := val.split():
                return parts[0]
    return None

def save_image(src_url: str, folder: Path) -> Optional[str]:
    """Сохраняет изображение на диск"""
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
            logging.warning("Timeout saving image %s (try %s/%s): %s; retry in %.1fs",
                          fn, attempt, MAX_RETRIES, e, delay)
            time.sleep(delay)
    
    logging.error("Failed saving image %s after %s attempts", fn, MAX_RETRIES)
    return None

def fetch_category_id(base_url: str, slug: str) -> int:
    """Получает ID категории по её slug"""
    endpoint = f"{base_url}/wp-json/wp/v2/categories?slug={slug}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = SCRAPER.get(endpoint, timeout=SCRAPER_TIMEOUT)
            r.raise_for_status()
            if data := r.json():
                return data[0]["id"]
            raise RuntimeError(f"Category '{slug}' not found")
        except (ReqTimeout, RequestException) as e:
            delay = BASE_DELAY * 2 ** (attempt - 1)
            logging.warning("Timeout fetching category (try %s/%s): %s; retry in %.1fs",
                          attempt, MAX_RETRIES, e, delay)
            time.sleep(delay)
    raise RuntimeError("Failed fetching category id")

def fetch_posts(base_url: str, cat_id: int, per_page: int = 10) -> List[Dict[str, Any]]:
    """Получает список постов из категории"""
    endpoint = f"{base_url}/wp-json/wp/v2/posts?categories={cat_id}&per_page={per_page}&_embed"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = SCRAPER.get(endpoint, timeout=SCRAPER_TIMEOUT)
            r.raise_for_status()
            return r.json()
        except (ReqTimeout, RequestException) as e:
            delay = BASE_DELAY * 2 ** (attempt - 1)
            logging.warning("Timeout fetching posts (try %s/%s): %s; retry in %.1fs",
                          attempt, MAX_RETRIES, e, delay)
            time.sleep(delay)
    return []

def load_catalog() -> List[Dict[str, Any]]:
    """Загружает каталог статей с блокировкой файла"""
    if not CATALOG_PATH.exists():
        return []
    
    try:
        with open(CATALOG_PATH, "r", encoding="utf-8") as f:
            fcntl.flock(f, fcntl.LOCK_SH)
            try:
                return [item for item in json.load(f) if isinstance(item, dict) and "id" in item]
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)
    except (json.JSONDecodeError, IOError) as e:
        logging.error("Catalog error: %s", e)
        return []

def save_catalog(catalog: List[Dict[str, Any]]) -> None:
    """Сохраняет каталог статей с блокировкой файла"""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    try:
        with open(CATALOG_PATH, "w", encoding="utf-8") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            try:
                json.dump(catalog, f, ensure_ascii=False, indent=2)
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)
    except IOError as e:
        logging.error("Failed to save catalog: %s", e)

def split_for_translation(text: str, max_length: int = MAX_TRANSLATE_LENGTH) -> List[str]:
    """Разбивает текст на части для перевода с сохранением целостности абзацев"""
    paragraphs = [p for p in text.split('\n\n') if p.strip()]
    chunks = []
    current_chunk = ""
    
    for para in paragraphs:
        if len(current_chunk) + len(para) + 2 > max_length:
            if current_chunk:
                chunks.append(current_chunk)
                current_chunk = ""
            if len(para) > max_length:
                # Если абзац слишком длинный, разбиваем по предложениям
                sentences = re.split(r'(?<=[.!?])\s+', para)
                current_sentence = ""
                for sent in sentences:
                    if len(current_sentence) + len(sent) + 1 > max_length:
                        if current_sentence:
                            chunks.append(current_sentence)
                            current_sentence = ""
                        if len(sent) > max_length:
                            # Если предложение слишком длинное, разбиваем по словам
                            words = sent.split()
                            current_words = ""
                            for word in words:
                                if len(current_words) + len(word) + 1 > max_length:
                                    if current_words:
                                        chunks.append(current_words)
                                        current_words = ""
                                current_words = f"{current_words} {word}".strip()
                            if current_words:
                                chunks.append(current_words)
                        else:
                            chunks.append(sent)
                    else:
                        current_sentence = f"{current_sentence} {sent}".strip()
                if current_sentence:
                    chunks.append(current_sentence)
            else:
                chunks.append(para)
        else:
            current_chunk = f"{current_chunk}\n\n{para}".strip()
    
    if current_chunk:
        chunks.append(current_chunk)
    
    return chunks

def translate_text(text: str, target_lang: str) -> Optional[str]:
    """Переводит текст с учетом максимальной длины"""
    if not text.strip():
        return text
    
    chunks = split_for_translation(text)
    translated_chunks = []
    
    for chunk in chunks:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                translated = GoogleTranslator(source="auto", target=target_lang).translate(chunk)
                translated_chunks.append(translated)
                break
            except Exception as e:
                if attempt == MAX_RETRIES:
                    logging.error("Failed to translate chunk: %s", e)
                    return None
                delay = BASE_DELAY * 2 ** (attempt - 1)
                logging.warning("Translation attempt %s failed: %s; retry in %.1fs",
                              attempt, e, delay)
                time.sleep(delay)
    
    return "\n\n".join(translated_chunks)

def process_article_content(content: str, img_dir: Path) -> Tuple[str, List[str]]:
    """Обрабатывает контент статьи: извлекает текст и изображения"""
    soup = BeautifulSoup(content, "html.parser")
    
    # Обработка текста
    paras = [p.get_text(strip=True) for p in soup.find_all("p")]
    raw_text = "\n\n".join(paras)
    raw_text = bad_re.sub("", raw_text)
    raw_text = re.sub(r"[ \t]+", " ", raw_text)
    raw_text = re.sub(r"\n{3,}", "\n\n", raw_text)
    
    # Обработка изображений
    images = []
    srcs = [extract_img_url(img) for img in soup.find_all("img")]
    srcs = [url for url in srcs if url]
    
    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = {ex.submit(save_image, url, img_dir): url for url in srcs}
        for fut in as_completed(futures):
            if path := fut.result():
                images.append(path)
    
    return raw_text, images

def parse_and_save(post: Dict[str, Any], translate_to: str, base_url: str) -> Optional[Dict[str, Any]]:
    """CHANGED: Добавлена проверка hash существующего контента"""
    aid, slug = post["id"], post["slug"]
    art_dir = OUTPUT_DIR / f"{aid}_{slug}"
    art_dir.mkdir(parents=True, exist_ok=True)

    # Проверяем существующую статью
    meta_path = art_dir / "meta.json"
    if meta_path.exists():
        try:
            existing_meta = json.loads(meta_path.read_text(encoding="utf-8"))
            current_hash = hashlib.sha256(post["content"]["rendered"].encode()).hexdigest()
            if existing_meta.get("hash") == current_hash and existing_meta.get("translated_to", "") == translate_to:
                logging.info("Skipping unchanged article ID=%d", aid)
                return existing_meta
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logging.warning("Failed to read existing meta for ID=%d: %s", aid, str(e))

    # Извлекаем заголовок из h2.entry-title
    soup = BeautifulSoup(post["content"]["rendered"], "html.parser")
    title_tag = soup.find("h2", class_="entry-title")
    
    if not title_tag:
        # Fallback 1: попробуем найти любой h2
        title_tag = soup.find("h2")
        if not title_tag:
            # Fallback 2: используем заголовок из API как резервный вариант
            title_tag = BeautifulSoup(post["title"]["rendered"], "html.parser")
    
    orig_title = title_tag.get_text(strip=True) if title_tag else "No Title Found"
    title = orig_title

    if translate_to:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                title = GoogleTranslator(source="auto", target=translate_to).translate(orig_title)
                break
            except Exception as e:
                delay = BASE_DELAY * 2 ** (attempt - 1)
                logging.warning(
                    "Translate title attempt %d/%d failed: %s; retry in %.1fs",
                    attempt, MAX_RETRIES, str(e), delay
                )
                time.sleep(delay)

    # Остальная часть функции
    paras = [p.get_text(strip=True) for p in soup.find_all("p")]
    raw_text = "\n\n".join(paras)
    raw_text = bad_re.sub("", raw_text)
    raw_text = re.sub(r"[ \t]+", " ", raw_text)
    raw_text = re.sub(r"\n{3,}", "\n\n", raw_text)

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

    if not images and "_embedded" in post:
        media = post["_embedded"].get("wp:featuredmedia", [{}])
        if media and media[0].get("source_url"):
            path = save_image(media[0]["source_url"], img_dir)
            if path:
                images.append(path)

    if not images:
        logging.warning("No images for ID=%d; skipping", aid)
        return None

    meta = {
        "id": aid,
        "slug": slug,
        "date": post.get("date"),
        "link": post.get("link"),
        "title": title,
        "text_file": str(art_dir / "content.txt"),
        "images": images,
        "posted": False,
        "hash": hashlib.sha256(raw_text.encode()).hexdigest()
    }
    
    (art_dir / "content.txt").write_text(raw_text, encoding="utf-8")

    if translate_to:
        h = meta["hash"]
        old = {}
        if meta_path.exists():
            try:
                old = json.loads(meta_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                pass

        if old.get("hash") != h or old.get("translated_to") != translate_to:
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    clean_paras = [bad_re.sub("", p) for p in paras]
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
                    logging.warning("Translate try %d/%d failed: %s; retry in %.1fs",
                                  attempt, MAX_RETRIES, str(e), delay)
                    time.sleep(delay)
        else:
            logging.info("Using cached translation %s for ID=%d", translate_to, aid)

    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    return meta

def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(description="Article parser with translation")
    parser.add_argument("--base-url", type=str, default="https://www.khmertimeskh.com")
    parser.add_argument("--slug", type=str, default="national")
    parser.add_argument("-n", "--limit", type=int, default=None)
    parser.add_argument("-l", "--lang", type=str, default="")
    args = parser.parse_args()

    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        cid = fetch_category_id(args.base_url, args.slug)
        posts = fetch_posts(args.base_url, cid, per_page=(args.limit or 10))

        catalog = load_catalog()
        existing_ids = {article["id"] for article in catalog}
        new_articles = 0

        for post in posts[:args.limit or len(posts)]:
            if post["id"] in existing_ids:
                continue

            if meta := parse_and_save(post, args.lang, args.base_url):
                catalog.append(meta)
                existing_ids.add(post["id"])
                new_articles += 1
                logging.info(f"Processed new article ID={post['id']}")

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
