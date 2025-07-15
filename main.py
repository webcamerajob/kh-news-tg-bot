#!/usr/bin/env python3
import os
import json
import argparse
import logging
import re
import hashlib
import time
import fcntl

from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from concurrent.futures import ThreadPoolExecutor, as_completed

import cloudscraper
from requests.exceptions import ReadTimeout as ReqTimeout, RequestException
from bs4 import BeautifulSoup
import translators as ts  # pip install translators

# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
# ──────────────────────────────────────────────────────────────────────────────

SCRAPER = cloudscraper.create_scraper()
SCRAPER_TIMEOUT = (10.0, 60.0)
MAX_RETRIES = 3
BASE_DELAY = 2.0

OUTPUT_DIR = Path("articles")
CATALOG_PATH = OUTPUT_DIR / "catalog.json"

# Фразы, которые нужно вырезать из текста
bad_patterns = [
    r"synopsis\s*:\s*",
    r"\(video inside\)",
    r"\bkhmer times\b"
]
bad_re = re.compile("|".join(bad_patterns), flags=re.IGNORECASE)


def extract_img_url(img_tag) -> Optional[str]:
    """Извлекает первую подходящую ссылку из атрибутов тега <img>."""
    for attr in ("data-src", "data-lazy-src", "data-srcset", "srcset", "src"):
        val = img_tag.get(attr)
        if not val:
            continue
        return val.split()[0]
    return None


def fetch_category_id(base_url: str, slug: str) -> int:
    """Возвращает ID категории по её slug из WP REST API."""
    endpoint = f"{base_url.rstrip('/')}/wp-json/wp/v2/categories?slug={slug}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = SCRAPER.get(endpoint, timeout=SCRAPER_TIMEOUT)
            r.raise_for_status()
            data = r.json()
            if not data:
                raise RuntimeError(f"Category '{slug}' not found")
            return int(data[0]["id"])
        except (ReqTimeout, RequestException) as e:
            delay = BASE_DELAY * 2 ** (attempt - 1)
            logging.warning("fetch_category_id try %s/%s failed: %s; retry in %.1fs",
                            attempt, MAX_RETRIES, e, delay)
            time.sleep(delay)
        except json.JSONDecodeError as e:
            logging.error("JSON decode error in fetch_category_id: %s", e)
            break
    raise RuntimeError("Failed fetching category id")


def fetch_all_post_ids(base_url: str, cat_id: int, per_page: int = 100) -> List[int]:
    """
    Собирает все ID постов из категории, перебирая страницы WP REST API.
    """
    ids: List[int] = []
    page = 1
    while True:
        url = (
            f"{base_url.rstrip('/')}/wp-json/wp/v2/posts"
            f"?categories={cat_id}"
            f"&per_page={per_page}"
            f"&page={page}"
            f"&_fields=id"
        )
        r = SCRAPER.get(url, timeout=SCRAPER_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, list) or not data:
            break
        ids.extend(int(item["id"]) for item in data)
        page += 1
    return ids


def fetch_post_by_id(base_url: str, post_id: int) -> Dict[str, Any]:
    """Запрашивает полный JSON одной статьи (с _embed для картинок)."""
    url = f"{base_url.rstrip('/')}/wp-json/wp/v2/posts/{post_id}?_embed"
    r = SCRAPER.get(url, timeout=SCRAPER_TIMEOUT)
    r.raise_for_status()
    return r.json()


def save_image(src_url: str, folder: Path) -> Optional[str]:
    """Скачивает картинку и сохраняет в папке folder, возвращает локальный путь."""
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
            logging.warning("save_image %s try %s/%s failed: %s; retry in %.1fs",
                            fn, attempt, MAX_RETRIES, e, delay)
            time.sleep(delay)
    logging.error("Failed saving image %s after %s attempts", fn, MAX_RETRIES)
    return None


def load_catalog() -> List[Dict[str, Any]]:
    """Читает catalog.json с разделяемой блокировкой, возвращает список записей."""
    if not CATALOG_PATH.exists():
        return []
    try:
        with open(CATALOG_PATH, "r", encoding="utf-8") as f:
            fcntl.flock(f, fcntl.LOCK_SH)
            data = json.load(f)
            return [item for item in data if isinstance(item, dict) and "id" in item]
    except Exception as e:
        logging.error("load_catalog error: %s", e)
        return []


def save_catalog(catalog: List[Dict[str, Any]]) -> None:
    """Сохраняет минимальные поля (id, hash, translated_to) с эксклюзивной блокировкой."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    minimal = [
        {"id": itm["id"], "hash": itm["hash"], "translated_to": itm.get("translated_to", "")}
        for itm in catalog
    ]
    try:
        with open(CATALOG_PATH, "w", encoding="utf-8") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            json.dump(minimal, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.error("save_catalog error: %s", e)

# Добавляем адаптер-функцию translate_text()
def translate_text(text: str, to_lang: str = "ru", provider: str = "yandex") -> str:
    """
    Перевод текста через translators с защитой от ошибок.
    Возвращает оригинал, если перевод недоступен.
    """
    if not text or not isinstance(text, str):
        return ""
    try:
        translated = ts.translate_text(text, translator=provider, from_language="en", to_language=to_lang)
        if isinstance(translated, str):
            return translated
        logging.warning("Translator returned non-str: %s", text[:50])
    except Exception as e:
        logging.warning("Translation error [%s → %s]: %s", provider, to_lang, e)
    return text

def chunk_text(text: str, size: int = 4096, preserve_formatting: bool = True) -> List[str]:
    """Аналогично исходной версии"""
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
                logging.info(f"Skipping unchanged article ID={aid}")
                return existing_meta
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logging.warning(f"Failed to read existing meta for ID={aid}: {e}")

    # Остальная логика функции остается без изменений
    orig_title = BeautifulSoup(post["title"]["rendered"], "html.parser").get_text(strip=True)
    title = orig_title

    if translate_to:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                title = translate_text(orig_title, to_lang=translate_to, provider="yandex")
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
    raw_text = bad_re.sub("", raw_text)
    raw_text = re.sub(r"[ \t]+", " ", raw_text)
    raw_text = re.sub(r"\n{3,}", "\n\n", raw_text)

    # Вставка заголовка в начало
    raw_text = f"**{title}**\n\n{raw_text}"

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
        "images": images, "posted": False,
        "hash": hashlib.sha256(raw_text.encode()).hexdigest()  # ADDED: hash контента
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
                    trans = [translate_text(p, to_lang=translate_to, provider="yandex") for p in clean_paras]

                    txt_t = art_dir / f"content.{translate_to}.txt"
                    trans_txt = "\n\n".join(trans)
                    header_t = f"{title}\n\n\n"
                    txt_t.write_text(header_t + trans_txt, encoding="utf-8")

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
    """CHANGED: Полностью переработанная логика обработки"""
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

    # 1) получаем ID категории и полный список ID
    cid = fetch_category_id(args.base_url, args.slug)
    all_ids = fetch_all_post_ids(args.base_url, cid)
    logging.info("Found %d total IDs", len(all_ids))

    # 2) загружаем уже обработанные ID
    catalog = load_catalog()
    existing_ids = {item["id"] for item in catalog}

    # 3) фильтруем только новые
    new_ids = [aid for aid in all_ids if aid not in existing_ids]
    if not new_ids:
        logging.info("🔍 No new articles to download")
        return

    # 4) ограничиваем по args.limit (если указан)
    to_process = new_ids[: args.limit] if args.limit else new_ids
    logging.info("Will process %d new articles: %s", len(to_process), to_process)

    # 5) скачиваем, парсим и сохраняем только новые
    new_count = 0
    for aid in to_process:
        try:
            post = fetch_post_by_id(args.base_url, aid)
            if meta := parse_and_save(post, args.lang, args.base_url):
                catalog.append(meta)
                existing_ids.add(aid)
                new_count += 1
                logging.info("✅ Processed ID=%s", aid)
        except Exception:
            logging.exception("❌ Failed processing ID=%s", aid)

    # 6) сохраняем каталог, если добавили что-то новое
    if new_count:
        save_catalog(catalog)
        logging.info("Added %d new articles; total now %d", new_count, len(catalog))
    else:
        logging.info("No new articles were processed")

except Exception:
    logging.exception("Fatal error in main:")
    exit(1)

if __name__ == "__main__":
    main()
