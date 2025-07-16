#!/usr/bin/env python3
import os
import json
import argparse
import logging
import re
import hashlib
import time
import fcntl # ADDED: для блокировки файла
import argparse
os.environ["translators_default_region"] = "EN"
import logging
import json
import hashlib
import time
import re
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Set, Tuple

# Убедитесь, что эти импорты у вас есть, если они используются
# Например: from bs4 import BeautifulSoup
# from your_module import fetch_category_id, fetch_posts, translate_text, save_image, extract_img_url, load_posted_ids, save_posted_ids, load_catalog, save_catalog

# Настройки логирования (если не определены глобально)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Предполагаемые константы (добавьте свои значения или убедитесь, что они импортированы)
OUTPUT_DIR = Path("parsed_articles")
CATALOG_PATH = OUTPUT_DIR / "catalog.json"
MAX_RETRIES = 3
BASE_DELAY = 1.0 # Базовая задержка для ретраев

# Предполагаемые заглушки для функций, которые должны быть определены в вашем проекте
def fetch_category_id(base_url: str, slug: str) -> int:
    logging.info(f"Fetching category ID for {slug} from {base_url}...")
    # Здесь должна быть реальная логика запроса к API
    # Пример заглушки:
    if slug == "national":
        return 1234 # Предположим ID для 'national'
    return 0

def fetch_posts(base_url: str, category_id: int, per_page: int) -> List[Dict[str, Any]]:
    logging.info(f"Fetching posts for category {category_id} from {base_url}, per_page={per_page}...")
    # Здесь должна быть реальная логика запроса к API для получения постов
    # Пример заглушки:
    return [{"id": i, "slug": f"test-article-{i}", "title": {"rendered": f"Title {i}"}, "content": {"rendered": f"<p>Content {i}</p>"}, "date": "2023-01-01T12:00:00", "link": f"{base_url}/article-{i}"} for i in range(1, per_page + 1)]

def translate_text(text: str, to_lang: str, provider: str) -> str:
    logging.info(f"Translating text (provider: {provider}) to {to_lang}...")
    # Здесь должна быть реальная логика перевода
    return f"Translated: {text}"

def save_image(url: str, img_dir: Path) -> Optional[str]:
    logging.info(f"Saving image from {url} to {img_dir}...")
    # Здесь должна быть реальная логика сохранения изображений
    # Создадим фиктивный файл, чтобы не возвращать None
    img_dir.mkdir(parents=True, exist_ok=True)
    file_name = url.split('/')[-1].split('?')[0] + ".jpg" # Простой способ получить имя файла
    file_path = img_dir / file_name
    file_path.write_text("dummy image content") # Запись фиктивного контента
    return str(file_path)

def extract_img_url(img_tag: Any) -> Optional[str]:
    # Заглушка, если BeautifulSoup не импортирован
    if not img_tag:
        return None
    return img_tag.get('src') # Предполагаем, что это объект BeautifulSoup

def load_posted_ids(file_path: Path) -> Set[str]:
    if file_path.exists():
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return set(json.load(f))
        except (json.JSONDecodeError, FileNotFoundError):
            logging.warning(f"Could not load posted IDs from {file_path}. Starting fresh.")
            return set()
    return set()

def save_posted_ids(ids: Set[str], file_path: Path):
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(list(ids), f, ensure_ascii=False, indent=2)

def load_catalog() -> List[Dict[str, Any]]:
    if CATALOG_PATH.exists():
        try:
            with open(CATALOG_PATH, "r", encoding="utf-8") as f:
                # В этом месте может быть fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            logging.warning(f"Could not load catalog from {CATALOG_PATH}. Starting fresh.")
            return []
    return []

def save_catalog(catalog: List[Dict[str, Any]]):
    with open(CATALOG_PATH, "w", encoding="utf-8") as f:
        # В этом месте может быть fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        json.dump(catalog, f, ensure_ascii=False, indent=2)


# Убедитесь, что BeautifulSoup импортирован, если он используется
from bs4 import BeautifulSoup


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
            # Проверяем, изменился ли контент или язык перевода
            if existing_meta.get("hash") == current_hash and existing_meta.get("translated_to", "") == translate_to:
                logging.info(f"Skipping unchanged article ID={aid} (content and translation match local cache).")
                return existing_meta
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logging.warning(f"Failed to read existing meta for ID={aid}: {e}. Reparsing.")

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
                    attempt, MAX_RETRIES, e, delay
                )
                time.sleep(delay)
    
    # Регулярные выражения для очистки текста (если используются)
    bad_re = re.compile(r"[\u200b-\u200f\uFEFF\u200E\u00A0]") # Пример

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

    for img in soup.find_all("img")[:10]:
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
        logging.warning("No images for ID=%s; skipping article parsing and saving.", aid)
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
                    # Убедитесь, что 'paras' определены, если это условие будет выполняться без предыдущего блока
                    # (хотя в вашем коде они определены ранее)
                    clean_paras = [bad_re.sub("", p) for p in paras] # Используем bad_re
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
            else: # Это `else` относится к `for` циклу, если не было `break`
                logging.warning("Translation failed after max retries for ID=%s.", aid)
        else:
            logging.info("Using cached translation %s for ID=%s", translate_to, aid)


    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    return meta

# main функция без лишних fd и с правильными отступами
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
    parser.add_argument(
        "--posted-state-file",
        type=str,
        default="articles/posted.json",
        help="Путь к файлу состояния с ID уже опубликованных статей (только для чтения)"
    )
    args = parser.parse_args()

    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        cid = fetch_category_id(args.base_url, args.slug)
        posts = fetch_posts(args.base_url, cid, per_page=(args.limit or 10))

        # ИСПРАВЛЕНО: Используем load_catalog() без fd
        catalog = load_catalog()
        existing_ids_in_catalog = {article["id"] for article in catalog}

        posted_ids_from_repo = load_posted_ids(Path(args.posted_state_file))

        logging.info(f"Loaded {len(posted_ids_from_repo)} posted IDs from {args.posted_state_file}.")
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.debug(f"Posted IDs: {posted_ids_from_repo}")

        new_articles_processed_in_run = 0

        for post in posts[:args.limit or len(posts)]:
            post_id = str(post["id"])

            if post_id in posted_ids_from_repo:
                logging.info(f"Skipping article ID={post_id} as it's already in {args.posted_state_file}.")
                continue

            is_in_local_catalog = post_id in existing_ids_in_catalog

            if is_in_local_catalog:
                logging.debug(f"Article ID={post_id} found in local catalog ({CATALOG_PATH}). Checking for content updates.")

            if meta := parse_and_save(post, args.lang, args.base_url):
                if is_in_local_catalog:
                    catalog = [item for item in catalog if item["id"] != post_id]
                    logging.info(f"Updated article ID={post_id} in local catalog (content changed or re-translated).")
                else:
                    new_articles_processed_in_run += 1
                    logging.info(f"Processed new article ID={post_id} and added to local catalog.")

                catalog.append(meta)
                existing_ids_in_catalog.add(post_id)

        if new_articles_processed_in_run > 0:
            save_catalog(catalog)
            logging.info(f"Added {new_articles_processed_in_run} truly new articles. Total parsed articles in catalog: {len(catalog)}")
            print("NEW_ARTICLES_STATUS:true")
        else:
            logging.info("No new articles found or processed that are not already in posted.json or local catalog.")
            print("NEW_ARTICLES_STATUS:false")

    except Exception as e:
        logging.exception("Fatal error in main:")
        exit(1)

# Убедитесь, что этот блок БЕЗ ОТСТУПА!
if __name__ == "__main__":
    main()
import translators as ts
from typing import Any, Dict, List, Optional
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import cloudscraper
from requests.exceptions import ReadTimeout as ReqTimeout, RequestException
from bs4 import BeautifulSoup

# списком — все фразы/слова, которые нужно вырезать
bad_patterns = [
    r"synopsis\s*:\s*",    # «Synopsis»
    r"\(video inside\)",   # «(video inside)»
    r"\bkhmer times\b"      # слово «khmer times»
]
# единое регулярное выражение с флагом IGNORECASE
bad_re = re.compile("|".join(bad_patterns), flags=re.IGNORECASE)

# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.DEBUG, # Вы можете временно изменить на logging.DEBUG для более подробных логов
    format="%(asctime)s [%(levelname)s] %(message)s"
)
# ──────────────────────────────────────────────────────────────────────────────

# cloudscraper для обхода Cloudflare
SCRAPER = cloudscraper.create_scraper()
SCRAPER_TIMEOUT = (10.0, 60.0)     # (connect_timeout, read_timeout) в секундах

MAX_RETRIES = 3
BASE_DELAY  = 2.0                # базовый интервал для backoff (сек)

# ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ, КОТОРЫЕ БЫЛИ УТЕРЯНЫ В ПРЕДЫДУЩИХ ОТВЕТАХ
OUTPUT_DIR   = Path("articles")
CATALOG_PATH = OUTPUT_DIR / "catalog.json"
# ──────────────────────────────────────────────────────────────────────────────

# ADDED: Функция для загрузки ID опубликованных статей из файла состояния posted.json
# Этот код дублируется здесь по вашему запросу, чтобы не создавать utils.py.
def load_posted_ids(state_file_path: Path) -> set[str]:
    """
    Загружает множество ID из файла состояния (например, posted.json).
    Используется блокировка файла для безопасного чтения.
    """
    try:
        if state_file_path.exists():
            with open(state_file_path, 'r', encoding='utf-8') as f:
                # Добавлена блокировка для чтения, чтобы избежать конфликтов с другими процессами
                fcntl.flock(f, fcntl.LOCK_SH)
                return {str(item) for item in json.load(f)}
        return set()
    except (FileNotFoundError, json.JSONDecodeError, IOError) as e:
        logging.warning(f"Could not load posted IDs from {state_file_path}: {e}. Assuming empty set.")
        return set()

def extract_img_url(img_tag):
    """Аналогично исходной версии"""
    for attr in ("data-src", "data-lazy-src", "data-srcset", "srcset", "src"):
        val = img_tag.get(attr)
        if not val:
            continue
        parts = val.split()
        if parts:
            return parts[0]
    return None

def fetch_category_id(base_url: str, slug: str) -> int:
    """Аналогично исходной версии"""
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
    """Аналогично исходной версии"""
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
    """Аналогично исходной версии"""
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
    """CHANGED: Добавлена блокировка файла и валидация данных"""
    if not CATALOG_PATH.exists():
        return []
    try:
        with open(CATALOG_PATH, "r", encoding="utf-8") as f:
            fcntl.flock(f, fcntl.LOCK_SH)  # Блокировка для чтения
            data = json.load(f)
            # Фильтруем некорректные записи
            return [item for item in data if isinstance(item, dict) and "id" in item]
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        logging.error("Catalog JSON decode error: %s", e)
        return []
    except IOError as e:
        logging.error("Catalog read error: %s", e)
        return []

def save_catalog(catalog: List[Dict[str, Any]]) -> None:
    """
    Сохраняет только минимальный набор полей для защиты от дублей:
    id, hash, translated_to
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    # Фильтруем каждую запись
    minimal = []
    for item in catalog:
        # Убесимся, что item является словарем и содержит 'id'
        if isinstance(item, dict) and "id" in item:
            minimal.append({
                "id": item["id"],
                "hash": item.get("hash", ""), # Используем .get() на случай отсутствия ключа
                "translated_to": item.get("translated_to", "")
            })
        else:
            logging.warning(f"Skipping malformed catalog entry: {item}")

    try:
        with open(CATALOG_PATH, "w", encoding="utf-8") as f:
            fcntl.flock(f, fcntl.LOCK_EX) # Блокировка для записи
            json.dump(minimal, f, ensure_ascii=False, indent=2)
    except IOError as e:
        logging.error("Failed to save catalog: %s", e)

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

    chunks, curr = "", ""
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
            # Проверяем, изменился ли контент или язык перевода
            if existing_meta.get("hash") == current_hash and existing_meta.get("translated_to", "") == translate_to:
                logging.info(f"Skipping unchanged article ID={aid} (content and translation match local cache).")
                return existing_meta
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logging.warning(f"Failed to read existing meta for ID={aid}: {e}. Reparsing.")

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
                    attempt, MAX_RETRIES, e, delay
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

    for img in soup.find_all("img")[:10]:
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
        logging.warning("No images for ID=%s; skipping article parsing and saving.", aid)
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

    try:
        
        catalog = load_catalog()

        if new_articles_processed_in_run > 0:
            print("NEW_ARTICLES_STATUS:true")
        else:
            print("NEW_ARTICLES_STATUS:false")

    except Exception as e:
        logging.exception("Fatal error in main:")
        exit(1)
    finally:
        # Этот блок гарантирует, что fd будет закрыт, даже если произошла ошибка
        if 'fd' in locals() and fd is not None:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            except Exception as e:
                logging.error(f"Error unlocking catalog.json: {e}")
            finally:
                os.close(fd)

if __name__ == "__main__":
     main()
