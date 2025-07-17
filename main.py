#!/usr/bin/env python3
import argparse
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
from bs4 import BeautifulSoup
import cloudscraper # Для fetch_category_id, fetch_posts, save_image
import translators as ts # Для translate_text
import fcntl # Для блокировки файлов в load_catalog и save_catalog

# Настройка переменной окружения (должна быть в начале, один раз)
os.environ["translators_default_region"] = "EN"

# Настройки логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Предполагаемые константы
OUTPUT_DIR = Path("articles") # Возвращаем к исходной директории
CATALOG_PATH = OUTPUT_DIR / "catalog.json"
MAX_RETRIES = 3
BASE_DELAY = 1.0 # Базовая задержка для ретраев

# cloudscraper для обхода Cloudflare
SCRAPER = cloudscraper.create_scraper()
SCRAPER_TIMEOUT = (10.0, 60.0)      # (connect_timeout, read_timeout) в секундах

# --- НОВЫЕ КОНСТАНТЫ И ФУНКЦИИ ДЛЯ ФИЛЬТРАЦИИ СЛОВ ---
# Список слов для фильтрации (добавьте сюда свои слова)
# Слова будут заменены на звездочки. Используйте re.escape() если слово содержит спецсимволы.
WORDS_TO_FILTER = ["(VIDEO)", "VIDEO:","Synopsis:","AKP"] # Ваши запрещенные слова

def filter_text(text: str, filter_words: List[str]) -> str:
    """
    Фильтрует текст, заменяя слова из filter_words на звездочки.
    Удаляет множественные пробелы, оставшиеся после замены.
    """
    if not text:
        return ""
    cleaned_text = text
    for word in filter_words:
        if word: # Пропускаем пустые слова
            # Используем re.sub для замены слова, учитывая регистр и границы слова.
            # Заменяем на пробел, чтобы потом схлопнуть множественные пробелы.
            cleaned_text = re.sub(r'\b' + re.escape(word) + r'\b', ' ', cleaned_text, flags=re.IGNORECASE)
    
    # Удаляем множественные пробелы и пробелы в начале/конце
    cleaned_text = re.sub(r"\s+", " ", cleaned_text).strip()
    return cleaned_text
# --- КОНЕЦ НОВЫХ КОНСТАНТ И ФУНКЦИЙ ---


# --- Вспомогательные функции (реальные реализации из нашего обсуждения) ---
def load_posted_ids(state_file_path: Path) -> Set[str]:
    """
    Загружает множество ID из файла состояния (например, posted.json).
    Используется блокировка файла для безопасного чтения.
    """
    try:
        if state_file_path.exists():
            with open(state_file_path, 'r', encoding='utf-8') as f:
                fcntl.flock(f, fcntl.LOCK_SH) # Блокировка для чтения
                return {str(item) for item in json.load(f)}
        return set()
    except (FileNotFoundError, json.JSONDecodeError, IOError) as e:
        logging.warning(f"Could not load posted IDs from {state_file_path}: {e}. Assuming empty set.")
    except Exception as e:
        logging.warning(f"An unexpected error occurred loading posted IDs: {e}. Assuming empty set.")
        return set()

def extract_img_url(img_tag: Any) -> Optional[str]:
    """Извлекает URL изображения из тега <img>."""
    for attr in ("data-src", "data-lazy-src", "data-srcset", "srcset", "src"):
        val = img_tag.get(attr)
        if not val:
            continue
        parts = val.split()
        if parts:
            return parts[0]
    return None

def fetch_category_id(base_url: str, slug: str) -> int:
    """Получает ID категории по ее 'slug'."""
    logging.info(f"Fetching category ID for {slug} from {base_url}...")
    endpoint = f"{base_url}/wp-json/wp/v2/categories?slug={slug}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = SCRAPER.get(endpoint, timeout=SCRAPER_TIMEOUT)
            r.raise_for_status()
            data = r.json()
            if not data:
                raise RuntimeError(f"Category '{slug}' not found")
            return data[0]["id"]
        except (requests.exceptions.Timeout, requests.exceptions.RequestException) as e: # Использовать requests.exceptions если cloudscraper возвращает их
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
    """Получает список постов из указанной категории."""
    logging.info(f"Fetching posts for category {cat_id} from {base_url}, per_page={per_page}...")
    endpoint = f"{base_url}/wp-json/wp/v2/posts?categories={cat_id}&per_page={per_page}&_embed"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = SCRAPER.get(endpoint, timeout=SCRAPER_TIMEOUT)
            r.raise_for_status()
            return r.json()
        except (requests.exceptions.Timeout, requests.exceptions.RequestException) as e: # Использовать requests.exceptions если cloudscraper возвращает их
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
    """Сохраняет изображение по URL в указанную папку."""
    logging.info(f"Saving image from {src_url} to {folder}...")
    folder.mkdir(parents=True, exist_ok=True)
    fn = src_url.rsplit('/', 1)[-1].split('?', 1)[0]
    dest = folder / fn
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = SCRAPER.get(src_url, timeout=SCRAPER_TIMEOUT)
            r.raise_for_status()
            dest.write_bytes(r.content)
            return str(dest)
        except (requests.exceptions.Timeout, requests.exceptions.RequestException) as e: # Использовать requests.exceptions если cloudscraper возвращает их
            delay = BASE_DELAY * 2 ** (attempt - 1)
            logging.warning(
                "Timeout saving image %s (try %s/%s): %s; retry in %.1fs",
                fn, attempt, MAX_RETRIES, e, delay
            )
            time.sleep(delay)
    logging.error("Failed saving image %s after %s attempts", fn, MAX_RETRIES)
    return None

def load_catalog() -> List[Dict[str, Any]]:
    """Загружает каталог статей из catalog.json с блокировкой файла."""
    if not CATALOG_PATH.exists():
        return []
    try:
        with open(CATALOG_PATH, "r", encoding="utf-8") as f:
            fcntl.flock(f, fcntl.LOCK_SH)  # Блокировка для чтения
            data = json.load(f)
            # Валидация данных: фильтруем некорректные записи
            return [item for item in data if isinstance(item, dict) and "id" in item]
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        logging.error("Catalog JSON decode error: %s", e)
        return []
    except IOError as e:
        logging.error("Catalog read error: %s", e)
        return []
    except Exception as e:
        logging.error("An unexpected error occurred loading catalog: %s", e)
        return []

def save_catalog(catalog: List[Dict[str, Any]]) -> None:
    """
    Сохраняет каталог статей в catalog.json с блокировкой файла.
    Сохраняет только минимальный набор полей для защиты от дублей:
    id, hash, translated_to
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    # Фильтруем каждую запись
    minimal = []
    for item in catalog:
        if isinstance(item, dict) and "id" in item:
            minimal.append({
                "id": item["id"],
                "hash": item.get("hash", ""),
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
    except Exception as e:
        logging.error("An unexpected error occurred saving catalog: %s", e)


def translate_text(text: str, to_lang: str = "ru", provider: str = "yandex") -> str:
    """
    Перевод текста через translators с защитой от ошибок.
    Возвращает оригинал, если перевод недоступен.
    """
    logging.info(f"Translating text (provider: {provider}) to {to_lang}...")
    if not text or not isinstance(text, str):
        return ""
    try:
        translated = ts.translate_text(text, translator=provider, from_language="en", to_language=to_lang)
        if isinstance(translated, str):
            return translated
        logging.warning("Translator returned non-str for text: %s", text[:50])
    except Exception as e:
        logging.warning("Translation error [%s → %s]: %s", provider, to_lang, e)
    return text

# Регулярные выражения для очистки текста
bad_re = re.compile(r"[\u200b-\u200f\uFEFF\u200E\u00A0]") # Пример

# Функция parse_and_save (без изменений в логике, только форматирование)
def parse_and_save(post: Dict[str, Any], translate_to: str, base_url: str) -> Optional[Dict[str, Any]]:
    """Парсит и сохраняет статью, включая перевод и загрузку изображений."""
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
        except Exception as e:
            logging.warning(f"An unexpected error occurred reading existing meta for ID={aid}: {e}. Reparsing.")


    # 1. Фильтрация оригинального заголовка
    orig_title = BeautifulSoup(post["title"]["rendered"], "html.parser").get_text(strip=True)
    orig_title = filter_text(orig_title, WORDS_TO_FILTER) # <-- Применяем фильтр к оригинальному заголовку
    title = orig_title # Заголовок для использования далее (может быть переведенным)

    if translate_to:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                # Переводим УЖЕ ОТФИЛЬТРОВАННЫЙ оригинальный заголовок
                title = translate_text(orig_title, to_lang=translate_to, provider="yandex")
                # ! НЕ ФИЛЬТРУЕМ ЗДЕСЬ ПЕРЕВЕДЕННЫЙ ЗАГОЛОВОК, ТАК КАК ЗАПРОС "ТОЛЬКО ПЕРЕД ПЕРЕВОДОМ"
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
    
    # 2. Фильтрация основного текста перед сохранением и использованием для перевода
    raw_text = filter_text(raw_text, WORDS_TO_FILTER) # <-- Применяем фильтр к основному тексту

    # Вставка заголовка в начало (заголовок уже отфильтрован, если это оригинал, или переведен из отфильтрованного оригинала)
    raw_text = f"**{title}**\n\n{raw_text}"

    img_dir = art_dir / "images"
    images: List[str] = []
    srcs = []

    # Используем ThreadPoolExecutor для параллельной загрузки изображений
    with ThreadPoolExecutor(max_workers=5) as ex:
        # Собираем URL'ы изображений из тегов <img>, ограничиваясь 10 первыми
        for img in soup.find_all("img")[:10]:
            url = extract_img_url(img)
            if url:
                srcs.append(url)

        futures = {ex.submit(save_image, url, img_dir): url for url in srcs}
        for fut in as_completed(futures):
            try:
                if path := fut.result():
                    images.append(path)
            except Exception as e:
                logging.warning(f"Error saving image: {e}")


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
        "title": title, # Используем title, который уже обработан
        "text_file": str(art_dir / "content.txt"),
        "images": images, "posted": False,
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
            except Exception as e:
                logging.warning(f"An unexpected error occurred reading old meta for ID={aid}: {e}. Skipping comparison.")


        if old.get("hash") != h or old.get("translated_to") != translate_to:
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    # Получаем параграфы из оригинального контента
                    original_paras_for_translation = [p.get_text(strip=True) for p in BeautifulSoup(post["content"]["rendered"], "html.parser").find_all("p")]
                    # Очищаем и фильтруем каждый параграф ПЕРЕД переводом
                    clean_and_filtered_paras_for_translation = [filter_text(bad_re.sub("", p), WORDS_TO_FILTER) for p in original_paras_for_translation]
                    
                    # Переводим ОТФИЛЬТРОВАННЫЕ параграфы
                    trans = [translate_text(p, to_lang=translate_to, provider="yandex") for p in clean_and_filtered_paras_for_translation]
                    
                    # ! НЕ ФИЛЬТРУЕМ ПЕРЕВЕДЕННЫЙ ТЕКСТ ЗДЕСЬ, ТАК КАК ЗАПРОС "ТОЛЬКО ПЕРЕД ПЕРЕВОДОМ"

                    txt_t = art_dir / f"content.{translate_to}.txt"
                    trans_txt = "\n\n".join(trans)
                    header_t = f"**{title}**\n\n" # title уже обработан
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
                logging.warning("Translation failed after max retries for ID=%s.", aid)
        else:
            logging.info("Using cached translation %s for ID=%s", translate_to, aid)

    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    return meta

# --- Основная функция main() ---
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
        default="articles/posted.json", # Убедитесь, что это правильный путь
        help="Путь к файлу состояния с ID уже опубликованных статей (только для чтения)"
    )
    args = parser.parse_args()

    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        cid = fetch_category_id(args.base_url, args.slug)
        posts = fetch_posts(args.base_url, cid, per_page=(args.limit or 10))

        catalog = load_catalog()
        existing_ids_in_catalog = {article["id"] for article in catalog}

        posted_ids_from_repo = load_posted_ids(Path(args.posted_state_file))

        logging.info(f"Loaded {len(posted_ids_from_repo)} posted IDs from {args.posted_state_file}.")
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.debug(f"Posted IDs: {posted_ids_from_repo}")

        # Инициализация переменной new_articles_processed_in_run
        new_articles_processed_in_run = 0

        for post in posts[:args.limit or len(posts)]:
            post_id = str(post["id"])

            if post_id in posted_ids_from_repo:
                logging.info(f"Skipping article ID={post_id} as it's already in {args.posted_state_file}.")
                continue

            is_in_local_catalog = post_id in existing_ids_in_catalog

            # Обработка статьи (парсинг и сохранение)
            if meta := parse_and_save(post, args.lang, args.base_url):
                # parse_and_save уже проверяет, изменился ли контент, и возвращает старые метаданные, если нет.
                # Поэтому здесь нужно только обновить каталог, если это новая статья или измененная.
                # Если parse_and_save вернул метаданные, это значит, что статья либо новая, либо обновленная.
                
                # Удаляем старую запись, если статья уже была в каталоге
                catalog = [item for item in catalog if item["id"] != meta["id"]]
                catalog.append(meta) # Добавляем новую/обновленную запись
                existing_ids_in_catalog.add(meta["id"]) # Обновляем множество ID

                # Проверяем, действительно ли это новая статья (не только обновленная)
                # Это можно сделать, сравнив до и после обработки parse_and_save.
                # Сейчас логика такая: если meta возвращается, и article_id не был в posted_ids_from_repo,
                # и это не дубликат в текущем каталоге, то это новая статья.
                # Так как мы удаляем из каталога и добавляем, нам нужно более тонкое условие для 'newly processed'.
                # Самый простой способ - если hash изменился или это полностью новый ID.

                # Для целей логирования NEW_ARTICLES_STATUS, нам нужно знать,
                # были ли добавлены статьи, которых не было ни в posted.json, ни в текущем catalog.json.
                # Но так как posted.json обрабатывается в poster.py, а catalog.json - здесь,
                # нам просто нужно отслеживать, сколько уникальных новых статей было обработано.
                if post_id not in posted_ids_from_repo and not is_in_local_catalog:
                    new_articles_processed_in_run += 1
                    logging.info(f"Processed new article ID={post_id} and added to local catalog.")
                elif post_id in posted_ids_from_repo and not is_in_local_catalog:
                    # Это случай, когда статья была в posted.json, но не в catalog.json
                    # (например, catalog.json был удален или устарел).
                    # Мы ее обрабатываем, но не считаем "новой" для статуса.
                    logging.info(f"Re-processed article ID={post_id} (found in posted.json, not in local catalog).")
                elif is_in_local_catalog:
                    logging.info(f"Updated article ID={post_id} in local catalog (content changed or re-translated).")
                

        # Сохранение каталога и вывод статуса
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
