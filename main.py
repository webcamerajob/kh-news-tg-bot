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

# Убедитесь, что эти импорты у вас есть.
# bs4 для парсинга HTML.
from bs4 import BeautifulSoup
# cloudscraper для обхода защиты Cloudflare при HTTP-запросах.
import cloudscraper
# translators для перевода текста.
import translators as ts
# fcntl для блокировки файлов, чтобы предотвратить одновременную запись/чтение.
import fcntl
# requests для обработки исключений HTTP-запросов, используемых cloudscraper.
import requests

# Настройка переменной окружения для translators (должна быть в начале, один раз)
os.environ["translators_default_region"] = "EN"

# Настройки логирования: INFO уровень, формат с временем, уровнем и сообщением.
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Предполагаемые константы
OUTPUT_DIR = Path("articles") # Директория для сохранения распарсенных статей.
CATALOG_PATH = OUTPUT_DIR / "catalog.json" # Путь к файлу каталога статей.
MAX_RETRIES = 3 # Максимальное количество попыток для HTTP-запросов и переводов.
BASE_DELAY = 1.0 # Базовая задержка в секундах для экспоненциальной задержки при ретраях.

# Инициализация cloudscraper для обхода Cloudflare.
SCRAPER = cloudscraper.create_scraper()
# Тайм-аут для HTTP-запросов: (тайм-аут соединения, тайм-аут чтения).
SCRAPER_TIMEOUT = (10.0, 60.0)

# --- КОНСТАНТЫ И ФУНКЦИИ ДЛЯ ФИЛЬТРАЦИИ СЛОВ ---
# Список слов, которые будут заменены на пробелы в тексте и заголовках.
# Используйте re.escape() для слов, содержащих специальные символы регулярных выражений.
WORDS_TO_FILTER = ["(VIDEO)", "VIDEO:", "Synopsis:", "AKP"]

def filter_text(text: str, filter_words: List[str]) -> str:
    """
    Фильтрует текст, заменяя слова из filter_words на пробелы.
    Удаляет множественные пробелы, оставшиеся после замены, и обрезает пробелы с краев.
    """
    if not text:
        return ""
    cleaned_text = text
    for word in filter_words:
        if word: # Пропускаем пустые слова, чтобы избежать ошибок re.escape().
            # Заменяем слово, учитывая границы слова (\b) и игнорируя регистр (re.IGNORECASE).
            # Замена на пробел позволяет затем схлопнуть множественные пробелы.
            cleaned_text = re.sub(r'\b' + re.escape(word) + r'\b', ' ', cleaned_text, flags=re.IGNORECASE)
    
    # Удаляем множественные пробелы и пробелы в начале/конце строки.
    cleaned_text = re.sub(r"\s+", " ", cleaned_text).strip()
    return cleaned_text
# --- КОНЕЦ КОНСТАНТ И ФУНКЦИЙ ФИЛЬТРАЦИИ ---


# --- Вспомогательные функции ---
def load_posted_ids(state_file_path: Path) -> Set[str]:
    """
    Загружает множество ID из файла состояния (например, posted.json).
    Использует блокировку файла для безопасного чтения, чтобы избежать конфликтов.
    """
    try:
        if state_file_path.exists():
            with open(state_file_path, 'r', encoding='utf-8') as f:
                fcntl.flock(f, fcntl.LOCK_SH) # Блокировка для чтения (разделяемая блокировка).
                return {str(item) for item in json.load(f)}
        return set()
    except (FileNotFoundError, json.JSONDecodeError, IOError) as e:
        logging.warning(f"Could not load posted IDs from {state_file_path}: {e}. Assuming empty set.")
        return set()
    except Exception as e:
        logging.warning(f"An unexpected error occurred loading posted IDs: {e}. Assuming empty set.")
        return set()

def extract_img_url(img_tag: Any) -> Optional[str]:
    """
    Извлекает URL изображения из HTML-тега <img>.
    Проверяет различные атрибуты, в которых может храниться URL изображения.
    """
    for attr in ("data-breeze", "data-src", "data-lazy-src", "data-srcset", "srcset", "src"):
        val = img_tag.get(attr)
        if not val:
            continue
        parts = val.split() # Обработка srcset, где URL может быть с дескриптором ширины.
        if parts:
            return parts[0]
    return None

def fetch_category_id(base_url: str, slug: str) -> int:
    """
    Получает ID категории WordPress по ее 'slug' (человекопонятному идентификатору).
    Использует API WordPress REST. Включает логику повторных попыток.
    """
    logging.info(f"Fetching category ID for {slug} from {base_url}...")
    endpoint = f"{base_url}/wp-json/wp/v2/categories?slug={slug}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = SCRAPER.get(endpoint, timeout=SCRAPER_TIMEOUT)
            r.raise_for_status() # Вызывает исключение для плохих статусов HTTP (4xx, 5xx).
            data = r.json()
            if not data:
                raise RuntimeError(f"Category '{slug}' not found")
            return data[0]["id"]
        except (requests.exceptions.Timeout, requests.exceptions.RequestException) as e:
            delay = BASE_DELAY * 2 ** (attempt - 1) # Экспоненциальная задержка.
            logging.warning(
                "Timeout fetching category (try %s/%s): %s; retry in %.1fs",
                attempt, MAX_RETRIES, e, delay
            )
            time.sleep(delay)
        except json.JSONDecodeError as e:
            logging.error("JSON decode error for categories: %s", e)
            break # Выход из цикла повторных попыток при ошибке JSON.
    raise RuntimeError("Failed fetching category id")

def fetch_posts(base_url: str, cat_id: int, per_page: int = 10) -> List[Dict[str, Any]]:
    """
    Получает список постов из указанной категории WordPress.
    Использует API WordPress REST. Включает логику повторных попыток и встраивание медиа (_embed).
    """
    logging.info(f"Fetching posts for category {cat_id} from {base_url}, per_page={per_page}...")
    endpoint = f"{base_url}/wp-json/wp/v2/posts?categories={cat_id}&per_page={per_page}&_embed"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = SCRAPER.get(endpoint, timeout=SCRAPER_TIMEOUT)
            r.raise_for_status()
            return r.json()
        except (requests.exceptions.Timeout, requests.exceptions.RequestException) as e:
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
    """
    Сохраняет изображение по URL в указанную папку на локальном диске.
    Включает логику повторных попыток.
    """
    logging.info(f"Saving image from {src_url} to {folder}...")
    folder.mkdir(parents=True, exist_ok=True) # Создаем папку, если ее нет.
    # Извлекаем имя файла из URL, убирая параметры запроса.
    fn = src_url.rsplit('/', 1)[-1].split('?', 1)[0]
    dest = folder / fn # Полный путь для сохранения файла.
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = SCRAPER.get(src_url, timeout=SCRAPER_TIMEOUT)
            r.raise_for_status()
            dest.write_bytes(r.content) # Сохраняем содержимое изображения.
            return str(dest) # Возвращаем путь к сохраненному файлу.
        except (requests.exceptions.Timeout, requests.exceptions.RequestException) as e:
            delay = BASE_DELAY * 2 ** (attempt - 1)
            logging.warning(
                "Timeout saving image %s (try %s/%s): %s; retry in %.1fs",
                fn, attempt, MAX_RETRIES, e, delay
            )
            time.sleep(delay)
    logging.error("Failed saving image %s after %s attempts", fn, MAX_RETRIES)
    return None

def load_catalog() -> List[Dict[str, Any]]:
    """
    Загружает каталог статей из catalog.json.
    Использует блокировку файла для безопасного чтения. Включает валидацию содержимого.
    """
    if not CATALOG_PATH.exists():
        return []
    try:
        with open(CATALOG_PATH, "r", encoding="utf-8") as f:
            fcntl.flock(f, fcntl.LOCK_SH) # Блокировка для чтения.
            data = json.load(f)
            # Валидация данных: фильтруем некорректные записи, чтобы убедиться, что они словари с "id".
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
    Сохраняет каталог статей в catalog.json.
    Использует блокировку файла для безопасной записи.
    Сохраняет только минимальный набор полей (id, hash, translated_to) для экономии места и безопасности.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True) # Убедимся, что выходная директория существует.
    # Фильтруем каждую запись, оставляя только необходимые поля.
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
            fcntl.flock(f, fcntl.LOCK_EX) # Эксклюзивная блокировка для записи.
            json.dump(minimal, f, ensure_ascii=False, indent=2) # Запись в JSON с форматированием.
    except IOError as e:
        logging.error("Failed to save catalog: %s", e)
    except Exception as e:
        logging.error("An unexpected error occurred saving catalog: %s", e)


def translate_text(text: str, to_lang: str = "ru", provider: str = "yandex") -> str:
    """
    Перевод текста с помощью библиотеки `translators`.
    Возвращает оригинальный текст, если перевод не удался или входной текст пуст.
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
    return text # Возвращаем оригинальный текст в случае ошибки.

# Регулярные выражения для очистки текста от невидимых/лишних символов.
bad_re = re.compile(r"[\u200b-\u200f\uFEFF\u200E\u00A0]")

# Функция parse_and_save - основная логика обработки одной статьи.
def parse_and_save(post: Dict[str, Any], translate_to: str, base_url: str) -> Optional[Dict[str, Any]]:
    """Парсит, фильтрует, переводит и сохраняет статью, включая загрузку изображений."""
    aid, slug = post["id"], post["slug"] # ID и slug статьи.
    art_dir = OUTPUT_DIR / f"{aid}_{slug}" # Директория для конкретной статьи.
    art_dir.mkdir(parents=True, exist_ok=True) # Создаем директорию.

    # Проверяем, существует ли уже метаданные для этой статьи и не изменился ли контент/язык перевода.
    meta_path = art_dir / "meta.json"
    if meta_path.exists():
        try:
            existing_meta = json.loads(meta_path.read_text(encoding="utf-8"))
            current_post_content_hash = hashlib.sha256(post["content"]["rendered"].encode()).hexdigest()
            
            # Если хэш контента и язык перевода совпадают, пропускаем перепарсинг.
            if existing_meta.get("hash") == current_post_content_hash and \
               existing_meta.get("translated_to", "") == translate_to:
                logging.info(f"Skipping unchanged article ID={aid} (content and translation match local cache).")
                return existing_meta
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logging.warning(f"Failed to read existing meta for ID={aid}: {e}. Reparsing.")
        except Exception as e:
            logging.warning(f"An unexpected error occurred reading existing meta for ID={aid}: {e}. Reparsing.")


    # 1. Фильтрация оригинального заголовка статьи.
    # Извлекаем чистый текст заголовка из HTML.
    orig_title = BeautifulSoup(post["title"]["rendered"], "html.parser").get_text(strip=True)
    orig_title = filter_text(orig_title, WORDS_TO_FILTER) # Применяем фильтр к оригинальному заголовку.
    title = orig_title # Инициализируем 'title', которое может быть переведено позже.

    # Если требуется перевод, переводим заголовок.
    if translate_to:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                # Переводим УЖЕ ОТФИЛЬТРОВАННЫЙ оригинальный заголовок.
                title = translate_text(orig_title, to_lang=translate_to, provider="yandex")
                # Важно: ПЕРЕВЕДЕННЫЙ заголовок НЕ ФИЛЬТРУЕТСЯ СНОВА, согласно требованию "только перед переводом".
                break
            except Exception as e:
                delay = BASE_DELAY * 2 ** (attempt - 1)
                logging.warning(
                    "Translate title attempt %s failed: %s; retry in %.1fs",
                    attempt, MAX_RETRIES, e, delay
                )
                time.sleep(delay)

    # Парсинг основного содержимого статьи.
    soup = BeautifulSoup(post["content"]["rendered"], "html.parser")

    # Извлечение параграфов и объединение их в один необработанный текст.
    paras = [p.get_text(strip=True) for p in soup.find_all("p")]
    raw_text = "\n\n".join(paras)
    raw_text = bad_re.sub("", raw_text) # Удаление невидимых символов.
    raw_text = re.sub(r"[ \t]+", " ", raw_text) # Схлопывание множественных пробелов.
    raw_text = re.sub(r"\n{3,}", "\n\n", raw_text) # Схлопывание множественных пустых строк.
    
    # 2. Фильтрация основного текста статьи.
    raw_text = filter_text(raw_text, WORDS_TO_FILTER) # Применяем фильтр к основному тексту.

    # !!! ВАЖНО: Заголовок НЕ добавляется к raw_text здесь.
    # Он будет отправлен отдельно и отформатирован 'poster.py'.
    # Это предотвращает дублирование и проблемы с экранированием Markdown.

    img_dir = art_dir / "images" # Директория для изображений статьи.
    images: List[str] = [] # Список путей к сохраненным изображениям.
    srcs = [] # Временный список URL изображений для параллельной загрузки.

    # Загрузка изображений параллельно с использованием ThreadPoolExecutor.
    with ThreadPoolExecutor(max_workers=5) as ex:
        # Собираем URL'ы изображений из тегов <img>, ограничиваясь первыми 10.
        for img in soup.find_all("img")[:10]:
            url = extract_img_url(img)
            if url:
                srcs.append(url)

        # Отправляем задачи на загрузку изображений в пул потоков.
        futures = {ex.submit(save_image, url, img_dir): url for url in srcs}
        # Ждем завершения задач и собираем результаты.
        for fut in as_completed(futures):
            try:
                if path := fut.result(): # Python 3.8+ "моржовый оператор" (walrus operator).
                    images.append(path)
            except Exception as e:
                logging.warning(f"Error saving image: {e}")

    # Если не найдено изображений в тегах <img>, пытаемся найти рекомендуемое медиа (_embedded).
    if not images and "_embedded" in post:
        media = post["_embedded"].get("wp:featuredmedia")
        if media and media[0].get("source_url"):
            path = save_image(media[0]["source_url"], img_dir)
            if path:
                images.append(path)

    # Если статья не имеет изображений, пропускаем ее (для публикации в Telegram нужны изображения).
    if not images:
        logging.warning("No images for ID=%s; skipping article parsing and saving.", aid)
        return None

    # Формирование метаданных статьи.
    meta = {
        "id": aid,
        "slug": slug,
        "date": post.get("date"),
        "link": post.get("link"),
        "title": title, # Используем отфильтрованный/переведенный заголовок.
        "text_file": str(art_dir / "content.txt"), # Путь к файлу с основным текстом статьи (не переведенным).
        "images": images,
        "posted": False, # Флаг, указывающий, была ли статья опубликована в Telegram.
        "hash": hashlib.sha256(raw_text.encode()).hexdigest() # Хэш основного текста для определения изменений.
    }
    # Сохраняем основной текст статьи в файл.
    (art_dir / "content.txt").write_text(raw_text, encoding="utf-8")

    # Если требуется перевод основного текста.
    if translate_to:
        current_hash = meta["hash"] # Хэш текущего состояния необработанного текста.
        old_meta = {}
        # Пробуем загрузить старые метаданные для сравнения.
        if meta_path.exists():
            try:
                old_meta = json.loads(meta_path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, Exception):
                pass # Игнорируем ошибки чтения старых метаданных.

        # Если контент изменился или язык перевода другой, переводим текст.
        if old_meta.get("hash") != current_hash or old_meta.get("translated_to") != translate_to:
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    # Получаем параграфы из оригинального HTML-контента статьи для перевода.
                    original_paras_for_translation = [
                        p.get_text(strip=True) for p in BeautifulSoup(post["content"]["rendered"], "html.parser").find_all("p")
                    ]
                    # Очищаем и фильтруем каждый параграф ПЕРЕД переводом.
                    clean_and_filtered_paras_for_translation = [
                        filter_text(bad_re.sub("", p), WORDS_TO_FILTER) for p in original_paras_for_translation
                    ]
                    
                    # Переводим ОТФИЛЬТРОВАННЫЕ параграфы.
                    trans = [
                        translate_text(p, to_lang=translate_to, provider="yandex") 
                        for p in clean_and_filtered_paras_for_translation
                    ]
                    
                    # Важно: ПЕРЕВЕДЕННЫЙ текст НЕ ФИЛЬТРУЕТСЯ СНОВА.

                    # Сохраняем переведенный текст в отдельный файл.
                    txt_t = art_dir / f"content.{translate_to}.txt"
                    trans_txt = "\n\n".join(trans)
                    # !!! ВАЖНО: Заголовок НЕ добавляется к переведенному тексту здесь.
                    txt_t.write_text(trans_txt, encoding="utf-8")

                    # Обновляем метаданные с информацией о переводе.
                    meta.update({
                        "translated_to": translate_to,
                        "translated_paras": trans,
                        "translated_file": str(txt_t),
                        "text_file": str(txt_t) # Теперь 'text_file' указывает на переведенный файл.
                    })
                    break # Выход из цикла повторных попыток после успешного перевода.
                except Exception as e:
                    delay = BASE_DELAY * 2 ** (attempt - 1)
                    logging.warning("Translate try %s failed: %s; retry in %.1fs", attempt, e, delay)
                    time.sleep(delay)
            else: # Если все попытки перевода провалились.
                logging.warning("Translation failed after max retries for ID=%s.", aid)
        else:
            logging.info("Using cached translation %s for ID=%s", translate_to, aid)

    # Сохраняем окончательные метаданные статьи в meta.json.
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    return meta

# --- Основная функция main() ---
def main():
    parser = argparse.ArgumentParser(description="Parser with translation")
    # Аргумент для базового URL сайта WordPress.
    parser.add_argument("--base-url", type=str,
                         default="https://www.khmertimeskh.com",
                         help="WP site base URL")
    # Аргумент для slug категории.
    parser.add_argument("--slug", type=str, default="national",
                         help="Category slug")
    # Аргумент для ограничения количества парсируемых постов.
    parser.add_argument("-n", "--limit", type=int, default=None,
                         help="Max posts to parse")
    # Аргумент для языка перевода (например, "ru" для русского).
    parser.add_argument("-l", "--lang", type=str, default="",
                         help="Translate to language code")
    # Аргумент для пути к файлу состояния уже опубликованных статей.
    parser.add_argument(
        "--posted-state-file",
        type=str,
        default="articles/posted.json",
        help="Путь к файлу состояния с ID уже опубликованных статей (только для чтения)"
    )
    args = parser.parse_args()

    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True) # Убеждаемся, что выходная директория существует.
        cid = fetch_category_id(args.base_url, args.slug) # Получаем ID категории.
        posts = fetch_posts(args.base_url, cid, per_page=(args.limit or 10)) # Получаем последние посты.

        catalog = load_catalog() # Загружаем существующий локальный каталог статей.
        existing_ids_in_catalog = {article["id"] for article in catalog} # Множество ID статей в текущем каталоге.

        # Загружаем ID статей, которые уже были опубликованы (из файла, который ведет poster.py).
        posted_ids_from_repo = load_posted_ids(Path(args.posted_state_file))

        logging.info(f"Loaded {len(posted_ids_from_repo)} posted IDs from {args.posted_state_file}.")
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.debug(f"Posted IDs: {posted_ids_from_repo}")

        new_articles_processed_in_run = 0 # Счетчик новых статей, обработанных в этом запуске.

        # Итерируем по полученным постам.
        for post in posts[:args.limit or len(posts)]:
            post_id = str(post["id"])

            # Проверяем, был ли пост уже опубликован (избегаем повторной публикации).
            if post_id in posted_ids_from_repo:
                logging.info(f"Skipping article ID={post_id} as it's already in {args.posted_state_file}.")
                continue

            is_in_local_catalog = post_id in existing_ids_in_catalog # Проверяем, есть ли статья в локальном каталоге.

            # Обработка статьи (парсинг и сохранение метаданных и содержимого).
            if meta := parse_and_save(post, args.lang, args.base_url):
                # parse_and_save возвращает meta, если статья новая или обновленная (изменен контент/перевод).
                
                # Удаляем старую запись статьи из каталога, если она уже там есть.
                catalog = [item for item in catalog if item["id"] != meta["id"]]
                catalog.append(meta) # Добавляем новую или обновленную запись в каталог.
                existing_ids_in_catalog.add(meta["id"]) # Обновляем множество ID в памяти.

                # Логика для подсчета "действительно новых" статей, которые не были ни опубликованы, ни в локальном каталоге ранее.
                if post_id not in posted_ids_from_repo and not is_in_local_catalog:
                    new_articles_processed_in_run += 1
                    logging.info(f"Processed new article ID={post_id} and added to local catalog.")
                elif post_id in posted_ids_from_repo and not is_in_local_catalog:
                    # Случай, когда статья была в posted.json, но по какой-то причине исчезла из catalog.json.
                    # Мы ее обрабатываем, но не считаем "новой" для индикатора статуса.
                    logging.info(f"Re-processed article ID={post_id} (found in posted.json, not in local catalog).")
                elif is_in_local_catalog:
                    # Случай, когда статья уже была в локальном каталоге, но ее контент или перевод изменились.
                    logging.info(f"Updated article ID={post_id} in local catalog (content changed or re-translated).")
                

        # Сохранение обновленного каталога и вывод статуса для CI/CD пайплайнов.
        if new_articles_processed_in_run > 0:
            save_catalog(catalog) # Сохраняем каталог, если были новые статьи.
            logging.info(f"Added {new_articles_processed_in_run} truly new articles. Total parsed articles in catalog: {len(catalog)}")
            print("NEW_ARTICLES_STATUS:true") # Статус для внешних скриптов.
        else:
            logging.info("No new articles found or processed that are not already in posted.json or local catalog.")
            print("NEW_ARTICLES_STATUS:false") # Статус для внешних скриптов.

    except Exception as e:
        logging.exception("Fatal error in main:") # Логирование критических ошибок.
        exit(1) # Выход с кодом ошибки.

# Точка входа в скрипт.
if __name__ == "__main__":
    main()
