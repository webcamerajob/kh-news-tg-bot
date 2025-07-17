#!/usr/bin/env python3
import argparse
import logging
import json
import hashlib
import time
import re
import os
import base64  # Для декодирования Data URIs
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Set, Tuple

# Убедитесь, что эти импорты у вас есть
from bs4 import BeautifulSoup
import cloudscraper  # Для fetch_category_id, fetch_posts, save_image
import translators as ts  # Для translate_text
import fcntl  # Для блокировки файлов в load_catalog и save_catalog
import requests # Для HTTP-запросов
from requests.exceptions import Timeout as ReqTimeout, RequestException # Для обработки ошибок HTTP-запросов

# Настройка переменной окружения (должна быть в начале, один раз)
os.environ["translators_default_region"] = "EN"

# Настройки логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Предполагаемые константы
OUTPUT_DIR = Path("articles")  # Директория для сохранения распарсенных статей.
CATALOG_PATH = OUTPUT_DIR / "catalog.json"  # Путь к файлу каталога статей.
MAX_RETRIES = 3  # Максимальное количество попыток для HTTP-запросов и переводов.
BASE_DELAY = 1.0  # Базовая задержка в секунду для экспоненциальной задержки при ретраях.

# Инициализация cloudscraper для обхода Cloudflare.
SCRAPER = cloudscraper.create_scraper()
# Тайм-аут для HTTP-запросов: (тайм-аут соединения, тайм-аут чтения).
SCRAPER_TIMEOUT = (10.0, 60.0)

# --- КОНСТАНТЫ И ФУНКЦИИ ДЛЯ ФИЛЬТРАЦИИ СЛОВ ---
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
        if word:
            cleaned_text = re.sub(r'\b' + re.escape(word) + r'\b', ' ', cleaned_text, flags=re.IGNORECASE)
    
    cleaned_text = re.sub(r"\s+", " ").strip()
    return cleaned_text
# --- КОНЕЦ КОНСТАНТ И ФУНКЦИЙ ФИЛЬТРАЦИИ ---


# --- Вспомогательные функции для извлечения изображений ---

def extract_main_image_url(html_content: str) -> Optional[str]:
    """
    Извлекает URL основного изображения из HTML-содержимого статьи.
    Приоритет: og:image, затем twitter:image, затем Schema.org (JSON-LD).
    """
    soup = BeautifulSoup(html_content, 'html.parser')

    og_image_meta = soup.find('meta', property='og:image')
    if og_image_meta and og_image_meta.get('content'):
        image_url = og_image_meta['content']
        logging.info(f"Main image found via og:image: {image_url}")
        return image_url

    twitter_image_meta = soup.find('meta', {'name': 'twitter:image'})
    if twitter_image_meta and twitter_image_meta.get('content'):
        image_url = twitter_image_meta['content']
        logging.info(f"Main image found via twitter:image: {image_url}")
        return image_url

    schema_script = soup.find('script', {'type': 'application/ld+json'})
    if schema_script and schema_script.string:
        try:
            schema_data = json.loads(schema_script.string)
            if isinstance(schema_data, list):
                for item in schema_data:
                    if isinstance(item, dict) and item.get('@type') == 'Article' and 'image' in item:
                        image_info = item['image']
                        if isinstance(image_info, dict) and 'url' in image_info:
                            image_url = image_info['url']
                            logging.info(f"Main image found via Schema.org (@graph list): {image_url}")
                            return image_url
            elif isinstance(schema_data, dict) and '@graph' in schema_data:
                for item in schema_data['@graph']:
                    if isinstance(item, dict) and item.get('@type') == 'Article' and 'image' in item:
                        image_info = item['image']
                        if isinstance(image_info, dict) and 'url' in image_info:
                            image_url = image_info['url']
                            logging.info(f"Main image found via Schema.org (@graph): {image_url}")
                            return image_url
            elif isinstance(schema_data, dict) and schema_data.get('@type') == 'Article' and 'image' in schema_data:
                image_info = schema_data['image']
                if isinstance(image_info, dict) and 'url' in image_info:
                    image_url = image_info['url']
                    logging.info(f"Main image found via Schema.org (direct): {image_url}")
                    return image_url
        except json.JSONDecodeError:
            logging.warning("JSON decode error from Schema.org script.")
        except Exception as e:
            logging.warning(f"An unexpected error occurred parsing Schema.org: {e}")

    logging.info("Main image not found via og:image, twitter:image, or Schema.org.")
    return None

def extract_img_url(img_tag: Any) -> Optional[str]:
    """
    Извлекает URL изображения из HTML-тега <img>.
    Проверяет различные атрибуты, в которых может храниться URL изображения.
    """
    for attr in ("data-src", "data-lazy-src", "data-srcset", "srcset", "src"):
        val = img_tag.get(attr)
        if not val:
            continue
        parts = val.split()
        if parts:
            return parts[0]
    return None

def get_extension_from_mime(mime_type: str) -> str:
    """Возвращает стандартное расширение файла для данного MIME-типа."""
    if 'jpeg' in mime_type or 'jpg' in mime_type:
        return 'jpg'
    elif 'png' in mime_type:
        return 'png'
    elif 'gif' in mime_type:
        return 'gif'
    elif 'svg' in mime_type:
        return 'svg'
    elif 'webp' in mime_type:
        return 'webp'
    return 'bin'  # По умолчанию .bin для неизвестных типов


def save_image(src_url: str, folder: Path, post_id: int) -> Optional[str]:
    """
    Сохраняет изображение по URL в указанную папку на локальном диске.
    Включает логику повторных попыток и обработку Data URIs.
    Возвращает относительный путь к сохраненному файлу, если успешно.
    """
    logging.info(f"Saving image from {src_url} to {folder} for post {post_id}...")
    folder.mkdir(parents=True, exist_ok=True)

    # Для HTTP/HTTPS URL, проверяем расширение в URL
    if not src_url.startswith("data:"):
        file_extension = Path(src_url.split('?', 1)[0]).suffix.lower()
        if file_extension not in ['.jpg', '.jpeg']:
            logging.warning(f"Skipping image {src_url} for post {post_id} as it's not a JPG/JPEG.")
            return None
    
    if src_url.startswith("data:"):
        try:
            # Парсим Data URI
            match = re.match(r"data:(?P<mime>[^;]+);base64,(?P<data>.+)", src_url)
            if not match:
                logging.warning(f"Invalid Data URI format for post {post_id}: {src_url[:100]}...")
                return None

            mime_type = match.group("mime")
            base64_data = match.group("data")

            # Проверяем MIME-тип для Data URI
            if 'jpeg' not in mime_type and 'jpg' not in mime_type:
                logging.warning(f"Skipping Data URI image for post {post_id} as its MIME type ({mime_type}) is not JPG/JPEG.")
                return None

            decoded_data = base64.b64decode(base64_data)
            extension = get_extension_from_mime(mime_type)
            
            # Создаем уникальное имя файла для Data URI
            # Используем хэш содержимого для уникальности имени файла
            data_hash = hashlib.sha256(decoded_data).hexdigest()[:10]
            dest = folder / f"{post_id}_{data_hash}.{extension}"

            dest.write_bytes(decoded_data)
            logging.info(f"Successfully saved Data URI image {dest} for post {post_id}.")
            return str(dest.relative_to(OUTPUT_DIR))

        except Exception as e:
            logging.error(f"Error saving Data URI image for post {post_id}: {e}")
            return None
    else:
        # Оригинальная логика для HTTP/HTTPS URL
        # Извлекаем имя файла из URL, убирая параметры запроса.
        fn = src_url.rsplit('/', 1)[-1].split('?', 1)[0]
        # Добавляем ID поста к имени файла.
        dest = folder / f"{post_id}_{fn}"
        
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = SCRAPER.get(src_url, timeout=SCRAPER_TIMEOUT)
                r.raise_for_status()
                dest.write_bytes(r.content)
                logging.info(f"Successfully saved image {dest} for post {post_id}.")
                return str(dest.relative_to(OUTPUT_DIR))
            except (ReqTimeout, RequestException) as e:
                delay = BASE_DELAY * 2 ** (attempt - 1)
                logging.warning(
                    "Timeout saving image %s (post %s) (try %s/%s): %s; retry in %.1fs",
                    fn, post_id, attempt, MAX_RETRIES, e, delay
                )
                time.sleep(delay)
            except Exception as e:
                logging.error(f"An unexpected error occurred while saving image {fn} (post {post_id}): {e}")
                break
        logging.error("Failed saving image %s (post %s) after %s attempts", fn, post_id, MAX_RETRIES)
        return None

# --- Вспомогательные функции (продолжение) ---
def load_posted_ids(state_file_path: Path) -> Set[str]:
    """
    Загружает множество ID из файла состояния (например, posted.json).
    Использует блокировку файла для безопасного чтения, чтобы избежать конфликтов.
    """
    try:
        if state_file_path.exists():
            with open(state_file_path, 'r', encoding='utf-8') as f:
                fcntl.flock(f, fcntl.LOCK_SH)
                return {str(item) for item in json.load(f)}
        return set()
    except (FileNotFoundError, json.JSONDecodeError, IOError) as e:
        logging.warning(f"Could not load posted IDs from {state_file_path}: {e}. Assuming empty set.")
        return set()
    except Exception as e:
        logging.warning(f"An unexpected error occurred loading posted IDs: {e}. Assuming empty set.")
        return set()


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

def load_catalog() -> List[Dict[str, Any]]:
    """
    Загружает каталог статей из catalog.json.
    Использует блокировку файла для безопасного чтения. Включает валидацию содержимого.
    """
    if not CATALOG_PATH.exists():
        return []
    try:
        with open(CATALOG_PATH, "r", encoding="utf-8") as f:
            fcntl.flock(f, fcntl.LOCK_SH)
            data = json.load(f)
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
    Сохраняет только минимальный набор полей (id, hash, translated_to, main_image_path) для экономии места и безопасности.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    minimal = []
    for item in catalog:
        if isinstance(item, dict) and "id" in item:
            minimal.append({
                "id": item["id"],
                "hash": item.get("hash", ""),
                "translated_to": item.get("translated_to", ""),
                "main_image_path": item.get("main_image_path", "")
            })
        else:
            logging.warning(f"Skipping malformed catalog entry: {item}")

    try:
        with open(CATALOG_PATH, "w", encoding="utf-8") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            json.dump(minimal, f, ensure_ascii=False, indent=2)
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
    return text

# Регулярные выражения для очистки текста от невидимых/лишних символов.
bad_re = re.compile(r"[\u200b-\u200f\uFEFF\u200E\u00A0]")

# Функция parse_and_save - основная логика обработки одной статьи.
def parse_and_save(post: Dict[str, Any], translate_to: str, base_url: str) -> Optional[Dict[str, Any]]:
    """Парсит, фильтрует, переводит и сохраняет статью, включая загрузку изображений."""
    aid, slug = post["id"], post["slug"]
    post_url = post["link"]
    art_dir = OUTPUT_DIR / f"{aid}_{slug}"
    art_dir.mkdir(parents=True, exist_ok=True)

    html_content = ""
    try:
        logging.info(f"Fetching full HTML for article ID={aid} from {post_url}")
        response = SCRAPER.get(post_url, timeout=SCRAPER_TIMEOUT)
        response.raise_for_status()
        html_content = response.text
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching HTML for article ID={aid}: {e}")
        return None

    meta_path = art_dir / "meta.json"
    if meta_path.exists():
        try:
            existing_meta = json.loads(meta_path.read_text(encoding="utf-8"))
            # Используем HTML-контент для хеширования, если он доступен
            current_post_content_hash = hashlib.sha256(html_content.encode()).hexdigest()
            
            # Проверяем, изменился ли контент (по хешу полного HTML) или язык перевода,
            # А также, были ли изображения успешно сохранены в прошлый раз.
            if existing_meta.get("hash") == current_post_content_hash and \
               existing_meta.get("translated_to", "") == translate_to and \
               existing_meta.get("images") and existing_meta.get("main_image_path"):
                logging.info(f"Skipping unchanged article ID={aid} (content, translation, and images match local cache).")
                return existing_meta
            elif existing_meta.get("hash") == current_post_content_hash and \
                 existing_meta.get("translated_to", "") == translate_to and \
                 (not existing_meta.get("images") or not existing_meta.get("main_image_path")):
                logging.info(f"Cached meta for ID={aid} missing 'images' or 'main_image_path', re-extracting images.")
                # Продолжаем выполнение, чтобы пересохранить изображения
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logging.warning(f"Failed to read existing meta for ID={aid}: {e}. Reparsing.")
        except Exception as e:
            logging.warning(f"An unexpected error occurred reading existing meta for ID={aid}: {e}. Reparsing.")


    orig_title = BeautifulSoup(post["title"]["rendered"], "html.parser").get_text(strip=True)
    orig_title = filter_text(orig_title, WORDS_TO_FILTER)
    title = orig_title

    if translate_to:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                title = translate_text(orig_title, to_lang=translate_to, provider="yandex")
                break
            except Exception as e:
                delay = BASE_DELAY * 2 ** (attempt - 1)
                logging.warning(
                    f"Translate title attempt {attempt}/{MAX_RETRIES} failed: {e}; retry in {delay:.1f}s"
                )
                time.sleep(delay)

    soup = BeautifulSoup(html_content, "html.parser")

    article_content_element = soup.find("div", class_="td-post-content")
    if not article_content_element:
        logging.warning(f"Could not find main content div (class='td-post-content') for article ID={aid}. Extracting paragraphs and images from full HTML, which may include non-content elements.")
        article_content_element = soup # Используем весь HTML как контент, если основной div не найден
    
    paras = [p.get_text(strip=True) for p in article_content_element.find_all("p")]
    
    raw_text = "\n\n".join(paras)
    raw_text = bad_re.sub("", raw_text)
    raw_text = re.sub(r"[ \t]+", " ", raw_text)
    raw_text = re.sub(r"\n{3,}", "\n\n", raw_text)
    
    raw_text = filter_text(raw_text, WORDS_TO_FILTER)

    img_dir = art_dir / "images"
    img_dir.mkdir(parents=True, exist_ok=True) # Ensure directory exists before saving
    
    images: List[str] = []
    main_image_path: Optional[str] = None
    collected_image_urls: Set[str] = set() # Для хранения уникальных URL-ов найденных изображений

    logging.info(f"Attempting to find main image for article ID={aid}...")
    extracted_main_img_url = extract_main_image_url(html_content)
    if extracted_main_img_url:
        file_extension = Path(extracted_main_img_url.split('?', 1)[0]).suffix.lower()
        # Проверяем, является ли изображение JPG/JPEG или Data URI с JPG MIME-типом
        if extracted_main_img_url.startswith("data:") or file_extension in ['.jpg', '.jpeg']:
            logging.info(f"Found main image URL: {extracted_main_img_url} for ID={aid}. Attempting to save.")
            saved_main_img_path = save_image(extracted_main_img_url, img_dir, aid)
            if saved_main_img_path:
                images.append(saved_main_img_path)
                main_image_path = saved_main_img_path
                collected_image_urls.add(extracted_main_img_url) # Добавляем оригинальный URL в set
                logging.info(f"Main image saved successfully: {main_image_path}")
            else:
                logging.warning(f"Failed to save main image from {extracted_main_img_url} for ID={aid}.")
        else:
            logging.warning(f"Skipping main image {extracted_main_img_url} for ID={aid} as it's not a JPG/JPEG.")
    else:
        logging.info(f"No main image found via meta tags for ID={aid}.")

    # Собираем вспомогательные изображения, убедившись, что это только JPG и всего до 10 изображений
    aux_srcs_to_save: List[str] = []
    current_image_count = len(images) # Количество уже сохраненных изображений (пока только главное, если есть)
    if current_image_count < 10:
        for img_tag in article_content_element.find_all("img"):
            if current_image_count >= 10: # Останавливаемся, если уже собрано достаточно изображений
                logging.info(f"Collected 10 images (including main). Skipping further auxiliary images for ID={aid}.")
                break
            
            url = extract_img_url(img_tag)
            
            if url and url not in collected_image_urls: # Проверяем на дубликаты
                # Фильтруем заведомо неконтентные изображения
                if any(keyword in url.lower() for keyword in ["logo", "icon", "ad", "spacer", "pixel.gif", "dot.gif", "empty.gif"]):
                    logging.debug(f"Skipping image with suspected logo/icon/ad/spacer URL: {url}")
                    continue
                
                # Проверяем, является ли изображение JPG/JPEG или Data URI с JPG MIME-типом
                file_extension = Path(url.split('?', 1)[0]).suffix.lower()
                if url.startswith("data:") or file_extension in ['.jpg', '.jpeg']:
                    aux_srcs_to_save.append(url)
                    collected_image_urls.add(url) # Добавляем в set, чтобы избежать дубликатов
                    current_image_count += 1 # Увеличиваем счетчик
                else:
                    logging.debug(f"Skipping auxiliary image {url} for ID={aid} as it's not a JPG/JPEG.")
    
    # Параллельно сохраняем собранные вспомогательные изображения
    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = {ex.submit(save_image, url, img_dir, aid): url for url in aux_srcs_to_save}
        for fut in as_completed(futures):
            try:
                if path := fut.result():
                    if path not in images: # Добавляем только если еще не в списке (главное фото уже добавлено)
                        images.append(path)
            except Exception as e:
                logging.warning(f"Error saving content image from future: {e}")

    # Fallback на _embedded featured media, если изображений все еще недостаточно
    if len(images) < 10 and "_embedded" in post:
        media = post["_embedded"].get("wp:featuredmedia")
        if media and media[0].get("source_url"):
            fallback_img_url = media[0]["source_url"]
            if fallback_img_url not in collected_image_urls: # Избегаем дубликатов
                file_extension = Path(fallback_img_url.split('?', 1)[0]).suffix.lower()
                if fallback_img_url.startswith("data:") or file_extension in ['.jpg', '.jpeg']:
                    logging.info(f"Attempting fallback to _embedded featured media: {fallback_img_url} for ID={aid}.")
                    path = save_image(fallback_img_url, img_dir, aid)
                    if path and path not in images: # Убеждаемся в уникальности
                        images.append(path)
                        if not main_image_path: # Устанавливаем как главное, если главное еще не было найдено
                            main_image_path = path
                    else:
                        logging.warning(f"Failed to save _embedded featured media or it was already present for ID={aid}.")
                else:
                    logging.warning(f"Skipping _embedded featured media {fallback_img_url} for ID={aid} as it's not a JPG/JPEG.")

    # Финализируем список изображений: главное фото первым, остальные до 10 штук.
    final_images_list = []
    if main_image_path and main_image_path in images:
        final_images_list.append(main_image_path)
        # Удаляем его из временного списка, чтобы не добавить снова
        images_copy = [p for p in images if p != main_image_path] 
    else:
        images_copy = list(images) # Если main_image_path не задан или не был сохранен

    # Добавляем остальные изображения до общего количества в 10
    for img_path in images_copy:
        if len(final_images_list) < 10:
            final_images_list.append(img_path)
        else:
            break # Останавливаемся, если достигли 10 изображений

    # Если главное фото не было найдено изначально, но есть другие фото,
    # назначаем первое сохраненное фото как главное
    if not main_image_path and final_images_list:
        main_image_path = final_images_list[0]
    elif not final_images_list:
        main_image_path = None # Явно устанавливаем None, если JPG-изображений не найдено вовсе

    if not final_images_list:
        logging.warning("No JPG images found for ID=%s after all attempts; skipping article parsing and saving.", aid)
        return None

    meta = {
        "id": aid,
        "slug": slug,
        "date": post.get("date"),
        "link": post.get("link"),
        "title": title,
        "text_file": str(art_dir / "content.txt"), 
        "images": final_images_list, # Теперь это будет содержать несколько JPG-изображений, до 10
        "main_image_path": main_image_path,
        "posted": False,
        "hash": hashlib.sha256(raw_text.encode("utf-8")).hexdigest() # Хеш текста
    }
    (art_dir / "content.txt").write_text(raw_text, encoding="utf-8")

    if translate_to:
        current_hash = meta["hash"]
        old_meta = {}
        if meta_path.exists():
            try:
                old_meta = json.loads(meta_path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, Exception):
                pass

        if old_meta.get("hash") != current_hash or old_meta.get("translated_to") != translate_to:
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    original_paras_for_translation = [
                        p.get_text(strip=True) for p in article_content_element.find_all("p")
                    ]
                    clean_and_filtered_paras_for_translation = [
                        filter_text(bad_re.sub("", p), WORDS_TO_FILTER) for p in original_paras_for_translation
                    ]
                    
                    trans = [
                        translate_text(p, to_lang=translate_to, provider="yandex") 
                        for p in clean_and_filtered_paras_for_translation
                    ]
                    
                    txt_t = art_dir / f"content.{translate_to}.txt"
                    trans_txt = "\n\n".join(trans)
                    # Добавляем заголовок к переведенному тексту
                    txt_t.write_text(f"**{title}**\n\n{trans_txt}", encoding="utf-8")

                    meta.update({
                        "translated_to": translate_to,
                        "translated_paras": trans,
                        "translated_file": str(txt_t),
                        "text_file": str(txt_t) # Обновляем основной файл для чтения на переведенный
                    })
                    break
                except Exception as e:
                    delay = BASE_DELAY * 2 ** (attempt - 1)
                    logging.warning(f"Translation try {attempt}/{MAX_RETRIES} failed: {e}; retry in {delay:.1f}s")
                    time.sleep(delay)
            else: # Это `else` относится к `for` циклу, если не было `break`
                logging.warning("Translation failed after max retries for ID=%s. Using original text.", aid)
                meta["text_file"] = str(art_dir / "content.txt") # Возвращаем к оригинальному тексту
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
        --posted-state-file",
        type=str,
        default="articles/posted.json",
        help="Путь к файлу состояния с ID уже опубликованных статей (только для чтения)"
    )
    args = parser.parse_args()

    new_articles_found_status = False

    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        cid = fetch_category_id(args.base_url, args.slug)
        
        posts = fetch_posts(args.base_url, cid, per_page=(args.limit if args.limit is not None else 30))

        catalog = load_catalog()
        existing_ids_in_catalog = {article["id"] for article in catalog}

        posted_ids_from_repo = load_posted_ids(Path(args.posted_state_file))

        logging.info(f"Loaded {len(posted_ids_from_repo)} posted IDs from {args.posted_state_file}.")
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.debug(f"Posted IDs: {posted_ids_from_repo}")

        new_articles_processed_in_run = 0
        
        for post in posts:
            aid = str(post["id"])
            
            if aid in posted_ids_from_repo:
                logging.info(f"Skipping article ID={aid} as it's already in {args.posted_state_file}.")
                continue
            
            logging.info(f"Processing article ID={aid}...")
            
            if meta := parse_and_save(post, args.lang, args.base_url):
                if aid not in existing_ids_in_catalog:
                    catalog.append(meta)
                    new_articles_processed_in_run += 1
                    new_articles_found_status = True
                    logging.info(f"Article ID={aid} added to catalog.")
                else:
                    # Обновляем запись в каталоге, если она уже существует
                    # Удаляем старую и добавляем новую, чтобы обновились все поля
                    catalog = [item for item in catalog if item["id"] != aid]
                    catalog.append(meta)
                    logging.info(f"Article ID={aid} updated in catalog (content or images re-processed).")
            else:
                logging.warning(f"Failed to parse or save article ID={aid}. Skipping.")

        if new_articles_processed_in_run > 0:
            save_catalog(catalog)
            logging.info(f"Saved updated catalog.json with {new_articles_processed_in_run} new/updated articles.")
        else:
            logging.info("No new articles processed or catalog updates needed in this run.")

    except RuntimeError as e:
        logging.critical(f"A critical error occurred: {e}")
        new_articles_found_status = False
    except Exception as e:
        logging.critical(f"An unhandled error occurred in main: {e}", exc_info=True)
        new_articles_found_status = False

    finally:
        print(f"NEW_ARTICLES_STATUS:{str(new_articles_found_status).lower()}")
        print("→ PARSER RUN COMPLETE")

if __name__ == "__main__":
    main()
