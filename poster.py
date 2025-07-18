import os
import json
import argparse
import asyncio
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from io import BytesIO
from collections import deque

import httpx
from httpx import HTTPStatusError, ReadTimeout, Timeout
from PIL import Image

# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
# --- Константа для ограничения количества записей в posted.json ---
MAX_POSTED_RECORDS = 200 # Максимальное количество ID в posted.json
# ──────────────────────────────────────────────────────────────────────────────
HTTPX_TIMEOUT = Timeout(connect=10.0, read=60.0, write=10.0, pool=5.0)
MAX_RETRIES   = 3
RETRY_DELAY   = 5.0
DEFAULT_DELAY = 10.0 # Изменен с 5.0 на 10.0, как в вашей версии

def escape_markdown(text: str) -> str:
    """
    Экранирует спецсимволы для MarkdownV2, кроме звездочки (*), которая используется для жирного текста.
    """
    # Удаляем '*' из списка символов для экранирования, так как он будет использоваться для жирного текста.
    markdown_chars = r'\_[]()~`>#+-=|{}.!'
    return re.sub(r'([%s])' % re.escape(markdown_chars), r'\\\1', text)

def chunk_text(text: str, size: int = 4096) -> List[str]:
    """
    Делит текст на чанки длиной <= size, сохраняя абзацы.
    ЭТА ВЕРСИЯ ФУНКЦИИ ВЗЯТА ИЗ ВАШЕГО ПРЕДОСТАВЛЕННОГО КОДА, ТАК КАК ОНА БОЛЕЕ ГИБКАЯ.
    """
    norm = text.replace('\r\n', '\n')
    paras = [p for p in norm.split('\n\n') if p.strip()]
    chunks, curr = [], ""

    def split_long(p: str) -> List[str]:
        parts, sub = [], ""
        for w in p.split(" "):
            if len(sub) + len(w) + 1 > size:
                parts.append(sub)
                sub = w
            else:
                sub = (sub + " " + w).lstrip()
        if sub:
            parts.append(sub)
        return parts

    for p in paras:
        if len(p) > size:
            if curr:
                chunks.append(curr)
                curr = ""
            chunks.extend(split_long(p))
        else:
            if not curr:
                curr = p
            elif len(curr) + 2 + len(p) <= size:
                curr += "\n\n" + p
            else:
                chunks.append(curr)
                curr = p

    if curr:
        chunks.append(curr)
    return chunks


def apply_watermark(img_path: Path, scale: float = 0.45) -> bytes:
    """
    Накладывает watermark.png в правый верхний угол изображения с отступом.
    ДОПОЛНЕНИЯ: Добавлены проверки на наличие файла водяного знака и общая обработка ошибок.
    """
    try:
        base_img = Image.open(img_path).convert("RGBA")
        base_width, base_height = base_img.size

        script_dir = Path(__file__).parent
        watermark_path = script_dir / "watermark.png"
        if not watermark_path.exists():
            logging.warning("Watermark file not found at %s. Skipping watermark.", watermark_path)
            # Возвращаем оригинал, если нет водяного знака
            img_byte_arr = BytesIO()
            base_img.save(img_byte_arr, format='PNG')
            return img_byte_arr.getvalue()

        watermark_img = Image.open(watermark_path).convert("RGBA")

        # Resize watermark
        wm_width, wm_height = watermark_img.size
        new_wm_width = int(base_width * scale)
        new_wm_height = int(wm_height * (new_wm_width / wm_width))
        filt = getattr(Image.Resampling, "LANCZOS", Image.LANCZOS) # Для совместимости версий Pillow
        watermark_img = watermark_img.resize((new_wm_width, new_wm_height), resample=filt)

        # Create a transparent overlay
        overlay = Image.new("RGBA", base_img.size, (0, 0, 0, 0))

        # Position watermark (top-right, with some padding)
        padding = int(base_width * 0.02) # 2% padding от вашей предыдущей версии
        position = (base_width - new_wm_width - padding, padding)
        overlay.paste(watermark_img, position, watermark_img)

        # Composite the images для лучшего смешивания
        composite_img = Image.alpha_composite(base_img, overlay)

        # Save to bytes
        img_byte_arr = BytesIO()
        composite_img.save(img_byte_arr, format='PNG') # Сохраняем как PNG
        return img_byte_arr.getvalue()
    except Exception as e:
        logging.error(f"Failed to apply watermark to {img_path}: {e}")
        # В случае ошибки возвращаем оригинал
        try:
            img_byte_arr = BytesIO()
            Image.open(img_path).save(img_byte_arr, format='PNG')
            return img_byte_arr.getvalue()
        except Exception as e_orig:
            logging.error(f"Failed to load original image {img_path} after watermark error: {e_orig}")
            return b"" # Возвращаем пустые байты, если даже оригинал не загрузить


async def _post_with_retry(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    data: Dict[str, Any],
    files: Optional[Dict[str, Any]] = None
) -> bool:
    """
    Выполняет HTTP POST-запрос с повторными попытками и обработкой 429 Too Many Requests.
    ВАША ВЕРСИЯ ФУНКЦИИ, ПРИНЯТА КАК БОЛЕЕ ГИБКАЯ И С ДЕТАЛЬНЫМ ЛОГИРОВАНИЕМ.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # Используем data= и files= вместо json= для обработки multipart/form-data
            resp = await client.request(method, url, data=data, files=files, timeout=HTTPX_TIMEOUT)
            resp.raise_for_status()
            return True

        except ReadTimeout:
            logging.warning("⏱ Timeout %s/%s for %s", attempt, MAX_RETRIES, url)

        except HTTPStatusError as e:
            code = e.response.status_code
            text = e.response.text
            if code == 429:
                # Telegram присылает retry_after в JSON-параметрах
                info = e.response.json().get("parameters", {})
                wait = info.get("retry_after", RETRY_DELAY)
                logging.warning("🐢 Rate limited %s/%s: retry after %s seconds", attempt, MAX_RETRIES, wait)
                await asyncio.sleep(wait)
                continue # Продолжаем попытки после ожидания
            if 400 <= code < 500:
                logging.error("❌ %s %s: %s", method, code, text)
                return False # Для клиентских ошибок не повторяем
            logging.warning("⚠️ %s %s, retry %s/%s", method, code, attempt, MAX_RETRIES)
        except httpx.RequestError as e: # Обработка других сетевых ошибок httpx
            logging.warning(f"Request error on attempt {attempt + 1}/{MAX_RETRIES}: {e}")
        except Exception as e:
            logging.error(f"An unexpected error occurred on attempt {attempt + 1}/{MAX_RETRIES}: {e}")

        await asyncio.sleep(RETRY_DELAY)

    logging.error("☠️ Failed %s after %s attempts", url, MAX_RETRIES)
    return False


async def send_media_group(
    client: httpx.AsyncClient,
    token: str,
    chat_id: str,
    images: List[Path]
) -> bool:
    """
    Отправляет альбом фотографий без подписи.
    Все изображения проходят через apply_watermark.
    ДОПОЛНЕНИЯ: Ограничение на 10 изображений для медиагруппы Telegram.
    """
    url   = f"https://api.telegram.org/bot{token}/sendMediaGroup"
    media = []
    files = {}
    photo_count = 0

    if not images:
        logging.warning("No images provided for media group.")
        return False

    for idx, img_path in enumerate(images):
        if photo_count >= 10: # Telegram limit for media groups
            logging.warning("Telegram media group limit (10 images) reached. Skipping remaining images.")
            break
        try:
            image_bytes = apply_watermark(img_path)
            if not image_bytes:
                logging.warning(f"Skipping image {img_path} due to empty bytes after watermark processing.")
                continue

            key = f"file{idx}"
            files[key] = (img_path.name, image_bytes, "image/png") # img_path.name для имени файла
            media.append({
                "type": "photo",
                "media": f"attach://{key}"
            })
            photo_count += 1
        except Exception as e:
            logging.error(f"Error processing image {img_path} for media group: {e}")
            # Не прекращаем обработку, пробуем другие изображения

    if not media:
        logging.warning("No valid images to send in media group after processing.")
        return False

    data = {
        "chat_id": chat_id,
        "media": json.dumps(media, ensure_ascii=False)
    }
    return await _post_with_retry(client, "POST", url, data, files)


async def send_message(
    client: httpx.AsyncClient,
    token: str,
    chat_id: str,
    text: str
) -> bool:
    """
    Отправляет текстовое сообщение с разбором MarkdownV2.
    """
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = {
        "chat_id": chat_id,
        "text": escape_markdown(text),
        "parse_mode": "MarkdownV2",
        "disable_web_page_preview": True # Обычно полезно для статей
    }
    return await _post_with_retry(client, "POST", url, data)


def validate_article(
    art: Dict[str, Any],
    article_dir: Path
) -> Optional[Tuple[str, Path, List[Path]]]:
    """
    Проверяет структуру папки статьи и возвращает подготовленные данные.
    ВАША ВЕРСИЯ ФУНКЦИИ, ПРИНЯТА КАК БОЛЕЕ НАДЕЖНАЯ.
    """
    aid      = art.get("id")
    title    = art.get("title", "").strip()
    txt_name = Path(art.get("text_file", "")).name if art.get("text_file") else None # Проверяем на None
    imgs     = art.get("images", [])

    if not title:
        logging.error("Invalid title for article in %s (ID: %s). Skipping.", article_dir, aid)
        return None

    # Поиск текстового файла
    text_path: Optional[Path] = None
    if txt_name:
        candidate_path = article_dir / txt_name
        if candidate_path.is_file():
            text_path = candidate_path
    
    if not text_path: # Если не найдено по text_file или его не было
        # Приоритет RU-файлу, затем EN, затем любой txt
        if (article_dir / "content.ru.txt").is_file():
            text_path = article_dir / "content.ru.txt"
        elif (article_dir / "content.txt").is_file():
            text_path = article_dir / "content.txt"
        else:
            candidates = list(article_dir.glob("*.txt"))
            if candidates:
                text_path = candidates[0] # Берем первый найденный txt

    if not text_path or not text_path.is_file():
        logging.error("No text file found for article in %s (ID: %s). Skipping.", article_dir, aid)
        return None

    # Сбор картинок
    valid_imgs: List[Path] = []
    # Сначала проверяем пути из meta.json
    for name in imgs:
        p = article_dir / Path(name).name # Предполагаем, что имя файла в images ссылается на файл в корне статьи
        if not p.is_file():
            p = article_dir / "images" / Path(name).name # Или в подпапке 'images'
        if p.is_file():
            valid_imgs.append(p)

    # Если по путям из meta.json не найдено, ищем в подпапке 'images'
    if not valid_imgs:
        imgs_dir = article_dir / "images"
        if imgs_dir.is_dir():
            valid_imgs = [
                p for p in imgs_dir.iterdir()
                if p.suffix.lower() in (".jpg", ".jpeg", ".png")
            ]
        # Если изображений все еще нет, это может быть статья без изображений
        # logging.warning("No images found for article in %s (ID: %s). Proceeding without images.", article_dir, aid)
        # В данном случае, если изображений нет, send_media_group вернет False, и мы перейдем к отправке текста.


    # Подпись для медиагруппы/сообщения (ограничение 1024 символа для медиагрупп, для сообщений 4096)
    # Здесь используется для общей подписи, которая может быть заголовком
    cap = title if len(title) <= 1024 else title[:1023] + "…" # Обрезаем с многоточием для подписи к медиа
    
    return cap, text_path, valid_imgs


def load_posted_ids(state_file: Path) -> Set[int]:
    """
    Читает state-файл и возвращает set опубликованных ID.
    Поддерживает:
      - отсутствующий или пустой файл
      - список чисел [1,2,3]
      - список объектов [{"id":1}, {"id":2}]
    ВАША ВЕРСИЯ ФУНКЦИИ, ПРИНЯТА КАК БОЛЕЕ НАДЕЖНАЯ.
    """
    if not state_file.is_file():
        logging.info("State file %s not found. Returning empty set.", state_file)
        return set()

    text = state_file.read_text(encoding="utf-8").strip()
    if not text:
        logging.warning("State file %s is empty. Returning empty set.", state_file)
        return set()

    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        logging.warning("State file %s is not valid JSON. Returning empty set.", state_file)
        return set()

    if not isinstance(data, list):
        logging.warning("State file %s content is not a list. Returning empty set.", state_file)
        return set()

    ids: Set[int] = set()
    for item in data:
        if isinstance(item, dict) and "id" in item:
            try:
                ids.add(int(item["id"]))
            except (ValueError, TypeError):
                logging.warning("Invalid ID format in state file: %s. Skipping.", item)
                pass
        elif isinstance(item, (int, str)) and str(item).isdigit():
            ids.add(int(item))
        else:
            logging.warning("Unexpected item type in state file: %s. Skipping.", item)
    return ids


def save_posted_ids(all_ids_to_save: Set[int], state_file: Path) -> None:
    """
    Сохраняет список опубликованных ID статей в файл состояния.
    Сохраняет максимум MAX_POSTED_RECORDS, добавляя новые в начало и вытесняя старые в конце.
    ЭТА ФУНКЦИЯ ОСТАЕТСЯ КАК В ПРЕДЫДУЩЕМ РЕШЕНИИ, ПЛЮС ИМПОРТ `deque`.
    """
    state_file.parent.mkdir(parents=True, exist_ok=True) # Убедимся, что директория существует

    # 1. Загружаем текущие ID из файла (для сохранения порядка и избегания дубликатов)
    current_ids_list: deque = deque()
    if state_file.exists():
        try:
            with state_file.open("r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    # Извлекаем только ID, игнорируя старые форматы с объектами
                    for item in data:
                        if isinstance(item, dict) and "id" in item:
                            current_ids_list.append(item["id"])
                        elif isinstance(item, int):
                            current_ids_list.append(item)
                else:
                    logging.warning(f"State file {state_file} has unexpected format. Starting with fresh records.")
        except json.JSONDecodeError:
            logging.warning(f"State file {state_file} is corrupted. Starting with fresh records.")
        except Exception as e:
            logging.error(f"Error reading existing state file {state_file}: {e}. Starting with fresh records.")

    # 2. Создаем Set из текущих ID для быстрого поиска
    current_ids_set = set(current_ids_list)

    # 3. Объединяем новые ID с текущими, добавляя новые в начало
    # Используем deque для эффективного добавления в начало и ограничения размера
    temp_ids_deque = deque(maxlen=MAX_POSTED_RECORDS)

    # Сначала добавляем новые ID, которых не было ранее
    # Сортируем новые ID в убывающем порядке, чтобы более новые ID (если они последовательные)
    # попадали в начало очереди первыми, если их было несколько в текущем батче.
    for aid in sorted(list(all_ids_to_save - current_ids_set), reverse=True):
        temp_ids_deque.appendleft(aid)

    # Затем добавляем старые ID, которые уже были в файле и не были только что добавлены
    # Проходим по старым ID в том порядке, в котором они были в файле
    for aid in current_ids_list:
        if aid in all_ids_to_save: # Убедимся, что ID должен быть в итоговом списке (т.е. не отброшен)
            if aid not in temp_ids_deque: # Избегаем дублирования, если новый ID совпадает со старым
                temp_ids_deque.append(aid)

    # temp_ids_deque автоматически обрезает размер до MAX_POSTED_RECORDS,
    # удаляя элементы с конца при добавлении в начало.

    # 4. Сохраняем список ID в файл
    try:
        # Преобразуем deque обратно в list для сохранения
        final_list_to_save = list(temp_ids_deque)
        with state_file.open("w", encoding="utf-8") as f:
            json.dump(final_list_to_save, f, ensure_ascii=False, indent=2)
        logging.info(f"Saved {len(final_list_to_save)} IDs to state file {state_file} (max {MAX_POSTED_RECORDS}).")
    except Exception as e:
        logging.error(f"Failed to save state file {state_file}: {e}")


async def main(parsed_dir: str, state_path: str, limit: Optional[int]):
    """
    Основная функция для запуска постера.
    """
    token       = os.getenv("TELEGRAM_TOKEN")
    chat_id     = os.getenv("TELEGRAM_CHANNEL")
    if not token or not chat_id:
        logging.error("TELEGRAM_TOKEN or TELEGRAM_CHANNEL environment variables must be set.")
        return

    delay       = float(os.getenv("POST_DELAY", DEFAULT_DELAY))
    parsed_root = Path(parsed_dir)
    state_file  = Path(state_path)

    if not parsed_root.is_dir():
        logging.error("Parsed directory %s does not exist. Exiting.", parsed_root)
        return

    # 1) Загрузка уже опубликованных ID
    posted_ids_old = load_posted_ids(state_file)
    logging.info("Loaded %d previously posted IDs from %s.", len(posted_ids_old), state_file.name)

    # 2) Сбор папок со статьями и их валидация
    articles_to_post: List[Dict[str, Any]] = []
    for d in sorted(parsed_root.iterdir()): # Итерируем по папкам
        meta_file = d / "meta.json"
        if d.is_dir() and meta_file.is_file():
            try:
                art_meta = json.loads(meta_file.read_text(encoding="utf-8"))
                # Проверяем, что ID статьи еще не был опубликован
                if art_meta.get("id") is not None and art_meta["id"] not in posted_ids_old:
                    validated_data = validate_article(art_meta, d)
                    if validated_data:
                        # Добавляем ID в данные, чтобы передать его дальше
                        validated_data_dict = {
                            "id": art_meta["id"],
                            "caption": validated_data[0],
                            "text_path": validated_data[1],
                            "image_paths": validated_data[2]
                        }
                        articles_to_post.append(validated_data_dict)
                    else:
                        logging.warning("Article metadata validation failed for %s. Skipping.", d.name)
                elif art_meta.get("id") is not None:
                    logging.debug("Skipping already posted article ID=%s.", art_meta["id"])
                else:
                    logging.warning("Article in %s has no ID in meta.json. Skipping.", d.name)
            except json.JSONDecodeError as e:
                logging.warning("Cannot load or parse meta.json in %s: %s. Skipping.", d.name, e)
            except Exception as e:
                logging.error("An unexpected error occurred while processing article %s: %s. Skipping.", d.name, e)
    
    # Сортируем статьи по ID для стабильного порядка обработки
    # Предполагаем, что article["id"] является числом
    articles_to_post.sort(key=lambda x: x["id"])

    if not articles_to_post:
        logging.info("🔍 No new articles to post. Exiting.")
        return

    logging.info("Found %d new articles to consider for posting.", len(articles_to_post))

    client    = httpx.AsyncClient()
    sent      = 0
    new_ids: Set[int] = set() # ID, которые были успешно опубликованы в текущем запуске

    # 3) Публикация каждой статьи
    for article in articles_to_post:
        if limit is not None and sent >= limit:
            logging.info("Batch limit of %d reached. Stopping.", limit)
            break

        aid       = article["id"]
        caption   = article["caption"]
        text_path = article["text_path"]
        image_paths = article["image_paths"]

        logging.info("Attempting to post ID=%s", aid)
        
        posted_successfully = False
        try:
            # 3.1) Отправляем изображения (если есть).
            if image_paths:
                if not await send_media_group(client, token, chat_id, image_paths):
                    logging.warning("Failed to send media group for ID=%s. Proceeding to send text only (title already in text).", aid)
                    # Если медиагруппа не отправлена, пропускаем отправку отдельного заголовка.
                    # Переходим сразу к отправке основного текста.
                else:
                    # Если медиагруппа отправлена, здесь НЕ отправляем заголовок как отдельное сообщение.
                    # Ничего не делаем, так как заголовок не должен отправляться отдельно.
                    pass
            else:
                # Если изображений нет совсем, НЕ отправляем заголовок как отдельное сообщение.
                logging.info("No images for ID=%s. Proceeding to send text only (title already in text).", aid)
                # Переходим сразу к отправке основного текста.
            
            # 3.2) Тело статьи по чанкам
            raw_text = text_path.read_text(encoding="utf-8")
            
            # Формируем текст для отправки с жирным заголовком
            # Экранируем заголовок и делаем его жирным
            formatted_title = f"*{escape_markdown(caption)}*" # <-- Новая строка
            
            # Объединяем заголовок и основной текст
            # Добавляем две новые строки для лучшего отделения заголовка от тела
            full_text_to_post = f"{formatted_title}\n\n{raw_text}"
            chunks = chunk_text(full_text_to_post)
            all_chunks_sent = True
            for part in chunks:
                if not await send_message(client, token, chat_id, part):
                    logging.error("Failed to send a text chunk for ID=%s. Skipping remaining chunks and article.", aid)
                    all_chunks_sent = False
                    break
            
            if all_chunks_sent:
                posted_successfully = True

        except Exception as e:
            logging.error(f"❌ An error occurred during posting article ID={aid}: {e}. Moving to next article.")
            posted_successfully = False # Убедимся, что флаг сброшен при ошибке

        if posted_successfully:
            new_ids.add(aid) # Добавляем в Set новых успешно опубликованных ID
            sent += 1
            logging.info("✅ Posted ID=%s", aid)
        
        await asyncio.sleep(delay)

    await client.aclose()

    # 4) Сохраняем обновлённый список ID
    # Объединяем старые опубликованные ID с новыми, успешно опубликованными в этом запуске
    all_ids_to_save = posted_ids_old.union(new_ids)
    save_posted_ids(all_ids_to_save, state_file)
    logging.info("State updated. Total unique IDs to be saved: %d.", len(all_ids_to_save))
    logging.info("📢 Done: sent %d articles in this run.", sent)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Poster: публикует статьи пакетами в Telegram"
    )
    parser.add_argument(
        "--parsed-dir",
        type=str,
        default="articles", # Возвращаем к исходной директории
        help="директория с распарсенными статьями"
    )
    parser.add_argument(
        "--state-file",
        type=str,
        default="articles/posted.json", # Возвращаем к исходному файлу состояния
        help="путь к state-файлу"
    )
    parser.add_argument(
        "-n", "--limit",
        type=int,
        default=None,
        help="максимальное число статей для отправки"
    )

    args = parser.parse_args()

    asyncio.run(main(
        parsed_dir=args.parsed_dir,
        state_path=args.state_file,
        limit=args.limit
    ))
