#!/usr/bin/env python3
# coding: utf-8

import os
import json
import argparse
import asyncio
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from io import BytesIO

import httpx
from httpx import HTTPStatusError, ReadTimeout, Timeout
from PIL import Image

# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO, # Уровень логирования: INFO и выше.
    format="%(asctime)s [%(levelname)s] %(message)s" # Формат сообщений лога.
)
# ──────────────────────────────────────────────────────────────────────────────

# Константы для HTTPX (HTTP-клиент).
HTTPX_TIMEOUT = Timeout(connect=10.0, read=60.0, write=10.0, pool=5.0) # Тайм-ауты для различных операций.
MAX_RETRIES     = 3 # Максимальное количество повторных попыток при сетевых ошибках.
RETRY_DELAY     = 5.0 # Задержка между повторными попытками.
DEFAULT_DELAY = 10.0 # Задержка между отправкой статей.
POSTED_IDS_LIMIT = 200 # Новый лимит для количества записей в posted.json

def escape_markdown(text: str) -> str:
    """
    Экранирует специальные символы для форматирования MarkdownV2 в Telegram,
    которые не должны интерпретироваться как часть форматирования.
    Применяется только к *содержимому* текста, а не к самим символам форматирования.
    """
    # Список символов, которые нужно экранировать.
    # Обратите внимание: `*` и `_` экранируются, так как текст должен быть уже подготовлен
    # и обёрнут в форматирующие символы (`*text*`) снаружи этой функции.
    
    markdown_chars_to_escape = r'\_*[]()~`>#+-=|{}.!' # Список символов, которые нужно экранировать.
    return re.sub(r'([%s])' % re.escape(markdown_chars_to_escape), r'\\\1', text)


def chunk_text(text: str, size: int = 4096) -> List[str]:
    """
    Делит длинный текст на чанки (части) длиной не более `size`,
    стараясь сохранить целостность абзацев (разделяя по двойным переносам строк).
    Максимальный размер сообщения в Telegram составляет 4096 символов.
    """
    chunks = []
    current_chunk = []
    current_length = 0

    # Разделяем текст на абзацы.
    paragraphs = text.split('\n\n')

    for para in paragraphs:
        # Если добавление следующего абзаца превысит лимит, сохраняем текущий чанк.
        if current_length + len(para) + (2 if current_chunk else 0) > size:
            chunks.append('\n\n'.join(current_chunk))
            current_chunk = [para]
            current_length = len(para)
        else:
            # Добавляем абзац в текущий чанк.
            current_chunk.append(para)
            current_length += len(para) + (2 if len(current_chunk) > 1 else 0)
    
    # Добавляем последний чанк, если он не пуст.
    if current_chunk:
        chunks.append('\n\n'.join(current_chunk))
    
    return chunks

# ──────────────────────────────────────────────────────────────────────────────

class TelegramAPI:
    """
    Класс для взаимодействия с API Telegram.
    Использует httpx для асинхронных HTTP-запросов.
    """
    def __init__(self, bot_token: str, chat_id: str):
        self.base_url = f"https://api.telegram.org/bot{bot_token}"
        self.chat_id = chat_id
        self.client = httpx.AsyncClient(timeout=HTTPX_TIMEOUT) # Асинхронный HTTP-клиент.

    async def _send_request(self, method: str, **kwargs: Any) -> Dict[str, Any]:
        """
        Внутренний метод для отправки HTTP-запросов к API Telegram.
        Реализует логику повторных попыток при сетевых ошибках.
        """
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                # Отправка POST-запроса к API Telegram.
                resp = await self.client.post(f"{self.base_url}/{method}", **kwargs)
                resp.raise_for_status() # Вызывает исключение для HTTP ошибок (4xx, 5xx).
            except (ReadTimeout, Timeout, HTTPStatusError, httpx.RequestError) as e:
                # Обработка сетевых ошибок и тайм-аутов.
                logging.warning(
                    "Error sending %s (attempt %s/%s): %s. Retrying in %.1fs...",
                    method, attempt, MAX_RETRIES, e, RETRY_DELAY
                )
                await asyncio.sleep(RETRY_DELAY)
                continue # Продолжаем к следующей попытке
            
            # Если запрос успешен, но Telegram API вернул ошибку
            json_resp = resp.json()
            if not json_resp.get("ok"):
                logging.error(f"Telegram API error for {method}: {json_resp.get('description', 'Unknown error')}")
                # Для некоторых ошибок, например, слишком длинного текста, может быть полезно не повторять.
                # Но для сетевых ошибок повторы важны.
                # Если это ошибка, которую можно исправить повтором, можно продолжить, иначе поднять исключение
                if json_resp.get("error_code") in [429, 500, 502, 503, 504]: # Retry for common transient errors
                    logging.warning(
                        "Telegram API returned non-OK (attempt %s/%s): %s. Retrying in %.1fs...",
                        attempt, MAX_RETRIES, json_resp.get('description', 'Unknown error'), RETRY_DELAY
                    )
                    await asyncio.sleep(RETRY_DELAY)
                    continue
                else:
                    raise RuntimeError(f"Telegram API error for {method}: {json_resp.get('description', 'Unknown error')}")
            return json_resp

        raise RuntimeError(f"Failed to send {method} after {MAX_RETRIES} attempts.")

    async def send_message(self, text: str) -> bool:
        """
        Отправляет текстовое сообщение в Telegram-канал.
        Предполагает, что текст уже правильно подготовлен для MarkdownV2,
        с экранированными спецсимволами, где это необходимо, и включенным форматированием.
        """
        payload = {
            "chat_id": self.chat_id,
            "text": text, # Используем текст напрямую, так как он уже должен быть подготовлен для MarkdownV2.
            "parse_mode": "MarkdownV2"
        }
        try:
            resp = await self._send_request("sendMessage", json=payload)
            return resp.get("ok", False)
        except Exception as e:
            logging.error("Failed to send message: %s", e)
            return False

    async def send_photo(self, photo_path: Path, caption: Optional[str] = None) -> bool:
        """
        Отправляет фотографию в Telegram-канал с подписью.
        Подпись (caption) также форматируется жирным и экранируется для MarkdownV2.
        Эта функция используется для отправки ОДИНОЧНЫХ фотографий, не входящих в альбом.
        """
        if not photo_path.exists():
            logging.error("Photo file not found: %s", photo_path)
            return False

        # Открываем изображение и сжимаем его, если оно слишком большое.
        try:
            img = Image.open(photo_path)
            # Изменение размера, если изображение слишком большое для Telegram (до 10MB и 10000px в любой стороне).
            max_dim = 1280 # Например, максимальная сторона.
            if img.width > max_dim or img.height > max_dim:
                img.thumbnail((max_dim, max_dim), Image.LANCZOS) # LANCZOS для лучшего качества.
            
            bio = BytesIO()
            # Сохраняем в JPEG с умеренным качеством для уменьшения размера.
            img.save(bio, format="JPEG", quality=85)
            bio.seek(0)
            
            if bio.tell() > 10 * 1024 * 1024: # Проверка размера файла > 10MB
                logging.warning("Compressed image too large (>10MB): %s. Skipping.", photo_path)
                return False

        except Exception as e:
            logging.error("Error processing image %s: %s", photo_path, e)
            return False

        files = {"photo": (photo_path.name, bio, "image/jpeg")}
        
        payload: Dict[str, Any] = {
            "chat_id": self.chat_id,
            "parse_mode": "MarkdownV2"
        }
        
        # Если есть подпись, форматируем ее как жирный текст и экранируем.
        if caption:
            payload["caption"] = caption # Предполагаем, что caption уже отформатирован/экранирован
            
        try:
            resp = await self._send_request("sendPhoto", files=files, data=payload)
            return resp.get("ok", False)
        except Exception as e:
            logging.error("Failed to send photo: %s", e)
            return False

    async def send_media_group(self, media_items: List[Dict[str, Any]]) -> bool:
        """
        Отправляет группу фотографий (альбом) в Telegram-канал.
        Максимум 10 фотографий за раз.
        `media_items` - список словарей, где каждый словарь содержит 'type' и 'media_path' (Path object).
        """
        if not media_items:
            logging.warning("No media items provided for send_media_group.")
            return False

        files = {}
        telegram_media = []
        
        # Подготавливаем файлы и объекты media для API Telegram
        for i, item in enumerate(media_items):
            media_type = item.get("type")
            photo_path = item.get("media_path") # Это кастомный ключ для передачи Path объекта
            
            if media_type != "photo" or not photo_path or not photo_path.exists():
                logging.warning(f"Invalid or non-existent media item {i} (path: {photo_path}). Skipping.")
                continue

            try:
                img = Image.open(photo_path)
                max_dim = 1280
                if img.width > max_dim or img.height > max_dim:
                    img.thumbnail((max_dim, max_dim), Image.LANCZOS)
                
                bio = BytesIO()
                img.save(bio, format="JPEG", quality=85)
                bio.seek(0)

                if bio.tell() > 10 * 1024 * 1024: # Проверка размера файла > 10MB
                    logging.warning(f"Compressed image {photo_path} for media group too large (>10MB). Skipping.")
                    continue
                
                # Прикрепляем файл к словарю 'files' с уникальным именем
                file_id = f"attach{i}"
                files[file_id] = (photo_path.name, bio, "image/jpeg")
                
                # Формируем объект InputMediaPhoto для Telegram API
                telegram_media_item = {
                    "type": "photo",
                    "media": f"attach://{file_id}"
                }
                # Подпись для медиа-группы может быть только у первого элемента,
                # но по вашему требованию, текст и заголовок не входят в группу фото,
                # поэтому здесь caption не добавляем.
                
                telegram_media.append(telegram_media_item)

            except Exception as e:
                logging.error(f"Error processing image {photo_path} for media group: {e}. Skipping.")
                continue
        
        if not telegram_media:
            logging.warning("No valid photo items to send in the media group.")
            return False

        payload = {
            "chat_id": self.chat_id,
            "media": json.dumps(telegram_media) # Массив media должен быть JSON-строкой
        }

        try:
            resp = await self._send_request("sendMediaGroup", files=files, data=payload)
            return resp.get("ok", False)
        except Exception as e:
            logging.error("Failed to send media group: %s", e)
            return False

    async def aclose(self):
        """Закрывает HTTPX клиент."""
        await self.client.aclose()


# --- Функции для управления файлом состояния (posted.json) ---
import fcntl # Импорт fcntl для блокировки файлов.

def load_posted_ids(state_file_path: Path) -> List[str]:
    """
    Загружает список ID уже опубликованных статей из файла состояния.
    Теперь возвращает список для сохранения порядка.
    """
    try:
        if state_file_path.exists():
            with open(state_file_path, 'r', encoding='utf-8') as f:
                fcntl.flock(f, fcntl.LOCK_SH) # Разделяемая блокировка для чтения.
                loaded_data = json.load(f)
                if isinstance(loaded_data, list):
                    return [str(item) for item in loaded_data]
                else:
                    logging.warning(f"Content of {state_file_path} is not a list. Reinitializing.")
                    return []
        return []
    except (FileNotFoundError, json.JSONDecodeError, IOError) as e:
        logging.warning(f"Could not load posted IDs from {state_file_path}: {e}. Assuming empty list.")
        return []
    except Exception as e:
        logging.warning(f"An unexpected error occurred loading posted IDs: {e}. Assuming empty list.")
        return []

def save_posted_ids(ids: List[str], state_file_path: Path) -> None:
    """
    Сохраняет текущий список опубликованных ID в файл состояния.
    Теперь принимает и сохраняет список, не выполняя сортировку.
    """
    state_file_path.parent.mkdir(parents=True, exist_ok=True) # Убедимся, что директория существует.
    try:
        with open(state_file_path, 'w', encoding='utf-8') as f:
            fcntl.flock(f, fcntl.LOCK_EX) # Эксклюзивная блокировка для записи.
            json.dump(ids, f, ensure_ascii=False, indent=2) # Сохраняем как есть (порядок важен).
    except IOError as e:
        logging.error(f"Failed to save posted IDs to {state_file_path}: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred saving posted IDs: {e}")

# ──────────────────────────────────────────────────────────────────────────────

async def main_poster(parsed_dir: Path, state_file: str, bot_token: str, chat_id: str,
                      delay: float = DEFAULT_DELAY, limit: Optional[int] = None):
    """
    Основная асинхронная функция для публикации статей.
    """
    client = TelegramAPI(bot_token, chat_id) # Инициализация Telegram клиента.
    
    # Загружаем уже опубликованные ID как список для сохранения порядка.
    posted_ids_old: List[str] = load_posted_ids(Path(state_file))    
    new_ids_this_run: List[str] = [] # Множество для ID, успешно опубликованных в этом запуске (в порядке их публикации).
    sent = 0 # Счетчик отправленных статей.

    # Собираем все метаданные статей, которые еще не были опубликованы.
    articles_to_post = []
    # Используем Set для быстрого поиска по уже загруженным ID.
    posted_ids_old_set = set(posted_ids_old)    
    
    for art_dir in parsed_dir.iterdir(): # Итерируем по переданному объекту Path
        if art_dir.is_dir():
            meta_path = art_dir / "meta.json"
            if meta_path.exists():
                try:
                    with open(meta_path, 'r', encoding='utf-8') as f:
                        meta = json.load(f)
                        # Проверяем, есть ли ID в старом списке опубликованных.
                        if str(meta.get("id")) not in posted_ids_old_set:
                            articles_to_post.append(meta)
                except (json.JSONDecodeError, IOError) as e:
                    logging.warning(f"Failed to read meta.json for {art_dir}: {e}. Skipping.")
                except Exception as e:
                    logging.warning(f"An unexpected error occurred reading meta.json for {art_dir}: {e}. Skipping.")

    # Сортируем статьи по дате (если доступно), чтобы публиковать более старые первыми.
    articles_to_post.sort(key=lambda x: x.get("date", ""), reverse=False)

    if limit: # Ограничиваем количество статей для публикации, если указан лимит.
        articles_to_post = articles_to_post[:limit]

    if not articles_to_post:
        logging.info("No new articles to post.")
        await client.aclose()
        return

    logging.info("Found %d new articles to post.", len(articles_to_post))

    for article in articles_to_post:
        aid = str(article["id"])
        logging.info("Attempting to post ID=%s...", aid)

        # 1) Подготовка и отправка группы изображений (альбома).
        images_for_media_group: List[Path] = []
        
        # Собираем все пути к изображениям из meta.json
        if article.get("images") and article["images"]:
            seen_full_paths = set()

            # Обрабатываем главное изображение первым (из article["images"][0])
            if len(article["images"]) > 0:
                main_image_path_str = article["images"][0]
                if main_image_path_str.startswith("articles/"):
                    relative_path = main_image_path_str[len("articles/"):]
                else:
                    relative_path = main_image_path_str
                
                full_path = parsed_dir / relative_path
                if full_path.exists():
                    images_for_media_group.append(full_path)
                    seen_full_paths.add(full_path)
                else:
                    logging.warning(f"Main image file not found for ID={aid} at path: {full_path}. It will be skipped.")
            
            # Собираем до 9 дополнительных изображений (всего 10)
            aux_images_count = 0
            for i, img_path_str in enumerate(article["images"]):
                if i == 0: # Пропускаем главное изображение, оно уже обработано
                    continue
                
                if aux_images_count >= 9: # Ограничиваем до 10 фотографий всего (1 главная + 9 дополнительных)
                    logging.info(f"Limiting media group to 10 images for ID={aid}. Skipping further auxiliary images.")
                    break

                if img_path_str.startswith("articles/"):
                    relative_path = img_path_str[len("articles/"):]
                else:
                    relative_path = img_path_str
                
                full_path = parsed_dir / relative_path
                
                if full_path.exists() and full_path not in seen_full_paths:
                    images_for_media_group.append(full_path)
                    seen_full_paths.add(full_path)
                    aux_images_count += 1
                elif not full_path.exists():
                    logging.warning(f"Auxiliary image file not found for ID={aid} at path: {full_path}. Skipping this image.")
        
        posted_successfully = False
        if images_for_media_group:
            media_group_items = []
            for img_path in images_for_media_group:
                media_group_items.append({
                    "type": "photo",
                    "media_path": img_path # Кастомный ключ для передачи объекта Path
                    # Нет подписи, согласно вашему требованию
                })
            
            logging.info(f"Attempting to send media group with {len(media_group_items)} photos for ID={aid}...")
            posted_successfully = await client.send_media_group(media_group_items)
            if not posted_successfully:
                logging.error("Failed to send photo group for ID=%s. Skipping article.", aid)
                continue # Если не удалось отправить группу фото, пропускаем всю статью
        else:
            logging.info("No valid images found for ID=%s to form a group. Proceeding with text only.", aid)
            # Если изображений нет, этот шаг считается "успешным", чтобы продолжить отправку текста.
            posted_successfully = True

        # 2) Отправка текста статьи, включая заголовок в начале.
        if posted_successfully: # Текст отправляется только если предыдущий шаг (фото) был успешен или если фото не было
            text_file_path = None
            if article.get("text_file"): # Проверяем, что ключ text_file существует
                original_text_path_str = article["text_file"] # Например, "articles/ID_SLUG/content.ru.txt"
                
                # Удаляем префикс "articles/" и строим путь относительно parsed_dir
                if original_text_path_str.startswith("articles/"):
                    relative_path_from_articles_root = original_text_path_str[len("articles/"):]
                else:
                    relative_path_from_articles_root = original_text_path_str
                
                text_file_path = parsed_dir / relative_path_from_articles_root

            if text_file_path and text_file_path.exists():
                try:
                    text_content = text_file_path.read_text(encoding="utf-8")
                    
                    # Экранируем только содержимое заголовка
                    escaped_title_content = escape_markdown(article['title'])
                    
                    # Форматируем заголовок как жирный текст для MarkdownV2, используя одинарные звездочки.
                    # Звездочки *не экранируются*, так как send_message теперь ожидает готовый Markdown.
                    formatted_title = f"*{escaped_title_content}*"
                    
                    # Экранируем основной текст, чтобы любые спецсимволы в нем не интерпретировались как Markdown
                    escaped_text_content = escape_markdown(text_content)

                    # Объединяем отформатированный заголовок и экранированный основной текст
                    full_text_to_send = f"{formatted_title}\n\n{escaped_text_content}"

                    # Разбиваем текст на чанки, так как Telegram имеет лимит на размер сообщения.
                    text_chunks = chunk_text(full_text_to_send)
                    for i, chunk in enumerate(text_chunks):
                        if not await client.send_message(chunk): # Отправка каждого чанка.
                            logging.error("Failed to send text chunk %d/%d for ID=%s.", i+1, len(text_chunks), aid)
                            posted_successfully = False # Если чанк не отправлен, вся статья считается неудачной.
                            break
                        await asyncio.sleep(1) # Небольшая задержка между чанками.
                except (IOError, UnicodeDecodeError) as e:
                    logging.error(f"Failed to read text file {text_file_path} for ID={aid}: {e}. Skipping text.")
                    posted_successfully = False
                except Exception as e:
                    logging.error(f"An unexpected error occurred reading text file for ID={aid}: {e}. Skipping text.")
                    posted_successfully = False
            else:
                logging.warning("Text file not found for ID=%s (path tried: %s). Skipping text.", aid, text_file_path)
                # Если текстовый файл не найден, это тоже считается неудачей.
                posted_successfully = False

        # 3) Обновляем список опубликованных ID, если статья была успешно отправлена.
        if posted_successfully:
            new_ids_this_run.append(aid) # Добавляем в список новых успешно опубликованных ID.
            sent += 1
            logging.info("✅ Posted ID=%s", aid)
            
        await asyncio.sleep(delay) # Задержка перед отправкой следующей статьи.

    await client.aclose() # Закрываем HTTPX клиент.

    # 4) Сохраняем обновлённый список ID с учетом лимита и порядка.
    combined_ids: List[str] = []
    seen_ids: Set[str] = set()

    # Добавляем новые ID из текущего запуска в начало списка.
    # Они уже в нужном порядке благодаря .append(aid) в цикле выше.
    for aid in new_ids_this_run:
        if aid not in seen_ids:
            combined_ids.append(aid)
            seen_ids.add(aid)
            
    # Добавляем старые ID, которые еще не были добавлены, до достижения лимита.
    # Это гарантирует, что старые записи будут в конце, а новые — в начале.
    for aid in posted_ids_old:
        if aid not in seen_ids and len(combined_ids) < POSTED_IDS_LIMIT:
            combined_ids.append(aid)
            seen_ids.add(aid)

    # Обрезаем список до POSTED_IDS_LIMIT, если он все равно превышает его (например, если new_ids_this_run > 200).
    final_ids_list_to_save = combined_ids[:POSTED_IDS_LIMIT]

    save_posted_ids(final_ids_list_to_save, Path(state_file))
    logging.info("State updated. Total unique IDs to be saved: %d.", len(final_ids_list_to_save))
    logging.info("📢 Done: sent %d articles in this run.", sent)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Poster: публикует статьи пакетами в Telegram"
    )
    parser.add_argument(
        "--parsed-dir",
        type=Path,
        default=Path("articles"),
        help="директория с распарсенными статьями"
    )
    parser.add_argument(
        "--state-file",
        type=str,
        default="articles/posted.json", # Файл состояния для отслеживания опубликованных статей.
        help="путь к state-файлу"
    )
    parser.add_argument(
        "--bot-token",
        type=str,
        required=True, # Обязательный аргумент: токен вашего Telegram-бота.
        help="токен Telegram-бота"
    )
    parser.add_argument(
        "--chat-id",
        type=str,
        required=True, # Обязательный аргумент: ID целевого чата/канала.
        help="ID чата/канала Telegram"
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY,
        help="задержка между отправкой статей (в секундах)"
    )
    parser.add_argument(
        "-n", "--limit",
        type=int,
        default=None,
        help="макс. кол-во статей для публикации за один запуск"
    )
    args = parser.parse_args()

    # Запускаем асинхронную основную функцию.
    asyncio.run(main_poster(
        parsed_dir=args.parsed_dir,
        state_file=args.state_file,
        bot_token=args.bot_token,
        chat_id=args.chat_id,
        delay=args.delay,
        limit=args.limit
    ))
