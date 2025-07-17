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
    """
    markdown_chars_to_escape = r'\_*[]()~`>#+-=|{}.!'
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
            "text": text,
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
        
        if caption:
            payload["caption"] = caption
            
        try:
            resp = await self._send_request("sendPhoto", files=files, data=payload)
            return resp.get("ok", False)
        except Exception as e:
            logging.error("Failed to send photo: %s", e)
            return False

    async def send_media_group(self, photo_paths: List[Path]) -> bool:
        """
        Отправляет группу фотографий (альбом) в Telegram-канал.
        Максимум 10 фотографий в группе.
        Подпись не используется.
        """
        if not photo_paths:
            logging.warning("No photo paths provided for media group.")
            return False

        media_items = []
        files_to_send = {}
        
        for i, photo_path in enumerate(photo_paths):
            if not photo_path.exists():
                logging.warning(f"Photo file not found for media group: {photo_path}. Skipping.")
                continue

            try:
                img = Image.open(photo_path)
                max_dim = 1280
                if img.width > max_dim or img.height > max_dim:
                    img.thumbnail((max_dim, max_dim), Image.LANCZOS)
                
                bio = BytesIO()
                img.save(bio, format="JPEG", quality=85)
                bio.seek(0)

                if bio.tell() > 10 * 1024 * 1024:
                    logging.warning(f"Compressed image for media group too large (>10MB): {photo_path}. Skipping.")
                    continue
                
                file_name = f"photo_{i}_{photo_path.name}"
                files_to_send[file_name] = (photo_path.name, bio, "image/jpeg")

                media_item = {
                    "type": "photo",
                    "media": f"attach://{file_name}",
                }
                # Подпись для группы не используется, как запрошено
                
                media_items.append(media_item)

            except Exception as e:
                logging.error(f"Error processing image {photo_path} for media group: {e}. Skipping.")
                continue
        
        if not media_items:
            logging.warning("No valid images left to send in media group after processing.")
            return False

        payload = {
            "chat_id": self.chat_id,
            "media": json.dumps(media_items)
        }

        try:
            resp = await self._send_request("sendMediaGroup", files=files_to_send, data=payload)
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
        
        posted_successfully = True # Assume success unless an error occurs

        # 1) Отправка группы изображений (до 10 штук), первое - главное
        image_paths_to_send: List[Path] = []
        
        # Сначала обрабатываем главное изображение, если оно явно указано
        main_image_path_str = article.get("main_image_path")
        if main_image_path_str:
            if main_image_path_str.startswith("articles/"):
                main_image_full_path = parsed_dir / main_image_path_str[len("articles/"):]
            else:
                main_image_full_path = parsed_dir / main_image_path_str
            
            if main_image_full_path.exists():
                image_paths_to_send.append(main_image_full_path)
            else:
                logging.warning(f"Main image file not found: {main_image_full_path} for ID={aid}. Skipping it as main.")
        
        # Добавляем остальные изображения, избегая дублирования и соблюдая лимит в 10
        # Проходим по списку article["images"], чтобы добавить остальные фото
        if article.get("images"):
            for img_path_str in article["images"]:
                if img_path_str.startswith("articles/"):
                    full_path = parsed_dir / img_path_str[len("articles/"):]
                else:
                    full_path = parsed_dir / img_path_str

                # Пропускаем, если это уже добавленное главное фото, или если достигнут лимит в 10
                if full_path in image_paths_to_send or len(image_paths_to_send) >= 10:
                    continue
                
                if full_path.exists():
                    image_paths_to_send.append(full_path)
                else:
                    logging.warning(f"Additional image file not found: {full_path} for ID={aid}. Skipping this image.")
        
        if not image_paths_to_send:
            logging.warning("No valid images found for ID=%s to send in media group. Skipping article.", aid)
            continue # Пропускаем статью, если нет изображений

        logging.info(f"Sending media group with {len(image_paths_to_send)} images for ID={aid}.")
        
        # Отправляем медиагруппу без подписи, как запрошено
        if not await client.send_media_group(image_paths_to_send):
            logging.error(f"Failed to send media group for ID={aid}.")
            posted_successfully = False
        else:
            logging.info(f"Successfully sent media group for ID={aid}.")
            await asyncio.sleep(1) # Небольшая задержка после отправки медиагруппы

        if not posted_successfully:
            continue # Переходим к следующей статье, если медиагруппа не отправлена

        # 2) Отправка текста статьи, включая заголовок в начале.
        text_file_path = None
        if article.get("text_file"):
            original_text_path_str = article["text_file"]
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
                
                # Форматируем заголовок как жирный текст для MarkdownV2
                formatted_title = f"*{escaped_title_content}*"
                
                # Экранируем основной текст
                escaped_text_content = escape_markdown(text_content)

                # Объединяем отформатированный заголовок и экранированный основной текст
                full_text_to_send = f"{formatted_title}\n\n{escaped_text_content}"

                text_chunks = chunk_text(full_text_to_send)
                for i, chunk in enumerate(text_chunks):
                    if not await client.send_message(chunk):
                        logging.error("Failed to send text chunk %d/%d for ID=%s.", i+1, len(text_chunks), aid)
                        posted_successfully = False
                        break
                    await asyncio.sleep(1)
            except (IOError, UnicodeDecodeError) as e:
                logging.error(f"Failed to read text file {text_file_path} for ID={aid}: {e}. Skipping text.")
                posted_successfully = False
            except Exception as e:
                logging.error(f"An unexpected error occurred reading text file for ID={aid}: {e}. Skipping text.")
                posted_successfully = False
        else:
            logging.warning("Text file not found for ID=%s (path tried: %s). Skipping text.", aid, text_file_path)
            posted_successfully = False

        if not posted_successfully:
            continue # Переходим к следующей статье, если текст не отправлен

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
    for aid in new_ids_this_run:
        if aid not in seen_ids:
            combined_ids.append(aid)
            seen_ids.add(aid)
    
    # Добавляем старые ID, которые еще не были добавлены, до достижения лимита.
    for aid in posted_ids_old:
        if aid not in seen_ids and len(combined_ids) < POSTED_IDS_LIMIT:
            combined_ids.append(aid)
            seen_ids.add(aid)

    # Обрезаем список до POSTED_IDS_LIMIT, если он все равно превышает его.
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
