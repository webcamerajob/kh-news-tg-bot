import os
import json
import argparse
import asyncio
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from io import BytesIO
from collections import deque # deque по-прежнему полезен, но не так централизованно

import httpx
from httpx import HTTPStatusError, ReadTimeout, Timeout
from PIL import Image

# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
# --- Константа для ограничения количества записей в posted.json ---
# Если при 200 записях хотим оставить 100, то лимит хранения = 100.
MAX_POSTED_RECORDS = 100
# ──────────────────────────────────────────────────────────────────────────────
HTTPX_TIMEOUT = Timeout(connect=10.0, read=60.0, write=10.0, pool=5.0)
MAX_RETRIES   = 3
RETRY_DELAY   = 5.0
DEFAULT_DELAY = 10.0

def escape_html(text: str) -> str:
    """
    Экранирует спецсимволы HTML (<, >, &, ") для корректного отображения в Telegram с parse_mode='HTML'.
    Этот метод должен применяться к СЫРОМУ тексту, который НЕ ЯВЛЯЕТСЯ HTML-тегами.
    """
    text = text.replace("&", "&amp;")
    text = text.replace("<", "&lt;")
    text = text.replace(">", "&gt;")
    text = text.replace('"', "&quot;")
    return text

def chunk_text(text: str, size: int = 4096) -> List[str]:
    """
    Делит текст на чанки длиной <= size, сохраняя абзацы.
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
    """
    try:
        base_img = Image.open(img_path).convert("RGBA")
        base_width, base_height = base_img.size

        script_dir = Path(__file__).parent
        watermark_path = script_dir / "watermark.png"
        if not watermark_path.exists():
            logging.warning("Watermark file not found at %s. Skipping watermark.", watermark_path)
            img_byte_arr = BytesIO()
            base_img.save(img_byte_arr, format='PNG')
            return img_byte_arr.getvalue()

        watermark_img = Image.open(watermark_path).convert("RGBA")

        wm_width, wm_height = watermark_img.size
        new_wm_width = int(base_width * scale)
        new_wm_height = int(wm_height * (new_wm_width / wm_width))
        filt = getattr(Image.Resampling, "LANCZOS", Image.LANCZOS)
        watermark_img = watermark_img.resize((new_wm_width, new_wm_height), resample=filt)

        overlay = Image.new("RGBA", base_img.size, (0, 0, 0, 0))

        padding = int(base_width * 0.02)
        position = (base_width - new_wm_width - padding, padding)
        overlay.paste(watermark_img, position, watermark_img)

        composite_img = Image.alpha_composite(base_img, overlay)

        img_byte_arr = BytesIO()
        composite_img.save(img_byte_arr, format='PNG')
        return img_byte_arr.getvalue()
    except Exception as e:
        logging.error(f"Failed to apply watermark to {img_path}: {e}")
        try:
            img_byte_arr = BytesIO()
            Image.open(img_path).save(img_byte_arr, format='PNG')
            return img_byte_arr.getvalue()
        except Exception as e_orig:
            logging.error(f"Failed to load original image {img_path} after watermark error: {e_orig}")
            return b""


async def _post_with_retry(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    data: Dict[str, Any],
    files: Optional[Dict[str, Any]] = None
) -> bool:
    """
    Выполняет HTTP POST-запрос с повторными попытками и обработкой 429 Too Many Requests.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = await client.request(method, url, data=data, files=files, timeout=HTTPX_TIMEOUT)
            resp.raise_for_status()
            return True

        except ReadTimeout:
            logging.warning("⏱ Timeout %s/%s for %s", attempt, MAX_RETRIES, url)
        except HTTPStatusError as e:
            code = e.response.status_code
            text = e.response.text
            if code == 429:
                info = e.response.json().get("parameters", {})
                wait = info.get("retry_after", RETRY_DELAY)
                logging.warning("🐢 Rate limited %s/%s: retry after %s seconds", attempt, MAX_RETRIES, wait)
                await asyncio.sleep(wait)
                continue
            if 400 <= code < 500:
                logging.error("❌ %s %s: %s", method, code, text)
                return False
            logging.warning("⚠️ %s %s, retry %s/%s", method, code, attempt, MAX_RETRIES)
        except httpx.RequestError as e:
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
    Отправляет альбом фотографий без подписи (подпись будет в первом текстовом чанке).
    Все изображения проходят через apply_watermark.
    Ограничение на 10 изображений для медиагруппы Telegram.
    """
    url   = f"https://api.telegram.org/bot{token}/sendMediaGroup"
    media = []
    files = {}
    photo_count = 0

    if not images:
        logging.warning("No images provided for media group.")
        return False

    for idx, img_path in enumerate(images):
        if photo_count >= 10:
            logging.warning("Telegram media group limit (10 images) reached. Skipping remaining images.")
            break
        try:
            image_bytes = apply_watermark(img_path)
            if not image_bytes:
                logging.warning(f"Skipping image {img_path} due to empty bytes after watermark processing.")
                continue

            key = f"file{idx}"
            files[key] = (img_path.name, image_bytes, "image/png")

            media_item = {
                "type": "photo",
                "media": f"attach://{key}"
            }
            media.append(media_item)
            photo_count += 1
        except Exception as e:
            logging.error(f"Error processing image {img_path} for media group: {e}")

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
    text: str,
    reply_markup: Optional[Dict[str, Any]] = None
) -> bool:
    """
    Отправляет текстовое сообщение с разбором HTML.
    Текст, передаваемый в эту функцию, должен быть уже отформатирован и экранирован для HTML.
    Опционально может содержать кнопки (inline keyboard).
    """
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    if reply_markup:
        data["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)
    return await _post_with_retry(client, "POST", url, data)


def validate_article(
    art: Dict[str, Any],
    article_dir: Path
) -> Optional[Tuple[str, Path, List[Path], str]]:
    """
    Проверяет структуру папки статьи и возвращает подготовленные данные.
    Возвращает HTML-отформатированный заголовок, путь к текстовому файлу,
    список путей к изображениям и ОРИГИНАЛЬНЫЙ неформатированный заголовок.
    """
    aid      = art.get("id")
    title    = art.get("title", "").strip()
    txt_name = Path(art.get("text_file", "")).name if art.get("text_file") else None

    if not title:
        logging.error("Invalid title for article in %s (ID: %s). Skipping.", article_dir, aid)
        return None

    text_path: Optional[Path] = None
    if txt_name:
        candidate_path = article_dir / txt_name
        if candidate_path.is_file():
            text_path = candidate_path
    
    if not text_path:
        if (article_dir / "content.ru.txt").is_file():
            text_path = article_dir / "content.ru.txt"
        elif (article_dir / "content.txt").is_file():
            text_path = article_dir / "content.txt"
        else:
            candidates = list(article_dir.glob("*.txt"))
            if candidates:
                text_path = candidates[0]

    if not text_path or not text_path.is_file():
        logging.error("No text file found for article in %s (ID: %s). Skipping.", article_dir, aid)
        return None

    valid_imgs: List[Path] = []
    for name in art.get("images", []):
        p = article_dir / Path(name).name
        if not p.is_file():
            p = article_dir / "images" / Path(name).name
        if p.is_file():
            valid_imgs.append(p)

    if not valid_imgs:
        imgs_dir = article_dir / "images"
        if imgs_dir.is_dir():
            valid_imgs = [
                p for p in imgs_dir.iterdir()
                if p.suffix.lower() in (".jpg", ".jpeg", ".png")
            ]

    html_title = f"<b>{escape_html(title)}</b>"
    
    return html_title, text_path, valid_imgs, title

def load_posted_ids(state_file: Path) -> Set[int]:
    """
    Читает state-файл и возвращает set из ID.
    Просто читает список чисел.
    """
    if not state_file.is_file():
        logging.info("Файл состояния %s не найден. Возвращается пустой набор.", state_file)
        return set()
    try:
        content = state_file.read_text(encoding="utf-8").strip()
        if not content:
            return set()
            
        data = json.loads(content)
        # Преобразуем все элементы в int, отфильтровывая некорректные
        return {int(item) for item in data if str(item).isdigit()}
        
    except (json.JSONDecodeError, ValueError) as e:
        logging.warning("Файл состояния %s поврежден или имеет неверный формат: %s. Возвращается пустой набор.", state_file, e)
        return set()
    except Exception as e:
        logging.error(f"Ошибка чтения файла состояния {state_file}: {e}. Возвращается пустой набор.")
        return set()

def save_posted_ids(all_ids_to_save: Set[int], state_file: Path) -> None:
    """
    Сохраняет отсортированный список ID, обрезая его до MAX_POSTED_RECORDS,
    сохраняя самые большие (новейшие) ID.
    """
    state_file.parent.mkdir(parents=True, exist_ok=True)

    # --- НАЧАЛО БЛОКА ОТЛАДКИ ---
    print("--- DEBUG: Entering save_posted_ids ---")
    print(f"Total unique IDs received to save: {len(all_ids_to_save)}")
    
    # Загружаем текущее состояние файла, чтобы сравнить
    original_content_set = set()
    if state_file.is_file():
        try:
            original_content_set = set(json.loads(state_file.read_text(encoding="utf-8")))
        except:
            print("DEBUG: Could not read or parse original state file for comparison.")
    
    print(f"Original IDs in file: {len(original_content_set)}")
    
    newly_added_ids = all_ids_to_save - original_content_set
    print(f"DEBUG: IDs that should be new: {sorted(list(newly_added_ids))}")
    # --- КОНЕЦ БЛОКА ОТЛАДКИ ---

    # 1. Конвертируем set в список и сортируем
    sorted_ids = sorted(list(all_ids_to_save))

    # 2. Обрезаем список, если он превышает лимит, сохраняя ПОСЛЕДНИЕ элементы
    if len(sorted_ids) > MAX_POSTED_RECORDS:
        final_list_to_save = sorted_ids[-MAX_POSTED_RECORDS:]
        logging.info(
            "Обрезаем список опубликованных ID с %d до %d (сохраняя самые новые).",
            len(sorted_ids),
            MAX_POSTED_RECORDS
        )
    else:
        final_list_to_save = sorted_ids

    # --- НАЧАЛО БЛОКА ОТЛАДКИ ---
    print(f"DEBUG: Final list size to be saved: {len(final_list_to_save)}")
    print(f"DEBUG: First 5 IDs in final list: {final_list_to_save[:5]}")
    print(f"DEBUG: Last 5 IDs in final list: {final_list_to_save[-5:]}")
    
    # Сравниваем финальный список с тем, что было в файле
    if set(final_list_to_save) == original_content_set:
        print("DEBUG: CRITICAL! The final list of IDs is identical to the original file content. This is why it's unchanged.")
    else:
        print("DEBUG: The final list is different from the original. The file should be updated.")
    # --- КОНЕЦ БЛОКА ОТЛАДКИ ---
    
    try:
        with state_file.open("w", encoding="utf-8") as f:
            json.dump(final_list_to_save, f, ensure_ascii=False, indent=2)
        logging.info(
            "Сохранено %d ID в файл состояния %s.",
            len(final_list_to_save),
            state_file
        )
    except Exception as e:
        logging.error(f"Не удалось сохранить файл состояния {state_file}: {e}")
        
async def main(parsed_dir: str, state_path: str, limit: Optional[int]):
    """
    Основная функция для запуска постера.
    """
    token       = os.getenv("TELEGRAM_TOKEN")
    chat_id     = os.getenv("TELEGRAM_CHANNEL")
    if not token or not chat_id:
        logging.error("TELEGRAM_TOKEN или TELEGRAM_CHANNEL переменные окружения должны быть установлены.")
        return

    delay       = float(os.getenv("POST_DELAY", DEFAULT_DELAY))
    parsed_root = Path(parsed_dir)
    state_file  = Path(state_path)

    if not parsed_root.is_dir():
        logging.error("Директория %s не существует. Выход.", parsed_root)
        return

    # 1) Загрузка уже опубликованных ID.
    # posted_ids_old теперь является set'ом, который уже обрезан до MAX_POSTED_RECORDS.
    posted_ids_old = load_posted_ids(state_file)
    logging.info("Загружено %d ранее опубликованных ID из %s (актуальных после обрезки).", len(posted_ids_old), state_file.name)

    # 2) Сбор папок со статьями и их валидация
    articles_to_post: List[Dict[str, Any]] = []
    for d in sorted(parsed_root.iterdir()):
        meta_file = d / "meta.json"
        if d.is_dir() and meta_file.is_file():
            try:
                art_meta = json.loads(meta_file.read_text(encoding="utf-8"))
                # Важно: здесь posted_ids_old уже содержит только "актуальные" ID
                if art_meta.get("id") is not None and art_meta["id"] not in posted_ids_old:
                    validated_data = validate_article(art_meta, d)
                    if validated_data:
                        html_title, text_path, image_paths, original_plain_title = validated_data
                        validated_data_dict = {
                            "id": art_meta["id"],
                            "html_title": html_title,
                            "text_path": text_path,
                            "image_paths": image_paths,
                            "original_plain_title": original_plain_title
                        }
                        articles_to_post.append(validated_data_dict)
                    else:
                        logging.warning("Валидация метаданных статьи не удалась для %s. Пропускаем.", d.name)
                elif art_meta.get("id") is not None:
                    logging.debug("Пропускаем уже опубликованную статью ID=%s.", art_meta["id"])
                else:
                    logging.warning("Статья в %s не имеет ID в meta.json. Пропускаем.", d.name)
            except json.JSONDecodeError as e:
                logging.warning("Не удается загрузить или разобрать meta.json в %s: %s. Пропускаем.", d.name, e)
            except Exception as e:
                logging.error("Произошла непредвиденная ошибка при обработке статьи %s: %s. Пропускаем.", d.name, e)
    
    articles_to_post.sort(key=lambda x: x["id"])

    if not articles_to_post:
        logging.info("🔍 Нет новых статей для публикации. Выход.")
        return

    logging.info("Найдено %d новых статей для рассмотрения к публикации.", len(articles_to_post))

    client    = httpx.AsyncClient()
    sent      = 0
    new_ids: Set[int] = set() # Множество для сбора ID, опубликованных в этом запуске

    # 3) Публикация каждой статьи
    for article in articles_to_post:
        if limit is not None and sent >= limit:
            logging.info("Лимит пачки в %d достигнут. Остановка.", limit)
            break

        aid         = article["id"]
        html_title  = article["html_title"]
        text_path   = article["text_path"]
        image_paths = article["image_paths"]
        original_plain_title = article["original_plain_title"]

        logging.info("Попытка публикации ID=%s", aid)
        
        posted_successfully = False
        try:
            if image_paths:
                if not await send_media_group(client, token, chat_id, image_paths):
                    logging.warning("Не удалось отправить медиагруппу для ID=%s. Продолжаем отправлять только текст.", aid)
            
            raw_text = text_path.read_text(encoding="utf-8")
            cleaned_raw_text = raw_text
            if original_plain_title:
                escaped_plain_title_for_regex = re.escape(original_plain_title)
                pattern = re.compile(rf"^{escaped_plain_title_for_regex}\s*", re.DOTALL | re.IGNORECASE)
                match = pattern.match(raw_text)
                if match:
                    cleaned_raw_text = raw_text[match.end():]
                else:
                    logging.warning(
                        f"Оригинальный заголовок '{original_plain_title}' не найден в начале текстового файла для ID={aid}. "
                        "Продолжаем без удаления. Убедитесь, что текстовый файл начинается с заголовка из meta.json."
                    )
                    cleaned_raw_text = raw_text

            escaped_raw_text = escape_html(cleaned_raw_text)
            full_html_content = f"{html_title}\n\n{escaped_raw_text}"
            
            chunks = chunk_text(full_html_content)
            num_chunks = len(chunks)

            all_chunks_sent = True
            for i, part in enumerate(chunks):
                current_reply_markup = None
                if i == num_chunks - 1:
                    keyboard = {
                        "inline_keyboard": [
                            [
                                {"text": "Обмен валют", "url": "https://t.me/mister1dollar"},
                                {"text": "Отзывы", "url": "https://t.me/feedback1dollar"}
                            ]
                        ]
                    }
                    current_reply_markup = keyboard

                if not await send_message(client, token, chat_id, part, reply_markup=current_reply_markup):
                    logging.error("Не удалось отправить текстовый чанк для ID=%s. Пропускаем оставшиеся чанки и статью.", aid)
                    all_chunks_sent = False
                    break
            
            if all_chunks_sent:
                posted_successfully = True

        except Exception as e:
            logging.error(f"❌ Произошла ошибка во время публикации статьи ID={aid}: {e}. Переход к следующей статье.")
            posted_successfully = False

        if posted_successfully:
            new_ids.add(aid) # Добавляем в сет новых ID, опубликованных в этом запуске
            sent += 1
            logging.info("✅ Опубликовано ID=%s", aid)
        
        await asyncio.sleep(delay)

    await client.aclose()

    # 4) Сохраняем обновлённый список ID
    # all_ids_to_save теперь будет содержать старые актуальные ID + новые ID
    all_ids_to_save = posted_ids_old.union(new_ids)
    save_posted_ids(all_ids_to_save, state_file)
    logging.info("Состояние обновлено. Всего уникальных ID для сохранения: %d.", len(all_ids_to_save))
    logging.info("📢 Завершено: отправлено %d статей в этом запуске.", sent)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Poster: публикует статьи пакетами в Telegram"
    )
    parser.add_argument(
        "--parsed-dir",
        type=str,
        default="articles",
        help="директория с распарсенными статьями"
    )
    parser.add_argument(
        "--state-file",
        type=str,
        default="articles/posted.json",
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
