import os
import json
import argparse
import asyncio
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from io import BytesIO
import httpx
from httpx import HTTPStatusError, ReadTimeout, Timeout
from PIL import Image

# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
# --- Константа для ограничения количества записей в posted.json ---
MAX_POSTED_RECORDS = 100

# ──────────────────────────────────────────────────────────────────────────────
HTTPX_TIMEOUT = Timeout(connect=10.0, read=60.0, write=10.0, pool=5.0)
MAX_RETRIES   = 3
RETRY_DELAY   = 5.0
DEFAULT_DELAY = 10.0

def escape_html(text: str) -> str:
    """
    Экранирует спецсимволы HTML (<, >, &, ") для корректного отображения.
    """
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")

def chunk_text(text: str, size: int = 4096) -> List[str]:
    """
    Делит текст на чанки длиной <= size, сохраняя абзацы.
    """
    paras = [p for p in text.replace('\r\n', '\n').split('\n\n') if p.strip()]
    chunks, current_chunk = [], ""

    for p in paras:
        if len(p) > size:
            if current_chunk:
                chunks.append(current_chunk)
            # Если абзац слишком длинный, грубо делим его по словам
            parts = []
            sub_part = ""
            for word in p.split():
                if len(sub_part) + len(word) + 1 > size:
                    parts.append(sub_part)
                    sub_part = word
                else:
                    sub_part = f"{sub_part} {word}".lstrip()
            if sub_part:
                parts.append(sub_part)
            chunks.extend(parts)
            current_chunk = ""
        else:
            if not current_chunk:
                current_chunk = p
            elif len(current_chunk) + len(p) + 2 <= size:
                current_chunk += f"\n\n{p}"
            else:
                chunks.append(current_chunk)
                current_chunk = p
    
    if current_chunk:
        chunks.append(current_chunk)
    return chunks


def apply_watermark(img_path: Path, scale: float = 0.4) -> bytes:
    """
    Накладывает watermark.png и возвращает изображение в виде байтов JPEG.
    """
    try:
        base_img = Image.open(img_path).convert("RGBA")
        base_width, _ = base_img.size

        watermark_path = Path(__file__).parent / "watermark.png"
        if not watermark_path.exists():
            logging.warning("Файл watermark.png не найден. Возвращаем оригинальное изображение.")
            img_byte_arr = BytesIO()
            base_img.convert("RGB").save(img_byte_arr, format='JPEG', quality=90)
            return img_byte_arr.getvalue()

        watermark_img = Image.open(watermark_path).convert("RGBA")
        
        # Масштабирование
        wm_width, wm_height = watermark_img.size
        new_wm_width = int(base_width * scale)
        new_wm_height = int(wm_height * (new_wm_width / wm_width))
        resample_filter = getattr(Image.Resampling, "LANCZOS", Image.LANCZOS)
        watermark_img = watermark_img.resize((new_wm_width, new_wm_height), resample=resample_filter)
        
        # Создаём пустой слой РАЗМЕРОМ С ОСНОВНОЕ ИЗОБРАЖЕНИЕ
        overlay = Image.new("RGBA", base_img.size, (0, 0, 0, 0))

        # Наносим вотермарку на этот слой в нужную позицию
        padding = int(base_width * 0.02)
        position = (base_width - new_wm_width - padding, padding)
        overlay.paste(watermark_img, position, watermark_img) # Используем маску для прозрачности

        # Совмещаем основное изображение со СЛОЕМ (overlay)
        composite_img = Image.alpha_composite(base_img, overlay).convert("RGB")

        # Сохраняем в байты
        img_byte_arr = BytesIO()
        composite_img.save(img_byte_arr, format='JPEG', quality=90)
        return img_byte_arr.getvalue()
        
    except Exception as e:
        logging.error(f"Не удалось наложить водяной знак на {img_path}: {e}")
        # Возвращаем оригинал в случае ошибки
        try:
            with open(img_path, 'rb') as f:
                return f.read()
        except Exception as e_orig:
            logging.error(f"Не удалось даже прочитать оригинальное изображение {img_path}: {e_orig}")
            return b""

async def _post_with_retry(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    data: Dict[str, Any],
    files: Optional[Dict[str, Any]] = None
) -> bool:
    """Выполняет HTTP POST-запрос с повторными попытками."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = await client.request(method, url, data=data, files=files, timeout=HTTPX_TIMEOUT)
            resp.raise_for_status()
            return True
        except HTTPStatusError as e:
            if e.response.status_code == 429:
                retry_after = int(e.response.json().get("parameters", {}).get("retry_after", RETRY_DELAY))
                logging.warning(f"🐢 Rate limited. Retrying after {retry_after} seconds...")
                await asyncio.sleep(retry_after)
            elif 400 <= e.response.status_code < 500:
                logging.error(f"❌ Client error {e.response.status_code}: {e.response.text}")
                return False # Не повторяем клиентские ошибки
            else:
                logging.warning(f"⚠️ Server error {e.response.status_code}. Retry {attempt}/{MAX_RETRIES}...")
                await asyncio.sleep(RETRY_DELAY * attempt)
        except (ReadTimeout, httpx.RequestError) as e:
            logging.warning(f"⏱️ Network error: {e}. Retry {attempt}/{MAX_RETRIES}...")
            await asyncio.sleep(RETRY_DELAY * attempt)
    logging.error(f"☠️ Failed to send request to {url} after {MAX_RETRIES} attempts.")
    return False

async def send_media_group(client: httpx.AsyncClient, token: str, chat_id: str, images: List[Path]) -> bool:
    """Отправляет альбом фотографий."""
    url = f"https://api.telegram.org/bot{token}/sendMediaGroup"
    media, files = [], {}
    for idx, img_path in enumerate(images[:10]):
        image_bytes = apply_watermark(img_path)
        if image_bytes:
            key = f"photo{idx}"
            files[key] = (img_path.name, image_bytes, "image/jpeg")
            media.append({"type": "photo", "media": f"attach://{key}"})
    if not media: return False
    data = {"chat_id": chat_id, "media": json.dumps(media)}
    return await _post_with_retry(client, "POST", url, data, files)

async def send_message(client: httpx.AsyncClient, token: str, chat_id: str, text: str, **kwargs) -> bool:
    """Отправляет текстовое сообщение."""
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if kwargs.get("reply_markup"):
        data["reply_markup"] = json.dumps(kwargs["reply_markup"])
    return await _post_with_retry(client, "POST", url, data)

def validate_article(art: Dict[str, Any], article_dir: Path) -> Optional[Tuple[str, Path, List[Path], str]]:
    """Проверяет статью и возвращает готовые к постингу данные."""
    aid = str(art.get("id", ''))
    title = art.get("title", "").strip()
    text_file_name = Path(art.get("text_file", "")).name
    if not all([aid, title, text_file_name]): return None
    
    text_path = article_dir / text_file_name
    if not text_path.is_file(): return None
    
    image_paths = sorted([p for p in (article_dir / "images").glob('*') if p.suffix.lower() in ['.jpg', '.jpeg', '.png']])
    
    html_title = f"<b>{escape_html(title)}</b>"
    return html_title, text_path, image_paths, title

def load_posted_ids(state_file: Path) -> Set[str]:
    """Читает state-файл и возвращает set из ID в виде СТРОК."""
    if not state_file.is_file(): return set()
    try:
        data = json.loads(state_file.read_text(encoding="utf-8"))
        ids = {str(item) for item in data if item is not None} if isinstance(data, list) else set()
        return set(list(ids)[-MAX_POSTED_RECORDS:])
    except (json.JSONDecodeError, Exception) as e:
        logging.warning(f"Ошибка чтения файла состояния {state_file}: {e}.")
        return set()

def save_posted_ids(all_ids_to_save: Set[str], state_file: Path) -> None:
    """Сохраняет отсортированный список ID в файл состояния."""
    state_file.parent.mkdir(parents=True, exist_ok=True)
    try:
        # Сортируем численно для красивого вывода в файле
        sorted_ids = sorted(list(all_ids_to_save), key=int)
        with state_file.open("w", encoding="utf-8") as f:
            json.dump(sorted_ids, f, ensure_ascii=False, indent=2)
        logging.info(f"Сохранено {len(sorted_ids)} ID в файл состояния {state_file}.")
    except Exception as e:
        logging.error(f"Не удалось сохранить файл состояния {state_file}: {e}")

async def main(parsed_dir: str, state_path: str, limit: Optional[int]):
    """Основная функция для запуска постера."""
    token, chat_id = os.getenv("TELEGRAM_TOKEN"), os.getenv("TELEGRAM_CHANNEL")
    if not token or not chat_id:
        logging.error("Переменные окружения TELEGRAM_TOKEN и TELEGRAM_CHANNEL должны быть установлены.")
        return

    parsed_root, state_file = Path(parsed_dir), Path(state_path)
    if not parsed_root.is_dir():
        logging.error(f"Директория {parsed_root} не существует. Выход.")
        return

    posted_ids = load_posted_ids(state_file)
    logging.info(f"Загружено {len(posted_ids)} ранее опубликованных ID.")

    articles_to_post = []
    for d in sorted(parsed_root.iterdir()):
        meta_file = d / "meta.json"
        if d.is_dir() and meta_file.is_file():
            try:
                art_meta = json.loads(meta_file.read_text(encoding="utf-8"))
                article_id = str(art_meta.get("id"))
                if article_id != 'None' and article_id not in posted_ids:
                    if validated_data := validate_article(art_meta, d):
                        _, text_path, image_paths, original_title = validated_data
                        articles_to_post.append({
                            "id": article_id,
                            "html_title": f"<b>{escape_html(original_title)}</b>",
                            "text_path": text_path,
                            "image_paths": image_paths,
                            "original_title": original_title
                        })
            except Exception as e:
                logging.warning(f"Не удалось обработать {d.name}: {e}")

    articles_to_post.sort(key=lambda x: int(x["id"]))
    if not articles_to_post:
        logging.info("🔍 Нет новых статей для публикации.")
        return

    logging.info(f"Найдено {len(articles_to_post)} новых статей для публикации.")
    
    async with httpx.AsyncClient() as client:
        sent_count = 0
        for article in articles_to_post:
            if limit is not None and sent_count >= limit:
                logging.info(f"Достигнут лимит в {limit} статей.")
                break
            
            logging.info(f"Публикуем статью ID={article['id']}...")
            try:
                if article["image_paths"]:
                    await send_media_group(client, token, chat_id, article["image_paths"])

                raw_text = article["text_path"].read_text(encoding="utf-8")
                cleaned_text = raw_text.lstrip()
                if cleaned_text.startswith(article["original_title"]):
                    cleaned_text = cleaned_text[len(article["original_title"]):].lstrip()

                full_html = f"{article['html_title']}\n\n{escape_html(cleaned_text)}"
                chunks = chunk_text(full_html)

                for i, chunk in enumerate(chunks):
                    is_last_chunk = (i == len(chunks) - 1)
                    reply_markup = {
                        "inline_keyboard": [[
                            {"text": "Обмен валют", "url": "https://t.me/mister1dollar"},
                            {"text": "Отзывы", "url": "https://t.me/feedback1dollar"}
                        ]]
                    } if is_last_chunk else None
                    await send_message(client, token, chat_id, chunk, reply_markup=reply_markup)

                logging.info(f"✅ Опубликовано ID={article['id']}")
                posted_ids.add(article['id'])
                sent_count += 1

            except Exception as e:
                logging.error(f"❌ Ошибка при публикации ID={article['id']}: {e}", exc_info=True)
            
            await asyncio.sleep(float(os.getenv("POST_DELAY", DEFAULT_DELAY)))

    save_posted_ids(posted_ids, state_file)
    logging.info(f"📢 Завершено: отправлено {sent_count} статей.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Публикует статьи в Telegram.")
    parser.add_argument("--parsed-dir", type=str, default="articles", help="Директория со статьями.")
    parser.add_argument("--state-file", type=str, default="articles/posted.json", help="Файл состояния.")
    parser.add_argument("-n", "--limit", type=int, default=None, help="Лимит статей за запуск.")
    args = parser.parse_args()
    asyncio.run(main(args.parsed_dir, args.state_file, args.limit))
