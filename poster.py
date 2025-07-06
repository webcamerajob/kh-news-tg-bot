import logging
import json
import traceback
import asyncio
import re
import io

from pathlib import Path
from datetime import datetime
from contextlib import ExitStack
from typing import List, Any

from telegram import Bot, InputMediaPhoto
from telegram.constants import ParseMode
from telegram.error import BadRequest, RetryAfter, TelegramError
from telegram.helpers import escape

from PIL import Image, ImageEnhance

# ====== Настройки перевода и фильтрации ======

# Слова (регистронезависимо), которые нужно вырезать из перевода
EXCLUDE_WORDS = ["Синопсис :", "Свежие новости", r"\(видео\-)"]

# попытка импортировать deep_translator
try:
    from deep_translator import GoogleTranslator
    DEEP_TRANSLATOR_AVAILABLE = True
except ImportError:
    logging.warning("deep_translator не установлен — перевод отключён")
    DEEP_TRANSLATOR_AVAILABLE = False

# ====== Настройки Telegram & путей ======

import os
bot_token = os.getenv("TELEGRAM_TOKEN")
channel_id = os.getenv("TELEGRAM_CHANNEL")

if not bot_token or not channel_id:
    raise RuntimeError("🚫 TELEGRAM_TOKEN или TELEGRAM_CHANNEL не передан!")

# отправка сообщений:
# bot.send_message(chat_id=channel_id, text="...")

CATALOG_PATH     = Path(__file__).parent / "articles" / "catalog.json"

MAX_MEDIA_CAPTION = 1024
MAX_TEXT_CHUNK    = 4096
MAX_MEDIA_FILES   = 10
POST_DELAY        = 300.0   # секунда между отправками

# ====== Настройки водяного знака ======

WATERMARK_PATH = Path(__file__).parent / "watermark.png"
WM_SCALE       = 0.45             # доля ширины оригинала
WM_POSITION    = "top_right"  # top_left, top_right, bottom_left, bottom_right, center
WM_OPACITY     = 1.0             # 0.0–1.0

# ====== Настройка логирования ======

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)


# ====== Retry-менеджер для FloodControl ======

async def safe_call(method: Any, *args, **kwargs) -> Any:
    """
    Обёртка для API-методов Telegram:
    при RetryAfter ждёт указанное время + 1 секунда запаса.
    """
    while True:
        try:
            return await method(*args, **kwargs)
        except RetryAfter as e:
            wait = e.retry_after + 1
            logging.warning("⚠️ Flood control: жду %d сек.", wait)
            await asyncio.sleep(wait)
        except TelegramError:
            raise


# ====== I/O каталога ======

def load_catalog() -> List[dict]:
    if not CATALOG_PATH.exists():
        logging.error("🛑 catalog.json не найден: %s", CATALOG_PATH)
        return []
    try:
        return json.loads(CATALOG_PATH.read_text("utf-8"))
    except Exception as e:
        logging.error("❌ Ошибка чтения catalog.json: %s\n%s", e, traceback.format_exc())
        return []

def save_catalog(catalog: List[dict]):
    try:
        CATALOG_PATH.write_text(json.dumps(catalog, ensure_ascii=False, indent=2), "utf-8")
        logging.debug("💾 catalog.json сохранён")
    except Exception as e:
        logging.error("❌ Ошибка сохранения catalog.json: %s\n%s", e, traceback.format_exc())


# ====== Утилиты текста ======

def filter_text(text: str) -> str:
    """
    Удаляет из текста все слова из EXCLUDE_WORDS,
    убирает лишние пробелы и пустые строки.
    """
    for w in EXCLUDE_WORDS:
        pattern = rf"\b{re.escape(w)}\b"
        text = re.sub(pattern, "", text, flags=re.IGNORECASE)
    text = re.sub(r"[ \t]{2,}", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()

def split_text_by_words(
    content: str,
    prefix: str,
    first_len: int = 1024,
    rest_len: int = 4096
) -> List[str]:
    """
    content — исходный текст
    prefix — префикс добавляется только к первой части
    first_len — макс длина первой части (включая prefix)
    rest_len — макс длина всех последующих частей
    """
    parts: List[str] = []
    text = content

    # 1) первая часть
    avail = first_len - len(prefix)
    if len(text) <= avail:
        parts.append(prefix + text)
        return parts

    # отрезаем максимум доступного, но не разрываем слово
    chunk = text[:avail]
    cut = chunk.rfind(" ")
    if cut > 0:
        parts.append(prefix + chunk[:cut])
        text = text[cut:].lstrip()
    else:
        parts.append(prefix + chunk)
        text = text[avail:].lstrip()

    # 2) все следующие части
    while text:
        if len(text) <= rest_len:
            parts.append(text)
            break

        chunk = text[:rest_len]
        cut = chunk.rfind(" ")
        if cut > 0:
            parts.append(chunk[:cut])
            text = text[cut:].lstrip()
        else:
            parts.append(chunk)
            text = text[rest_len:].lstrip()

    return parts

async def translate_to_ru(text: str) -> str:
    """
    Синхронная библиотека deep_translator в executor,
    разбивает на сегменты по 2000 символов.
    """
    if not DEEP_TRANSLATOR_AVAILABLE:
        return text

    def _sync_translate(t: str) -> str:
        try:
            result = []
            idx = 0
            while idx < len(t):
                end = min(len(t), idx + 2000)
                segment = t[idx:end]
                if end < len(t):
                    cut = segment.rfind(" ")
                    if cut > 0:
                        segment, end = segment[:cut], idx + cut
                res = GoogleTranslator(source="auto", target="ru").translate(segment)
                result.append(res)
                idx = end
            return "".join(result)
        except Exception as e:
            logging.warning("Перевод упал: %s", e)
            return text

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _sync_translate, text)


# ====== Наложение водяного знака ======

def apply_watermark(
        img_path: Path,
        watermark_path: Path,
        scale: float,
        position: str,
        opacity: float
    ) -> io.BytesIO:
    base = Image.open(img_path).convert("RGBA")
    wm   = Image.open(watermark_path).convert("RGBA")

    # ресемплинг-фильтр
    try:
        resample = Image.Resampling.LANCZOS
    except AttributeError:
        resample = Image.LANCZOS

    wm_w = int(base.width * scale)
    wm_h = int(wm.height * (wm_w / wm.width))
    wm   = wm.resize((wm_w, wm_h), resample=resample)

    # позиционирование
    if position == "center":
        xy = ((base.width - wm_w)//2, (base.height - wm_h)//2)
    elif position == "top_left":
        xy = (0, 0)
    elif position == "top_right":
        xy = (base.width - wm_w, 0)
    elif position == "bottom_left":
        xy = (0, base.height - wm_h)
    else:  # bottom_right
        xy = (base.width - wm_w, base.height - wm_h)

    # настраиваем прозрачность
    alpha = wm.split()[3]
    alpha = ImageEnhance.Brightness(alpha).enhance(opacity)
    wm.putalpha(alpha)

    # объединяем слои
    layer  = Image.new("RGBA", base.size)
    layer.paste(wm, xy, wm)
    merged = Image.alpha_composite(base, layer).convert("RGB")

    bio = io.BytesIO()
    merged.save(bio, format="JPEG", quality=90)
    bio.seek(0)
    return bio


# ====== Отправка статьи ======

async def send_article(bot: Bot, article: dict) -> bool:
    art_id = article.get("id")
    if article.get("posted"):
        logging.info("⏩ %s уже отправлена", art_id)
        return False

    text_file = article.get("text_file")
    if not text_file or not Path(text_file).exists():
        logging.error("❌ Текст не найден: %s", text_file)
        return False

    raw = Path(text_file).read_text("utf-8")
    txt = re.sub(r'\r\n', '\n', raw)
    txt = re.sub(r'\n{3,}', '\n\n', txt).strip()

    # перевод и фильтрация заголовка
    title_raw = article.get("title", "").strip()
    title_tr  = await translate_to_ru(title_raw)
    title_tr  = filter_text(title_tr)

    # перевод и фильтрация основного текста
    txt_tr    = await translate_to_ru(txt)
    txt_tr    = filter_text(txt_tr)

    # готовим префикс
    prefix = f"<b>{escape(title_tr)}</b>\n\n"

    # разбиваем для подписи
    parts     = split_text_by_words(txt_tr, prefix, MAX_MEDIA_CAPTION)
    text_rest = parts[1:] if len(parts) > 1 else []

    # картинки
    imgs  = article.get("images", [])[:MAX_MEDIA_FILES]
    valid = [p for p in imgs if Path(p).exists()]

    try:
        # 1) media_group с caption на первой картинке
        if valid:
            with ExitStack() as st:
                media = []
                for idx, img in enumerate(valid):
                    wm_bio = apply_watermark(img, WATERMARK_PATH,
                                              WM_SCALE, WM_POSITION, WM_OPACITY)
                    media.append(InputMediaPhoto(
                        media=wm_bio,
                        caption=parts[0] if idx == 0 else None,
                        parse_mode=ParseMode.HTML if idx == 0 else None
                    ))
                await safe_call(bot.send_media_group,
                                chat_id=channel_id,
                                media=media)
            logging.info("✅ media_group отправлен: %s", art_id)
        else:
            # без картинок — отправляем первую часть как сообщение
            await safe_call(bot.send_message,
                            chat_id=channel_id,
                            text=parts[0],
                            parse_mode=ParseMode.HTML)
            logging.info("✅ Caption-текст отправлен: %s", art_id)


        # 2) отправляем оставшиеся части
        for idx, frag in enumerate(text_rest, start=1):
            await safe_call(bot.send_message,
                            chat_id=channel_id,
                            text=frag,
                            parse_mode=ParseMode.HTML)
            logging.debug("➡️ Часть %d/%d отправлена: %s", idx, len(text_rest), art_id)

    except BadRequest as e:
        logging.error("❌ BadRequest для %s: %s", art_id, e)
        return False
    except Exception as e:
        logging.error("❌ Ошибка при отправке %s: %s\n%s",
                      art_id, e, traceback.format_exc())
        return False

    # помечаем как отправленное
    meta = Path(text_file).parent / "meta.json"
    if meta.exists():
        try:
            m = json.loads(meta.read_text("utf-8"))
            m["posted"]    = True
            m["posted_at"] = datetime.now().isoformat()
            meta.write_text(json.dumps(m, ensure_ascii=False, indent=2), "utf-8")
        except Exception:
            pass

    article["posted"] = True
    return True


# ====== Entry point ======

async def main():
    try:
        bot = Bot(token=bot_token)
    except Exception as e:
        logging.error("🚫 Bot init error: %s", e)
        return

    catalog = load_catalog()
    if not catalog:
        logging.error("🛑 Нечего отправлять")
        return

    sent = 0
    for art in catalog:
        result = await send_article(bot, art)
        if result:
            sent += 1
        logging.debug("⏳ Жду %d сек перед следующей статьей", POST_DELAY)
        await asyncio.sleep(POST_DELAY)  # ⬅ вот здесь пауза
            
    save_catalog(catalog)
    logging.info("📢 Отправлено %d из %d статей", sent, len(catalog))

if __name__ == "__main__":
    asyncio.run(main())
