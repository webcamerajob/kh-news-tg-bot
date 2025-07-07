#!/usr/bin/env python3
import os
import json
import argparse
import asyncio
import logging
from io import BytesIO

import httpx
from PIL import Image

# 1) Константы таймаутов и retry
TIMEOUT = httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=5.0)
MAX_RETRIES = 3
RETRY_DELAY = 5  # секунд
DEFAULT_POST_DELAY = 60.0   # пауза в секундах по умолчанию

# Путь к файлу каталога
CATALOG_PATH = "articles/catalog.json"

def load_catalog() -> list[dict]:
    if not os.path.isfile(CATALOG_PATH):
        logging.error(f"catalog.json не найден по пути {CATALOG_PATH}")
        return []
    with open(CATALOG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def save_catalog(catalog: list[dict]) -> None:
    with open(CATALOG_PATH, "w", encoding="utf-8") as f:
        json.dump(catalog, f, ensure_ascii=False, indent=2)

def apply_watermark(image_path: str, watermark_path: str = "watermark.png") -> bytes:
    base = Image.open(image_path).convert("RGBA")
    mark = Image.open(watermark_path).convert("RGBA")

    ratio = base.width * 0.3 / mark.width
    mark = mark.resize((int(mark.width * ratio), int(mark.height * ratio)), Image.ANTIALIAS)

        # выбираем фильтр ресэмплинга
    try:
        resample_filter = Image.Resampling.LANCZOS
    except AttributeError:
        resample_filter = Image.LANCZOS

    ratio = base.width * 0.3 / mark.width
    new_size = (int(mark.width * ratio), int(mark.height * ratio))
    mark = mark.resize(new_size, resample=resample_filter)

    pos = ((base.width - mark.width) // 2, (base.height - mark.height) // 2)
    base.paste(mark, pos, mark)

    buf = BytesIO()
    base.convert("RGB").save(buf, format="PNG")
    return buf.getvalue()

async def safe_send_photo(client: httpx.AsyncClient, token: str,
                          chat_id: str, photo_bytes: bytes,
                          caption: str) -> bool:
    url = f"https://api.telegram.org/bot{token}/sendPhoto"
    data = {"chat_id": chat_id, "caption": caption, "parse_mode": "Markdown"}
    files = {"photo": ("img.png", photo_bytes, "image/png")}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = await client.post(url, data=data, files=files)
            resp.raise_for_status()
            return True
        except httpx.ReadTimeout:
            logging.warning(f"⏱ ReadTimeout {attempt}/{MAX_RETRIES}, retry через {RETRY_DELAY}s")
        except httpx.HTTPError as e:
            logging.error(f"❌ HTTP error on attempt {attempt}: {e}")
            break
        await asyncio.sleep(RETRY_DELAY)

    logging.error("☠️ Не удалось отправить фото после всех попыток")
    return False

async def main(limit: int | None):
    # 0) Логирование (достаточно один раз)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )

    # 1) Читаем задержку из ENV, иначе DEFAULT_POST_DELAY
    delay = float(os.getenv("POST_DELAY", DEFAULT_POST_DELAY))

    # 2) Telegram-параметры
    token   = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHANNEL")
    if not token or not chat_id:
        logging.error("❌ TELEGRAM_TOKEN или TELEGRAM_CHANNEL не заданы")
        return

    # 3) Загружаем каталог
    catalog = load_catalog()
    if not catalog:
        logging.info("✅ Нечего отправлять")
        return

    client = httpx.AsyncClient(timeout=TIMEOUT)
    sent = 0

    # 4) Основной цикл — обязательно внутри for!
    for art in catalog:
        if art.get("posted"):
            continue

        if limit is not None and sent >= limit:
            logging.info(f"🔔 Достигнут лимит {limit}, выходим")
            break

        # берём первый путь из списка images
        imgs = art.get("images", [])
        if not imgs:
            logging.error(f"❌ Нет изображений для статьи ID={art.get('id')}")
            continue

        img_path = imgs[0]
        if not os.path.isfile(img_path):
            logging.error(f"❌ Файл изображения не найден: {img_path}")
            continue

        # 5) Водяной знак и отправка
        photo = apply_watermark(img_path)
        text_path = art.get("text_file")
        caption = Path(text_path).read_text(encoding="utf-8") if text_path else ""

        logging.info(f"▶️ Отправляем статью ID={art.get('id')}")
        ok = await safe_send_photo(client, token, chat_id, photo, caption)
        if ok:
            art["posted"] = True
            sent += 1
            logging.info(f"✅ Отправлено ID={art.get('id')}")

        # 6) Пауза
        logging.info(f"⏳ Ждём {delay}s перед следующей")
        await asyncio.sleep(delay)

    # 7) Завершение
    await client.aclose()
    save_catalog(catalog)
    logging.info(f"📢 Завершено: отправлено {sent} статей")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Poster: публикует статьи батчами"
    )
    parser.add_argument("--limit", "-n", type=int, default=None,
                        help="максимальное число статей для отправки")
    args = parser.parse_args()
    asyncio.run(main(limit=args.limit))
