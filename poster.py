#!/usr/bin/env python3
import os
import json
import argparse
import asyncio
import logging
import re
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
from io import BytesIO

import httpx
from httpx import HTTPStatusError, ReadTimeout, Timeout
from PIL import Image

# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
# ──────────────────────────────────────────────────────────────────────────────

# HTTP retry parameters for Telegram
HTTPX_TIMEOUT = Timeout(connect=10.0, read=60.0, write=10.0, pool=5.0)
MAX_RETRIES   = 3
RETRY_DELAY   = 5.0
DEFAULT_DELAY = 5.0

# Path to minimal state-file (id, hash, translated_to)
STATE_PATH = Path("articles/catalog.json")


def escape_markdown(text: str) -> str:
    """
    Экранирует спецсимволы для MarkdownV2.
    """
    markdown_chars = r'\_*[]()~`>#+-=|{}.!'
    return re.sub(r'([%s])' % re.escape(markdown_chars), r'\\\1', text)


def chunk_text(
    text: str,
    size: int = 4096,
    preserve_formatting: bool = True
) -> List[str]:
    """
    Делит text на чанки длиной <= size.
    Сохраняет двойные переводы строк как разделители параграфов.
    """
    norm = text.replace('\r\n', '\n')
    paras = [p for p in norm.split('\n\n') if p.strip()]
    if not preserve_formatting:
        paras = [re.sub(r'\n+', ' ', p) for p in paras]

    chunks, curr = [], ""

    def _split_long(p: str) -> List[str]:
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
            chunks.extend(_split_long(p))
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
    Накладывает watermark.png (45% ширины) в правый верхний угол изображения.
    """
    base = Image.open(img_path).convert("RGBA")
    wm = Image.open("watermark.png").convert("RGBA")
    try:
        filt = Image.Resampling.LANCZOS
    except AttributeError:
        filt = Image.LANCZOS
    ratio = base.width * scale / wm.width
    wm = wm.resize((int(wm.width * ratio), int(wm.height * ratio)), resample=filt)
    base.paste(wm, (base.width - wm.width, 0), wm)
    buf = BytesIO()
    base.convert("RGB").save(buf, "PNG")
    return buf.getvalue()


async def _post_with_retry(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    data: Dict[str, Any],
    files: Optional[Dict[str, Any]] = None
) -> bool:
    """
    Общая логика HTTP POST с retry.
    4xx — без retry, 5xx и таймауты — retry.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = await client.request(
                method, url, data=data, files=files, timeout=HTTPX_TIMEOUT
            )
            resp.raise_for_status()
            return True

        except ReadTimeout:
            logging.warning("⏱ Timeout %s/%s for %s", attempt, MAX_RETRIES, url)

        except HTTPStatusError as e:
            code = e.response.status_code
            if 400 <= code < 500:
                logging.error("❌ %s %s: %s", method, code, e.response.text)
                return False
            logging.warning("⚠️ %s %s, retrying %s/%s", method, code, attempt, MAX_RETRIES)

        await asyncio.sleep(RETRY_DELAY)

    logging.error("☠️ Failed %s after %s attempts", url, MAX_RETRIES)
    return False


async def send_media_group(
    client: httpx.AsyncClient,
    token: str,
    chat_id: str,
    images: List[Path],
    caption: str,
    use_caption=False
) -> bool:
    """
    Отправляет несколько фото как альбом. Подпись даётся первому фото.
    """
    url = f"https://api.telegram.org/bot{token}/sendMediaGroup"
    media, files = [], {}
    for idx, img in enumerate(images):
        key = f"photo{idx}"
        img_bytes = apply_watermark(img)
        files[key] = (img.name, img_bytes, "image/png")
        item = {"type": "photo", "media": f"attach://{key}"}
        if idx == 0 and use_caption:
            item["caption"] = escape_markdown(caption)
            item["parse_mode"] = "MarkdownV2"
        media.append(item)

    data = {"chat_id": chat_id, "media": json.dumps(media, ensure_ascii=False)}
    return await _post_with_retry(client, "POST", url, data, files)


async def send_message(
    client: httpx.AsyncClient,
    token: str,
    chat_id: str,
    text: str
) -> bool:
    """
    Отправляет текстовое сообщение.
    """
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = {
        "chat_id": chat_id,
        "text": escape_markdown(text),
        "parse_mode": "MarkdownV2"
    }
    return await _post_with_retry(client, "POST", url, data, None)


def validate_article(art: Dict[str, Any]) -> Optional[Tuple[str, Path, List[Path]]]:
    """
    Проверяет обязательные поля и возвращает
    (caption, text_file, image_paths) или None.
    """
    title = art.get("title")
    txt = art.get("text_file")
    imgs = art.get("images", [])

    if not title or not isinstance(title, str):
        logging.error("Missing or invalid title in article %s", art.get("id"))
        return None
    if not txt or not Path(txt).is_file():
        logging.error("Missing or invalid text_file in article %s", art.get("id"))
        return None
    valid_imgs = [Path(p) for p in imgs if Path(p).is_file()]
    if not valid_imgs:
        logging.error("No valid images for article %s", art.get("id"))
        return None

    raw_title = title.strip()
    short = raw_title if len(raw_title) <= 1024 else raw_title[:1023] + "…"
    caption = escape_markdown(short)
    return caption, Path(txt), valid_imgs


async def main(limit: Optional[int]):
    token   = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHANNEL")
    if not token or not chat_id:
        logging.error("TELEGRAM_TOKEN or TELEGRAM_CHANNEL not set")
        return

    delay = float(os.getenv("POST_DELAY", DEFAULT_DELAY))

    # 1) Загружаем уже опубликованные ID из repo-state
    posted_ids = set()
    if STATE_PATH.is_file():
        try:
            lst = json.loads(STATE_PATH.read_text(encoding="utf-8"))
            posted_ids = {item["id"] for item in lst if "id" in item}
        except Exception as e:
            logging.warning(f"Не удалось прочитать state-файл {STATE_PATH}: {e}")
    else:
        logging.info(f"{STATE_PATH} не найден — публикуем всё как новое")

    # 2) Собираем список всех parsed-статей из артефакта
    parsed_articles: List[Dict[str, Any]] = []
    for art_dir in sorted(Path("articles").iterdir()):
        meta_file = art_dir / "meta.json"
        if not (art_dir.is_dir() and meta_file.is_file()):
            continue
        try:
            meta = json.loads(meta_file.read_text(encoding="utf-8"))
            parsed_articles.append(meta)
        except Exception as e:
            logging.warning(f"Failed to load meta for {art_dir.name}: {e}")

    client = httpx.AsyncClient(timeout=HTTPX_TIMEOUT)
    sent = 0

    # 3) Перебираем parsed_articles, пропуская уже опубликованные
    for art in parsed_articles:
        aid = art.get("id")
        if aid in posted_ids:
            logging.info(f"Skipping already posted article {aid}")
            continue
        if limit and sent >= limit:
            break

        validated = validate_article(art)
        if not validated:
            continue
        caption, text_path, images = validated

        if not await send_media_group(client, token, chat_id, images, caption, use_caption=True):
            continue

        raw = text_path.read_text(encoding="utf-8")
        chunks = chunk_text(raw, size=4096, preserve_formatting=True)
        body_chunks = chunks[1:] if len(chunks) > 1 else chunks

        for part in body_chunks:
            await send_message(client, token, chat_id, part)

        art["posted"] = True
        sent += 1
        logging.info("✅ Posted ID=%s", aid)
        await asyncio.sleep(delay)

    await client.aclose()

    # 4) Сохраняем обновлённый minimal-state в STATE_PATH
    minimal = [
        {"id": x["id"], "hash": x["hash"], "translated_to": x.get("translated_to", "")}
        for x in parsed_articles
    ]
    try:
        STATE_PATH.write_text(json.dumps(minimal, ensure_ascii=False, indent=2), encoding="utf-8")
        logging.info(f"State saved to {STATE_PATH}")
    except Exception as e:
        logging.error(f"Failed to save state-file {STATE_PATH}: {e}")

    logging.info("📢 Done: sent %d articles", sent)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Poster: публикует статьи пакетами"
    )
    parser.add_argument(
        "-n", "--limit", type=int, default=None,
        help="максимальное число статей для отправки"
    )
    args = parser.parse_args()
    asyncio.run(main(limit=args.limit))
