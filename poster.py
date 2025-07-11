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

CATALOG_PATH = Path("articles/catalog.json")


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
    caption: str
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
#        if idx == 0:
#            item["caption"] = escape_markdown(caption)
#            item["parse_mode"] = "MarkdownV2"
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


def load_catalog() -> List[Dict[str, Any]]:
    """
    Загружает и возвращает список артиклей из JSON.
    """
    if not CATALOG_PATH.is_file():
        logging.error("catalog.json not found at %s", CATALOG_PATH)
        return []
    try:
        return json.loads(CATALOG_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        logging.error("JSON decode error: %s", e)
        return []


def save_catalog_atomic(catalog: List[Dict[str, Any]]) -> None:
    """
    Сохраняет обновлённый каталог в JSON атомарно.
    """
    OUTPUT_DIR = CATALOG_PATH.parent
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    tmp = CATALOG_PATH.with_suffix(".json.tmp")
    text = json.dumps(catalog, ensure_ascii=False, indent=2)
    # write to temp file
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())
    # atomic replace
    os.replace(tmp, CATALOG_PATH)


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

    # используем только заголовок статьи в качестве подписи
    # экранируем MarkdownV2 и обрезаем до 100 символов
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

    delay   = float(os.getenv("POST_DELAY", DEFAULT_DELAY))
    catalog = load_catalog()
    client  = httpx.AsyncClient(timeout=HTTPX_TIMEOUT)
    sent    = 0

    for art in catalog:
        if art.get("posted"):
            continue
        if limit and sent >= limit:
            break

        validated = validate_article(art)
        if not validated:
            continue
        caption, text_path, images = validated

        # send media group with first paragraph as caption
        if not await send_media_group(client, token, chat_id, images, caption):
            continue
            
        title = art.get("title", "").strip()
        bold_title = f"*{escape_markdown(title)}*\n\n"
        raw = text_path.read_text(encoding="utf-8")
        full_text = bold_title + raw

        # send body chunks after skipping first paragraph
        raw = text_path.read_text(encoding="utf-8")
        chunks = chunk_text(full_text, size=4096, preserve_formatting=True)

        # Если чанков несколько — убираем первый (он в caption),
        # иначе — шлём единственный
        if len(chunks) > 1:
            body_chunks = chunks[1:]
        else:
            body_chunks = chunks

        for part in body_chunks:
            await send_message(client, token, chat_id, part)


        art["posted"] = True
        sent += 1
        logging.info("✅ Posted ID=%s", art.get("id"))
        await asyncio.sleep(delay)

    await client.aclose()
    save_catalog_atomic(catalog)
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
