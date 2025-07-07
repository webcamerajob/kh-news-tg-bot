#!/usr/bin/env python3
import os
import json
import argparse
import asyncio
import logging
import re
from typing import List
from pathlib import Path
from io import BytesIO

import httpx
from httpx import HTTPStatusError
from PIL import Image

# таймауты, ретраи, пауза
TIMEOUT       = httpx.Timeout(10.0, read=60.0, write=10.0, pool=5.0)
MAX_RETRIES   = 3
RETRY_DELAY   = 5
DEFAULT_DELAY = 60.0
CATALOG_PATH  = "articles/catalog.json"

def escape_markdown(text: str) -> str:
    markdown_chars = r'\_*[]()~`>#+-=|{}.!'
    return re.sub(r'([%s])' % re.escape(markdown_chars), r'\\\1', text)

def chunk_text(text: str, size: int = 4096) -> List[str]:
    words = text.split(" ")
    chunks, curr = [], ""
    for w in words:
        if len(curr) + len(w) + 1 > size:
            chunks.append(curr); curr = w
        else:
            curr = (curr + " " + w).lstrip()
    if curr: chunks.append(curr)
    return chunks

def apply_watermark(img_path: str) -> bytes:
    base = Image.open(img_path).convert("RGBA")
    mark = Image.open("watermark.png").convert("RGBA")
    try:
        filt = Image.Resampling.LANCZOS
    except AttributeError:
        filt = Image.LANCZOS
    ratio = base.width * 0.3 / mark.width
    mark = mark.resize((int(mark.width*ratio), int(mark.height*ratio)), resample=filt)
    base.paste(mark, (base.width-mark.width,0), mark)
    buf = BytesIO(); base.convert("RGB").save(buf, "PNG")
    return buf.getvalue()

async def safe_send_photo(
    client: httpx.AsyncClient,
    token: str,
    chat_id: str,
    photo: bytes,
    caption: str
) -> bool:
    url = f"https://api.telegram.org/bot{token}/sendPhoto"
    data = {
        "chat_id": chat_id,
        "caption": escape_markdown(caption),
        "parse_mode": "MarkdownV2"
    }
    files = {"photo": ("img.png", photo, "image/png")}

    for i in range(1, MAX_RETRIES+1):
        try:
            resp = await client.post(url, data=data, files=files, timeout=TIMEOUT)
            resp.raise_for_status()
            return True
        except httpx.ReadTimeout:
            logging.warning(f"⏱ Timeout {i}/{MAX_RETRIES}, retry in {RETRY_DELAY}s")
        except httpx.HTTPStatusError as e:
            logging.error(f"❌ {e.response.status_code}: {e.response.text}")
            return False
        await asyncio.sleep(RETRY_DELAY)
    return False

async def safe_send_message(
    client: httpx.AsyncClient,
    token: str,
    chat_id: str,
    text: str
) -> bool:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = {
        "chat_id": chat_id,
        "text": escape_markdown(text),
        "parse_mode": "MarkdownV2"
    }
    for i in range(1, MAX_RETRIES+1):
        try:
            resp = await client.post(url, data=data, timeout=TIMEOUT)
            resp.raise_for_status()
            return True
        except httpx.ReadTimeout:
            logging.warning(f"⏱ Timeout {i}/{MAX_RETRIES}, retry in {RETRY_DELAY}s")
        except httpx.HTTPStatusError as e:
            logging.error(f"❌ {e.response.status_code}: {e.response.text}")
            return False
        await asyncio.sleep(RETRY_DELAY)
    return False

def load_catalog() -> List[dict]:
    return json.loads(Path(CATALOG_PATH).read_text()) if Path(CATALOG_PATH).exists() else []

def save_catalog(c: List[dict]) -> None:
    Path(CATALOG_PATH).write_text(json.dumps(c, ensure_ascii=False, indent=2))

async def main(limit: int | None):
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")

    token   = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHANNEL")
    delay   = float(os.getenv("POST_DELAY", DEFAULT_DELAY))
    catalog = load_catalog()
    client  = httpx.AsyncClient(timeout=TIMEOUT)
    sent    = 0

    for art in catalog:
        if art.get("posted"): continue
        if limit and sent >= limit: break

        imgs = art.get("images", [])
        if not imgs: continue

        # формируем подпись: заголовок жирным + первый чанк текста
        raw = Path(art["text_file"]).read_text(encoding="utf-8")
        title = f"*{escape_markdown(art['title'])}*"
        chunks = chunk_text(raw)
        first_cap = title + ("\n\n" + chunks[0] if chunks else "")
        
        photo = apply_watermark(imgs[0])
        logging.info(f"▶️ Отправляем ID={art['id']}")
        if await safe_send_photo(client, token, chat_id, photo, first_cap):
            art["posted"] = True; sent += 1
            # остальные чанки как сообщения
            for part in chunks[1:]:
                await safe_send_message(client, token, chat_id, part)

        logging.info(f"⏳ Sleep {delay}s")
        await asyncio.sleep(delay)

    await client.aclose()
    save_catalog(catalog)
    logging.info(f"📢 Done: {sent} sent")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("-n","--limit",type=int,default=None)
    args = p.parse_args()
    asyncio.run(main(limit=args.limit))
