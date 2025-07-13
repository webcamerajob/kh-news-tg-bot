#!/usr/bin/env python3
import os
import json
import argparse
import asyncio
import logging
import re
from typing import Any, Dict, List, Optional, Set, Tuple
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

HTTPX_TIMEOUT   = Timeout(connect=10.0, read=60.0, write=10.0, pool=5.0)
MAX_RETRIES     = 3
RETRY_DELAY     = 5.0
DEFAULT_DELAY   = 5.0

ARTICLES_DIR    = Path("articles")
STATE_FILE_PATH = ARTICLES_DIR / "catalog.json"


def escape_markdown(text: str) -> str:
    markdown_chars = r'\_*[]()~`>#+-=|{}.!'
    return re.sub(r'([%s])' % re.escape(markdown_chars), r'\\\1', text)


def chunk_text(text: str, size: int = 4096) -> List[str]:
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
                chunks.append(curr); curr = ""
            chunks.extend(split_long(p))
        else:
            if not curr:
                curr = p
            elif len(curr) + 2 + len(p) <= size:
                curr += "\n\n" + p
            else:
                chunks.append(curr); curr = p

    if curr:
        chunks.append(curr)
    return chunks


def apply_watermark(img_path: Path, scale: float = 0.45) -> bytes:
    base = Image.open(img_path).convert("RGBA")
    wm   = Image.open("watermark.png").convert("RGBA")
    filt = getattr(Image.Resampling, "LANCZOS", Image.LANCZOS)
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
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = await client.request(
                method, url, data=data, files=files, timeout=HTTPX_TIMEOUT
            )
            resp.raise_for_status()
            return True

        except ReadTimeout:
            logging.warning("Timeout %s/%s for %s", attempt, MAX_RETRIES, url)

        except HTTPStatusError as e:
            code = e.response.status_code
            if 400 <= code < 500:
                logging.error("%s %s: %s", method, code, e.response.text)
                return False
            logging.warning("%s %s, retry %s/%s", method, code, attempt, MAX_RETRIES)

        await asyncio.sleep(RETRY_DELAY)

    logging.error("Failed %s after %s attempts", url, MAX_RETRIES)
    return False


async def send_media_group(
    client: httpx.AsyncClient,
    token: str,
    chat_id: str,
    images: List[Path],
    caption: str
) -> bool:
    url = f"https://api.telegram.org/bot{token}/sendMediaGroup"
    media, files = [], {}
    for idx, img in enumerate(images):
        key = f"photo{idx}"
        files[key] = (img.name, apply_watermark(img), "image/png")
        item = {"type": "photo", "media": f"attach://{key}"}
        if idx == 0:
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
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = {
        "chat_id": chat_id,
        "text": escape_markdown(text),
        "parse_mode": "MarkdownV2"
    }
    return await _post_with_retry(client, "POST", url, data)


def validate_article(art: Dict[str, Any]) -> Optional[Tuple[str, Path, List[Path]]]:
    title = art.get("title")
    txt   = art.get("text_file")
    imgs  = art.get("images", [])

    if not title or not isinstance(title, str):
        logging.error("Invalid title in %s", art.get("id"))
        return None
    if not txt or not Path(txt).is_file():
        logging.error("Invalid text_file in %s", art.get("id"))
        return None
    valid = [Path(p) for p in imgs if Path(p).is_file()]
    if not valid:
        logging.error("No valid images in %s", art.get("id"))
        return None

    raw = title.strip()
    cap = raw if len(raw) <= 1024 else raw[:1023] + "…"
    return escape_markdown(cap), Path(txt), valid


def load_posted_ids() -> Set[int]:
    if not STATE_FILE_PATH.is_file():
        return set()
    try:
        return set(json.loads(STATE_FILE_PATH.read_text(encoding="utf-8")))
    except Exception as e:
        logging.warning("Failed to read %s: %s", STATE_FILE_PATH, e)
        return set()


def save_posted_ids(ids: Set[int]) -> None:
    ARTICLES_DIR.mkdir(parents=True, exist_ok=True)
    STATE_FILE_PATH.write_text(json.dumps(sorted(ids), ensure_ascii=False, indent=2),
                               encoding="utf-8")


async def main(limit: Optional[int]):
    token   = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHANNEL")
    if not token or not chat_id:
        logging.error("TELEGRAM_TOKEN or TELEGRAM_CHANNEL not set")
        return

    delay = float(os.getenv("POST_DELAY", DEFAULT_DELAY))
    posted_ids_old = load_posted_ids()
    logging.info("Loaded %d published IDs", len(posted_ids_old))

    # Собираем parsed-артикли из подпапок articles/
    parsed = []
    for d in sorted(ARTICLES_DIR.iterdir()):
        meta_f = d / "meta.json"
        if d.is_dir() and meta_f.is_file():
            try:
                parsed.append(json.loads(meta_f.read_text(encoding="utf-8")))
            except Exception as e:
                logging.warning("Cannot load meta %s: %s", d, e)

    client = httpx.AsyncClient(timeout=HTTPX_TIMEOUT)
    sent = 0
    new_ids = set()

    for art in parsed:
        aid = art.get("id")
        if aid in posted_ids_old:
            logging.info("Skipping already posted %s", aid)
            continue
        if limit and sent >= limit:
            break

        val = validate_article(art)
        if not val:
            continue
        caption, txt_path, imgs = val

        if not await send_media_group(client, token, chat_id, imgs, caption):
            continue

        raw    = txt_path.read_text(encoding="utf-8")
        chunks = chunk_text(raw)
        body   = chunks[1:] if len(chunks) > 1 else chunks
        for part in body:
            await send_message(client, token, chat_id, part)

        new_ids.add(aid)
        sent += 1
        logging.info("✅ Posted ID=%s", aid)
        await asyncio.sleep(delay)

    await client.aclose()

    # Сохраняем объединение старых и новых ID
    all_ids = posted_ids_old.union(new_ids)
    save_posted_ids(all_ids)
    logging.info("State updated with %d total IDs", len(all_ids))
    logging.info("📢 Done: sent %d articles", sent)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Poster: пакеты статей")
    parser.add_argument(
        "-n", "--limit", type=int, default=None,
        help="максимальное число статей для отправки"
    )
    args = parser.parse_args()
    asyncio.run(main(limit=args.limit))

