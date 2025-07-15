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
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
# ──────────────────────────────────────────────────────────────────────────────

HTTPX_TIMEOUT = Timeout(connect=10.0, read=60.0, write=10.0, pool=5.0)
MAX_RETRIES   = 3
RETRY_DELAY   = 5.0
DEFAULT_DELAY = 5.0


def escape_markdown(text: str) -> str:
    """
    Экранирует спецсимволы для MarkdownV2.
    """
    markdown_chars = r'\_*[]()~`>#+-=|{}.!'
    return re.sub(r'([%s])' % re.escape(markdown_chars), r'\\\1', text)


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
    Накладывает watermark.png в правый верхний угол изображения.
    """
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
    """
    HTTP POST с retry: 4xx — без retry, 5xx/timeout — retry.
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
    Отправляет альбом фото в Telegram с подписью к первому фото.
    """
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
    """
    Отправляет текстовое сообщение в Telegram.
    """
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = {
        "chat_id": chat_id,
        "text": escape_markdown(text),
        "parse_mode": "MarkdownV2"
    }
    return await _post_with_retry(client, "POST", url, data)

def validate_article(
    art: Dict[str, Any],
    article_dir: Path
) -> Optional[Tuple[str, Path, List[Path]]]:
    """
    Ищем:
      1) заголовок
      2) текстовый .txt внутри article_dir
      3) картинки внутри article_dir/images
    Если meta.json даёт какие-то кривые пути — они будут отброшены.
    """
    aid       = art.get("id")
    title     = art.get("title", "").strip()
    txt_name  = art.get("text_file", "")
    img_names = art.get("images", [])

    # 1) Title
    if not title:
        logging.error("Invalid title in article %s", aid)
        return None

    # 2) Text file: сначала basename из meta.json, потом scan *.txt
    txt_basename = Path(txt_name).name
    text_path    = article_dir / txt_basename
    if not text_path.is_file():
        # fallback: любой .txt в корне article_dir
        candidates = list(article_dir.glob("*.txt"))
        if not candidates:
            logging.error("No text file found in %s for article %s", article_dir, aid)
            return None
        # берём первый (обычно content.txt или content.ru.txt)
        text_path = candidates[0]

    # 3) Images: сначала пытаем из meta.json, потом scan папку images
    valid_imgs: List[Path] = []
    for name in img_names:
        img_basename = Path(name).name
        # 3.1 пробуем прямо в корне
        p = article_dir / img_basename
        # 3.2 пробуем в подкаталоге images
        if not p.is_file():
            p = article_dir / "images" / img_basename
        if p.is_file():
            valid_imgs.append(p)

    if not valid_imgs:
        # fallback: все картинки из папки images/
        imgs_dir = article_dir / "images"
        if imgs_dir.is_dir():
            valid_imgs = [p for p in imgs_dir.iterdir()
                          if p.suffix.lower() in (".jpg", ".jpeg", ".png")]
        if not valid_imgs:
            logging.error("No valid images in %s for article %s", article_dir, aid)
            return None

    # 4) Caption
    raw = title
    cap = raw if len(raw) <= 1024 else raw[:1023] + "…"

    return escape_markdown(cap), text_path, valid_imgs

def load_posted_ids(state_file: Path) -> Set[int]:
    """
    Читает state-файл и возвращает set опубликованных ID.
    Поддерживает:
      - отсутствующий или пустой файл → пустой set
      - список чисел [1,2,3]
      - список объектов [{"id":1}, {"id":2}]
    """
    if not state_file.is_file():
        return set()

    text = state_file.read_text(encoding="utf-8").strip()
    if not text:
        return set()

    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        logging.warning("State file not JSON: %s", state_file)
        return set()

    if not isinstance(data, list):
        logging.warning("State file is not a list: %s", state_file)
        return set()

    ids: Set[int] = set()
    for item in data:
        if isinstance(item, dict) and "id" in item:
            try:
                ids.add(int(item["id"]))
            except (ValueError, TypeError):
                pass
        elif isinstance(item, (int, str)) and str(item).isdigit():
            ids.add(int(item))
    return ids


def save_posted_ids(ids: Set[int], state_file: Path) -> None:
    """
    Сохраняет отсортированный список ID в state-файл.
    """
    # Создаём папку, если её нет
    state_file.parent.mkdir(parents=True, exist_ok=True)

    # Сортируем и пишем в файл
    arr = sorted(ids)
    state_file.write_text(
        json.dumps(arr, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    logging.info("Saved %d IDs to %s", len(arr), state_file)

    delay        = float(os.getenv("POST_DELAY", DEFAULT_DELAY))
    parsed_root  = Path(parsed_dir)
    state_file   = Path(state_path)

    if not parsed_root.is_dir():
        logging.error("Parsed directory %s does not exist", parsed_root)

    posted_ids_old = load_posted_ids(state_file)
    logging.info("Loaded %d published IDs", len(posted_ids_old))

    parsed: List[Tuple[Dict[str, Any], Path]] = []
    for d in sorted(parsed_root.iterdir()):
        meta_file = d / "meta.json"
        if d.is_dir() and meta_file.is_file():
            try:
                art = json.loads(meta_file.read_text(encoding="utf-8"))
                parsed.append((art, d))
            except Exception as e:
                logging.warning("Cannot load meta %s: %s", d.name, e)

    logging.info("🔍 Found %d folders with meta.json in %s", len(parsed), parsed_root)
    ids = [art.get("id") for art, _ in parsed]
    logging.info("🔍 Parsed IDs: %s", ids)

    client  = httpx.AsyncClient(timeout=HTTPX_TIMEOUT)
    sent    = 0
    new_ids: Set[int] = set()

    for art, article_dir in parsed:
        aid = art.get("id")
        if aid in posted_ids_old:
            logging.info("Skipping already posted %s", aid)
            continue
        if limit and sent >= limit:
            break

        validated = validate_article(art, article_dir)
        if not validated:
            continue

        _, text_path, images = validated

async def send_media_group(
    client: httpx.AsyncClient,
    token: str,
    chat_id: str,
    images: List[Path],
    caption: Optional[str] = None
) -> bool:
    url   = f"https://api.telegram.org/bot{token}/sendMediaGroup"
    media = []
    files = {}

    for idx, img in enumerate(images):
        key = f"file{idx}"
        files[key] = (img.name, apply_watermark(img), "image/png")
        item = {"type": "photo", "media": f"attach://{key}"}
        if idx == 0 and isinstance(caption, str) and caption.strip():
            item["caption"]    = escape_markdown(caption)
            item["parse_mode"] = "MarkdownV2"
        media.append(item)

    data = {
        "chat_id": chat_id,
        "media": json.dumps(media, ensure_ascii=False)
    }

    try:
        resp = await client.post(url, data=data, files=files)
        return resp.status_code == 200
    except Exception as e:
        logging.error("Failed to send media group: %s", e)
        return False

async def main(parsed_dir: str, state_path: str, limit: Optional[int]):
    token       = os.getenv("TELEGRAM_TOKEN")
    chat_id     = os.getenv("TELEGRAM_CHANNEL")
    if not token or not chat_id:
        logging.error("TELEGRAM_TOKEN or TELEGRAM_CHANNEL not set")
        return

    delay       = float(os.getenv("POST_DELAY", DEFAULT_DELAY))
    parsed_root = Path(parsed_dir)
    state_file  = Path(state_path)

    if not parsed_root.is_dir():
        logging.error("Parsed directory %s does not exist", parsed_root)
        return

    posted_ids_old = load_posted_ids(state_file)
    logging.info("Loaded %d published IDs", len(posted_ids_old))

    parsed: List[Tuple[Dict[str, Any], Path]] = []
    for d in sorted(parsed_root.iterdir()):
        meta_file = d / "meta.json"
        if d.is_dir() and meta_file.is_file():
            try:
                art = json.loads(meta_file.read_text("utf-8"))
                parsed.append((art, d))
            except Exception as e:
                logging.warning("Cannot load meta %s: %s", d.name, e)

    client   = httpx.AsyncClient(timeout=HTTPX_TIMEOUT)
    sent     = 0
    new_ids: Set[int] = set()

    for art, article_dir in parsed:
        aid = art.get("id")
        if aid in posted_ids_old:
            continue
        if limit and sent >= limit:
            break

        validated = validate_article(art, article_dir)
        if not validated:
            continue

        caption, text_path, images = validated
        if not await send_media_group(client, token, chat_id, images, caption):
            continue

        raw    = text_path.read_text("utf-8")
        chunks = chunk_text(raw)
        body   = chunks[1:] if len(chunks) > 1 else chunks
        for part in body:
            await send_message(client, token, chat_id, part)

        new_ids.add(aid)
        sent += 1
        logging.info("✅ Posted ID=%s", aid)
        await asyncio.sleep(delay)

    await client.aclose()

    all_ids = posted_ids_old.union(new_ids)
    save_posted_ids(all_ids, state_file)
    logging.info("State updated with %d total IDs", len(all_ids))
    logging.info("📢 Done: sent %d articles", sent)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Poster: публикует статьи пакетами")
    parser.add_argument(
        "--parsed-dir",
        type=str,
        default="articles",
        help="директория с распарсенными статьями"
    )
    parser.add_argument(
        "--state-file",
        type=str,
        default="articles/catalog.json",
        help="путь к state-файлу в репо"
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
