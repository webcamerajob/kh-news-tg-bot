import random
import threading
import argparse
import logging
import json
import hashlib
import time
import re
import os
import shutil
import html
import fcntl
import subprocess  # Нужно для вызова FFmpeg
import sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Set, Tuple
import yt_dlp

# Для перевода
import requests
from bs4 import BeautifulSoup
# Для парсинга
from curl_cffi import requests as cffi_requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# --- КОНФИГУРАЦИЯ ---
OUTPUT_DIR = Path("articles")
CATALOG_PATH = OUTPUT_DIR / "catalog.json"
MAX_RETRIES = 3
BASE_DELAY = 1.0
MAX_POSTED_RECORDS = 300
FETCH_DEPTH = 50

# --- НАСТРОЙКИ AI ---
OPENROUTER_KEY = os.getenv("OPENROUTER_API_KEY")

AI_MODELS = [
    "nvidia/nemotron-3-ultra-550b-a55b:free",
    "openrouter/free",  # авто-роутер: сам подберёт живую free-модель, не протухает
    # --- платные фолбэки (за кредиты) ---
    # "qwen/qwen-2.5-72b-instruct",
    # "google/gemini-2.0-flash-001",
    # "deepseek/deepseek-chat",
    # "openai/gpt-4o-mini",
]

# Константы прокси и каналов
EGRESS_PROXY = os.getenv("EGRESS_PROXY", "").strip()
WARP_PROXY = "socks5h://127.0.0.1:40000"


def build_sessions() -> List[Tuple[str, Any]]:
    """Возвращает список (имя_канала, сессия) от приоритетного к запасному."""
    channels: List[Tuple[str, Any]] = []

    if EGRESS_PROXY:
        channels.append((
            "EGRESS_PROXY",
            cffi_requests.Session(
                impersonate="chrome124",
                proxies={"http": EGRESS_PROXY, "https": EGRESS_PROXY},
            ),
        ))

    channels.append((
        "WARP",
        cffi_requests.Session(
            impersonate="chrome124",
            proxies={"http": WARP_PROXY, "https": WARP_PROXY},
        ),
    ))

    channels.append((
        "DIRECT",
        cffi_requests.Session(impersonate="chrome124"),
    ))

    return channels


SCRAPER_CHANNELS = build_sessions()
SCRAPER = SCRAPER_CHANNELS[0][1]
TRANSLATE_SESSION = cffi_requests.Session(impersonate="chrome124")


def is_cf_block(resp: Any) -> bool:
    if resp is None:
        return True
    if resp.status_code in (403, 429, 503):
        return True
    headers_lower = {k.lower(): v for k, v in (resp.headers or {}).items()}
    ctype = (headers_lower.get("content-type") or "").lower()
    body_head = (resp.text or "")[:300].lower()
    if "text/html" in ctype and (
        "cf-mitigated" in headers_lower
        or "just a moment" in body_head
        or "cloudflare" in body_head
    ):
        return True
    return False


def smart_get(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    timeout: int = 30,
    expect_json: bool = False,
) -> Tuple[Any, Optional[str]]:
    """Пробует каналы по очереди (EGRESS_PROXY -> WARP -> DIRECT)."""
    for name, sess in SCRAPER_CHANNELS:
        try:
            resp = sess.get(url, params=params, timeout=timeout)
            if is_cf_block(resp):
                logging.warning(f"⚠️ [{name}] Блок/челлендж ({resp.status_code}) на {url}")
                continue
            if expect_json:
                ctype = (resp.headers.get("Content-Type") or "").lower()
                txt = (resp.text or "").lstrip()
                if "json" not in ctype and not txt.startswith(("{", "[")):
                    logging.warning(f"⚠️ [{name}] Ожидали JSON, пришёл не-JSON на {url}")
                    continue
            return resp, name
        except Exception as e:
            logging.warning(f"⚠️ [{name}] Ошибка запроса к {url}: {e}")
    return None, None


IPHONE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/18.5 Mobile/15E148 Safari/604.1"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
}

FALLBACK_HEADERS = IPHONE_HEADERS

# Лок для защиты от параллельного вызова ротации из разных потоков
WARP_LOCK = threading.Lock()


def rotate_warp(hard: bool = False):
    """Переподключает WARP с защитой от зависания (timeout) и гонки потоков (lock)."""
    with WARP_LOCK:
        try:
            def run_warp_cmd(cmd_list: list):
                """Вспомогательный вызов warp-cli с жестким таймаутом 15 секунд."""
                subprocess.run(
                    ["warp-cli", "--accept-tos"] + cmd_list,
                    check=False,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=15,
                )

            if hard:
                logging.info("♻️ WARP: HARD ротация (новая регистрация)...")
                run_warp_cmd(["disconnect"])
                time.sleep(2)
                run_warp_cmd(["registration", "delete"])
                time.sleep(2)
                run_warp_cmd(["registration", "new"])
                time.sleep(2)
                run_warp_cmd(["mode", "proxy"])
                run_warp_cmd(["proxy", "port", "40000"])
                run_warp_cmd(["connect"])
                time.sleep(8)
                logging.info("✅ WARP: Перерегистрирован.")
            else:
                logging.info("♻️ WARP: Ротация IP...")
                run_warp_cmd(["disconnect"])
                time.sleep(2)
                run_warp_cmd(["connect"])
                time.sleep(5)
                logging.info("✅ WARP: Переподключено.")

        except subprocess.TimeoutExpired:
            logging.error("❌ Ошибка: команда warp-cli превысила таймаут (15 сек) и была принудительно завершена.")
        except Exception as e:
            logging.error(f"❌ Ошибка ротации WARP: {e}")


# --- БЛОК 1: ПЕРЕВОД И ИИ ---

def direct_google_translate(text: str, to_lang: str = "ru") -> str:
    if not text:
        return ""
    chunks = []
    current_chunk = ""
    for paragraph in text.split('\n'):
        if len(current_chunk) + len(paragraph) < 1800:
            current_chunk += paragraph + "\n"
        else:
            chunks.append(current_chunk)
            current_chunk = paragraph + "\n"
    if current_chunk:
        chunks.append(current_chunk)

    translated_parts = []

    endpoints = [
        ("https://translate.googleapis.com/translate_a/single", {"client": "gtx", "sl": "en", "tl": to_lang, "dt": "t"}),
        ("https://clients5.google.com/translate_a/t", {"client": "dict-chrome-ex", "sl": "en", "tl": to_lang}),
    ]

    for chunk in chunks:
        if not chunk.strip():
            translated_parts.append("")
            continue

        translated_text = None

        for url, base_params in endpoints:
            params = base_params.copy()
            params["q"] = chunk.strip()

            try:
                r = TRANSLATE_SESSION.get(url, params=params, timeout=7)
                if r.status_code == 200:
                    data = r.json()
                    if "clients5" in url:
                        translated_text = data[0] if isinstance(data[0], str) else data[0][0]
                    else:
                        translated_text = "".join([item[0] for item in data[0] if item and item[0]])
                    if translated_text:
                        break
            except Exception:
                pass

            try:
                r = requests.get(url, params=params, headers=FALLBACK_HEADERS, timeout=7)
                if r.status_code == 200:
                    data = r.json()
                    if "clients5" in url:
                        translated_text = data[0] if isinstance(data[0], str) else data[0][0]
                    else:
                        translated_text = "".join([item[0] for item in data[0] if item and item[0]])
                    if translated_text:
                        break
            except Exception:
                pass

        if translated_text:
            translated_parts.append(translated_text)
        else:
            translated_parts.append(chunk)

        time.sleep(0.3)

    return "\n".join(translated_parts)


def strip_ai_chatter(text: str) -> str:
    text = text.strip()

    text = re.sub(
        r'^\s*(?:\*\*|\*|#)*\s*(?:Here is|Here\'s|Edited story|Summary|Final text|Edited text|Note|Response)[:\s\*\#\-\=]*',
        '',
        text,
        flags=re.IGNORECASE,
    ).strip()

    match = re.match(r'^\s*\*\*(.*?)\*\*', text)
    if match:
        header_content = match.group(1).strip().lower()
        meta_keywords = ['here is', 'summary', 'edited', 'note', 'final story', 'revision', 'cleaned']
        if any(kw in header_content for kw in meta_keywords):
            logging.info(f"✂️ Вырезан заголовок ИИ: '**{match.group(1).strip()}**'")
            return text[match.end():].strip()

    return text


def smart_process_and_translate(title: str, body: str, lang: str) -> Tuple[str, str]:
    clean_body = body
    if OPENROUTER_KEY and len(body) > 500:
        logging.info("⏳ Подготовка к ИИ-чистке (OpenRouter)...")
        safe_body = body[:15000].replace('\x00', '')

        system_prompt = (
            "You are a ruthless news editor. Your ONLY task is to return the final edited text in ENGLISH.\n\n"
            "STRICT EDITING RULES:\n"
            "1. CONSOLIDATE NARRATIVE & SPEECH: If the author states a fact, and then a speaker repeats the same meaning, DELETE the speaker's part.\n"
            "2. KEEP UNIQUE DETAILS: Only keep quotes if they add numbers, dates, or emotion.\n"
            "3. REMOVE FLUFF: Delete ads and diplomatic praise.\n"
            "4. ABSOLUTE ZERO META-TALK: Do NOT write intros, explanations, thought processes, notes, or change logs. Start with the story immediately."
        )

        user_prompt = f"RAW TEXT:\n{safe_body}\n\nFINAL EDITED STORY:"

        ai_result = ""
        for model in AI_MODELS:
            try:
                logging.info(f"🚀 Запрос к OpenRouter: {model}...")

                if model != AI_MODELS[0]:
                    time.sleep(2)

                response = requests.post(
                    url="https://openrouter.ai/api/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {OPENROUTER_KEY}",
                        "HTTP-Referer": "https://github.com/kh-news-bot",
                        "X-Title": "NewsBot",
                    },
                    json={
                        "model": model,
                        "messages": [
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": user_prompt},
                        ],
                        "temperature": 0.0,
                        "max_tokens": 4096,
                    },
                    timeout=50,
                )
                if response.status_code == 200:
                    result = response.json()
                    if 'choices' in result and result['choices']:
                        ai_result = result['choices'][0]['message']['content'].strip()
                        logging.info(f"✅ Успех! Модель: {model}")
                        break
                else:
                    logging.warning(f"⚠️ Сбой {model} (Код {response.status_code}): {response.text[:100]}")
                    continue
            except Exception as e:
                logging.error(f"⚠️ Ошибка сети с {model}: {e}")
                continue
        if ai_result:
            clean_body = strip_ai_chatter(ai_result)
        else:
            logging.warning("❌ Все модели ИИ недоступны или вернули ошибку. Используем сырой текст.")

    DELIMITER = " ||| "
    combined_text = f"{title}{DELIMITER}{clean_body}"
    logging.info("🌍 [Google] Перевод...")
    translated_full = direct_google_translate(combined_text, lang)

    final_title = title
    final_text = clean_body

    if translated_full:
        parts = re.split(r'\s*\|\s*\|\s*\|\s*', translated_full, maxsplit=1)
        if len(parts) == 2 and parts[1].strip():
            final_title = parts[0].strip()
            final_text = parts[1].strip()
        else:
            logging.warning("⚠️ Разделитель искажен Google Translate. Выполняется раздельный перевод...")
            final_title = direct_google_translate(title, lang) or title
            final_text = direct_google_translate(clean_body, lang) or clean_body

    return final_title, final_text


# --- БЛОК 2: ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

def cleanup_old_articles(posted_ids_path: Path, articles_dir: Path):
    if not posted_ids_path.is_file() or not articles_dir.is_dir():
        return
    try:
        with open(posted_ids_path, 'r', encoding='utf-8') as f:
            all_posted = json.load(f)
            ids_to_keep = set(str(x) for x in all_posted[-MAX_POSTED_RECORDS:])
        cleaned = 0
        for f in articles_dir.iterdir():
            if f.is_dir():
                parts = f.name.split('_', 1)
                if parts and parts[0].isdigit():
                    if parts[0] not in ids_to_keep:
                        shutil.rmtree(f)
                        cleaned += 1
        if cleaned:
            logging.info(f"🧹 Удалено {cleaned} старых папок.")
    except Exception:
        pass


def sanitize_text(text: str) -> str:
    if not text:
        return ""
    text = html.unescape(text)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'mce_SELRES_[^ ]+', '', text)
    return re.sub(r'\n{3,}', '\n\n', text).strip()


def load_posted_ids(state_file_path: Path) -> Set[str]:
    try:
        if state_file_path.exists():
            with open(state_file_path, 'r', encoding='utf-8') as f:
                fcntl.flock(f, fcntl.LOCK_SH)
                data = json.load(f)
                fcntl.flock(f, fcntl.LOCK_UN)
                return {str(item) for item in data}
        return set()
    except Exception:
        return set()


def load_stopwords(file_path: Optional[Path]) -> List[str]:
    if not file_path or not file_path.exists():
        return []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return [line.strip().lower() for line in f if line.strip()]
    except Exception:
        return []


# --- БЛОК 3: УМНЫЙ ПОИСК И СКАЧИВАНИЕ ---

def extract_img_url(img_tag: Any) -> Optional[str]:
    def is_junk(url_str: str) -> bool:
        u = url_str.lower()
        bad = ["gif", "logo", "banner", "icon", "avatar", "button", "share", "pixel", "tracker"]
        if any(b in u for b in bad):
            return True
        if re.search(r'-\d{2,3}x\d{2,3}\.', u):
            return True
        return False

    parent_a = img_tag.find_parent("a")
    if parent_a:
        href = parent_a.get("href")
        if href and any(href.lower().endswith(ext) for ext in ['.jpg', '.jpeg', '.png', '.webp']):
            if not is_junk(href):
                return href.split('?')[0]
    srcset = img_tag.get("data-brsrcset") or img_tag.get("srcset") or img_tag.get("data-srcset")
    if srcset:
        try:
            links = []
            for p in srcset.split(','):
                match = re.search(r'(\S+)\s+(\d+)w', p.strip())
                if match:
                    w_val = int(match.group(2))
                    u_val = match.group(1)
                    if w_val >= 400:
                        links.append((w_val, u_val))
            if links:
                best_link = sorted(links, key=lambda x: x[0], reverse=True)[0][1]
                if not is_junk(best_link):
                    return best_link.split('?')[0]
        except Exception:
            pass
    width_attr = img_tag.get("width")
    if width_attr and width_attr.isdigit() and int(width_attr) < 300:
        return None
    for attr in ["data-breeze", "data-src", "src"]:
        val = img_tag.get(attr)
        if val:
            clean_url = val.split()[0].split(',')[0].split('?')[0]
            if not is_junk(clean_url):
                return clean_url
    return None


def save_image(url, folder):
    if not url or url.startswith('data:'):
        return None
    folder.mkdir(parents=True, exist_ok=True)
    url_hash = hashlib.md5(url.encode()).hexdigest()[:10]
    orig_fn = url.rsplit('/', 1)[-1].split('?', 1)[0]
    if '.' in orig_fn:
        ext = orig_fn.split('.')[-1].lower()
    else:
        ext = 'jpg'
    if len(ext) > 4 and ext not in ['mp4', 'mov', 'm4v']:
        ext = 'jpg'

    fn = f"{url_hash}.{ext}"
    dest = folder / fn
    timeout = 60 if ext in ['mp4', 'mov', 'm4v'] else 20

    resp, _ = smart_get(url, timeout=timeout, expect_json=False)
    if resp is not None and resp.status_code == 200 and resp.content:
        dest.write_bytes(resp.content)
        return str(dest)

    try:
        resp = requests.get(url, headers=FALLBACK_HEADERS, timeout=timeout)
        if resp.status_code == 200:
            dest.write_bytes(resp.content)
            return str(dest)
    except Exception as e:
        logging.error(f"❌ Не удалось скачать файл {url}: {e}")
    return None


# ==============================================================================
# === ВНЕДРЕННЫЕ ФУНКЦИИ (LOADER.TO + FFMPEG SUBPROCESS) ===
# ==============================================================================

def get_video_duration(video_path: Path) -> float:
    """Получает длительность видео через ffprobe."""
    try:
        cmd = ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", str(video_path)]
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        return float(result.stdout.strip())
    except Exception:
        return 0.0


def download_youtube_via_loader_to(video_url, output_path):
    """Скачивает видео с YouTube через yt_dlp + WARP с авто-ротацией IP при блоках."""
    max_attempts = 3
    proxy_to_use = EGRESS_PROXY or WARP_PROXY

    for attempt in range(1, max_attempts + 1):
        try:
            logging.info(f"⬇️ [yt_dlp] Попытка {attempt} для: {video_url}")

            ydl_opts = {
                'format': 'bestvideo[height<=360]+bestaudio/best[height<=360]/best',
                'outtmpl': str(output_path),
                'merge_output_format': 'mp4',
                'quiet': True,
                'no_warnings': True,
                'proxy': proxy_to_use,
            }

            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([video_url])

            if Path(output_path).exists() and Path(output_path).stat().st_size > 0:
                logging.info("✅ Видео успешно скачано.")
                return True
            else:
                logging.warning("⚠️ yt_dlp завершил работу, но файл пустой.")

        except Exception as e:
            logging.warning(f"⚠️ Ошибка скачивания (попытка {attempt}): {e}")

        if attempt < max_attempts and not EGRESS_PROXY:
            logging.info("♻️ YouTube заблокировал IP или произошла ошибка. Запускаем ротацию WARP...")
            rotate_warp(hard=True)
            time.sleep(3)

    logging.error("❌ Не удалось скачать видео после всех попыток.")
    return False


def download_via_loader_to(video_url, output_path):
    """Скачивает видео через yt-dlp."""
    ydl_opts = {
        'outtmpl': str(output_path),
        'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
        'merge_output_format': 'mp4',
        'quiet': True,
        'no_warnings': True,
        'noplaylist': True,
    }
    if EGRESS_PROXY:
        ydl_opts['proxy'] = EGRESS_PROXY

    for attempt in range(1, 4):
        try:
            logging.info(f"⬇️ Качаем видео (Попытка {attempt}): {video_url}")
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([video_url])

            if Path(output_path).exists() and Path(output_path).stat().st_size > 0:
                logging.info("✅ Видео успешно скачано!")
                return True
            else:
                logging.warning("⚠️ yt-dlp отработал, но файл пуст.")
                time.sleep(3)
        except Exception as e:
            logging.error(f"❌ Ошибка yt-dlp: {type(e).__name__}: {e}")
            time.sleep(5)

    return False


def add_watermark(input_video, watermark_img, output_video):
    if not Path(watermark_img).exists():
        logging.error(f"❌ Вотермарка не найдена: {watermark_img}")
        return False

    duration = get_video_duration(input_video)

    c_start, c_end, t_tail = 0.0, 0.0, 11.0
    wm_scale = 0.4
    pad_rel = 0.03

    scale_expr = f"[0:v]scale='if(gt(ih,720),-2,iw)':'min(ih,720)'[main];[1:v][main]scale2ref=w=iw*{wm_scale}:h=ow/(main_w/main_h)[wm][vid]"
    wm_sar_fix = "[wm]setsar=1[wm_fixed]"
    x_expr = f"W-w-(W*{pad_rel})"
    y_expr = f"if(gt(H,W), H*0.07, H*{pad_rel})"
    overlay_expr = f"[vid][wm_fixed]overlay=x='{x_expr}':y='{y_expr}'"

    if duration > 25.0:
        f_point = duration - t_tail
        logging.info(f"✂️ Обрезка + Правый верхний угол (Scale: {wm_scale})")

        v_filter = (
            f"{scale_expr};"
            f"{wm_sar_fix};"
            f"{overlay_expr},select='lt(t,{c_start})+between(t,{c_end},{f_point})',setpts=N/FRAME_RATE/TB"
        )
        a_filter = f"aselect='lt(t,{c_start})+between(t,{c_end},{f_point})',asetpts=N/SR/TB"

        cmd = [
            "ffmpeg", "-y", "-i", str(input_video), "-i", str(watermark_img),
            "-filter_complex", v_filter,
            "-af", a_filter,
            "-c:v", "libx264", "-preset", "superfast", "-crf", "30", "-pix_fmt", "yuv420p", "-movflags", "+faststart",
            "-c:a", "aac", "-b:a", "128k", str(output_video),
        ]
    else:
        logging.info(f"⚠️ Видео короткое, Правый верхний угол (Scale: {wm_scale})")

        full_filter = (
            f"{scale_expr};"
            f"{wm_sar_fix};"
            f"{overlay_expr}"
        )

        cmd = [
            "ffmpeg", "-y", "-i", str(input_video), "-i", str(watermark_img),
            "-filter_complex", full_filter,
            "-c:v", "libx264", "-preset", "superfast", "-crf", "30", "-pix_fmt", "yuv420p", "-movflags", "+faststart",
            "-c:a", "copy", str(output_video),
        ]

    try:
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"❌ FFmpeg Error: {e.stderr.decode()}")
        return False


# --- БЛОК 4: API И ПАРСИНГ ---

def fetch_cat_id(url, slug):
    if slug == "national":
        logging.info(f"ℹ️ [Skip Net] Используем Hardcoded ID для '{slug}': 19")
        return 19

    endpoint = f"{url}/wp-json/wp/v2/categories?slug={slug}"

    for attempt in range(1, 4):
        try:
            logging.info(f"📡 Попытка {attempt}/3: Запрос к API {slug}...")
            resp, ch = smart_get(endpoint, timeout=30, expect_json=True)
            if resp is None:
                raise ValueError("Cloudflare challenge or network error across channels")

            resp.raise_for_status()
            data = resp.json()

            if data and isinstance(data, list):
                cat_id = data[0]["id"]
                logging.info(f"✅ ID категории найден через [{ch}]: {cat_id}")
                return cat_id
            else:
                logging.error(f"❌ Категория '{slug}' не найдена.")
                return None

        except Exception as e:
            logging.warning(f"⚠️ Попытка {attempt} провалена: {e}")
            if attempt < 3:
                time.sleep(5 * attempt)
            else:
                logging.error("💀 Все попытки исчерпаны. Возвращаем дефолтный ID 19.")
                return 19


def fetch_posts_light(url: str, cid: int, limit: int) -> Optional[List[Dict]]:
    """
    Список постов при успехе; [] если API ответил пусто/ошибкой;
    None если не пробились (Cloudflare) — сигнал воркфлоу взять свежий раннер.
    """
    params = {"categories": cid, "per_page": limit, "_embed": "1"}
    endpoint = f"{url}/wp-json/wp/v2/posts"

    for attempt in range(1, 3):
        logging.info(f"📥 Скачиваем список статей (Попытка {attempt}/2)...")
        resp, ch = smart_get(endpoint, params=params, timeout=45, expect_json=True)
        if resp is not None:
            try:
                data = resp.json()
                if isinstance(data, list):
                    logging.info(f"✅ Список получен через [{ch}] ({len(data)} шт.)")
                    return data
                if isinstance(data, dict) and "code" in data:
                    logging.error(f"❌ WP API Error: {data.get('message')}")
                    return []
            except Exception as e:
                logging.warning(f"⚠️ Ошибка JSON через [{ch}]: {e}")
        time.sleep(2)

    logging.error("💀 Список не получен (Cloudflare заблокировал все каналы на этом раннере).")
    return None


def fetch_single_post_full(url: str, aid: str) -> Optional[Dict]:
    endpoint = f"{url}/wp-json/wp/v2/posts/{aid}?_embed"
    resp, ch = smart_get(endpoint, timeout=60, expect_json=True)
    if resp is None:
        logging.error(f"❌ Не удалось загрузить детали ID={aid} (блок/ошибка сети).")
        return None
    try:
        return resp.json()
    except Exception as e:
        logging.error(f"❌ Ошибка JSON для ID={aid} [{ch}]: {e}")
        return None


def parse_and_save(post, lang, stopwords, watermark_img_path: Optional[Path] = None):
    time.sleep(2)
    aid, slug, link = str(post["id"]), post["slug"], post.get("link")

    raw_title = BeautifulSoup(post["title"]["rendered"], "html.parser").get_text(strip=True)
    title = sanitize_text(raw_title)

    if stopwords:
        for ph in stopwords:
            if ph in title.lower():
                logging.info(f"🚫 ID={aid}: Стоп-слово '{ph}'")
                return None

    resp, ch = smart_get(link, timeout=30, expect_json=False)
    html_txt = resp.text if (resp is not None and resp.status_code == 200) else ""
    if not html_txt:
        logging.error(f"❌ ID={aid}: Не удалось открыть HTML статьи через доступные каналы.")
        return None

    meta_path = OUTPUT_DIR / f"{aid}_{slug}" / "meta.json"
    curr_hash = hashlib.sha256(html_txt.encode()).hexdigest()
    if meta_path.exists():
        try:
            m = json.loads(meta_path.read_text(encoding="utf-8"))
            if m.get("hash") == curr_hash:
                logging.info(f"⏭️ ID={aid}: Без изменений.")
                return m
        except Exception:
            pass

    logging.info(f"Processing ID={aid} [{ch}]: {title[:30]}...")
    soup = BeautifulSoup(html_txt, "html.parser")

    c_div = soup.find("div", class_="entry-content")

    # --- ШАГ 1: ПОИСК ВИДЕО ---
    fb_video_tasks = []
    youtube_tasks = []
    import urllib.parse as urlparse

    for fb_el in soup.find_all("div", class_=re.compile(r"\bfb-video\b")):
        raw = fb_el.get("data-href", "")
        if not raw:
            continue
        p = urlparse.urlparse(raw)
        qs = urlparse.parse_qs(p.query)
        vid = qs.get("v", [None])[0]
        if vid:
            canonical = f"https://www.facebook.com/reel/{vid}"
        else:
            m = re.search(r"/(?:reel|videos|watch)/(\d+)", raw)
            canonical = f"https://www.facebook.com/reel/{m.group(1)}" if m else raw
        if canonical not in fb_video_tasks:
            fb_video_tasks.append(canonical)
            logging.info(f"Найдено FB видео (div.fb-video): {canonical}")

    for bq in soup.find_all("blockquote", class_=re.compile(r"fb-xfbml-parse-ignore")):
        cite = bq.get("cite", "")
        m = re.search(r"/(?:reel|videos|watch)/(\d+)", cite)
        if not m:
            continue
        canonical = f"https://www.facebook.com/reel/{m.group(1)}"
        if canonical not in fb_video_tasks:
            fb_video_tasks.append(canonical)
            logging.info(f"Найдено FB видео (blockquote): {canonical}")

    for iframe in soup.find_all("iframe"):
        src = iframe.get("src", "")

        if "facebook.com" in src and "plugins/video.php" in src:
            parsed = urlparse.urlparse(src)
            fb_url = urlparse.parse_qs(parsed.query).get('href', [None])[0]

            if fb_url:
                fb_parsed = urlparse.urlparse(fb_url)
                fb_qs = urlparse.parse_qs(fb_parsed.query)
                video_id = fb_qs.get('v', [None])[0]

                if video_id:
                    canonical = f"https://www.facebook.com/watch/?v={video_id}"
                else:
                    canonical = fb_url

                if canonical not in fb_video_tasks:
                    fb_video_tasks.append(canonical)
                    logging.info(f"Найдено FB видео: {canonical}")

        elif "youtube.com/embed" in src or "youtu.be" in src:
            if src.startswith("//"):
                src = "https:" + src
            if src not in youtube_tasks:
                youtube_tasks.append(src)
                logging.info(f"Найдено YouTube iframe: {src}")

    # --- ШАГ 2: ОЧИСТКА МУСОРА ---
    for garbage in soup.find_all(
        ["div", "ul", "ol", "section", "aside"],
        class_=re.compile(r"rp4wp|related|ad-|post-widget-thumbnail|sharedaddy"),
    ):
        garbage.decompose()

    for j in soup.find_all(["span", "script", "style"]):
        if not hasattr(j, 'attrs') or j.attrs is None:
            continue
        c = str(j.get("class", ""))
        if j.get("data-mce-type") or "mce_SELRES" in c or "widget" in c:
            j.decompose()

    # --- ШАГ 3: СБОР МЕДИА-РЕСУРСОВ ---
    ordered_srcs = []
    seen_srcs = set()

    def add_src(url):
        if url and url not in seen_srcs:
            ordered_srcs.append(url)
            seen_srcs.add(url)

    og = soup.find("meta", attrs={"property": "og:image"}) or soup.find("meta", attrs={"name": "twitter:image"})
    if og and (u := og.get("content")) and "logo" not in u.lower():
        add_src(u)
    else:
        logging.warning(f"DIAG ID={aid}: og:image НЕ найден | html_len={len(html_txt)}")

    video_srcs = []
    if c_div:
        for img in c_div.find_all("img"):
            if u := extract_img_url(img):
                add_src(u)

        for yt_a in c_div.find_all("a"):
            href = yt_a.get("href", "")
            if "youtube.com/watch" in href or "youtu.be/" in href:
                if href not in youtube_tasks:
                    youtube_tasks.append(href)

        for a_tag in c_div.find_all("a"):
            href = a_tag.get("href", "")
            if href.lower().endswith(('.mp4', '.mov', '.m4v')):
                if href not in seen_srcs:
                    video_srcs.append(href)
                    seen_srcs.add(href)

    for v in video_srcs:
        ordered_srcs.append(v)

    images_dir = OUTPUT_DIR / f"{aid}_{slug}" / "images"
    images_dir.mkdir(parents=True, exist_ok=True)

    images_results = [None] * len(ordered_srcs)
    if ordered_srcs:
        with ThreadPoolExecutor(3) as ex:
            future_to_idx = {
                ex.submit(save_image, url, images_dir): i
                for i, url in enumerate(ordered_srcs)
            }
            for f in as_completed(future_to_idx):
                idx = future_to_idx[f]
                if res := f.result():
                    images_results[idx] = Path(res).name

    youtube_files = []
    for yt_url in youtube_tasks:
        video_hash = hashlib.md5(yt_url.encode()).hexdigest()[:10]
        raw_vid_path = images_dir / f"temp_{video_hash}.mp4"
        final_vid_path = images_dir / f"{video_hash}.mp4"

        if final_vid_path.exists():
            youtube_files.append(final_vid_path.name)
            continue

        if download_youtube_via_loader_to(yt_url, raw_vid_path):
            if watermark_img_path and watermark_img_path.exists():
                if add_watermark(raw_vid_path, watermark_img_path, final_vid_path):
                    youtube_files.append(final_vid_path.name)
                    if raw_vid_path.exists():
                        raw_vid_path.unlink()
                else:
                    raw_vid_path.rename(final_vid_path)
                    youtube_files.append(final_vid_path.name)
            else:
                raw_vid_path.rename(final_vid_path)
                youtube_files.append(final_vid_path.name)
        elif raw_vid_path.exists():
            raw_vid_path.unlink()

    fb_files = []
    for fb_url in fb_video_tasks:
        video_hash = hashlib.md5(fb_url.encode()).hexdigest()[:10]
        raw_vid_path = images_dir / f"raw_fb_{video_hash}.mp4"
        final_vid_path = images_dir / f"fb_{video_hash}.mp4"

        if final_vid_path.exists():
            fb_files.append(final_vid_path.name)
            continue

        if download_via_loader_to(fb_url, raw_vid_path):
            if watermark_img_path and watermark_img_path.exists():
                if add_watermark(raw_vid_path, watermark_img_path, final_vid_path):
                    fb_files.append(final_vid_path.name)
                    if raw_vid_path.exists():
                        raw_vid_path.unlink()
                else:
                    raw_vid_path.rename(final_vid_path)
                    fb_files.append(final_vid_path.name)
            else:
                raw_vid_path.rename(final_vid_path)
                fb_files.append(final_vid_path.name)

    final_images = [img for img in images_results if img is not None]
    final_images.extend(youtube_files)
    final_images.extend(fb_files)

    if not final_images:
        logging.warning(f"⚠️ ID={aid}: Нет медиафайлов. Skip.")
        return None

    # --- ШАГ 4: ТЕКСТ И ПЕРЕВОД ---
    if c_div:
        for iframe in c_div.find_all("iframe"):
            iframe.decompose()

        paras = [sanitize_text(p.get_text(strip=True)) for p in c_div.find_all("p")]
        raw_body_text = "\n\n".join(paras)
    else:
        raw_body_text = ""

    final_title = title
    translated_body = ""
    if lang:
        final_title, translated_body = smart_process_and_translate(title, raw_body_text, lang)
        final_title = sanitize_text(final_title)

    art_dir = OUTPUT_DIR / f"{aid}_{slug}"
    art_dir.mkdir(parents=True, exist_ok=True)

    (art_dir / "content.txt").write_text(raw_body_text, encoding="utf-8")

    meta = {
        "id": aid,
        "slug": slug,
        "date": post.get("date"),
        "link": link,
        "title": final_title,
        "text_file": "content.txt",
        "images": final_images,
        "posted": False,
        "hash": curr_hash,
        "translated_to": "",
    }

    if translated_body:
        (art_dir / f"content.{lang}.txt").write_text(f"{final_title}\n\n{translated_body}", encoding="utf-8")
        meta.update({"translated_to": lang, "text_file": f"content.{lang}.txt"})

    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    return meta


# --- MAIN ---
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--slug", default="national")
    parser.add_argument("-n", "--limit", type=int, default=10)
    parser.add_argument("-l", "--lang", default="ru")
    parser.add_argument("--posted-state-file", default="articles/posted.json")
    parser.add_argument("--stopwords-file", default="stopwords.txt")
    parser.add_argument("--watermark-image", default="watermark.png", help="Path to watermark PNG for videos")
    args = parser.parse_args()

    watermark_path = Path(args.watermark_image) if args.watermark_image else None

    if watermark_path and watermark_path.exists():
        logging.info(f"🔧 Режим вотермарки: ВКЛ (файл: {watermark_path})")
    else:
        logging.warning(f"⚠️ Режим вотермарки: ВЫКЛ (файл {watermark_path} не найден)")
        watermark_path = None

    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        cleanup_old_articles(Path(args.posted_state_file), OUTPUT_DIR)

        cid = fetch_cat_id(args.base_url, args.slug)

        posts_light = fetch_posts_light(args.base_url, cid, FETCH_DEPTH)
        if posts_light is None:
            logging.error("💀 Не пробились через Cloudflare. Эстафета следующему раннеру.")
            print("FETCH_FAILED:true", flush=True)
            sys.exit(0)

        posted = load_posted_ids(Path(args.posted_state_file))
        stop = load_stopwords(Path(args.stopwords_file))

        catalog = []
        if CATALOG_PATH.exists():
            try:
                with open(CATALOG_PATH, 'r', encoding='utf-8') as f:
                    catalog = json.load(f)
            except Exception:
                logging.warning("Не удалось прочитать существующий каталог. Создаем новый.")

        new_metas = []
        count = 0

        for p_short in posts_light:
            if count >= args.limit:
                break

            aid = str(p_short["id"])
            if aid in posted:
                continue

            logging.info(f"🆕 Найдена новая статья ID={aid}. Загружаем детали...")

            time.sleep(10)

            full_post = fetch_single_post_full(args.base_url, aid)

            if full_post:
                if meta := parse_and_save(full_post, args.lang, stop, watermark_path):
                    new_metas.append(meta)
                    count += 1

        if new_metas:
            new_ids = {str(m["id"]) for m in new_metas}
            catalog = [item for item in catalog if str(item.get("id")) not in new_ids]
            catalog.extend(new_metas)

            with open(CATALOG_PATH, "w", encoding="utf-8") as f:
                json.dump(catalog, f, ensure_ascii=False, indent=2)

            print("NEW_ARTICLES_STATUS:true")
            logging.info(f"✅ Обработка завершена. Добавлено статей: {len(new_metas)}")
        else:
            print("NEW_ARTICLES_STATUS:false")
            logging.info("🔍 Новых статей не найдено.")

    except Exception:
        logging.exception("🚨 Критическая ошибка в main:")
        exit(1)

if __name__ == "__main__":
    main()
