import random
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
import subprocess # Нужно для вызова FFmpeg
import sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Set
import yt_dlp

# Для перевода
import requests 
from bs4 import BeautifulSoup
# Для парсинга
from curl_cffi import requests as cffi_requests, CurlHttpVersion

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
    "openai/gpt-oss-120b:free",                  # 131K, MoE, сильнейшая в списке
    "z-ai/glm-4.5-air:free",                     # 131K, агентная, держит инструкции
    "meta-llama/llama-3.3-70b-instruct:free",    # 66K, проверенная рабочая лошадка
    "google/gemma-3-27b-it:free",                # 131K, многоязычная
    "google/gemma-3-12b-it:free",                # 33K, легче и быстрее
    "openai/gpt-oss-20b:free",                   # 131K, запасной от OpenAI
    "nousresearch/hermes-3-llama-3.1-405b:free", # 131K, тяжёлый fallback
    "qwen/qwen-2.5-72b-instruct",        # Убрал :free
    "google/gemini-2.0-flash-001",       # Рабочий эндпоинт
    "deepseek/deepseek-chat",            # Твой единственный живой вариант в логах
    "openai/gpt-4o-mini",
]

# Константа для порта WARP
WARP_PROXY = "socks5h://127.0.0.1:40000"

# Глобальная сессия для парсинга сайтов
SCRAPER = cffi_requests.Session(
    impersonate="chrome119",
    proxies={
        "http": WARP_PROXY,
        "https": WARP_PROXY
    },
    http_version=CurlHttpVersion.V1_1
)

IPHONE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com/",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "cross-site",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1"
}

FALLBACK_HEADERS = IPHONE_HEADERS

def rotate_warp(hard: bool = False):
    """Переподключает WARP. hard=True — полная перерегистрация (новый device, новый IP)."""
    try:
        if hard:
            logging.info("♻️ WARP: HARD ротация (новая регистрация)...")
            subprocess.run(["warp-cli", "--accept-tos", "disconnect"], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(2)
            subprocess.run(["warp-cli", "--accept-tos", "registration", "delete"], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(2)
            subprocess.run(["warp-cli", "--accept-tos", "registration", "new"], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(2)
            subprocess.run(["warp-cli", "--accept-tos", "mode", "proxy"], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            subprocess.run(["warp-cli", "--accept-tos", "proxy", "port", "40000"], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            subprocess.run(["warp-cli", "--accept-tos", "connect"], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(8)
            logging.info("✅ WARP: Перерегистрирован.")
        else:
            logging.info("♻️ WARP: Ротация IP...")
            subprocess.run(["warp-cli", "--accept-tos", "disconnect"], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(2)
            subprocess.run(["warp-cli", "--accept-tos", "connect"], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(5)
            logging.info("✅ WARP: Переподключено.")
    except Exception as e:
        logging.error(f"❌ Ошибка ротации WARP: {e}")

# --- БЛОК 1: ПЕРЕВОД И ИИ ---

def direct_google_translate(text: str, to_lang: str = "ru") -> str:
    if not text: return ""
    chunks = []
    current_chunk = ""
    for paragraph in text.split('\n'):
        if len(current_chunk) + len(paragraph) < 1800:
            current_chunk += paragraph + "\n"
        else:
            chunks.append(current_chunk)
            current_chunk = paragraph + "\n"
    if current_chunk: chunks.append(current_chunk)
    
    translated_parts = []
    url = "https://translate.googleapis.com/translate_a/single"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"}
    
    for chunk in chunks:
        if not chunk.strip():
            translated_parts.append("")
            continue
        try:
            params = {"client": "gtx", "sl": "en", "tl": to_lang, "dt": "t", "q": chunk.strip()}
            r = requests.get(url, params=params, headers=headers, timeout=10)
            if r.status_code == 200:
                data = r.json()
                text_part = "".join([item[0] for item in data[0] if item and item[0]])
                translated_parts.append(text_part)
            else:
                translated_parts.append(chunk)
            time.sleep(0.3)
        except Exception:
            translated_parts.append(chunk)
    return "\n".join(translated_parts)

def strip_ai_chatter(text: str) -> str:
    text = text.strip()
    match = re.match(r'^\s*\*\*(.*?)\*\*', text, re.DOTALL)
    if match:
        removed_header = match.group(1).strip()
        logging.info(f"✂️ Вырезан заголовок ИИ: '**{removed_header}**'")
        return text[match.end():].strip()
    return text

def smart_process_and_translate(title: str, body: str, lang: str) -> (str, str):
    clean_body = body
    if OPENROUTER_KEY and len(body) > 500:
        logging.info("⏳ Подготовка к ИИ-чистке (OpenRouter)...")
        safe_body = body[:15000].replace('\x00', '')
        prompt = (
            f"You are a ruthless news editor.\n"
            f"INPUT: Raw news text.\n"
            f"OUTPUT: A cleaned-up version of the story in ENGLISH.\n\n"
            "STRICT EDITING RULES:\n"
            "1. CONSOLIDATE NARRATIVE & SPEECH: If the author states a fact, and then a speaker repeats the same meaning, DELETE the speaker's part.\n"
            "2. KEEP UNIQUE DETAILS: Only keep quotes if they add numbers, dates, or emotion.\n"
            "3. REMOVE FLUFF: Delete ads and diplomatic praise.\n"
            "4. NO META-TALK: Start with the story immediately.\n\n"
            f"RAW TEXT:\n{safe_body}"
        )
        ai_result = ""
        for model in AI_MODELS:
            try:
                logging.info(f"🚀 Запрос к OpenRouter: {model}...")

                if model != AI_MODELS[0]: 
                    time.sleep(2)

                response = requests.post(
                    url="https://openrouter.ai/api/v1/chat/completions",
                    headers={"Authorization": f"Bearer {OPENROUTER_KEY}", "HTTP-Referer": "https://github.com/kh-news-bot", "X-Title": "NewsBot"},
                    json={"model": model, "messages": [{"role": "user", "content": prompt}], "temperature": 0.3, "max_tokens": 4096},
                    timeout=50
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
    logging.info(f"🌍 [Google] Перевод...")
    translated_full = direct_google_translate(combined_text, lang)
    final_title = title
    final_text = clean_body
    if translated_full:
        if DELIMITER in translated_full:
            parts = translated_full.split(DELIMITER, 1)
            final_title = parts[0].strip()
            final_text = parts[1].strip()
        elif "|||" in translated_full:
            parts = translated_full.split("|||", 1)
            final_title = parts[0].strip()
            final_text = parts[1].strip()
        else:
            parts = translated_full.split('\n', 1)
            final_title = parts[0].strip()
            final_text = parts[1].strip() if len(parts) > 1 else ""
    return final_title, final_text

# --- БЛОК 2: ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

def cleanup_old_articles(posted_ids_path: Path, articles_dir: Path):
    if not posted_ids_path.is_file() or not articles_dir.is_dir(): return
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
                        shutil.rmtree(f); cleaned += 1
        if cleaned: logging.info(f"🧹 Удалено {cleaned} старых папок.")
    except Exception: pass

def sanitize_text(text: str) -> str:
    if not text: return ""
    text = html.unescape(text)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'mce_SELRES_[^ ]+', '', text)
    return re.sub(r'\n{3,}', '\n\n', text).strip()

def load_posted_ids(state_file_path: Path) -> Set[str]:
    try:
        if state_file_path.exists():
            with open(state_file_path, 'r', encoding='utf-8') as f:
                fcntl.flock(f, fcntl.LOCK_SH)
                return {str(item) for item in json.load(f)}
        return set()
    except Exception: return set()

def load_stopwords(file_path: Optional[Path]) -> List[str]:
    if not file_path or not file_path.exists(): return []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return [line.strip().lower() for line in f if line.strip()]
    except Exception: return []

# --- БЛОК 3: УМНЫЙ ПОИСК И СКАЧИВАНИЕ ---

def extract_img_url(img_tag: Any) -> Optional[str]:
    def is_junk(url_str: str) -> bool:
        u = url_str.lower()
        bad = ["gif", "logo", "banner", "icon", "avatar", "button", "share", "pixel", "tracker"]
        if any(b in u for b in bad): return True
        if re.search(r'-\d{2,3}x\d{2,3}\.', u): return True
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
        except Exception: pass
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
    if not url or url.startswith('data:'): return None
    folder.mkdir(parents=True, exist_ok=True)
    url_hash = hashlib.md5(url.encode()).hexdigest()[:10]
    orig_fn = url.rsplit('/', 1)[-1].split('?', 1)[0]
    if '.' in orig_fn:
        ext = orig_fn.split('.')[-1].lower()
    else:
        ext = 'jpg'
    # Video extensions handling
    if len(ext) > 4 and ext not in ['mp4', 'mov', 'm4v']: ext = 'jpg'
    
    fn = f"{url_hash}.{ext}"
    dest = folder / fn
    timeout = 60 if ext in ['mp4', 'mov', 'm4v'] else 20

    try:
        resp = SCRAPER.get(url, timeout=timeout)
        if resp.status_code == 200:
            dest.write_bytes(resp.content)
            return str(dest)
    except Exception:
        pass 
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
    except: return 0.0

def download_via_loader_to(video_url, output_path):
    """
    Скачивает видео через yt-dlp.
    Название функции оставил старым, чтобы не ломать твой остальной код.
    """
    ydl_opts = {
        'outtmpl': str(output_path),
        'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
        'merge_output_format': 'mp4',
        'quiet': True,
        'no_warnings': True,
        'noplaylist': True,
    }
    
    for attempt in range(1, 4):
        try:
            logging.info(f"⬇️ Качаем видео (Попытка {attempt}): {video_url}")
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([video_url])
            
            # Проверяем, что файл реально скачался и он не пустой
            if Path(output_path).exists() and Path(output_path).stat().st_size > 0:
                logging.info("✅ Видео успешно скачано!")
                return True
            else:
                logging.warning("⚠️ yt-dlp отработал, но файл пуст.")
                time.sleep(3)
        except Exception as e:
            logging.error(f"❌ Ошибка yt-dlp: {type(e).__name__}: {e}")
            time.sleep(5) # Ждем перед ретраем
            
    return False

def add_watermark(input_video, watermark_img, output_video):
    if not Path(watermark_img).exists():
        logging.error(f"❌ Вотермарка не найдена: {watermark_img}")
        return False

    duration = get_video_duration(input_video)
    
    # ПАРАМЕТРЫ ВИДЕО
    c_start, c_end, t_tail = 0.0, 0.0, 11.0
    wm_scale = 0.4

    # --- НАСТРОЙКИ ОТСТУПОВ (Относительные) ---
    # 0.03 означает 3% от ширины/высоты видео
    pad_rel = 0.03

    # --- ФОРМУЛЫ ---
    # 1. Масштабирование (привязываем ширину ВМ к ширине видео)
    scale_expr = f"scale2ref=w=iw*{wm_scale}:h=ow/(main_w/main_h)[wm][vid]"
    
    # 2. Фикс пикселей (чтобы лого не плющило)
    wm_sar_fix = "[wm]setsar=1[wm_fixed]"
    
    # 3. ПОЗИЦИОНИРОВАНИЕ (Правый верхний угол)
    # x = Ширина_видео - Ширина_ВМ - Отступ
    # y = Отступ (зависит от ориентации: для вертикальных чуть больше - 7%)
    x_expr = f"W-w-(W*{pad_rel})"
    y_expr = f"if(gt(H,W), H*0.07, H*{pad_rel})"
    
    overlay_expr = f"[vid][wm_fixed]overlay=x='{x_expr}':y='{y_expr}'"

    if duration > 25.0:
        f_point = duration - t_tail
        logging.info(f"✂️ Обрезка + Правый верхний угол (Scale: {wm_scale})")
        
        # ВАЖНО: Сначала накладываем вотермарк на весь поток, а ПОТОМ обрезаем. 
        # Это исключает черные экраны и пропадание видео.
        v_filter = (
            f"[1:v][0:v]{scale_expr};"
            f"{wm_sar_fix};"
            f"{overlay_expr},select='lt(t,{c_start})+between(t,{c_end},{f_point})',setpts=N/FRAME_RATE/TB"
        )
        a_filter = f"aselect='lt(t,{c_start})+between(t,{c_end},{f_point})',asetpts=N/SR/TB"
        
        cmd = [
            "ffmpeg", "-y", "-i", str(input_video), "-i", str(watermark_img),
            "-filter_complex", v_filter,
            "-af", a_filter,
            "-c:v", "libx264", "-preset", "superfast", "-crf", "26",
            "-c:a", "aac", "-b:a", "128k", str(output_video)
        ]
    else:
        logging.info(f"⚠️ Видео короткое, Правый верхний угол (Scale: {wm_scale})")
        
        full_filter = (
            f"[1:v][0:v]{scale_expr};"
            f"{wm_sar_fix};"
            f"{overlay_expr}"
        )
        
        cmd = [
            "ffmpeg", "-y", "-i", str(input_video), "-i", str(watermark_img),
            "-filter_complex", full_filter,
            "-c:v", "libx264", "-preset", "superfast", "-crf", "26",
            "-c:a", "copy", str(output_video)
        ]
    
    try:
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"❌ FFmpeg Error: {e.stderr.decode()}")
        return False

# --- БЛОК 4: API И ПАРСИНГ ---

def fetch_cat_id(url, slug):
    # --- HARDCODE BLOCK ---
    # Чтобы лишний раз не дёргать Cloudflare на старте,
    # возвращаем известные ID сразу.
    if slug == "national":
        logging.info(f"ℹ️ [Skip Net] Используем Hardcoded ID для '{slug}': 19")
        return 19
    # ----------------------

    endpoint = f"{url}/wp-json/wp/v2/categories?slug={slug}"
    # Используем Fallback заголовки
    fallback_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    for attempt in range(1, 4):
        try:
            logging.info(f"📡 Попытка {attempt}/3: Запрос к API {slug}...")
            
            # Попытка 1: Через curl_cffi (WARP)
            try:
                r = SCRAPER.get(endpoint, timeout=30)
            except Exception as e:
                logging.warning(f"⚠️ SCRAPER fail: {e}, пробуем requests...")
                r = requests.get(endpoint, headers=fallback_headers, timeout=30)

            content_type = r.headers.get("Content-Type", "")
            
            # Проверка на Cloudflare
            if "text/html" in content_type or "<!DOCTYPE html>" in r.text[:100]:
                logging.warning(f"⚠️ Cloudflare Challenge detected.")
                raise ValueError("Cloudflare JS Challenge active")
            
            r.raise_for_status()
            data = r.json()
            
            if data and isinstance(data, list):
                cat_id = data[0]["id"]
                logging.info(f"✅ ID категории найден: {cat_id}")
                return cat_id
            else:
                logging.error(f"❌ Категория '{slug}' не найдена.")
                return None

        except Exception as e:
            logging.warning(f"⚠️ Попытка {attempt} провалена: {e}")
            if attempt < 3:
                time.sleep(5 * attempt)
            else:
                # ВМЕСТО ПАДЕНИЯ ВОЗВРАЩАЕМ 19 КАК ПОСЛЕДНЮЮ НАДЕЖДУ
                logging.error(f"💀 Все попытки исчерпаны. Возвращаем дефолтный ID 19.")
                return 19

def fetch_posts_light(url: str, cid: int, limit: int) -> List[Dict]:
    params = {"categories": cid, "per_page": limit, "_fields": "id,slug,link,title,date"}
    endpoint = f"{url}/wp-json/wp/v2/posts"
    
    # Заголовки
    fallback_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Referer": "https://www.google.com/"
    }

    # Используем глобальную переменную, чтобы можно было её обновить
    global SCRAPER 

    for attempt in range(1, 9):
        try:
            logging.info(f"📥 Скачиваем список статей (Попытка {attempt}/5)...")
            
            response = None
            # 1. Пробуем через WARP (curl_cffi)
            try:
                response = SCRAPER.get(endpoint, params=params, timeout=45)
            except Exception as e:
                logging.warning(f"⚠️ SCRAPER ошибка: {e}. Пробуем ротацию...")
                
            # АНАЛИЗ ОТВЕТА
            is_blocked = False
            if not response:
                is_blocked = True
            elif response.status_code in [403, 503, 429]:
                is_blocked = True
                logging.warning(f"⚠️ Cloudflare Block (Code {response.status_code}).")
            elif "text/html" in response.headers.get("Content-Type", "") or "<!DOCTYPE html>" in response.text[:100]:
                is_blocked = True
                logging.warning(f"⚠️ Получен HTML (Cloudflare Challenge) вместо JSON.")

            # ЕСЛИ ЗАБЛОКИРОВАЛИ -> РОТАЦИЯ
            if is_blocked:
                logging.info("🔄 Запускаем процедуру смены IP и сессии...")
                
                # Первые 2 попытки — мягкая ротация, потом hard (новая регистрация)
                rotate_warp(hard=(attempt >= 2))
                
                # 2. Пересоздаем сессию (ВАЖНО: сброс TLS fingerprint)
                logging.info("🛠 Пересоздание сессии SCRAPER...")
                SCRAPER = cffi_requests.Session(
                    impersonate="chrome120", # Можно попробовать повысить версию
                    proxies={"http": WARP_PROXY, "https": WARP_PROXY},
                    http_version=CurlHttpVersion.V1_1
                )
                
                # Короткая пауза перед новым запросом
                time.sleep(3)
                continue # Идем на следующий круг цикла (attempt + 1)

            # 4. Безопасный парсинг (если не заблокированы)
            try:
                data = response.json()
                if isinstance(data, list):
                    return data
                elif isinstance(data, dict) and "code" in data:
                     logging.error(f"❌ API Error: {data.get('message')}")
                return []
            except Exception:
                logging.error("❌ Ошибка парсинга JSON (видимо, пришел мусор).")
                rotate_warp(hard=(attempt >= 2))
                continue
                
        except Exception as e:
            logging.error(f"❌ Общая ошибка цикла: {e}")
            rotate_warp(hard=(attempt >= 2))
            time.sleep(5)

    logging.error("💀 Не удалось получить список статей после всех попыток.")
    return []

def fetch_single_post_full(url: str, aid: str) -> Optional[Dict]:
    try:
        r = SCRAPER.get(f"{url}/wp-json/wp/v2/posts/{aid}?_embed", timeout=60)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logging.error(f"Ошибка загрузки контента для ID={aid}: {e}")
        return None

def parse_and_save(post, lang, stopwords, watermark_img_path: Optional[Path] = None):
    # Задержка для обхода лимитов
    time.sleep(2)
    aid, slug, link = str(post["id"]), post["slug"], post.get("link")
    
    # Извлекаем и чистим заголовок
    raw_title = BeautifulSoup(post["title"]["rendered"], "html.parser").get_text(strip=True)
    title = sanitize_text(raw_title)

    # Проверка на стоп-слова
    if stopwords:
        for ph in stopwords:
            if ph in title.lower():
                logging.info(f"🚫 ID={aid}: Стоп-слово '{ph}'")
                return None

    # Шаг 1: Загрузка HTML (основной скрапер + fallback)
    html_txt = ""
    try:
        resp = SCRAPER.get(link, timeout=30)
        if resp.status_code == 200:
            html_txt = resp.text
    except Exception as e:
        logging.warning(f"⚠️ ID={aid}: Scraper не открыл ссылку ({e}). Пробуем requests...")

    if not html_txt:
        try:
            resp = requests.get(link, headers=FALLBACK_HEADERS, timeout=30)
            if resp.status_code == 200:
                html_txt = resp.text
            else:
                logging.error(f"❌ ID={aid}: Ошибка загрузки HTML {resp.status_code}")
                return None
        except Exception as e:
            logging.error(f"❌ ID={aid}: Не удалось открыть статью: {e}")
            return None

    # Проверка на изменения через хеш контента
    meta_path = OUTPUT_DIR / f"{aid}_{slug}" / "meta.json"
    curr_hash = hashlib.sha256(html_txt.encode()).hexdigest()
    if meta_path.exists():
        try:
            m = json.loads(meta_path.read_text(encoding="utf-8"))
            if m.get("hash") == curr_hash:
                logging.info(f"⏭️ ID={aid}: Без изменений.")
                return m
        except:
            pass

    logging.info(f"Processing ID={aid}: {title[:30]}...")
    soup = BeautifulSoup(html_txt, "html.parser")
    
    # Находим основной контент (важно сделать это до очистки soup)
    c_div = soup.find("div", class_="entry-content")
    
    # --- ШАГ 1: ПОИСК ВИДЕО (В т.ч. вне c_div) ---
    fb_video_tasks = []
    youtube_tasks = []
    import urllib.parse as urlparse

    # Сбор FB-видео из заглушек div.fb-video / blockquote.fb-xfbml-parse-ignore
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
    # Удаляем виджеты, рекламу и связанные посты
    for garbage in soup.find_all(["div", "ul", "ol", "section", "aside"], 
                                class_=re.compile(r"rp4wp|related|ad-|post-widget-thumbnail|sharedaddy")):
        garbage.decompose()

    # Удаляем скрипты и стили
    for j in soup.find_all(["span", "script", "style"]):
        if not hasattr(j, 'attrs') or j.attrs is None: continue 
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

    # Featured Image (миниатюра записи)
    if "_embedded" in post and (m := post["_embedded"].get("wp:featuredmedia")):
        if isinstance(m, list) and (u := m[0].get("source_url")):
            if "logo" not in u.lower():
                add_src(u)

    video_srcs = []
    if c_div:
        # Картинки из текста
        for img in c_div.find_all("img"):
            if u := extract_img_url(img):
                add_src(u)
        
        # Ссылки на YouTube (текстовые)
        for yt_a in c_div.find_all("a"):
            href = yt_a.get("href", "")
            if "youtube.com/watch" in href or "youtu.be/" in href:
                if href not in youtube_tasks:
                    youtube_tasks.append(href)

        # Прямые ссылки на видеофайлы
        for a_tag in c_div.find_all("a"):
            href = a_tag.get("href", "")
            if href.lower().endswith(('.mp4', '.mov', '.m4v')):
                if href not in seen_srcs: 
                    video_srcs.append(href)
                    seen_srcs.add(href)

    # Добавляем найденные файлы видео в общую очередь скачивания
    for v in video_srcs:
        ordered_srcs.append(v)
    
    images_dir = OUTPUT_DIR / f"{aid}_{slug}" / "images"
    images_dir.mkdir(parents=True, exist_ok=True)
    
    # 1. Скачивание статических файлов (картинки, mp4 по ссылкам)
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

    # 2. Обработка YouTube (скачивание + вотермарка)
    youtube_files = []
    for yt_url in youtube_tasks:
        video_hash = hashlib.md5(yt_url.encode()).hexdigest()[:10]
        raw_vid_path = images_dir / f"temp_{video_hash}.mp4"
        final_vid_path = images_dir / f"{video_hash}.mp4"
        
        if final_vid_path.exists():
            youtube_files.append(final_vid_path.name)
            continue

        if download_via_loader_to(yt_url, raw_vid_path):
            if watermark_img_path and watermark_img_path.exists():
                if add_watermark(raw_vid_path, watermark_img_path, final_vid_path):
                    youtube_files.append(final_vid_path.name)
                    if raw_vid_path.exists(): raw_vid_path.unlink()
                else:
                    raw_vid_path.rename(final_vid_path)
                    youtube_files.append(final_vid_path.name)
            else:
                raw_vid_path.rename(final_vid_path)
                youtube_files.append(final_vid_path.name)
        elif raw_vid_path.exists(): raw_vid_path.unlink()

    # 3. Обработка Facebook видео
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
                    if raw_vid_path.exists(): raw_vid_path.unlink()
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
        # Финальная очистка контента от iframe перед извлечением текста
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

    # Сохранение файлов
    art_dir = OUTPUT_DIR / f"{aid}_{slug}"
    art_dir.mkdir(parents=True, exist_ok=True)
    
    (art_dir / "content.txt").write_text(raw_body_text, encoding="utf-8")
    
    meta = {
        "id": aid, "slug": slug, "date": post.get("date"), "link": link,
        "title": final_title, "text_file": "content.txt",
        "images": final_images,
        "posted": False,
        "hash": curr_hash, "translated_to": ""
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
    # ТУТ ИЗМЕНЕНИЕ: default="watermark.png"
    parser.add_argument("--watermark-image", default="watermark.png", help="Path to watermark PNG for videos")
    args = parser.parse_args()

    watermark_path = Path(args.watermark_image) if args.watermark_image else None
    
    # Логируем, нашел он вотермарку или нет
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
