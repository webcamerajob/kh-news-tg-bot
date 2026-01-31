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
FETCH_DEPTH = 100

# --- НАСТРОЙКИ AI ---
OPENROUTER_KEY = os.getenv("OPENROUTER_API_KEY")

AI_MODELS = [
    "deepseek/deepseek-chat",           
    "openai/gpt-4o-mini",               
    "google/gemini-2.0-flash-exp:free", 
]

# Константа для порта WARP
WARP_PROXY = "socks5h://127.0.0.1:40000"

# Глобальная сессия для парсинга сайтов
SCRAPER = cffi_requests.Session(
    impersonate="chrome110",
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
    session = cffi_requests.Session(impersonate="chrome120")
    try:
        resp = session.get("https://loader.to/ajax/download.php", params={"format": "360", "url": video_url}, timeout=15)
        task_id = resp.json().get("id")
        for _ in range(25):
            time.sleep(3)
            status = session.get("https://loader.to/ajax/progress.php", params={"id": task_id}, timeout=10).json()
            if status.get("success") == 1:
                file_resp = session.get(status.get("download_url"), stream=True, timeout=120)
                with open(output_path, 'wb') as f:
                    for chunk in file_resp.iter_content(8192): f.write(chunk)
                return True
    except: pass
    return False

def add_watermark(input_video, watermark_img, output_video):
    if not Path(watermark_img).exists(): return False
    duration = get_video_duration(input_video)
    
    # ПАРАМЕТРЫ ОБРЕЗКИ: с 8 по 10 сек и последние 12 сек
    c_start, c_end, t_tail = 8.0, 10.0, 12.0

    if duration > 25.0:
        f_point = duration - t_tail
        logging.info(f"✂️ Обрезка: вырезаем 8-10с и хвост после {f_point:.2f}с")
        
        # Фильтр для видео (вырезаем кусок + накладываем вотермарку)
        v_filter = (
            f"[0:v]select='lt(t,{c_start})+between(t,{c_end},{f_point})',setpts=N/FRAME_RATE/TB[main];"
            f"[main][1:v]scale2ref=iw*0.35:-1[vid][wm];[vid][wm]overlay=W-w-10:10"
        )
        # Фильтр для звука
        a_filter = f"aselect='lt(t,{c_start})+between(t,{c_end},{f_point})',asetpts=N/SR/TB"
        
        cmd = [
            "ffmpeg", "-y", "-i", str(input_video), "-i", str(watermark_img),
            "-filter_complex", v_filter,
            "-af", a_filter,
            "-c:v", "libx264", "-preset", "superfast", "-crf", "28",
            "-c:a", "aac", "-b:a", "128k", str(output_video)
        ]
    else:
        # Просто вотермарка без обрезки
        cmd = [
            "ffmpeg", "-y", "-i", str(input_video), "-i", str(watermark_img),
            "-filter_complex", "[1:v][0:v]scale2ref=iw*0.35:-1[wm][vid];[vid][wm]overlay=W-w-10:10",
            "-c:v", "libx264", "-preset", "superfast", "-crf", "28",
            "-c:a", "copy", str(output_video)
        ]
    try:
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        return True
    except: return False

# --- БЛОК 4: API И ПАРСИНГ ---

def fetch_cat_id(url, slug):
    endpoint = f"{url}/wp-json/wp/v2/categories?slug={slug}"
    for attempt in range(1, 4):
        try:
            logging.info(f"📡 Попытка {attempt}/3: Запрос к API {slug}...")
            r = SCRAPER.get(endpoint, timeout=30)
            content_type = r.headers.get("Content-Type", "")
            if "text/html" in content_type:
                logging.warning(f"⚠️ Cloudflare Challenge detected (получен HTML).")
                raise ValueError("Cloudflare JS Challenge active")
            r.raise_for_status()
            data = r.json()
            if data and isinstance(data, list):
                cat_id = data[0]["id"]
                logging.info(f"✅ ID категории найден: {cat_id}")
                return cat_id
            else:
                logging.error(f"❌ Категория '{slug}' не найдена в API.")
                return None
        except Exception as e:
            logging.warning(f"⚠️ Попытка {attempt} провалена: {e}")
            if attempt < 3:
                wait_time = attempt * 10
                logging.info(f"⏳ Ожидание {wait_time} сек перед повторной попыткой...")
                time.sleep(wait_time)
            else:
                logging.error(f"💀 Все попытки исчерпаны. Не удалось получить ID категории.")
                raise

def fetch_posts_light(url: str, cid: int, limit: int) -> List[Dict]:
    params = {"categories": cid, "per_page": limit, "_fields": "id,slug"}
    endpoint = f"{url}/wp-json/wp/v2/posts"
    try:
        r = SCRAPER.get(endpoint, params=params, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception:
        logging.warning("⚠️ Переход на Plan B для получения списка статей...")
        r = requests.get(endpoint, params=params, headers=FALLBACK_HEADERS, timeout=30)
        return r.json()

def fetch_single_post_full(url: str, aid: str) -> Optional[Dict]:
    try:
        r = SCRAPER.get(f"{url}/wp-json/wp/v2/posts/{aid}?_embed", timeout=60)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logging.error(f"Ошибка загрузки контента для ID={aid}: {e}")
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

    meta_path = OUTPUT_DIR / f"{aid}_{slug}" / "meta.json"
    curr_hash = hashlib.sha256(html_txt.encode()).hexdigest()
    if meta_path.exists():
        try:
            m = json.loads(meta_path.read_text(encoding="utf-8"))
            if m.get("hash") == curr_hash:
                logging.info(f"⏭️ ID={aid}: Без изменений.")
                return m
        except: pass

    logging.info(f"Processing ID={aid}: {title[:30]}...")

    soup = BeautifulSoup(html_txt, "html.parser")
    
    for garbage in soup.find_all(["div", "ul", "ol", "section", "aside"], 
                                class_=re.compile(r"rp4wp|related|ad-|post-widget-thumbnail|sharedaddy")):
        garbage.decompose()

    for j in soup.find_all(["span", "script", "style", "iframe"]):
        src = j.get("src", "")
        if "youtube" in src or "youtu.be" in src:
            continue
        if not hasattr(j, 'attrs') or j.attrs is None: continue 
        c = str(j.get("class", ""))
        if j.get("data-mce-type") or "mce_SELRES" in c or "widget" in c: 
            j.decompose()

    ordered_srcs = []
    seen_srcs = set()

    def add_src(url):
        if url and url not in seen_srcs:
            ordered_srcs.append(url)
            seen_srcs.add(url)

    if "_embedded" in post and (m := post["_embedded"].get("wp:featuredmedia")):
        if isinstance(m, list) and (u := m[0].get("source_url")):
            if "logo" not in u.lower():
                add_src(u)

    for link_tag in soup.find_all("a", class_="ci-lightbox", limit=10):
        if h := link_tag.get("href"): 
            if "gif" not in h.lower():
                add_src(h)

    c_div = soup.find("div", class_="entry-content")
    video_srcs = []
    youtube_tasks = []

    if c_div:
        for img in c_div.find_all("img"):
            if u := extract_img_url(img):
                add_src(u)
        
        # Видео (mp4/mov)
        for vid in c_div.find_all("video"):
            if src := vid.get("src"):
                if src not in seen_srcs: video_srcs.append(src); seen_srcs.add(src)
            for source in vid.find_all("source"):
                if src := source.get("src"):
                    if src not in seen_srcs: video_srcs.append(src); seen_srcs.add(src)
        
        for a_tag in c_div.find_all("a"):
            if href := a_tag.get("href"):
                if href.lower().endswith(('.mp4', '.mov', '.m4v')):
                    if href not in seen_srcs: video_srcs.append(href); seen_srcs.add(href)
        
        # YouTube iframe
        for iframe in c_div.find_all("iframe"):
            src = iframe.get("src", "")
            if "youtube.com/embed" in src or "youtu.be" in src:
                if src.startswith("//"): src = "https:" + src
                youtube_tasks.append(src)
        
        # YouTube links
        for yt_a in c_div.find_all("a"):
            href = yt_a.get("href", "")
            if "youtube.com/watch" in href or "youtu.be/" in href:
                if href not in youtube_tasks:
                    youtube_tasks.append(href)
    
    for v in video_srcs:
        ordered_srcs.append(v)
    
    images_dir = OUTPUT_DIR / f"{aid}_{slug}" / "images"
    
    # 1. Скачивание обычных файлов
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

    # 2. Скачивание YouTube и наложение вотермарки
    youtube_files = []
    if youtube_tasks:
        logging.info(f"▶️ Найдено {len(youtube_tasks)} видео с YouTube.")
        
        for idx, yt_url in enumerate(youtube_tasks):
            # Хешируем ссылку для имени файла
            video_hash = hashlib.md5(yt_url.encode()).hexdigest()[:10]
            
            raw_vid_path = images_dir / f"temp_{video_hash}.mp4"
            final_vid_path = images_dir / f"{video_hash}.mp4"
            
            # Если финальное видео уже есть - пропускаем
            if final_vid_path.exists():
                youtube_files.append(final_vid_path.name)
                continue

            # Скачиваем (360p)
            images_dir.mkdir(parents=True, exist_ok=True)
            if download_via_loader_to(yt_url, raw_vid_path):
                # Если вотермарка задана - рендерим
                if watermark_img_path and watermark_img_path.exists():
                    if add_watermark(raw_vid_path, watermark_img_path, final_vid_path):
                        youtube_files.append(final_vid_path.name)
                        # Удаляем сырое
                        if raw_vid_path.exists(): raw_vid_path.unlink()
                    else:
                        # Если рендер не вышел, берем сырое (переименовываем)
                        raw_vid_path.rename(final_vid_path)
                        youtube_files.append(final_vid_path.name)
                else:
                    # Без вотермарки просто переименовываем
                    raw_vid_path.rename(final_vid_path)
                    youtube_files.append(final_vid_path.name)
            else:
                # Очистка мусора при ошибке
                if raw_vid_path.exists(): raw_vid_path.unlink()

    final_images = [img for img in images_results if img is not None]
    final_images.extend(youtube_files)

    if not final_images:
        logging.warning(f"⚠️ ID={aid}: Нет норм картинок/видео. Skip.")
        return None

    if c_div:
        for iframe in c_div.find_all("iframe"):
            iframe.decompose()

    paras = []
    if c_div:
        for r in c_div.find_all(["ul", "ol", "div"], class_=re.compile(r"rp4wp|related|ad-")): 
            r.decompose()
        paras = [sanitize_text(p.get_text(strip=True)) for p in c_div.find_all("p")]
    
    raw_body_text = "\n\n".join(paras)

    final_title = title
    translated_body = ""
    if lang:
        final_title, translated_body = smart_process_and_translate(title, raw_body_text, lang)
        final_title = sanitize_text(final_title)

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
