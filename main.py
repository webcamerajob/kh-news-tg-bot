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
import subprocess
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Set

# Для перевода используем requests (стабильно работает с Google GTX)
import requests 
from bs4 import BeautifulSoup
# Для парсинга используем curl_cffi с профилем Safari (чтобы сайт не банил)
from curl_cffi import requests as cffi_requests, CurlHttpVersion

# Настройка максимально подробного логирования
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# --- КОНФИГУРАЦИЯ ---
OUTPUT_DIR = Path("articles")
CATALOG_PATH = OUTPUT_DIR / "catalog.json"
MAX_RETRIES = 3
BASE_DELAY = 1.0
MAX_POSTED_RECORDS = 100 
FETCH_DEPTH = 100 

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")

AI_MODELS = [
    "meta-llama/llama-3.3-70b-instruct:free",
    "google/gemini-2.0-flash-exp:free",
    "deepseek/deepseek-r1-distill-llama-70b:free",
]

# --- НАСТРОЙКИ СЕТИ (PARSER) ---
SCRAPER = cffi_requests.Session(impersonate="safari15_5")
SCRAPER.headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.5 Safari/605.1.15",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com/"
}
SCRAPER_TIMEOUT = 60 
BAD_RE = re.compile(r"[\u200b-\u200f\uFEFF\u200E\u00A0]")

# --- БЛОК 1: ПЕРЕВОД И ИИ ---

def direct_google_translate(text: str, to_lang: str = "ru") -> str:
    """Перевод через Google GTX с логированием прогресса"""
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
    
    logger.info(f"🌍 Перевод: разбито на {len(chunks)} частей.")
    for i, chunk in enumerate(chunks):
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
                logger.info(f"   - Часть {i+1} переведена.")
            else:
                translated_parts.append(chunk)
            time.sleep(0.3)
        except Exception as e:
            logger.error(f"   - Ошибка перевода части {i+1}: {e}")
            translated_parts.append(chunk)
    return "\n".join(translated_parts)

def strip_ai_chatter(text: str) -> str:
    bad_prefixes = ["Here is", "The article", "Summary:", "Cleaned text:"]
    for prefix in bad_prefixes:
        if text.lower().startswith(prefix.lower()):
            parts = text.split('\n', 1)
            if len(parts) > 1: return parts[1].strip()
    return text

def smart_process_and_translate(title: str, body: str, lang: str) -> (str, str):
    clean_body = body
    if OPENROUTER_API_KEY and len(body) > 500:
        logger.info("🤖 AI: Начинаем чистку текста...")
        time.sleep(3)
        prompt = (
            f"You are a ruthless news editor.\nINPUT: Raw news text.\nOUTPUT: A cleaned-up version of the story in ENGLISH.\n\n"
            "STRICT EDITING RULES:\n1. CONSOLIDATE NARRATIVE & SPEECH: If a fact is repeated, delete the repetition.\n"
            "2. REMOVE ADS: Delete all fluff and promotional links.\n"
            f"RAW TEXT:\n{body[:15000]}"
        )
        ai_result = ""
        for model in AI_MODELS:
            try:
                logger.info(f"   - Пробуем модель: {model}")
                response = requests.post(
                    url="https://openrouter.ai/api/v1/chat/completions",
                    headers={"Authorization": f"Bearer {OPENROUTER_API_KEY}", "Content-Type": "application/json"},
                    data=json.dumps({"model": model, "messages": [{"role": "user", "content": prompt}], "temperature": 0.3}),
                    timeout=60
                )
                if response.status_code == 200:
                    result = response.json()
                    ai_result = result['choices'][0]['message']['content'].strip()
                    logger.info("   ✅ AI успешно почистил текст.")
                    break
            except Exception as e: 
                logger.warning(f"   ⚠️ Ошибка AI ({model}): {e}")
                continue
        if ai_result: clean_body = strip_ai_chatter(ai_result)

    DELIMITER = " ||| "
    combined_text = f"{title}{DELIMITER}{clean_body}"
    translated_full = direct_google_translate(combined_text, lang)
    final_title, final_text = title, clean_body
    if translated_full:
        if DELIMITER in translated_full:
            parts = translated_full.split(DELIMITER, 1)
            final_title, final_text = parts[0].strip(), parts[1].strip()
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
            if f.is_dir() and f.name.split('_', 1)[0] not in ids_to_keep:
                shutil.rmtree(f)
                cleaned += 1
        if cleaned > 0: logger.info(f"🧹 Очищено старых статей: {cleaned}")
    except Exception as e: logger.error(f"Ошибка очистки: {e}")

def sanitize_text(text: str) -> str:
    if not text: return ""
    text = html.unescape(text)
    text = re.sub(r'<[^>]+>', '', text)
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

# --- БЛОК 3: УМНЫЙ ПОИСК КАРТИНОК И ВИДЕО ---

def apply_watermark_to_image(img_path: Path, watermark_path: str = "watermark.png"):
    """Накладывает вотермарку (35% ширины) на изображение"""
    if not os.path.exists(watermark_path) or not img_path.exists(): return
    temp_out = img_path.with_name(f"wm_{img_path.name}")
    
    # Расчет: 35% ширины, правый верхний угол (отступ 10px)
    cmd = [
        "ffmpeg", "-y", "-i", str(img_path), "-i", watermark_path,
        "-filter_complex", "[1:v][0:v]scale2ref=iw*0.35:-1[wm][img];[img][wm]overlay=W-w-10:10",
        "-q:v", "2", str(temp_out)
    ]
    try:
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        os.replace(temp_out, img_path)
    except Exception as e: logger.error(f"   ❌ FFmpeg Image Error ({img_path.name}): {e}")

def process_video_logic(video_url, watermark_path="watermark.png"):
    """Скачивание и обработка видео (360p + вотермарка)"""
    if not video_url: return None
    logger.info(f"🎬 Видео: Найдена ссылка {video_url}. Начинаем обработку...")
    ts = int(time.time())
    raw, final = Path(f"raw_{ts}.mp4"), Path(f"video_{ts}.mp4")
    session = cffi_requests.Session(impersonate="chrome120")
    try:
        # 1. Запрос к Loader.to
        resp = session.get("https://loader.to/ajax/download.php", params={"format": "360", "url": video_url}, timeout=15)
        task_id = resp.json().get("id")
        
        # 2. Ожидание
        download_url = None
        for i in range(25):
            time.sleep(3)
            status = session.get("https://loader.to/ajax/progress.php", params={"id": task_id}).json()
            logger.info(f"   - Ожидание видео ({i+1}/25): {status.get('text')}")
            if status.get("success") == 1:
                download_url = status.get("download_url")
                break
        
        if not download_url: 
            logger.error("   ❌ Видео не было получено от Loader.to")
            return None

        # 3. Скачивание
        logger.info("   - Скачивание файла...")
        with session.get(download_url, stream=True) as r:
            with open(raw, 'wb') as f:
                for chunk in r.iter_content(8192): f.write(chunk)
        
        # 4. FFmpeg
        logger.info("   - Рендеринг вотермарки (360p)...")
        cmd = [
            "ffmpeg", "-y", "-i", str(raw), "-i", watermark_path,
            "-filter_complex", "[1:v][0:v]scale2ref=iw*0.35:-1[wm][vid];[vid][wm]overlay=W-w-10:10",
            "-c:v", "libx264", "-preset", "superfast", "-crf", "28", "-c:a", "copy", str(final)
        ]
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        if raw.exists(): raw.unlink()
        logger.info(f"   ✅ Видео готово: {final}")
        return str(final)
    except Exception as e:
        logger.error(f"   ❌ Ошибка видео: {e}")
        if raw.exists(): raw.unlink()
        return None

def extract_img_url(img_tag: Any) -> Optional[str]:
    width_attr = img_tag.get("width")
    if width_attr and width_attr.isdigit() and int(width_attr) < 400: return None
    attrs = ["data-orig-file", "data-large-file", "data-src", "data-lazy-src", "src"]
    for attr in attrs:
        if val := img_tag.get(attr):
            clean_val = val.split()[0].split(',')[0].split('?')[0]
            return clean_val
    return None

def save_image(url, folder):
    folder.mkdir(parents=True, exist_ok=True)
    fn = hashlib.md5(url.encode()).hexdigest() + ".jpg"
    dest = folder / fn
    try:
        dest.write_bytes(SCRAPER.get(url, timeout=SCRAPER_TIMEOUT).content)
        # Накладываем вотермарку сразу после сохранения
        apply_watermark_to_image(dest)
        logger.info(f"   🖼️ Фото сохранено и вотермаркнуто: {fn}")
        return str(dest)
    except Exception as e:
        logger.error(f"   ❌ Ошибка сохранения фото {url}: {e}")
        return None

# --- БЛОК 4: API И ПАРСИНГ ---

def fetch_cat_id(url, slug):
    r = SCRAPER.get(f"{url}/wp-json/wp/v2/categories?slug={slug}", timeout=SCRAPER_TIMEOUT)
    r.raise_for_status()
    return r.json()[0]["id"]

def fetch_posts(url, cid, limit):
    logger.info(f"📡 Запрашиваем {limit} последних статей из API...") 
    try:
        r = SCRAPER.get(f"{url}/wp-json/wp/v2/posts?categories={cid}&per_page={limit}&_embed", timeout=SCRAPER_TIMEOUT)
        r.raise_for_status()
        posts = r.json()
        logger.info(f"✅ Получено {len(posts)} постов.")
        return posts
    except Exception as e:
        logger.error(f"❌ Ошибка API: {e}")
        return []
        
def parse_and_save(post, lang, stopwords):
    time.sleep(2)
    aid, slug, link = str(post["id"]), post["slug"], post.get("link")
    raw_title = BeautifulSoup(post["title"]["rendered"], "html.parser").get_text(strip=True)
    title = sanitize_text(raw_title)

    logger.info(f"📖 Обработка статьи ID {aid}: {title[:50]}...")

    if stopwords and any(ph in title.lower() for ph in stopwords):
        logger.info(f"   🚫 Пропуск: Стоп-слово в заголовке.")
        return None

    try:
        html_txt = SCRAPER.get(link, timeout=SCRAPER_TIMEOUT).text
    except Exception as e:
        logger.error(f"   ❌ Ошибка загрузки HTML статьи: {e}")
        return None

    meta_path = OUTPUT_DIR / f"{aid}_{slug}" / "meta.json"
    curr_hash = hashlib.sha256(html_txt.encode()).hexdigest()
    
    # Проверка на изменения
    if meta_path.exists():
        try:
            m = json.loads(meta_path.read_text(encoding="utf-8"))
            if m.get("hash") == curr_hash:
                logger.info(f"   ⏭️ Пропуск: Статья не изменилась (хэш совпадает).")
                return m
        except: pass

    soup = BeautifulSoup(html_txt, "html.parser")
    
    # --- ИЩЕМ ВИДЕО ДО ОЧИСТКИ ---
    video_url = None
    if iframe := soup.find("iframe"):
        src = iframe.get("src", "")
        if "youtube" in src or "youtu.be" in src:
            video_url = src

    # Очистка мусора
    for r in soup.find_all("div", class_="post-widget-thumbnail"): r.decompose()
    for j in soup.find_all(["span", "div", "script", "style", "iframe"]):
        if not hasattr(j, 'attrs') or j.attrs is None: continue 
        c = str(j.get("class", ""))
        if j.get("data-mce-type") or "mce_SELRES" in c or "widget" in c: j.decompose()

    paras = []
    if c_div := soup.find("div", class_="entry-content"):
        # Удаление связанных постов внутри текста
        for r in c_div.find_all(["ul", "ol", "div"], class_=re.compile(r"rp4wp|related|ad-")): r.decompose()
        paras = [sanitize_text(p.get_text(strip=True)) for p in c_div.find_all("p")]
    
    raw_body_text = BAD_RE.sub("", "\n\n".join(paras))
    
    # Сбор картинок
    srcs = set()
    for link_tag in soup.find_all("a", class_="ci-lightbox", limit=10):
        if h := link_tag.get("href"): srcs.add(h)
    if c_div:
        for img in c_div.find_all("img"):
            if u := extract_img_url(img): srcs.add(u)
    
    images = []
    if srcs:
        logger.info(f"   🖼️ Найдено {len(srcs)} картинок. Скачиваем...")
        with ThreadPoolExecutor(5) as ex:
            futs = {ex.submit(save_image, u, OUTPUT_DIR / f"{aid}_{slug}" / "images"): u for u in list(srcs)[:10]}
            for f in as_completed(futs):
                if p:=f.result(): images.append(p)

    # Обработка видео, если есть
    processed_video = None
    if video_url:
        processed_video = process_video_logic(video_url)

    # AI и Перевод
    final_title, translated_body = title, raw_body_text
    if lang:
        final_title, translated_body = smart_process_and_translate(title, raw_body_text, lang)

    art_dir = OUTPUT_DIR / f"{aid}_{slug}"
    art_dir.mkdir(parents=True, exist_ok=True)
    
    meta = {
        "id": aid, 
        "slug": slug, 
        "title": final_title,
        "images": sorted([Path(p).name for p in images]), 
        "posted": False,
        "hash": curr_hash, 
        "video_url": video_url,
        "processed_video": processed_video
    }
    
    (art_dir / f"content.{lang}.txt").write_text(f"{final_title}\n\n{translated_body}", encoding="utf-8")
    with open(meta_path, "w", encoding="utf-8") as f: 
        json.dump(meta, f, ensure_ascii=False, indent=2)
    
    logger.info(f"   ✅ Статья сохранена.")
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
    args = parser.parse_args()

    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        cleanup_old_articles(Path(args.posted_state_file), OUTPUT_DIR)
        
        cid = fetch_cat_id(args.base_url, args.slug)
        posts = fetch_posts(args.base_url, cid, FETCH_DEPTH)
        
        posted = load_posted_ids(Path(args.posted_state_file))
        stop = load_stopwords(Path(args.stopwords_file))
        
        catalog = []
        if CATALOG_PATH.exists():
            with open(CATALOG_PATH, 'r') as f: 
                try: catalog = json.load(f)
                except: catalog = []

        processed_count = 0
        new_items = []
        
        for post in posts:
            if processed_count >= args.limit:
                logger.info(f"🛑 Достигнут лимит {args.limit} статей.")
                break
                
            if str(post["id"]) in posted:
                continue
                
            meta = parse_and_save(post, args.lang, stop)
            if meta:
                new_items.append(meta)
                processed_count += 1
        
        if new_items:
            # Обновление каталога
            for m in new_items:
                catalog = [i for i in catalog if i.get("id") != m["id"]]
                catalog.append(m)
            
            with open(CATALOG_PATH, "w", encoding="utf-8") as f: 
                json.dump(catalog, f, indent=2, ensure_ascii=False)
            
            # ВАЖНО: Тот самый принт для GitHub Actions
            print("NEW_ARTICLES_STATUS:true")
            logger.info(f"🎉 Завершено! Добавлено новых статей: {len(new_items)}")
        else:
            logger.info("ℹ️ Новых статей не найдено.")

    except Exception as e:
        logger.exception(f"💥 Критическая ошибка:")
        exit(1)

if __name__ == "__main__":
    main()
