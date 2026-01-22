import argparse
import logging
import json
import hashlib
import time
import re
import os
import shutil
import html
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Set
import fcntl

# --- ИМПОРТЫ ---
from bs4 import BeautifulSoup
from curl_cffi import requests as cffi_requests 

# Работаем с новым SDK v1.0+
try:
    from google import genai
    from google.genai import types
except ImportError:
    logging.error("❌ Library 'google-genai' not found. Run: pip install google-genai")
    genai = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# --- НАСТРОЙКА КЛИЕНТА GOOGLE ---
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")
AI_CLIENT = None

if GOOGLE_API_KEY and genai:
    try:
        # В 2026 году используем только стабильный эндпоинт v1
        AI_CLIENT = genai.Client(
            api_key=GOOGLE_API_KEY,
            http_options={'api_version': 'v1'} # Явно заставляем использовать стабильную версию
        )
        logging.info("✅ Google AI Client initialized (v1 stable)")
    except Exception as e:
        logging.warning(f"⚠️ Failed to initialize Google AI Client: {e}")
else:
    logging.warning("⚠️ GOOGLE_API_KEY not found! AI processing will be skipped.")

OUTPUT_DIR = Path("articles")
CATALOG_PATH = OUTPUT_DIR / "catalog.json"
MAX_RETRIES = 3
BASE_DELAY = 1.0

# --- СЕТЕВЫЕ НАСТРОЙКИ ---
SCRAPER = cffi_requests.Session(impersonate="safari15_5")
SCRAPER.headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.5 Safari/605.1.15",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com/"
}
SCRAPER_TIMEOUT = 30
BAD_RE = re.compile(r"[\u200b-\u200f\uFEFF\u200E\u00A0]")

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
# (normalize_text, sanitize_text, load_posted_ids, extract_img_url - 
#  остаются как в вашем оригинале, для краткости не дублирую)

def normalize_text(text: str) -> str:
    replacements = {'–': '-', '—': '-', '“': '"', '”': '"', '‘': "'", '’': "'"}
    for s, v in replacements.items(): text = text.replace(s, v)
    return text

def sanitize_text(text: str) -> str:
    if not text: return ""
    text = html.unescape(text)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'mce_SELRES_[^ ]+', '', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()

def load_posted_ids(p: Path) -> Set[str]:
    try:
        if p.exists():
            with open(p, 'r', encoding='utf-8') as f:
                fcntl.flock(f, fcntl.LOCK_SH)
                return {str(i) for i in json.load(f)}
        return set()
    except Exception: return set()

def extract_img_url(tag: Any) -> Optional[str]:
    attrs = ["data-brsrcset", "data-breeze", "data-src", "data-lazy-src", "data-original", "srcset", "src"]
    for a in attrs:
        if v := tag.get(a): return v.split(',')[0].split()[0]
    return None

def fetch_category_id(base_url: str, slug: str) -> int:
    logging.info(f"Fetching ID for {slug}...")
    endpoint = f"{base_url}/wp-json/wp/v2/categories?slug={slug}"
    r = SCRAPER.get(endpoint, timeout=SCRAPER_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    if not data: raise RuntimeError(f"Category {slug} not found")
    return data[0]["id"]

def fetch_posts(base_url: str, cat_id: int, per_page: int = 15) -> List[Dict[str, Any]]:
    endpoint = f"{base_url}/wp-json/wp/v2/posts?categories={cat_id}&per_page={per_page}&_embed"
    time.sleep(2)
    r = SCRAPER.get(endpoint, timeout=SCRAPER_TIMEOUT)
    r.raise_for_status()
    return r.json()

def save_image(url: str, folder: Path) -> Optional[str]:
    folder.mkdir(parents=True, exist_ok=True)
    fn = url.rsplit('/', 1)[-1].split('?', 1)[0]
    dest = folder / fn
    try:
        r = SCRAPER.get(url, timeout=SCRAPER_TIMEOUT)
        r.raise_for_status()
        dest.write_bytes(r.content)
        return str(dest)
    except Exception: return None

def load_catalog() -> List[Dict[str, Any]]:
    if not CATALOG_PATH.exists(): return []
    with open(CATALOG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def save_catalog(catalog: List[Dict[str, Any]]) -> None:
    minimal = [{"id": i["id"], "hash": i.get("hash", ""), "translated_to": i.get("translated_to", "")} for i in catalog]
    with open(CATALOG_PATH, "w", encoding="utf-8") as f:
        json.dump(minimal, f, ensure_ascii=False, indent=2)

def load_stopwords(p: Optional[Path]) -> List[str]:
    if not p or not p.exists(): return []
    return [l.strip().lower() for l in p.read_text(encoding='utf-8').splitlines() if l.strip()]

# --- НОВАЯ ФУНКЦИЯ: Обработка через GEMINI 2.0 FLASH ---
def process_article_with_ai(title_en: str, text_en: str) -> Optional[Dict[str, str]]:
    if not AI_CLIENT: return None
    if not text_en or len(text_en) < 50: return None

    # Промпт для качественной очистки и перевода
    prompt = f"""
    You are an expert news editor. Translate to Russian and SUMMARY this article.
    
    TITLE: {title_en}
    BODY: {text_en}

    INSTRUCTIONS:
    - Language: Russian.
    - Remove marketing, spam, redundant intros/outros.
    - Keep factual data (names, dates, locations, numbers).
    - Tone: Neutral, professional.
    
    FORMAT:
    [TITLE_START]
    (Russian Title)
    [TITLE_END]
    [BODY_START]
    (Refined Russian Content)
    [BODY_END]
    """

    try:
        # В 2026 году используем модель gemini-2.0-flash (стабильная v1)
        response = AI_CLIENT.models.generate_content(
            model='gemini-2.0-flash', 
            contents=prompt,
            config=types.GenerateContentConfig(temperature=0.2)
        )
        
        if not response.text: return None
        text_out = response.text
        
        t_match = re.search(r'\[TITLE_START\](.*?)\[TITLE_END\]', text_out, re.DOTALL)
        b_match = re.search(r'\[BODY_START\](.*?)\[BODY_END\]', text_out, re.DOTALL)
        
        if t_match and b_match:
            return {"title": t_match.group(1).strip(), "text": b_match.group(1).strip()}
        return {"title": title_en, "text": text_out} # Fallback

    except Exception as e:
        logging.error(f"❌ AI Error: {e}")
        return None

# --- ПАРСИНГ И СОХРАНЕНИЕ ---
def parse_and_save(post: Dict[str, Any], lang: str, stopwords: List[str]) -> Optional[Dict[str, Any]]:
    aid = str(post["id"])
    slug = post["slug"]
    link = post.get("link")
    art_dir = OUTPUT_DIR / f"{aid}_{slug}"
    art_dir.mkdir(parents=True, exist_ok=True)
    meta_path = art_dir / "meta.json"

    raw_title = BeautifulSoup(post["title"]["rendered"], "html.parser").get_text(strip=True)
    orig_title = sanitize_text(raw_title)
    
    if any(s in orig_title.lower() for s in stopwords): return None

    logging.info(f"Processing ID={aid}...")
    try:
        r = SCRAPER.get(link, timeout=SCRAPER_TIMEOUT)
        r.raise_for_status()
        page_html = r.text
    except Exception: return None

    current_hash = hashlib.sha256(page_html.encode()).hexdigest()
    
    # Парсинг контента
    soup = BeautifulSoup(page_html, "html.parser")
    article_content = soup.find("div", class_="entry-content")
    paras = []
    if article_content:
        for p in article_content.find_all("p"):
            txt = sanitize_text(p.get_text(strip=True))
            if txt: paras.append(txt)
    raw_text_en = "\n\n".join(paras)

    # Картинки
    img_dir = art_dir / "images"
    srcs = {extract_img_url(img) for img in soup.find_all("img") if extract_img_url(img)}
    images = []
    if srcs:
        with ThreadPoolExecutor(max_workers=5) as ex:
            futures = [ex.submit(save_image, url, img_dir) for url in list(srcs)[:5]]
            for f in as_completed(futures):
                if res := f.result(): images.append(res)

    if not images: return None

    # AI ОБРАБОТКА
    logging.info(f"🤖 Sending ID={aid} to AI (Gemini 2.0)...")
    ai_result = process_article_with_ai(orig_title, raw_text_en)
    if not ai_result: return None

    final_title, final_text = ai_result["title"], ai_result["text"]
    text_file = art_dir / "content.ru.txt"
    text_file.write_text(f"{final_title}\n\n{final_text}", encoding="utf-8")

    meta = {
        "id": aid, "slug": slug, "date": post.get("date"), "link": link,
        "title": final_title, "text_file": text_file.name,
        "images": sorted([Path(p).name for p in images]), "posted": False,
        "hash": current_hash, "processed_by_ai": True
    }
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    return meta

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--slug", default="national")
    parser.add_argument("-n", "--limit", type=int, default=10)
    parser.add_argument("--posted-state-file", default="articles/posted.json")
    parser.add_argument("--stopwords-file")
    args = parser.parse_args()

    try:
        cid = fetch_category_id(args.base_url, args.slug)
        posts = fetch_posts(args.base_url, cid, per_page=args.limit)
        posted_ids = load_posted_ids(Path(args.posted_state_file))
        catalog = load_catalog()
        stopwords = load_stopwords(Path(args.stopwords_file) if args.stopwords_file else None)

        processed = []
        for post in posts:
            if str(post["id"]) not in posted_ids:
                if meta := parse_and_save(post, "ru", stopwords):
                    processed.append(meta)

        if processed:
            for m in processed:
                catalog = [i for i in catalog if i.get("id") != m["id"]]
                catalog.append(m)
            save_catalog(catalog)
            print("NEW_ARTICLES_STATUS:true")
        else:
            print("NEW_ARTICLES_STATUS:false")

    except Exception as e:
        logging.exception("Fatal error:")
        exit(1)

if __name__ == "__main__":
    main()
