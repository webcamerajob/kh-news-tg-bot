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

from bs4 import BeautifulSoup
from curl_cffi import requests as cffi_requests 
import translators as ts

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

OUTPUT_DIR = Path("articles")
CATALOG_PATH = OUTPUT_DIR / "catalog.json"
MAX_RETRIES = 3
BASE_DELAY = 1.0

# --- НАСТРОЙКА ЭМУЛЯЦИИ SAFARI (Обход Cloudflare) ---
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

def load_posted_ids(state_file_path: Path) -> Set[str]:
    try:
        if state_file_path.exists():
            with open(state_file_path, 'r', encoding='utf-8') as f:
                fcntl.flock(f, fcntl.LOCK_SH)
                data = json.load(f)
                return {str(item) for item in data}
        return set()
    except Exception: return set()

# ПРАВКА: Извлечение самого высокого разрешения и фильтрация мусора
def extract_img_url(img_tag: Any) -> Optional[str]:
    # 1. Сначала ищем самое высокое разрешение в srcset
    srcset = img_tag.get("srcset") or img_tag.get("data-srcset")
    if srcset:
        try:
            parts = srcset.split(',')
            links = []
            for p in parts:
                match = re.search(r'(\S+)\s+(\d+)w', p.strip())
                if match:
                    links.append((int(match.group(2)), match.group(1)))
            if links:
                # Сортируем по ширине и берем самую большую
                return sorted(links, key=lambda x: x[0], reverse=True)[0][1]
        except Exception:
            pass

    # 2. Если srcset нет, проверяем атрибуты в порядке убывания качества
    attrs = ["data-orig-file", "data-large-file", "src", "data-src", "data-original"]
    for attr in attrs:
        if val := img_tag.get(attr):
            # Пропускаем баннеры, рекламные гифки и логотипы банков
            if any(x in val.lower() for x in ["gif", "logo", "banner", "mastercard", "aba-", "payway", "advertis"]):
                continue
            return val
    return None

# --- ЛОГИКА ПЕРЕВОДА ---

PROVIDER_LIMITS = {"google": 3000, "bing": 3000}

def chunk_text_by_limit(text: str, limit: int) -> List[str]:
    chunks = []
    while text:
        if len(text) <= limit:
            chunks.append(text); break
        split_pos = text.rfind('\n\n', 0, limit)
        if split_pos == -1: split_pos = text.rfind('. ', 0, limit)
        if split_pos == -1: split_pos = limit
        chunk_end = max(1, split_pos + (2 if text[split_pos:split_pos+2] == '\n\n' else 1))
        chunks.append(text[:chunk_end])
        text = text[chunk_end:].lstrip()
    return chunks

def translate_text(text: str, to_lang: str = "ru") -> Optional[str]:
    if not text or not text.strip(): return ""
    
    providers = ["google", "bing"] 
    normalized_text = normalize_text(text)

    for provider in providers:
        limit = PROVIDER_LIMITS.get(provider, 3000)
        try:
            chunks = chunk_text_by_limit(normalized_text, limit)
            translated_chunks = []
            provider_failed = False
            
            for i, chunk in enumerate(chunks):
                if i > 0: time.sleep(1.5) 
                
                try:
                    res = ts.translate_text(chunk, translator=provider, to_language=to_lang, timeout=20)
                    if res:
                        translated_chunks.append(res)
                    else:
                        provider_failed = True; break
                except Exception as e:
                    logging.warning(f"   [!] {provider} error: {e}")
                    provider_failed = True
                    if "resolve" in str(e).lower() or "name" in str(e).lower():
                        break
                    break

            if not provider_failed and translated_chunks:
                return "".join(translated_chunks)
        except Exception:
            continue
            
    return normalized_text 

# --- РАБОТА С САЙТОМ ---

def fetch_category_id(base_url: str, slug: str) -> int:
    logging.info(f"Получение ID категории '{slug}'...")
    endpoint = f"{base_url}/wp-json/wp/v2/categories?slug={slug}"
    r = SCRAPER.get(endpoint, timeout=SCRAPER_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    if not data: raise RuntimeError(f"Категория '{slug}' не найдена")
    return data[0]["id"]

def fetch_posts(base_url: str, cat_id: int, per_page: int = 10) -> List[Dict[str, Any]]:
    time.sleep(5) 
    endpoint = f"{base_url}/wp-json/wp/v2/posts?categories={cat_id}&per_page={per_page}&_embed"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = SCRAPER.get(endpoint, timeout=SCRAPER_TIMEOUT)
            if r.status_code == 429:
                time.sleep(20); continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            time.sleep(BASE_DELAY * attempt)
    return []

def save_image(src_url: str, folder: Path) -> Optional[str]:
    folder.mkdir(parents=True, exist_ok=True)
    fn = src_url.rsplit('/', 1)[-1].split('?', 1)[0]
    dest = folder / fn
    try:
        r = SCRAPER.get(src_url, timeout=SCRAPER_TIMEOUT)
        r.raise_for_status()
        dest.write_bytes(r.content)
        return str(dest)
    except Exception: return None

# --- ОБРАБОТКА СТАТЬИ ---

def parse_and_save(post: Dict[str, Any], translate_to: str, stopwords: List[str]) -> Optional[Dict[str, Any]]:
    aid = str(post["id"])
    link = post.get("link")
    time.sleep(6) 

    art_dir = OUTPUT_DIR / f"{aid}_{post['slug']}"
    art_dir.mkdir(parents=True, exist_ok=True)
    meta_path = art_dir / "meta.json"

    raw_title = BeautifulSoup(post["title"]["rendered"], "html.parser").get_text(strip=True)
    orig_title = sanitize_text(raw_title)
    
    if any(phrase in orig_title.lower() for phrase in stopwords):
        logging.info(f"Стоп-слово в ID={aid}. Пропуск.")
        return None

    logging.info(f"Обработка ID={aid}: {link}")
    try:
        page_response = SCRAPER.get(link, timeout=SCRAPER_TIMEOUT)
        page_response.raise_for_status()
        page_html = page_response.text
    except Exception as e:
        logging.error(f"Ошибка загрузки ID={aid}: {e}"); return None

    title = translate_text(orig_title, translate_to) if translate_to else orig_title
    title = sanitize_text(title)

    soup = BeautifulSoup(page_html, "html.parser")
    for junk in soup.find_all(["span", "div", "script", "style", "iframe"]):
        if junk.get("data-mce-type") or "mce_SELRES" in str(junk.get("class", "")):
            junk.decompose()
            
    content_div = soup.find("div", class_="entry-content")
    paras = [sanitize_text(p.get_text(strip=True)) for p in content_div.find_all("p")] if content_div else []
    raw_text = "\n\n".join([p for p in paras if p])
    raw_text = BAD_RE.sub("", raw_text)

    # ПРАВКА: Умный сбор качественных картинок
    img_dir = art_dir / "images"
    srcs = set()

    # 1. Приоритет: Featured Image (обложка) из WordPress API
    if "_embedded" in post and "wp:featuredmedia" in post["_embedded"]:
        fm_list = post["_embedded"]["wp:featuredmedia"]
        if isinstance(fm_list, list) and len(fm_list) > 0:
            if fm_url := fm_list[0].get("source_url"):
                srcs.add(fm_url)

    # 2. Сбор картинок из текста с фильтрацией мусора
    if content_div:
        for img in content_div.find_all("img"):
            u = extract_img_url(img)
            if u:
                # Игнорируем гифки, баннеры и логотипы платежных систем
                if not any(x in u.lower() for x in ["gif", "logo", "banner", "advertis", "payway", "mastercard"]):
                    srcs.add(u)

    images = []
    if srcs:
        with ThreadPoolExecutor(max_workers=5) as ex:
            # Ограничиваем до 10 фото, чтобы не перегружать пост
            futures = {ex.submit(save_image, url, img_dir): url for url in list(srcs)[:10]}
            for fut in as_completed(futures):
                if path := fut.result(): images.append(path)

    if not images:
        logging.warning(f"Нет валидных картинок для ID={aid}. Пропуск.")
        return None
    
    (art_dir / "content.txt").write_text(raw_text, encoding="utf-8")

    meta = {
        "id": aid, "slug": post["slug"], "date": post.get("date"), "link": link,
        "title": title, "text_file": "content.txt",
        "images": sorted([Path(p).name for p in images]), "posted": False,
        "hash": hashlib.sha256(page_html.encode()).hexdigest(), "translated_to": ""
    }

    if translate_to:
        trans_text = translate_text(raw_text, to_lang=translate_to)
        if trans_text:
            trans_text = sanitize_text(trans_text)
            trans_file = art_dir / f"content.{translate_to}.txt"
            trans_file.write_text(f"{title}\n\n{trans_text}", encoding="utf-8")
            meta.update({"translated_to": translate_to, "text_file": trans_file.name})

    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    return meta

# --- УПРАВЛЕНИЕ КАТАЛОГОМ ---

def load_catalog() -> List[Dict[str, Any]]:
    if not CATALOG_PATH.exists(): return []
    try:
        with open(CATALOG_PATH, "r", encoding="utf-8") as f:
            return [item for item in json.load(f) if isinstance(item, dict) and "id" in item]
    except Exception: return []

def save_catalog(catalog: List[Dict[str, Any]]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    minimal = [{"id": item["id"], "hash": item.get("hash", ""), "translated_to": item.get("translated_to", "")}
               for item in catalog if isinstance(item, dict) and "id" in item]
    with open(CATALOG_PATH, "w", encoding="utf-8") as f:
        json.dump(minimal, f, ensure_ascii=False, indent=2)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--slug", default="national")
    parser.add_argument("-n", "--limit", type=int, default=10)
    parser.add_argument("-l", "--lang", default="ru")
    parser.add_argument("--posted-state-file", default="articles/posted.json")
    parser.add_argument("--stopwords-file")
    args = parser.parse_args()

    try:
        cid = fetch_category_id(args.base_url, args.slug)
        posts = fetch_posts(args.base_url, cid, per_page=args.limit)
        catalog = load_catalog()
        posted_ids = load_posted_ids(Path(args.posted_state_file))
        
        sw_file = Path(args.stopwords_file) if args.stopwords_file else None
        stopwords = [l.strip().lower() for l in sw_file.read_text(encoding='utf-8').splitlines() if l.strip()] if sw_file and sw_file.exists() else []
        
        processed = []
        for post in posts:
            if str(post["id"]) not in posted_ids:
                if meta := parse_and_save(post, args.lang, stopwords):
                    processed.append(meta)
        
        if processed:
            for m in processed:
                catalog = [i for i in catalog if i.get("id") != m["id"]]
                catalog.append(m)
            save_catalog(catalog)
            print("NEW_ARTICLES_STATUS:true")
        else:
            print("NEW_ARTICLES_STATUS:false")

    except Exception:
        logging.exception("Fatal error:")
        exit(1)

if __name__ == "__main__":
    main()
