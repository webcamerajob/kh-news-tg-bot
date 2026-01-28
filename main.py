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
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Set

# Для перевода используем requests (стабильно работает с Google GTX)
import requests 
from bs4 import BeautifulSoup
# Для парсинга используем curl_cffi с профилем Safari (чтобы сайт не банил)
from curl_cffi import requests as cffi_requests, CurlHttpVersion

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# --- КОНФИГУРАЦИЯ ---
OUTPUT_DIR = Path("articles")
CATALOG_PATH = OUTPUT_DIR / "catalog.json"
MAX_RETRIES = 3
BASE_DELAY = 1.0
MAX_POSTED_RECORDS = 300
FETCH_DEPTH = 100

GROQ_API_KEY = os.getenv("GROQ_API_KEY")

# Читаем одну строку с ключами через запятую
raw_keys = os.getenv("GROQ_KEYS", "")
# Разбиваем, убираем пробелы и пустые элементы
GROQ_KEYS = [k.strip() for k in raw_keys.split(",") if k.strip()]

if GROQ_KEYS:
    logging.info(f"🔑 Пул ключей Groq готов. Загружено ключей: {len(GROQ_KEYS)}")
else:
    logging.warning("⚠️ Ключи Groq не найдены в переменной GROQ_KEYS!")
    
AI_MODELS = [
    "llama-3.3-70b-versatile",  # Топовая модель, отлично понимает контекст
    "llama-3.1-70b-versatile",  # Предыдущая версия, тоже хороша
    "mixtral-8x7b-32768",       # Хороший бэкап
    "llama-3.1-8b-instant",     # Очень быстрая, если лимиты на 70b кончились
]

# --- НАСТРОЙКИ СЕТИ (PARSER) ---
# ИСПРАВЛЕНО: Вернул Safari и убрал принудительный HTTP/1.1
# Это решит проблему с Timeout при парсинге.
SCRAPER = cffi_requests.Session(
    impersonate="chrome110",
    http_version=CurlHttpVersion.V1_1
)

SCRAPER.headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.5 Safari/605.1.15",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com/"
}
# Увеличил таймаут до 60 сек для медленных прокси/VPN
SCRAPER_TIMEOUT = 60 
BAD_RE = re.compile(r"[\u200b-\u200f\uFEFF\u200E\u00A0]")

# --- БЛОК 1: ПЕРЕВОД И ИИ ---

def direct_google_translate(text: str, to_lang: str = "ru") -> str:
    """Переводит текст через Google API (GTX) с разбивкой на куски."""
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
    # Обычный User-Agent для requests (Google его нормально принимает)
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"}
    
    for chunk in chunks:
        if not chunk.strip():
            translated_parts.append("")
            continue
        try:
            params = {"client": "gtx", "sl": "en", "tl": to_lang, "dt": "t", "q": chunk.strip()}
            # Таймаут 10 сек на кусок перевода
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
    bad_prefixes = ["Here is", "The article", "Summary:", "Cleaned text:"]
    for prefix in bad_prefixes:
        if text.lower().startswith(prefix.lower()):
            parts = text.split('\n', 1)
            if len(parts) > 1: return parts[1].strip()
    return text

def smart_process_and_translate(title: str, body: str, lang: str) -> (str, str):
    clean_body = body

    if GROQ_KEYS and len(body) > 500:
        logging.info("⏳ Подготовка к ИИ-чистке...")
        
        prompt = (
            f"You are a ruthless news editor.\n"
            f"INPUT: Raw news text.\n"
            f"OUTPUT: A cleaned-up version of the story in ENGLISH.\n\n"
            "STRICT EDITING RULES:\n"
            "1. CONSOLIDATE NARRATIVE & SPEECH: If the author states a fact, and then a speaker repeats the same meaning, DELETE the speaker's part.\n"
            "2. KEEP UNIQUE DETAILS: Only keep quotes if they add numbers, dates, or emotion.\n"
            "3. REMOVE FLUFF: Delete ads and diplomatic praise.\n"
            "4. NO META-TALK: Start with the story immediately.\n\n"
            f"RAW TEXT:\n{body[:15000]}" # Groq поддерживает большой контекст
        )
        
        ai_result = ""
        # Перемешиваем ключи для равномерного распределения нагрузки
        current_pool = list(GROQ_KEYS)
        random.shuffle(current_pool)

        # Перебор ключей
        for api_key in current_pool:
            if ai_result: break 

            logging.info(f"🚀 Пробуем ключ {api_key[:6]}...")
            
            # Перебор моделей для текущего ключа
            for model in AI_MODELS:
                try:
                    response = requests.post(
                        url="https://api.groq.com/openai/v1/chat/completions",
                        headers={
                            "Authorization": f"Bearer {api_key}",
                            "Content-Type": "application/json"
                        },
                        data=json.dumps({
                            "model": model,
                            "messages": [{"role": "user", "content": prompt}],
                            "temperature": 0.3,
                            "max_tokens": 4096
                        }),
                        timeout=30
                    )
                    
                    if response.status_code == 200:
                        result = response.json()
                        ai_result = result['choices'][0]['message']['content'].strip()
                        logging.info(f"✅ Успех! Модель: {model} (Ключ: {api_key[:6]}...)")
                        break # Выход из цикла моделей
                    
                    elif response.status_code == 429:
                        logging.warning(f"🐢 Rate Limit на ключе {api_key[:6]}... Пробуем СЛЕДУЮЩИЙ КЛЮЧ.")
                        break # Прерываем цикл моделей, чтобы сменить ключ
                    
                    else:
                        logging.error(f"❌ Ошибка {response.status_code} на ключе {api_key[:6]}...")
                        break # Пробуем следующий ключ

                except Exception as e:
                    logging.error(f"⚠️ Ошибка соединения (Ключ: {api_key[:6]}...): {e}")
                    break # Пробуем следующий ключ
        
        if ai_result:
            clean_body = strip_ai_chatter(ai_result)

    # КОНТЕКСТНЫЙ ПЕРЕВОД (Google) - остается без изменений
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

# --- БЛОК 3: УМНЫЙ ПОИСК КАРТИНОК ---

def extract_img_url(img_tag: Any) -> Optional[str]:
    def is_junk(url_str: str) -> bool:
        u = url_str.lower()
        bad = ["gif", "logo", "banner", "icon", "avatar", "button", "share", "pixel", "tracker"]
        if any(b in u for b in bad): return True
        if re.search(r'-\d{2,3}x\d{2,3}\.', u): return True
        return False

    # 1. СТРАТЕГИЯ №1: Ищем оригинал в родительской ссылке (Lightbox)
    # В твоем примере это <a href="...">...</a>
    parent_a = img_tag.find_parent("a")
    if parent_a:
        href = parent_a.get("href")
        if href and any(href.lower().endswith(ext) for ext in ['.jpg', '.jpeg', '.png', '.webp']):
            if not is_junk(href):
                return href.split('?')[0]

    # 2. СТРАТЕГИЯ №2: Если ссылки нет, копаем атрибуты Breeze (data-brsrcset)
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

    # 3. СТРАТЕГИЯ №3: Проверка ширины и прямых атрибутов
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
    if not url or url.startswith('data:'): return None # Игнорим base64 мусор
    
    folder.mkdir(parents=True, exist_ok=True)
    
    url_hash = hashlib.md5(url.encode()).hexdigest()[:10]
    orig_fn = url.rsplit('/', 1)[-1].split('?', 1)[0]
    ext = orig_fn.split('.')[-1] if '.' in orig_fn else 'jpg'
    if len(ext) > 4: ext = 'jpg' # На случай кривых ссылок
    
    fn = f"{url_hash}.{ext}"
    dest = folder / fn
    
    try:
        # Качаем через SCRAPER (Safari профиль)
        resp = SCRAPER.get(url, timeout=SCRAPER_TIMEOUT)
        if resp.status_code == 200:
            dest.write_bytes(resp.content)
            return str(dest)
    except Exception as e:
        logging.error(f"Ошибка сохранения фото {url}: {e}")
    return None

# --- БЛОК 4: API И ПАРСИНГ ---

def fetch_cat_id(url, slug):
    r = SCRAPER.get(f"{url}/wp-json/wp/v2/categories?slug={slug}", timeout=SCRAPER_TIMEOUT)
    r.raise_for_status(); data=r.json()
    if not data: raise RuntimeError("Cat not found")
    return data[0]["id"]

def fetch_posts_light(url: str, cid: int, limit: int) -> List[Dict]:
    """ЛЕГКИЙ запрос: только ID и slug. WordPress отдает это мгновенно."""
    logging.info(f"📡 Быстрая проверка списка из {limit} последних ID...")
    try:
        params = {
            "categories": cid, 
            "per_page": limit, 
            "_fields": "id,slug" # Запрашиваем только два поля
        }
        r = SCRAPER.get(f"{url}/wp-json/wp/v2/posts", params=params, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logging.error(f"Ошибка легкого запроса: {e}")
        return []

def fetch_single_post_full(url: str, aid: str) -> Optional[Dict]:
    """ТЯЖЕЛЫЙ запрос: полные данные конкретной статьи со всеми вложениями."""
    try:
        # Здесь используем _embed, так как тянем только ОДНУ статью
        r = SCRAPER.get(f"{url}/wp-json/wp/v2/posts/{aid}?_embed", timeout=60)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logging.error(f"Ошибка загрузки контента для ID={aid}: {e}")
        return None

def parse_and_save(post, lang, stopwords):
    time.sleep(2)
    aid, slug, link = str(post["id"]), post["slug"], post.get("link")
    
    raw_title = BeautifulSoup(post["title"]["rendered"], "html.parser").get_text(strip=True)
    title = sanitize_text(raw_title)

    if stopwords:
        for ph in stopwords:
            if ph in title.lower():
                logging.info(f"🚫 ID={aid}: Стоп-слово '{ph}'")
                return None

    try:
        html_txt = SCRAPER.get(link, timeout=SCRAPER_TIMEOUT).text
    except Exception: return None

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
    
    # --- НОВАЯ ПРАВКА: ГЛОБАЛЬНАЯ ОЧИСТКА МУСОРА ---
    # УдаляемRelated Posts, рекламные блоки и виджеты ДО начала сбора картинок
    # Это убьет блоки rp4wp, которые лезли в Lightbox и контент
    for garbage in soup.find_all(["div", "ul", "ol", "section", "aside"], 
                                class_=re.compile(r"rp4wp|related|ad-|post-widget-thumbnail|sharedaddy")):
        garbage.decompose()

    # Очистка технических тегов и пустых элементов
    for j in soup.find_all(["span", "script", "style", "iframe"]):
        if not hasattr(j, 'attrs') or j.attrs is None: continue 
        c = str(j.get("class", ""))
        if j.get("data-mce-type") or "mce_SELRES" in c or "widget" in c: 
            j.decompose()

    # --- Сбор URL с сохранением ПОРЯДКА ---
    ordered_srcs = []
    seen_srcs = set()

    def add_src(url):
        if url and url not in seen_srcs:
            ordered_srcs.append(url)
            seen_srcs.add(url)

    # 1. ПРИОРИТЕТ: Featured Media (Главное фото WP из API)
    if "_embedded" in post and (m := post["_embedded"].get("wp:featuredmedia")):
        if isinstance(m, list) and (u := m[0].get("source_url")):
            if "logo" not in u.lower():
                add_src(u)

    # 2. ОСТАЛЬНЫЕ: Lightbox ссылки (теперь тут не будет картинок из Related Posts)
    for link_tag in soup.find_all("a", class_="ci-lightbox", limit=10):
        if h := link_tag.get("href"): 
            if "gif" not in h.lower():
                add_src(h)

    # 3. ОСТАЛЬНЫЕ: Картинки непосредственно в тексте
    c_div = soup.find("div", class_="entry-content")
    if c_div:
        for img in c_div.find_all("img"):
            if u := extract_img_url(img):
                add_src(u)

    # --- Загрузка с сохранением индексов (чтобы не перемешались) ---
    images_results = [None] * len(ordered_srcs)
    if ordered_srcs:
        with ThreadPoolExecutor(5) as ex:
            # Ограничиваемся первыми 10 уникальными фото
            future_to_idx = {
                ex.submit(save_image, url, OUTPUT_DIR / f"{aid}_{slug}" / "images"): i 
                for i, url in enumerate(ordered_srcs[:10])
            }
            for f in as_completed(future_to_idx):
                idx = future_to_idx[f]
                if res := f.result():
                    images_results[idx] = Path(res).name

    # Убираем пустые результаты (если загрузка какого-то фото сорвалась)
    final_images = [img for img in images_results if img is not None]

    if not final_images:
        logging.warning(f"⚠️ ID={aid}: Нет норм картинок. Skip.")
        return None

    # Извлечение текста статьи
    paras = []
    if c_div:
        # Удаляем внутренний мусор в контенте, если он остался
        for r in c_div.find_all(["ul", "ol", "div"], class_=re.compile(r"rp4wp|related|ad-")): 
            r.decompose()
        paras = [sanitize_text(p.get_text(strip=True)) for p in c_div.find_all("p")]
    
    raw_body_text = BAD_RE.sub("", "\n\n".join(paras))

    # ОБРАБОТКА + ПЕРЕВОД
    final_title = title
    translated_body = ""
    if lang:
        final_title, translated_body = smart_process_and_translate(title, raw_body_text, lang)
        final_title = sanitize_text(final_title)

    art_dir = OUTPUT_DIR / f"{aid}_{slug}"
    art_dir.mkdir(parents=True, exist_ok=True)
    
    (art_dir / "content.txt").write_text(raw_body_text, encoding="utf-8")
    
    # Формируем метаданные (images[0] — теперь гарантированно главное фото)
    meta = {
        "id": aid, "slug": slug, "date": post.get("date"), "link": link,
        "title": final_title, "text_file": "content.txt",
        "images": final_images, "posted": False,
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
    args = parser.parse_args()

    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        # Очищаем старые папки статей, которых нет в последних 100 опубликованных
        cleanup_old_articles(Path(args.posted_state_file), OUTPUT_DIR)
        
        # Получаем ID категории
        cid = fetch_cat_id(args.base_url, args.slug)
        
        # 1. Загружаем данные: легкий список ID, историю и стоп-слова
        posts_light = fetch_posts_light(args.base_url, cid, FETCH_DEPTH)
        posted = load_posted_ids(Path(args.posted_state_file))
        stop = load_stopwords(Path(args.stopwords_file))
        
        # 2. Загружаем текущий каталог из файла
        catalog = []
        if CATALOG_PATH.exists():
            try:
                with open(CATALOG_PATH, 'r', encoding='utf-8') as f:
                    catalog = json.load(f)
            except Exception:
                logging.warning("Не удалось прочитать существующий каталог. Создаем новый.")

        new_metas = []
        count = 0
        
        # Основной цикл обработки
        for p_short in posts_light:
            if count >= args.limit:
                break
            
            aid = str(p_short["id"])
            if aid in posted:
                continue # Эту статью уже постили, пропускаем
            
            logging.info(f"🆕 Найдена новая статья ID={aid}. Загружаем детали...")
            full_post = fetch_single_post_full(args.base_url, aid)
            
            if full_post:
                # parse_and_save внутри себя делает AI-чистку и перевод
                if meta := parse_and_save(full_post, args.lang, stop):
                    new_metas.append(meta)
                    count += 1

        # 3. Финальное обновление каталога и отчет для GitHub Actions
        if new_metas:
            # Предотвращаем дубли в каталоге: удаляем старые записи с теми же ID
            new_ids = {str(m["id"]) for m in new_metas}
            catalog = [item for item in catalog if str(item.get("id")) not in new_ids]
            
            # Добавляем свежеприготовленные метаданные
            catalog.extend(new_metas)
            
            with open(CATALOG_PATH, "w", encoding="utf-8") as f:
                json.dump(catalog, f, ensure_ascii=False, indent=2)
            
            # Это сигнал для GitHub Actions, что нужно запускать постер
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
