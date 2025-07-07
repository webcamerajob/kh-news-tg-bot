import logging
import httpx
import json
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime
import time
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

GITHUB_REPO = "webcamerajob/kh-news-tg-bot"
TOKEN = os.environ.get("GH_TOKEN")
URL = f"https://api.github.com/repos/{GITHUB_REPO}/dispatches"

def trigger_poster():
    if not TOKEN:
        logging.warning("❌ GH_TOKEN не найден в переменных окружения")
        return

    response = httpx.post(
        URL,
        headers={
            "Authorization": f"Bearer {TOKEN}",
            "Accept": "application/vnd.github+json"
        },
        json={"event_type": "start-poster"}
    )
    if response.status_code == 204:
        logging.info("🚀 Постер запущен через repository_dispatch")
    else:
        logging.error(f"❌ Ошибка запуска: {response.text}")
        


BASE_URL      = "https://www.khmertimeskh.com/wp-json/wp/v2"
OUTPUT_FOLDER = Path("articles")
PER_PAGE      = 10
HEADERS       = {
    "User-Agent": "ParserBot/1.0"
}

IMG_FILTER_CONFIG = {
    "include_classes": {
        "attachment-post-thumbnail", "aligncenter", "size-full",
        "featured-image", "wp-image", "wp-post-image", "size-post-thumbnail"
    },
    "exclude_classes": {"advertisement", "banner", "sponsored", "skip"},
    "src_must_contain": ["/uploads/", "cdn.khmertimeskh.com"],
    "src_must_not_contain": ["promo/", "tracking", ".gif", ".svg", ".ico"],
    "min_width": 300,
    "min_height": 200,
    "allowed_extensions": [".jpg", ".jpeg"]
}

def fetch_category_id(slug="national") -> int:
    url = f"{BASE_URL}/categories?slug={slug}"
    r = httpx.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not data:
        raise RuntimeError(f"Category {slug} not found")
    return data[0]["id"]

def fetch_posts(cat_id, per_page=PER_PAGE, retries=3, backoff=5):
    url = f"{BASE_URL}/posts?categories={cat_id}&per_page={per_page}&_embed"
    for i in range(retries):
        try:
            r = httpx.get(url, headers=HEADERS, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logging.warning(f"Attempt {i+1} failed: {e}")
            time.sleep(backoff)
    logging.error("Failed to fetch posts")
    return []

def save_image(url, folder: Path):
    r = httpx.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    fn = url.split("/")[-1].split("?")[0]
    path = folder / fn
    path.write_bytes(r.content)
    return str(path)

def is_valid_image(img):
    cfg  = IMG_FILTER_CONFIG
    src  = img.get("src","")
    ext  = src.lower().split(".")[-1].split("?")[0]
    rawc = img.get("class",[])
    classes = set()
    for c in rawc:
        if isinstance(c,str):
            classes |= set(c.split())
        else:
            classes.add(str(c))

    good = {
        "class": bool(classes & cfg["include_classes"]),
        "src_in": any(x in src for x in cfg["src_must_contain"]),
        "ext": src.lower().endswith(tuple(cfg["allowed_extensions"])),
        "size": True
    }
    try:
        w,h = int(img.get("width",0)), int(img.get("height",0))
        if w<cfg["min_width"] or h<cfg["min_height"]:
            good["size"] = False
    except: pass

    if not good["class"]:
        return False
    if not good["src_in"]:
        return False
    for bad in cfg["src_must_not_contain"]:
        if bad in src:
            return False
    if not good["ext"]:
        return False
    if not good["size"]:
        return False
    return True

def parse_and_save(post):
    aid, slug = post["id"], post["slug"]
    art_dir   = OUTPUT_FOLDER / f"{aid}_{slug}"
    meta_path = art_dir / "meta.json"
    img_dir   = art_dir / "images"

    # 1) Узнаём, что было в старом meta.json
    if meta_path.exists():
        old = json.loads(meta_path.read_text(encoding="utf-8"))
        posted_flag = old.get("posted", False)
    else:
        posted_flag = False

    art_dir.mkdir(parents=True, exist_ok=True)
    img_dir.mkdir(exist_ok=True)

    # 2) Собираем новое meta, но не сбрасываем posted
    meta = {
        "id": aid,
        "slug": slug,
        "date": post.get("date"),
        "link": post.get("link"),
        "title": BeautifulSoup(post["title"]["rendered"],"html.parser").get_text(strip=True),
        "posted": posted_flag
    }

    # 3) Сохраняем текст
    soup = BeautifulSoup(post["content"]["rendered"],"html.parser")
    paras = [p.get_text(strip=True) for p in soup.find_all("p")]
    txt = art_dir / "content.txt"
    txt.write_text("\n\n".join(paras), encoding="utf-8")
    meta["text_file"] = str(txt)

    # 4) Скачиваем изображения из content
    images = []
    for img in soup.find_all("img"):
        src = img.get("src")
        if not src or not is_valid_image(img):
            continue
        try:
            path = save_image(src, img_dir)
            images.append(path)
        except: pass

    # 5) Скачиваем featured_media, если есть
    try:
        links = post.get("_links",{})
        if "wp:featuredmedia" in links:
            href = links["wp:featuredmedia"][0]["href"]
            r = httpx.get(href, headers=HEADERS, timeout=10); r.raise_for_status()
            src = r.json().get("source_url","")
            if src and any(x in src for x in IMG_FILTER_CONFIG["src_must_contain"]) \
                   and src.lower().endswith(tuple(IMG_FILTER_CONFIG["allowed_extensions"])):
                dummy = {"src":src,"class":["featured-image"],"width":"999","height":"999"}
                class T:
                    def get(self,k,d=None): return dummy.get(k,d)
                    @property
                    def attrs(self): return dummy
                if is_valid_image(T()):
                    path = save_image(src, img_dir)
                    images.append(path)
    except: pass

    meta["images"] = images

    # 6) Пишем meta.json, **с флагом** из старого файла
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    return meta

def main():
    logging.info("🚀 Start parsing")
    OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)
    cid   = fetch_category_id("national")
    posts = fetch_posts(cid)
    catalog = []

    for i, post in enumerate(posts,1):
        logging.info(f"→ Post {i}/{len(posts)}: {post['id']}")
        m = parse_and_save(post)
        catalog.append(m)

    # Сохраняем итоговый каталог
catalog_path = OUTPUT_FOLDER / "catalog.json"
catalog_path.write_text(
    json.dumps(catalog, ensure_ascii=False, indent=2),
    encoding="utf-8"
)
logging.info(f"📁 catalog.json saved: {catalog_path.resolve()} — {len(catalog)} articles")

# 🚀 Запускаем постер, если есть статьи
if catalog:
    trigger_poster()
else:
    logging.warning("⚠️ Catalog is empty — poster not triggered")

print("✅ main.py завершён")

    
if __name__ == "__main__":
    main()



