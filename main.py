#!/usr/bin/env python3
import os # Оставляем этот импорт
import json
import argparse
import logging
import re
import hashlib
import time
import fcntl # ADDED: для блокировки файла

# import os # Эту строку можно удалить, так как os уже импортирован
os.environ["translators_default_region"] = "EN"
import translators as ts
from typing import Any, Dict, List, Optional
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import cloudscraper
from requests.exceptions import ReadTimeout as ReqTimeout, RequestException
from bs4 import BeautifulSoup

# списком — все фразы/слова, которые нужно вырезать
bad_patterns = [
    r"synopsis\s*:\s*",    # «Synopsis»
    r"\(video inside\)",   # «(video inside)»
    r"\bkhmer times\b"      # слово «khmer times»
]
# единое регулярное выражение с флагом IGNORECASE
bad_re = re.compile("|".join(bad_patterns), flags=re.IGNORECASE)

# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO, # Возможно, для отладки вы захотите временно поставить logging.DEBUG
    format="%(asctime)s [%(levelname)s] %(message)s"
)
# ──────────────────────────────────────────────────────────────────────────────

# ... (остальной код до функции main) ...

def main():
    """CHANGED: Полностью переработанная логика обработки"""
    parser = argparse.ArgumentParser(description="Parser with translation")
    parser.add_argument("--base-url", type=str,
                        default="https://www.khmertimeskh.com",
                        help="WP site base URL")
    parser.add_argument("--slug", type=str, default="national",
                        help="Category slug")
    parser.add_argument("-n", "--limit", type=int, default=None,
                        help="Max posts to parse")
    parser.add_argument("-l", "--lang", type=str, default="",
                        help="Translate to language code")
    # ADDED: Аргумент для указания файла состояния с опубликованными ID
    parser.add_argument(
        "--posted-state-file",
        type=str,
        default="articles/posted.json", # Путь по умолчанию к файлу, который ведет постер
        help="Путь к файлу состояния с ID уже опубликованных статей (только для чтения)"
    )
    args = parser.parse_args()

    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        cid = fetch_category_id(args.base_url, args.slug)
        posts = fetch_posts(args.base_url, cid, per_page=(args.limit or 10))

        catalog = load_catalog()
        existing_ids_in_catalog = {article["id"] for article in catalog}
        
        # ADDED: Загружаем ID статей, которые уже были опубликованы (из файла posted.json)
        # Это позволяет парсеру не обрабатывать статьи, которые уже прошли через постер.
        posted_ids_from_repo = load_posted_ids(Path(args.posted_state_file))
        
        # ДОБАВЛЕНО: Отладочное логирование
        logging.info(f"Loaded {len(posted_ids_from_repo)} posted IDs from {args.posted_state_file}.")
        if logging.getLogger().isEnabledFor(logging.DEBUG):
             logging.debug(f"Posted IDs: {posted_ids_from_repo}")


        new_articles_processed_in_run = 0

        for post in posts[:args.limit or len(posts)]:
            post_id = str(post["id"]) # Убедитесь, что ID строковый для согласованности

            # ADDED: Пропускаем статьи, если их ID уже есть в файле опубликованных статей (posted.json)
            if post_id in posted_ids_from_repo:
                logging.info(f"Skipping article ID={post_id} as it's already in {args.posted_state_file}.") # Изменено на INFO, чтобы видеть это чаще
                continue

            # Проверяем, существует ли статья в локальном каталоге.
            # `parse_and_save` сам решит, нужно ли перезаписывать, если контент изменился.
            is_in_local_catalog = post_id in existing_ids_in_catalog

            if is_in_local_catalog:
                logging.debug(f"Article ID={post_id} found in local catalog ({CATALOG_PATH}). Checking for content updates.")
            
            # Пытаемся парсить и сохранять. parse_and_save вернет meta только если есть новые данные
            # или если статья была изменена (и ее нужно обновить).
            if meta := parse_and_save(post, args.lang, args.base_url):
                if is_in_local_catalog:
                    # Если статья уже была в каталоге, удаляем старую запись и добавляем новую, чтобы обновить
                    catalog = [item for item in catalog if item["id"] != post_id]
                    logging.info(f"Updated article ID={post_id} in local catalog (content changed or re-translated).")
                else:
                    new_articles_processed_in_run += 1
                    logging.info(f"Processed new article ID={post_id} and added to local catalog.")
                
                catalog.append(meta) # Добавляем (или обновляем) в список
                existing_ids_in_catalog.add(post_id) # Убеждаемся, что ID в наборе для будущих проверок

        if new_articles_processed_in_run > 0:
            save_catalog(catalog)
            logging.info(f"Added {new_articles_processed_in_run} truly new articles. Total parsed articles in catalog: {len(catalog)}")
        else:
            logging.info("No new articles found or processed that are not already in posted.json or local catalog.")

    except Exception as e:
        logging.exception("Fatal error in main:")
        exit(1)

if __name__ == "__main__":
    main()
