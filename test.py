import os
import cloudscraper
import requests
from bs4 import BeautifulSoup
from googletrans import Translator
from aiogram import Bot, Dispatcher, types
from aiogram.types import Message
from aiogram.filters import Command
import asyncio
import time

# Загрузка переменных окружения (Secrets через GitHub)
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")  # Токен Telegram бота
TELEGRAM_CHANNEL = os.getenv("TELEGRAM_CHANNEL")  # ID чата для отправки сообщений
PUBLISHED_ARTICLES_FILE = "published_articles.txt"  # Локальный файл для защиты от повторных публикаций
URL = "https://www.khmertimeskh.com/national"  # Сайт для парсинга статей

# Настройка Telegram бота
bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()

def chunk_text(text, max_length=4096):
    """Разделение текста на чанки длиной не более max_length."""
    return [text[i:i + max_length] for i in range(0, len(text), max_length)]

def fetch_html_with_cloudflare(url):
    """Получение HTML с обходом защиты Cloudflare."""
    scraper = cloudscraper.create_scraper()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    for attempt in range(3):  # Три попытки с экспоненциальной задержкой
        try:
            response = scraper.get(url, headers=headers)
            response.raise_for_status()
            return response.text
        except Exception as e:
            print(f"[WARNING] Timeout fetching HTML (try {attempt + 1}/3): {e}; retry in {2**attempt}s")
            time.sleep(2**attempt)
    raise RuntimeError("Failed to fetch HTML after 3 attempts")

def fetch_articles():
    """Получение последних 10 статей с сайта."""
    html = fetch_html_with_cloudflare(URL)
    soup = BeautifulSoup(html, "html.parser")
    articles = soup.find_all("article", limit=10)  # Настраиваем поиск статей
    parsed_articles = []

    for article in articles:
        title = article.find("h2").text.strip() if article.find("h2") else None
        link = article.find("a")["href"] if article.find("a") else None
        if title and link:
            parsed_articles.append({"title": title, "link": link})

    return parsed_articles

def translate_to_russian(text):
    """Перевод текста на русский язык с учетом ограничения длины."""
    translator = Translator()
    chunks = chunk_text(text, max_length=4096)
    translated_chunks = [translator.translate(chunk, src="en", dest="ru").text for chunk in chunks]
    return ''.join(translated_chunks)

def load_published_articles():
    """Загрузка списка уже опубликованных статей."""
    if not os.path.exists(PUBLISHED_ARTICLES_FILE):
        return set()
    with open(PUBLISHED_ARTICLES_FILE, "r", encoding="utf-8") as file:
        return set(file.read().splitlines())

def save_published_articles(articles):
    """Сохранение списка опубликованных статей."""
    with open(PUBLISHED_ARTICLES_FILE, "a", encoding="utf-8") as file:
        for article in articles:
            file.write(article + "\n")

async def send_articles_to_telegram(articles):
    """Отправка статей в Telegram."""
    for article in articles:
        text = f"📢 {article['title']}\nСсылка: {article['link']}"
        await bot.send_message(TELEGRAM_CHANNEL, text)

async def main():
    """Основная логика парсера."""
    try:
        articles = fetch_articles()
        published_articles = load_published_articles()
        new_articles = []

        for article in articles:
            if article["link"] not in published_articles:
                article["title"] = translate_to_russian(article["title"])
                new_articles.append(article)

        if new_articles:
            await send_articles_to_telegram(new_articles)
            save_published_articles([article["link"] for article in new_articles])
    except Exception as e:
        print(f"[ERROR] Fatal error in main: {e}")

# Обработчик команды /start
@dp.message(Command("start"))
async def start_command(message: Message):
    await message.answer("Бот готов к работе!")
    if name == "main":
    asyncio.run(main())
