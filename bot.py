import os
import json
import subprocess
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from datetime import datetime
from pathlib import Path

REPO_URL = "https://github.com/webcamerajob/kh-news-tg-bot"  # Заменить
SETTINGS_FILE = Path("settings.json")

def load_settings():
    return json.loads(SETTINGS_FILE.read_text(encoding="utf-8"))

def save_settings(settings):
    SETTINGS_FILE.write_text(json.dumps(settings, indent=2, ensure_ascii=False), encoding="utf-8")

def commit_settings():
    subprocess.run(["git", "config", "--global", "user.name", "TelegramBot"])
    subprocess.run(["git", "config", "--global", "user.email", "bot@telegram.ai"])
    subprocess.run(["git", "add", SETTINGS_FILE.name])
    ts = datetime.utcnow().isoformat(timespec="seconds")
    subprocess.run(["git", "commit", "-m", f"🤖 Update settings via Telegram at {ts}"])
    subprocess.run(["git", "push", "origin", "main"])

async def set_value(update: Update, context: ContextTypes.DEFAULT_TYPE, key: str, label: str, validate=lambda x: True):
    try:
        value = " ".join(context.args).strip()
        if not validate(value):
            raise ValueError("Неверный формат")
        settings = load_settings()
        settings[key] = value
        save_settings(settings)
        commit_settings()
        await update.message.reply_text(f"✅ {label} установлен: {value}")
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")

async def reset_published(update: Update, context: ContextTypes.DEFAULT_TYPE):
    settings = load_settings()
    settings["published_reset"] = True
    save_settings(settings)
    commit_settings()
    await update.message.reply_text("♻️ published.json будет сброшен и workflow запущен")

async def show_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        catalog = json.loads(Path("articles/catalog.json").read_text(encoding="utf-8"))
        published = json.loads(Path("articles/published.json").read_text(encoding="utf-8"))
        posted = sum(1 for item in catalog if item.get("posted"))
        await update.message.reply_text(
            f"📊 Статистика:\nВсего: {len(catalog)}\nОпубликовано: {posted}\nНе опубликовано: {len(catalog)-posted}\npublished.json: {len(published)}"
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка чтения статуса: {e}")

async def show_config(update: Update, context: ContextTypes.DEFAULT_TYPE):
    settings = load_settings()
    text = "\n".join(f"{k}: {v}" for k, v in settings.items())
    await update.message.reply_text(f"⚙️ Настройки:\n{text}")

# Обработчики команд
application = ApplicationBuilder().token(os.getenv("TELEGRAM_TOKEN")).build()
application.add_handler(CommandHandler("set_lang", lambda u, c: set_value(u, c, "lang", "Язык")))
application.add_handler(CommandHandler("set_slug", lambda u, c: set_value(u, c, "slug", "Категория")))
application.add_handler(CommandHandler("set_base", lambda u, c: set_value(u, c, "base_url", "Сайт", lambda x: x.startswith("http"))))
application.add_handler(CommandHandler("set_limit", lambda u, c: set_value(u, c, "limit", "Лимит", lambda x: x.isdigit())))
application.add_handler(CommandHandler("set_channel", lambda u, c: set_value(u, c, "telegram_channel", "Канал")))
application.add_handler(CommandHandler("reset_published", reset_published))
application.add_handler(CommandHandler("stats", show_stats))
application.add_handler(CommandHandler("config", show_config))

application.run_polling()
