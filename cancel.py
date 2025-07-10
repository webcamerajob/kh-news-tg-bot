#!/usr/bin/env python3
import os
import json
from pathlib import Path
import fcntl
import logging
import telebot

# Получение токена из переменной окружения (GitHub Secrets)
API_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
if not API_TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN не установлен в переменных окружения!")

CATALOG_PATH = Path('articles/catalog.json')
bot = telebot.TeleBot(API_TOKEN, parse_mode='HTML')

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def load_catalog():
    """Загрузка каталога с блокировкой файла"""
    if not CATALOG_PATH.exists():
        return []
    try:
        with open(CATALOG_PATH, "r", encoding="utf-8") as f:
            fcntl.flock(f, fcntl.LOCK_SH)
            data = json.load(f)
            return [item for item in data if isinstance(item, dict) and "id" in item]
    except Exception as e:
        logging.error(f"Ошибка загрузки catalog.json: {e}")
        return []

def save_catalog(catalog):
    """Сохранение каталога с блокировкой файла"""
    CATALOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(CATALOG_PATH, "w", encoding="utf-8") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            json.dump(catalog, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.error(f"Ошибка сохранения catalog.json: {e}")

@bot.message_handler(commands=['cancelmenu'])
def send_cancel_menu(message):
    """
    /cancelmenu — показать список запланированных публикаций с кнопками для отмены.
    """
    catalog = load_catalog()
    markup = telebot.types.InlineKeyboardMarkup()
    for art in catalog:
        if not art.get('posted', False):
            btn = telebot.types.InlineKeyboardButton(
                f"Отменить: {art.get('title', '')[:32]}",
                callback_data=f"cancel_{art['id']}"
            )
            markup.add(btn)
    if markup.keyboard:
        bot.send_message(message.chat.id, "Запланированные публикации:", reply_markup=markup)
    else:
        bot.send_message(message.chat.id, "Нет запланированных публикаций для отмены.")

@bot.callback_query_handler(func=lambda call: call.data.startswith('cancel_'))
def cancel_post(call):
    """
    Обработка нажатия на кнопку "Отменить": помечаем публикацию как отменённую.
    """
    post_id = int(call.data.split('_')[1])
    catalog = load_catalog()
    changed = False
    for art in catalog:
        if art["id"] == post_id and not art.get("posted", False):
            art["posted"] = True
            changed = True
    if changed:
        save_catalog(catalog)
        bot.answer_callback_query(call.id, text="Публикация отменена!")
        bot.edit_message_text("Публикация отменена.", call.message.chat.id, call.message.message_id)
    else:
        bot.answer_callback_query(call.id, text="Уже отменено или не найдено.", show_alert=True)

@bot.message_handler(commands=['help', 'start'])
def send_help(message):
    """
    /help — выводит справку и описание команд
    """
    help_text = (
        "🤖 <b>Управление публикациями Telegram-бота</b>\n\n"
        "<b>/cancelmenu</b> — показать запланированные публикации с кнопками для отмены.\n"
        "<b>/help</b> — показать это руководство.\n\n"
        "Чтобы отменить публикацию, выберите её в меню отмены. "
        "Отменённые публикации не будут отправлены в канал.\n\n"
        "<b>Как запустить бота и парсер на GitHub:</b>\n"
        "1. Откройте вкладку <b>Actions</b> на GitHub.\n"
        "2. Выберите нужный workflow и нажмите <b>Run workflow</b>.\n"
        "3. Не забудьте добавить секрет <b>TELEGRAM_BOT_TOKEN</b> в настройках репозитория (Settings → Secrets → Actions).\n"
    )
    bot.send_message(message.chat.id, help_text, parse_mode='HTML')

if name == "main":
    print("Бот отмены публикаций запущен.")
    bot.polling(none_stop=True)
