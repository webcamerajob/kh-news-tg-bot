import os
import sys
import json
import logging
import time
import requests
import translators as ts  # Библиотека для обычного перевода
import main  # Твой оригинальный main.py

# --- СПИСОК МОДЕЛЕЙ ---
AI_MODELS = [
    "meta-llama/llama-3.3-70b-instruct:free",      # Отлично понимает запреты
    "google/gemini-2.0-flash-exp:free",
    "deepseek/deepseek-r1-distill-llama-70b:free",
    "meta-llama/llama-3.2-3b-instruct:free",
]

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")

# --- ОБЫЧНЫЙ ПЕРЕВОД ---
def standard_translate(text: str, to_lang: str = "ru") -> str:
    if not text: return ""
    providers = ["google", "bing", "yandex"]
    for provider in providers:
        try:
            time.sleep(1) 
            result = ts.translate_text(
                query_text=text,
                translator=provider,
                from_language="en",
                to_language=to_lang,
                timeout=20
            )
            return result
        except Exception: continue
            
    logging.error("❌ Все провайдеры перевода отказали.")
    return text

# --- ФОРМАТИРОВАНИЕ ---
def format_paragraphs(text: str) -> str:
    paragraphs = [p.strip() for p in text.replace('\r', '').split('\n') if p.strip()]
    return "\n\n".join(paragraphs)

# --- УДАЛЕНИЕ ВСТУПЛЕНИЙ (ПОСТ-ОБРАБОТКА) ---
def strip_ai_chatter(text: str) -> str:
    """Удаляет типичный мусор, если ИИ все-таки ослушался."""
    bad_prefixes = [
        "Here is a summary", "Here is the summary", "In this article", 
        "The article discusses", "According to the report", "Summary:",
        "Вот краткое содержание", "Эта статья о том", "Резюме:"
    ]
    # Если текст начинается с мусора, ищем первое двоеточие или новую строку
    for prefix in bad_prefixes:
        if text.lower().startswith(prefix.lower()):
            # Пробуем обрезать по двоеточию (Here is the summary: News...)
            parts = text.split(':', 1)
            if len(parts) > 1:
                return parts[1].strip()
            # Или просто выкидываем первую строку
            parts = text.split('\n', 1)
            if len(parts) > 1:
                return parts[1].strip()
    return text

# --- ГЛАВНАЯ ЛОГИКА ---
def ai_clean_and_then_translate(text: str, to_lang: str = "ru", provider: str = "ai") -> str:
    if not text or not text.strip(): return ""
    
    if not OPENROUTER_API_KEY: 
        logging.warning("⚠️ [AI] Ключ не найден. Используем обычный перевод.")
        return standard_translate(text, to_lang)

    logging.info("⏳ Пауза 5 сек перед ИИ...")
    time.sleep(5) 
    logging.info(f"🤖 [AI] Чистка (Strict Mode)...")

    # 🔥 ЖЕСТКИЙ ПРОМПТ 🔥
    prompt = (
        f"You are a backend news processor API.\n"
        f"INPUT: Raw news text with ads and noise.\n"
        f"OUTPUT: A clean, concise summary in ENGLISH.\n\n"
        "STRICT NEGATIVE CONSTRAINTS (DO NOT IGNORE):\n"
        "1. NO INTRODUCTIONS (Never write 'Here is a summary', 'The text says', etc.).\n"
        "2. NO OUTROS (No 'Hope this helps').\n"
        "3. NO LABELS (Do not write 'Summary:' or 'Headline:').\n"
        "4. NO META-TALK. Start directly with the first word of the news story.\n\n"
        "CONTENT RULES:\n"
        "- Remove ads, links, and 'Related Articles'.\n"
        "- Keep dates, names, and locations exact.\n"
        "- Use neutral, journalistic tone.\n\n"
        f"RAW TEXT:\n{text[:15000]}"
    )

    clean_english_text = ""

    for model in AI_MODELS:
        try:
            response = requests.post(
                url="https://openrouter.ai/api/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                    "Content-Type": "application/json",
                    "HTTP-Referer": "https://github.com/parser-bot",
                    "X-Title": "NewsBot",
                },
                data=json.dumps({
                    "model": model,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.2 # Минимальная температура = меньше отсебятины
                }),
                timeout=55
            )

            if response.status_code == 200:
                result = response.json()
                if 'choices' in result and result['choices']:
                    clean_english_text = result['choices'][0]['message']['content'].strip()
                    logging.info(f"✅ [AI] Очистка успешна ({model}).")
                    break
            elif response.status_code == 429:
                time.sleep(2)
            else:
                logging.warning(f"⚠️ [AI] {model} ошибка {response.status_code}.")
        
        except Exception: continue

    if not clean_english_text:
        logging.error("❌ [AI] Сбой. Переводим оригинал.")
        clean_english_text = text

    # Дополнительная страховка: чистим мусорные фразы программно
    clean_english_text = strip_ai_chatter(clean_english_text)

    # 2. ПЕРЕВОД
    logging.info(f"🌍 [Translators] Перевод чистого текста...")
    final_russian_text = standard_translate(clean_english_text, to_lang)
    
    return format_paragraphs(final_russian_text)

if __name__ == "__main__":
    main.translate_text = ai_clean_and_then_translate
    main.main()
