import os
import sys
import json
import logging
import requests
import main  # Твой оригинальный main.py

# --- СПИСОК РАБОЧИХ БЕСПЛАТНЫХ МОДЕЛЕЙ ---
AI_MODELS = [
    "google/gemini-2.0-flash-exp:free",
    "google/gemini-2.0-pro-exp-02-05:free",
    "meta-llama/llama-3.3-70b-instruct:free",
    "meta-llama/llama-3.2-3b-instruct:free",
]

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")

def format_paragraphs(text: str) -> str:
    """
    Добавляет 'красную строку' (отступ) перед каждым абзацем.
    Использует неразрывные пробелы, чтобы Telegram их не удалял.
    """
    # 1. Разбиваем текст на абзацы по переносам строки
    # (учитываем, что ИИ может дать один \n или два \n\n)
    paragraphs = [p.strip() for p in text.replace('\r', '').split('\n') if p.strip()]
    
    # 2. Собираем обратно, добавляя отступ (3 неразрывных пробела)
    # \u00A0 - это неразрывный пробел
    indent = "\u00A0\u00A0\u00A0" 
    
    # Соединяем двойным переносом строки (для воздуха между абзацами)
    formatted_text = "\n\n".join([f"{indent}{p}" for p in paragraphs])
    return formatted_text

def translate_with_ai(text: str, to_lang: str = "ru", provider: str = "ai") -> str:
    if not text or not text.strip(): return ""
    
    if not OPENROUTER_API_KEY:
        logging.warning("⚠️ [AI] API KEY НЕ НАЙДЕН! Возвращаем оригинал.")
        return text

    logging.info(f"🤖 [AI] Попытка перевода статьи ({len(text)} симв.)...")

    prompt = (
        f"Translate the text below into Russian language.\n"
        "RULES:\n"
        "1. REMOVE 'Related Articles', ads, and links.\n"
        "2. OUTPUT ONLY the Russian translation.\n"
        "3. Keep paragraphs separated.\n\n"
        f"TEXT:\n{text[:15000]}"
    )

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
                    "temperature": 0.3
                }),
                timeout=45
            )

            if response.status_code == 200:
                result = response.json()
                if 'choices' in result and result['choices']:
                    raw_translated = result['choices'][0]['message']['content'].strip()
                    
                    # --- ПРИМЕНЯЕМ ФОРМАТИРОВАНИЕ ---
                    final_text = format_paragraphs(raw_translated)
                    
                    logging.info(f"✅ [AI] Успех! Перевел модель: {model}")
                    return final_text
            else:
                logging.warning(f"⚠️ [AI] {model} ошибка {response.status_code}. Пробуем следующую...")
        
        except Exception as e:
            logging.error(f"⚠️ [AI] Сбой подключения к {model}: {e}")
            continue

    logging.error("❌ [AI] ВСЕ МОДЕЛИ ОТКАЗАЛИ. Возвращаем оригинал.")
    return text

# --- ЗАПУСК ---
if __name__ == "__main__":
    main.translate_text = translate_with_ai
    main.main()
