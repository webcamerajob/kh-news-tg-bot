import os
import sys
import json
import logging
import requests
import main  # Твой оригинальный main.py

# --- СПИСОК РАБОЧИХ БЕСПЛАТНЫХ МОДЕЛЕЙ (ОБНОВЛЕН) ---
AI_MODELS = [
    # 1. Самая стабильная на данный момент (Flash Experimental)
    "google/gemini-2.0-flash-exp:free",
    
    # 2. Новая мощная Pro версия (если Flash занята)
    "google/gemini-2.0-pro-exp-02-05:free",
    
    # 3. Llama от Meta (хороший запасной вариант)
    "meta-llama/llama-3.3-70b-instruct:free",
    
    # 4. Легкая модель для крайнего случая
    "meta-llama/llama-3.2-3b-instruct:free",
]

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")

def translate_with_ai(text: str, to_lang: str = "ru", provider: str = "ai") -> str:
    if not text or not text.strip(): return ""
    
    # Если ключа нет — сразу пишем Warning, но возвращаем оригинал, чтобы процесс не падал
    if not OPENROUTER_API_KEY:
        logging.warning("⚠️ [AI] API KEY НЕ НАЙДЕН! Возвращаем оригинал.")
        return text

    logging.info(f"🤖 [AI] Попытка перевода статьи ({len(text)} симв.)...")

    # Жесткий промпт на русский язык
    prompt = (
        f"Translate the text below into Russian language.\n"
        "RULES:\n"
        "1. REMOVE 'Related Articles', ads, and links.\n"
        "2. OUTPUT ONLY the Russian translation.\n\n"
        f"TEXT:\n{text[:15000]}"
    )

    # Перебор моделей (Ротация)
    for model in AI_MODELS:
        try:
            # logging.info(f"Trying model: {model}...") 
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
                    translated = result['choices'][0]['message']['content'].strip()
                    logging.info(f"✅ [AI] Успех! Перевел модель: {model}")
                    return translated
            else:
                # Если 404 или 400 - пробуем следующую
                logging.warning(f"⚠️ [AI] {model} ошибка {response.status_code}. Пробуем следующую...")
        
        except Exception as e:
            logging.error(f"⚠️ [AI] Сбой подключения к {model}: {e}")
            continue

    logging.error("❌ [AI] ВСЕ МОДЕЛИ ОТКАЗАЛИ. Возвращаем оригинал.")
    return text

# --- ЗАПУСК ---
if __name__ == "__main__":
    # Подменяем функцию перевода
    main.translate_text = translate_with_ai
    
    # Запускаем основной скрипт
    main.main()
