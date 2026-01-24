import os
import sys
import json
import logging
import time      # <--- Добавили модуль времени
import requests
import main      # Твой оригинальный main.py

# --- СПИСОК МОДЕЛЕЙ (Llama 3.3 сейчас самая надежная) ---
AI_MODELS = [
    "meta-llama/llama-3.3-70b-instruct:free",      # Ставим первой, она реже дает 429
    "google/gemini-2.0-flash-exp:free",            # Вторая (быстрая, но часто занята)
    "deepseek/deepseek-r1-distill-llama-70b:free", # Резерв
    "meta-llama/llama-3.2-3b-instruct:free",       # На самый крайний случай
]

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")

def format_paragraphs(text: str) -> str:
    """Убирает лишние отступы, делает пустую строку между абзацами."""
    paragraphs = [p.strip() for p in text.replace('\r', '').split('\n') if p.strip()]
    return "\n\n".join(paragraphs)

def translate_with_ai(text: str, to_lang: str = "ru", provider: str = "ai") -> str:
    if not text or not text.strip(): return ""
    
    if not OPENROUTER_API_KEY: 
        logging.warning("⚠️ [AI] Ключ не найден. Возврат оригинала.")
        return text

    # --- 🛑 COOL-DOWN: Пауза 10 секунд перед запросом ---
    # Это снизит шанс получить ошибку 429 (Too Many Requests)
    logging.info("⏳ Пауза 5 сек перед обращением к ИИ...")
    time.sleep(5) 

    logging.info(f"🤖 [AI] Генерация краткого пересказа ({len(text)} симв.)...")

    # Промпт для Summary (Краткий пересказ)
    prompt = (
        f"You are a professional news editor for a Russian Telegram channel.\n"
        f"TASK: Read the English news below and write a CONCISE SUMMARY in Russian.\n\n"
        "GUIDELINES:\n"
        "1. DO NOT translate word-for-word. Write naturally in Russian.\n"
        "2. BE BRIEF: Cut out fluff, repetition, and minor details. Keep it tight.\n"
        "3. FACTS: Preserve all names, dates, numbers, and locations accurately.\n"
        "4. STRUCTURE: Use short paragraphs.\n"
        "5. TONE: Neutral, journalistic, factual.\n"
        "6. CLEAN: No ads, no 'Related Articles', no intros.\n\n"
        f"SOURCE TEXT:\n{text[:15000]}"
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
                    "temperature": 0.4
                }),
                timeout=55
            )

            if response.status_code == 200:
                result = response.json()
                if 'choices' in result and result['choices']:
                    raw_text = result['choices'][0]['message']['content'].strip()
                    final_text = format_paragraphs(raw_text)
                    logging.info(f"✅ [AI] Успех! ({model})")
                    return final_text
            
            # Обработка ошибки 429 (Too Many Requests)
            elif response.status_code == 429:
                logging.warning(f"⚠️ [AI] {model} перегружена (429). Ждем 2 сек и меняем модель...")
                time.sleep(2) # Маленькая пауза перед сменой модели
            
            else:
                logging.warning(f"⚠️ [AI] {model} ошибка {response.status_code}. Следующая...")
        
        except Exception as e:
            logging.error(f"⚠️ [AI] Сбой {model}: {e}")
            continue

    logging.error("❌ [AI] Все модели заняты. Возвращаем оригинал.")
    return text

# --- ЗАПУСК ---
if __name__ == "__main__":
    main.translate_text = translate_with_ai
    main.main()
