import os
import sys
import json
import logging
import requests
import main  # Твой оригинальный main.py

# --- СПИСОК МОДЕЛЕЙ ---
AI_MODELS = [
    "google/gemini-2.0-flash-exp:free",      # Быстрая и умная (идеальна для саммари)
    "google/gemini-2.0-pro-exp-02-05:free",  # Если нужен более глубокий анализ
    "meta-llama/llama-3.3-70b-instruct:free",
    "deepseek/deepseek-r1-distill-llama-70b:free",
]

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")

def format_paragraphs(text: str) -> str:
    """Делает красивые отступы и разделяет абзацы."""
    paragraphs = [p.strip() for p in text.replace('\r', '').split('\n') if p.strip()]
    indent = "\u00A0\u00A0\u00A0" 
    return "\n\n".join([f"{indent}{p}" for p in paragraphs])

def translate_with_ai(text: str, to_lang: str = "ru", provider: str = "ai") -> str:
    if not text or not text.strip(): return ""
    if not OPENROUTER_API_KEY: 
        logging.warning("⚠️ [AI] Ключ не найден. Возврат оригинала.")
        return text

    logging.info(f"🤖 [AI] Генерация краткого пересказа ({len(text)} симв.)...")

    # --- НОВЫЙ ПРОМПТ: ПЕРЕСКАЗ ВМЕСТО ПЕРЕВОДА ---
    prompt = (
        f"You are a professional news editor for a Russian Telegram channel.\n"
        f"TASK: Read the English news below and write a CONCISE SUMMARY in Russian.\n\n"
        "GUIDELINES:\n"
        "1. DO NOT translate word-for-word. Write naturally in Russian.\n"
        "2. BE BRIEF: Cut out fluff, repetition, and minor details. Keep it tight.\n"
        "3. FACTS: Preserve all names, dates, numbers, and locations accurately.\n"
        "4. STRUCTURE: Use short paragraphs.\n"
        "5. TONE: Neutral, journalistic, factual.\n"
        "6. CLEAN: No ads, no 'Related Articles', no intros like 'Here is the summary'.\n\n"
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
                    "temperature": 0.4 # Чуть повышаем креативность для хорошего слога
                }),
                timeout=50
            )

            if response.status_code == 200:
                result = response.json()
                if 'choices' in result and result['choices']:
                    raw_text = result['choices'][0]['message']['content'].strip()
                    
                    # Форматируем (отступы)
                    final_text = format_paragraphs(raw_text)
                    
                    logging.info(f"✅ [AI] Успешный пересказ через {model}")
                    return final_text
            else:
                logging.warning(f"⚠️ [AI] {model} ошибка {response.status_code}. Пробуем следующую...")
        
        except Exception as e:
            logging.error(f"⚠️ [AI] Ошибка {model}: {e}")
            continue

    logging.error("❌ [AI] Все модели недоступны. Возвращаем оригинал.")
    return text

# --- ЗАПУСК ---
if __name__ == "__main__":
    main.translate_text = translate_with_ai
    main.main()
