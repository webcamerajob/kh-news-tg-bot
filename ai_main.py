import os
import json
import logging
import requests
import main  # Твой основной файл main.py

# --- НАСТРОЙКИ ---
# Проверь, чтобы в GitHub Secrets имя было в точности OPENROUTER_API_KEY
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")

# Список моделей для пробы (если одна выдаст 404, можно будет легко сменить)
# Попробуй сначала эту (сейчас она самая актуальная из бесплатных Flash):
MODEL = "google/gemini-2.0-flash-lite-preview-02-05:free" 

def translate_with_ai(text: str, to_lang: str = "ru", provider: str = "ai") -> str:
    if not text or not text.strip():
        return ""

    if not OPENROUTER_API_KEY:
        logging.error("❌ [AI ERROR] API ключ не найден! Проверь переменные окружения.")
        return text

    logging.info(f"🤖 [AI] Пробуем перевод через {MODEL}...")

    prompt = (
        f"Translate this news article to {to_lang}. "
        "Remove all ads, social media links, and 'Related Articles' blocks. "
        "Return ONLY the translated Russian text.\n\n"
        f"TEXT:\n{text}"
    )

    try:
        response = requests.post(
            url="https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                "Content-Type": "application/json",
                # OpenRouter просит эти два заголовка для корректной работы бесплатных моделей:
                "HTTP-Referer": "https://github.com/your-repo", 
                "X-Title": "News Parser Bot",
            },
            data=json.dumps({
                "model": MODEL,
                "messages": [
                    {"role": "system", "content": "You are a professional translator. English to Russian."},
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0.3
            }),
            timeout=60
        )
        
        # Если получили ошибку, выводим подробности
        if response.status_code != 200:
            logging.error(f"❌ [AI ERROR] OpenRouter вернул {response.status_code}: {response.text}")
            return text

        result = response.json()
        translated_text = result['choices'][0]['message']['content']
        return translated_text.strip()
            
    except Exception as e:
        logging.error(f"❌ [AI ERROR] Критическая ошибка: {e}")
        return text

# Подменяем функцию
main.translate_text = translate_text

if __name__ == "__main__":
    main.main()
