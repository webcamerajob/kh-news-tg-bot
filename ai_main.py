import os
import json
import logging
import requests
import main  # Твой оригинальный main.py

# --- НАСТРОЙКИ ---
# Берем ключ из секретов GitHub
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")

# Модель (выбрал самую стабильную из бесплатных на сегодня)
MODEL = "google/gemini-2.0-flash-lite-preview-02-05:free"

def translate_with_ai(text: str, to_lang: str = "ru", provider: str = "ai") -> str:
    """
    Функция-обертка, которая заменяет стандартный перевод на ИИ.
    """
    if not text or not text.strip():
        return ""

    if not OPENROUTER_API_KEY:
        logging.error("❌ [AI ERROR] API ключ не найден в переменных окружения!")
        return text

    logging.info(f"🤖 [AI] Перевод и очистка через {MODEL}...")

    # Просим перевести и убрать мусор
    prompt = (
        f"Translate the following news article to {to_lang}. "
        "Strictly remove all advertisements, social media 'follow us' links, and 'Related Articles' sections. "
        "Return ONLY the translated text in Russian.\n\n"
        f"ARTICLE TEXT:\n{text}"
    )

    try:
        response = requests.post(
            url="https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                "Content-Type": "application/json",
                "HTTP-Referer": "https://github.com/your-repo", # Для OpenRouter Free
                "X-Title": "News Parser Bot",
            },
            data=json.dumps({
                "model": MODEL,
                "messages": [
                    {"role": "system", "content": "You are a professional editor. Translate English to Russian accurately and remove clutter."},
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0.3
            }),
            timeout=60
        )
        
        if response.status_code != 200:
            logging.error(f"❌ [AI ERROR] OpenRouter вернул {response.status_code}: {response.text}")
            return text

        result = response.json()
        if 'choices' in result and len(result['choices']) > 0:
            return result['choices'][0]['message']['content'].strip()
        else:
            logging.error(f"❌ [AI ERROR] Странный ответ от API: {result}")
            return text
            
    except Exception as e:
        logging.error(f"❌ [AI ERROR] Ошибка при обращении к ИИ: {e}")
        return text

# --- МОНКЕЙ-ПАТЧИНГ (Та самая магия подмены) ---
# Теперь имена совпадают: присваиваем нашу функцию оригинальной
main.translate_text = translate_with_ai

if __name__ == "__main__":
    # Запускаем оригинальный main() из твоего файла
    main.main()
