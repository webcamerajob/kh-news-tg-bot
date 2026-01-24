import os
import sys
import json
import logging
import time
import requests
import translators as ts  # Библиотека для обычного перевода
import main  # Твой оригинальный main.py

# --- СПИСОК МОДЕЛЕЙ ---
# Используем Llama 3.3 как основную, она отлично структурирует английский текст
AI_MODELS = [
    "meta-llama/llama-3.3-70b-instruct:free",
    "google/gemini-2.0-flash-exp:free",
    "deepseek/deepseek-r1-distill-llama-70b:free",
    "meta-llama/llama-3.2-3b-instruct:free",
]

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")

# --- ФУНКЦИЯ ОБЫЧНОГО ПЕРЕВОДА (ГУГЛ/БИНГ) ---
def standard_translate(text: str, to_lang: str = "ru") -> str:
    """
    Берет чистый английский текст и переводит через провайдеров.
    """
    if not text: return ""
    
    # Список провайдеров по приоритету
    providers = ["google", "bing", "yandex"]
    
    for provider in providers:
        try:
            # logging.info(f"   🌍 Перевод через {provider}...")
            # sleep чтобы не банили
            time.sleep(1) 
            result = ts.translate_text(
                query_text=text,
                translator=provider,
                from_language="en",
                to_language=to_lang,
                timeout=20
            )
            return result
        except Exception as e:
            # logging.warning(f"   ⚠️ {provider} не смог: {e}")
            continue
            
    # Если никто не смог перевести, возвращаем английский (лучше чем ничего)
    logging.error("❌ Все провайдеры перевода отказали.")
    return text

# --- ФУНКЦИЯ ФОРМАТИРОВАНИЯ ---
def format_paragraphs(text: str) -> str:
    """Делает двойные переносы строк для читаемости в Telegram."""
    paragraphs = [p.strip() for p in text.replace('\r', '').split('\n') if p.strip()]
    return "\n\n".join(paragraphs)

# --- ГЛАВНАЯ ЛОГИКА ---
def ai_clean_and_then_translate(text: str, to_lang: str = "ru", provider: str = "ai") -> str:
    if not text or not text.strip(): return ""
    
    # Если ключа нет — используем только обычный переводчик на грязном тексте
    if not OPENROUTER_API_KEY: 
        logging.warning("⚠️ [AI] Ключ не найден. Используем обычный перевод оригинала.")
        return standard_translate(text, to_lang)

    # 1. ЭТАП ОЧИСТКИ (ИИ)
    logging.info("⏳ Пауза 5 сек перед ИИ...")
    time.sleep(5) 
    logging.info(f"🤖 [AI] 1. Чистка и саммари на английском...")

    # Промпт: просим сделать чистое резюме на АНГЛИЙСКОМ
    prompt = (
        f"You are a professional news editor.\n"
        f"TASK: Read the raw text below and write a CONCISE SUMMARY in ENGLISH.\n\n"
        "GUIDELINES:\n"
        "1. LANGUAGE: English only.\n"
        "2. CONTENT: Remove ads, 'Related Articles', links, and fluff.\n"
        "3. STYLE: Journalistic, objective, factual.\n"
        "4. STRUCTURE: Keep paragraphs clear.\n\n"
        f"RAW TEXT:\n{text[:15000]}"
    )

    clean_english_text = ""

    # Цикл по моделям
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
                    "temperature": 0.3 # Пониже, чтобы было четко
                }),
                timeout=55
            )

            if response.status_code == 200:
                result = response.json()
                if 'choices' in result and result['choices']:
                    clean_english_text = result['choices'][0]['message']['content'].strip()
                    logging.info(f"✅ [AI] Очистка успешна ({model}).")
                    break # Выходим из цикла моделей
            
            elif response.status_code == 429:
                logging.warning(f"⚠️ [AI] {model} (429). Ждем...")
                time.sleep(2)
            else:
                logging.warning(f"⚠️ [AI] {model} ошибка {response.status_code}.")
        
        except Exception as e:
            logging.error(f"⚠️ [AI] Сбой {model}: {e}")
            continue

    # Если ИИ не справился, используем оригинальный текст как "чистый"
    if not clean_english_text:
        logging.error("❌ [AI] Не удалось очистить текст. Переводим оригинал.")
        clean_english_text = text

    # 2. ЭТАП ПЕРЕВОДА (ПРОВАЙДЕРЫ)
    logging.info(f"🌍 [Translators] 2. Перевод чистого текста на русский...")
    
    final_russian_text = standard_translate(clean_english_text, to_lang)
    
    # Финальное форматирование
    return format_paragraphs(final_russian_text)

# --- ЗАПУСК ---
if __name__ == "__main__":
    # Подменяем функцию перевода в main на нашу гибридную
    main.translate_text = ai_clean_and_then_translate
    
    # Запускаем
    main.main()
