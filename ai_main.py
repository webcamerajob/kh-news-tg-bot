import os
import sys
import json
import logging
import time
import requests
import main  # Твой обновленный main.py (где вырезан translators)

# --- СПИСОК МОДЕЛЕЙ ---
AI_MODELS = [
    "meta-llama/llama-3.3-70b-instruct:free",
    "google/gemini-2.0-flash-exp:free",
    "deepseek/deepseek-r1-distill-llama-70b:free",
]

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")

# --- ПРЯМОЙ GOOGLE ПЕРЕВОД (МГНОВЕННЫЙ) ---
def direct_google_translate(text: str, to_lang: str = "ru") -> str:
    """Переводит текст без использования тяжелых библиотек."""
    if not text: return ""
    
    # Режем на куски, чтобы Google не отклонил запрос
    chunks = []
    current_chunk = ""
    for paragraph in text.split('\n'):
        if len(current_chunk) + len(paragraph) < 1800:
            current_chunk += paragraph + "\n"
        else:
            chunks.append(current_chunk)
            current_chunk = paragraph + "\n"
    if current_chunk: chunks.append(current_chunk)
    
    translated_parts = []
    # Используем публичный мобильный API Google (GTX)
    url = "https://translate.googleapis.com/translate_a/single"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"}
    
    for chunk in chunks:
        if not chunk.strip():
            translated_parts.append("")
            continue
        try:
            params = {"client": "gtx", "sl": "en", "tl": to_lang, "dt": "t", "q": chunk.strip()}
            r = requests.get(url, params=params, headers=headers, timeout=5)
            if r.status_code == 200:
                data = r.json()
                text_part = "".join([item[0] for item in data[0] if item and item[0]])
                translated_parts.append(text_part)
            else:
                translated_parts.append(chunk)
            time.sleep(0.2)
        except Exception:
            translated_parts.append(chunk)

    return "\n".join(translated_parts)

# --- ФОРМАТИРОВАНИЕ ---
def format_paragraphs(text: str) -> str:
    paragraphs = [p.strip() for p in text.replace('\r', '').split('\n') if p.strip()]
    return "\n\n".join(paragraphs)

def strip_ai_chatter(text: str) -> str:
    bad_prefixes = ["Here is", "The article", "Summary:", "Cleaned text:"]
    for prefix in bad_prefixes:
        if text.lower().startswith(prefix.lower()):
            parts = text.split('\n', 1)
            if len(parts) > 1: return parts[1].strip()
    return text

# --- ГЛАВНАЯ ЛОГИКА ---
def ai_clean_and_then_translate(text: str, to_lang: str = "ru", provider: str = "ai") -> str:
    if not text or not text.strip(): return ""

    # 🛑 ФИЛЬТР ЗАГОЛОВКОВ (Чтобы не запускать ИИ 2 раза) 🛑
    # Если текст короткий (меньше 500 символов), это скорее всего заголовок.
    # Просто переводим его Google-ом без ИИ-чистки.
    if len(text) < 500:
        # logging.info(f"⚡ [Fast Translate] Короткий текст ({len(text)} симв.), пропускаем ИИ...")
        return direct_google_translate(text, to_lang)
    
    # Если текст длинный — запускаем тяжелую артиллерию (ИИ)
    if OPENROUTER_API_KEY: 
        logging.info("⏳ Пауза 5 сек перед ИИ...")
        time.sleep(5) 
        logging.info(f"🤖 [AI] Глубокая чистка текста...")

        # ПРОМПТ: Слияние дублей + Удаление воды
        prompt = (
            f"You are a ruthless news editor.\n"
            f"INPUT: Raw news text.\n"
            f"OUTPUT: A cleaned-up version of the story in ENGLISH.\n\n"
            "STRICT EDITING RULES:\n"
            "1. CONSOLIDATE NARRATIVE & SPEECH: If the author states a fact, and then a speaker repeats the same meaning, DELETE the speaker's part.\n"
            "2. KEEP UNIQUE DETAILS: Only keep quotes if they add numbers, dates, or emotion.\n"
            "3. REMOVE FLUFF: Delete ads and diplomatic praise.\n"
            "4. NO META-TALK: Start with the story immediately.\n\n"
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
                        "temperature": 0.3
                    }),
                    timeout=60
                )
                if response.status_code == 200:
                    result = response.json()
                    if 'choices' in result and result['choices']:
                        clean_english_text = result['choices'][0]['message']['content'].strip()
                        logging.info(f"✅ [AI] Чистка успешна ({model}).")
                        break
                elif response.status_code == 429:
                    time.sleep(2)
            except Exception: continue

        if not clean_english_text: clean_english_text = text
        clean_english_text = strip_ai_chatter(clean_english_text)
    else:
        clean_english_text = text

    # 2. ПЕРЕВОД (Google)
    logging.info(f"🌍 [Google Direct] Перевод основного текста...")
    final_russian_text = direct_google_translate(clean_english_text, to_lang)
    
    return format_paragraphs(final_russian_text)

if __name__ == "__main__":
    # Подменяем функцию в main.py на нашу умную
    main.translate_text = ai_clean_and_then_translate
    main.main()
