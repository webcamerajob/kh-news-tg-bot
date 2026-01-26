import os
import sys
import json
import logging
import time
import requests
import main  # Твой main.py с умным фильтром картинок

# --- СПИСОК МОДЕЛЕЙ ---
AI_MODELS = [
    "meta-llama/llama-3.3-70b-instruct:free",
    "google/gemini-2.0-flash-exp:free",
    "deepseek/deepseek-r1-distill-llama-70b:free",
]

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")

# --- ПРЯМОЙ GOOGLE ПЕРЕВОД (GTX) ---
def direct_google_translate(text: str, to_lang: str = "ru") -> str:
    """
    Переводит текст напрямую через Google API.
    Режет текст на куски по 1800 символов, чтобы не было ошибок длины URL.
    """
    if not text: return ""
    
    chunks = []
    current_chunk = ""
    # Разбиваем по строкам, чтобы не рвать предложения
    for paragraph in text.split('\n'):
        # Если чанк переполняется, сохраняем его и начинаем новый
        if len(current_chunk) + len(paragraph) < 1800:
            current_chunk += paragraph + "\n"
        else:
            chunks.append(current_chunk)
            current_chunk = paragraph + "\n"
    if current_chunk: chunks.append(current_chunk)
    
    translated_parts = []
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
                # Если сбой, возвращаем оригинал куска, чтобы не терять текст
                translated_parts.append(chunk)
            time.sleep(0.2)
        except Exception:
            translated_parts.append(chunk)

    return "\n".join(translated_parts)

# --- УТИЛИТЫ ---
def format_paragraphs(text: str) -> str:
    paragraphs = [p.strip() for p in text.replace('\r', '').split('\n') if p.strip()]
    return "\n\n".join(paragraphs)

def strip_ai_chatter(text: str) -> str:
    # Удаляет вступления типа "Here is the summary"
    bad_prefixes = ["Here is", "The article", "Summary:", "Cleaned text:"]
    for prefix in bad_prefixes:
        if text.lower().startswith(prefix.lower()):
            parts = text.split('\n', 1)
            if len(parts) > 1: return parts[1].strip()
    return text

# --- ГЛАВНАЯ ЛОГИКА (AI + CONTEXT) ---
def ai_clean_and_then_translate(text: str, to_lang: str = "ru", provider: str = "ai") -> str:
    if not text or not text.strip(): return ""

    # 1. ПРОВЕРЯЕМ РАЗДЕЛИТЕЛЬ ИЗ MAIN.PY
    # main.py отправляет нам строку вида: "Заголовок ||| Текст статьи"
    DELIMITER = " ||| "
    title_part = ""
    body_part = text
    has_delimiter = False

    if DELIMITER in text:
        parts = text.split(DELIMITER, 1)
        title_part = parts[0]
        body_part = parts[1]
        has_delimiter = True

    # 2. ЕСЛИ ТЕКСТ КОРОТКИЙ - НЕ ТРАТИМ ВРЕМЯ НА ИИ
    # Переводим сразу всю склейку, контекст заголовка сохранится
    if len(body_part) < 500:
        return direct_google_translate(text, to_lang)
    
    # 3. ЕСЛИ ТЕКСТ ДЛИННЫЙ - ЧИСТИМ ТОЛЬКО ТЕЛО (BODY)
    clean_body_english = body_part

    if OPENROUTER_API_KEY: 
        logging.info("⏳ Пауза 5 сек перед ИИ...")
        time.sleep(5) 
        logging.info(f"🤖 [AI] Глубокая чистка текста...")

        # Промпт: удаляем воду и дублирующиеся цитаты
        prompt = (
            f"You are a ruthless news editor.\n"
            f"INPUT: Raw news text.\n"
            f"OUTPUT: A cleaned-up version of the story in ENGLISH.\n\n"
            "STRICT EDITING RULES:\n"
            "1. CONSOLIDATE NARRATIVE & SPEECH: If the author states a fact, and then a speaker repeats the same meaning, DELETE the speaker's part.\n"
            "2. KEEP UNIQUE DETAILS: Only keep quotes if they add numbers, dates, or emotion.\n"
            "3. REMOVE FLUFF: Delete ads and diplomatic praise.\n"
            "4. NO META-TALK: Start with the story immediately.\n\n"
            f"RAW TEXT:\n{body_part[:15000]}"
        )

        ai_result = ""
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
                        ai_result = result['choices'][0]['message']['content'].strip()
                        logging.info(f"✅ [AI] Чистка успешна ({model}).")
                        break
                elif response.status_code == 429:
                    time.sleep(2)
            except Exception: continue

        # Если ИИ ответил - используем его, иначе оставляем оригинал
        if ai_result:
            clean_body_english = strip_ai_chatter(ai_result)
        else:
            clean_body_english = body_part 
    
    # 4. СКЛЕИВАЕМ ОБРАТНО ДЛЯ ПЕРЕВОДА
    # Оригинальный заголовок + ||| + Очищенный текст
    if has_delimiter:
        final_text_to_translate = f"{title_part}{DELIMITER}{clean_body_english}"
    else:
        final_text_to_translate = clean_body_english

    # 5. ПЕРЕВОДИМ СКЛЕЙКУ
    logging.info(f"🌍 [Google Direct] Перевод (контекстный)...")
    translated_text = direct_google_translate(final_text_to_translate, to_lang)
    
    # Возвращаем строку с разделителем. main.py сам её разрежет и сохранит жирный заголовок.
    return translated_text

if __name__ == "__main__":
    # Монтируем нашу функцию в main.py
    main.translate_text = ai_clean_and_then_translate
    main.main()
