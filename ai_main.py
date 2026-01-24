import os
import sys
import json
import logging
import time
import requests
import translators as ts
import main  # Твой оригинальный main.py

# --- СПИСОК МОДЕЛЕЙ ---
AI_MODELS = [
    "meta-llama/llama-3.3-70b-instruct:free",
    "google/gemini-2.0-flash-exp:free",
    "deepseek/deepseek-r1-distill-llama-70b:free",
]

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")

# --- ОБЫЧНЫЙ ПЕРЕВОД (FIXED) ---
def standard_translate(text: str, to_lang: str = "ru") -> str:
    if not text: return ""
    
    # ИЗМЕНЕНИЕ: Google смещен в конец списка из-за ошибок HTTP/3
    # Bing работает мгновенно и без сбоев протокола
    providers = ["google", "yandex", "bing"]
    
    for provider in providers:
        try:
            # logging.info(f"   Trying {provider}...")
            time.sleep(1)
            result = ts.translate_text(
                query_text=text,
                translator=provider,
                from_language="en",
                to_language=to_lang,
                timeout=30
            )
            return result
        except Exception: 
            continue
            
    logging.error("❌ Все провайдеры перевода отказали.")
    return text

# --- ФОРМАТИРОВАНИЕ ---
def format_paragraphs(text: str) -> str:
    paragraphs = [p.strip() for p in text.replace('\r', '').split('\n') if p.strip()]
    return "\n\n".join(paragraphs)

def strip_ai_chatter(text: str) -> str:
    bad_prefixes = ["Here is", "The article", "This text", "Summary:", "Revised text:", "Cleaned text:"]
    for prefix in bad_prefixes:
        if text.lower().startswith(prefix.lower()):
            parts = text.split('\n', 1)
            if len(parts) > 1: return parts[1].strip()
            parts = text.split(':', 1)
            if len(parts) > 1: return parts[1].strip()
    return text

# --- ГЛАВНАЯ ЛОГИКА ---
def ai_clean_and_then_translate(text: str, to_lang: str = "ru", provider: str = "ai") -> str:
    if not text or not text.strip(): return ""
    
    if not OPENROUTER_API_KEY: 
        return standard_translate(text, to_lang)

    logging.info("⏳ Пауза 5 сек перед ИИ...")
    time.sleep(5) 
    logging.info(f"🤖 [AI] Редакторская чистка (без сжатия)...")

    prompt = (
        f"You are a professional news editor.\n"
        f"INPUT: Raw news text.\n"
        f"OUTPUT: A cleaned-up version of the story in ENGLISH.\n\n"
        "EDITING RULES:\n"
        "1. RETAIN DETAIL: Keep all facts, names, dates, and the full story structure.\n"
        "2. REMOVE FLUFF: Delete ads, 'Related Articles', repetitive sentences, and overly formal diplomatic praise.\n"
        "3. TIGHTEN: Rewrite wordy sentences to be direct and clear, but DO NOT summarize the whole story into one paragraph.\n"
        "4. NO META-TALK: Do not write 'Here is the cleaned text'. Start with the story.\n\n"
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

    if not clean_english_text:
        clean_english_text = text

    clean_english_text = strip_ai_chatter(clean_english_text)

    logging.info(f"🌍 [Translators] Перевод очищенного текста (через Bing)...")
    final_russian_text = standard_translate(clean_english_text, to_lang)
    
    return format_paragraphs(final_russian_text)

if __name__ == "__main__":
    main.translate_text = ai_clean_and_then_translate
    main.main()
