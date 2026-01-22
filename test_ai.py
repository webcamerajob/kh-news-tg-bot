import os
import time
from google import genai

def test_gemini():
    api_key = os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        print("❌ Ошибка: GOOGLE_API_KEY не найден в переменных окружения.")
        return

    # Инициализируем клиент (версия API v1)
    client = genai.Client(api_key=api_key, http_options={'api_version': 'v1'})
    
    test_prompt = "Напиши одну короткую фразу: 'ИИ работает'."
    
    print("🤖 Проверка связи с Gemini 1.5 Flash...")
    try:
        # Для бесплатного тарифа в 2026 году лучше делать паузу даже перед первым запросом
        time.sleep(2) 
        response = client.models.generate_content(
            model='gemini-1.5-flash',
            contents=test_prompt
        )
        if response.text:
            print(f"✅ Успех! Ответ ИИ: {response.text.strip()}")
        else:
            print("⚠️ ИИ вернул пустой ответ.")
    except Exception as e:
        print(f"❌ Ошибка при вызове API: {e}")
        if "429" in str(e):
            print("ℹ️ Это ошибка лимитов. Убедитесь, что в Google AI Studio привязана карта (даже для Free Tier).")

if __name__ == "__main__":
    test_gemini()
