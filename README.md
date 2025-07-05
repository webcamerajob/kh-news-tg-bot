# 📰 KhmerTimes Article Parser

Скрипт на Python для парсинга новостей с [KhmerTimesKH](https://www.khmertimeskh.com/). Извлекает статьи по категории, сохраняет текст, фильтрует изображения по продвинутым правилам и сохраняет метаданные.

---

## ⚙️ Установка

1. Перейди в директорию проекта:

    ```bash
    cd C:\Users\model\PycharmProjects\parser_040725
    ```

2. Создай виртуальное окружение:

    ```powershell
    python -m venv .venv
    ```

3. Активируй окружение (PowerShell):

    ```powershell
    .venv\Scripts\Activate.ps1
    ```

4. Установи зависимости:

    ```bash
    pip install httpx beautifulsoup4
    ```

---

## ▶️ Запуск из PyCharm

1. Открой проект в PyCharm
2. Нажми **File → Settings → Project → Python Interpreter**  
   Выбери: `.venv\Scripts\python.exe`
3. Нажми **Run → Edit Configurations**
    - Название: `Khmer Parser`
    - Script path: `parser.py`
    - Python Interpreter: выбери созданное `.venv`
4. Нажми зелёную кнопку ▶️ — парсинг стартует

---

## 📁 Структура результата

Каждая статья сохраняется в `articles/{ID}_{slug}/`, где содержится:

- `content.txt` — чистый текст
- `images/` — отфильтрованные изображения
- `meta.json` — метаданные (ID, заголовок, дата, ссылка, список файлов)
- `catalog.json` — общий список всех статей

---

## 🔍 Техническое описание фильтрации изображений

Функция `is_valid_image(img: Tag) -> bool` фильтрует `<img>` по:

- Классам (`class`) — допускаются `attachment-post-thumbnail`, `aligncenter`, `featured-image` и др.
- Путь в `src` — содержит `/uploads/`, не содержит `ads/`, `.gif` и др.
- `alt` текст — допускаются `photo`, `image`; исключаются `emoji`, `ad`, `decorative`
- Размер (`width`, `height`) — минимум 300×200 пикселей
- Расширения — только `.jpg`, `.jpeg`, `.png`, `.webp`

Конфигурация фильтра гибко задаётся через `IMG_FILTER_CONFIG`

---

## 🧩 Планы расширения

- Поддержка `featured_media` через REST API
- Автопубликация в Telegram
- Кеширование / повторная обработка
- Обработка нескольких категорий

---

## 📜 Лицензия

MIT — используй, дорабатывай, улучшай 🛠️
