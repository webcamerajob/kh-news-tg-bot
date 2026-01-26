# video_utils.py
import os
import time
import logging
import subprocess
from pathlib import Path
from curl_cffi import requests as cffi_requests

# Настройка логгера
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def download_and_process_video(video_url, watermark_path="watermark.png"):
    """
    Главная функция.
    Принимает: ссылку на видео (YouTube).
    Возвращает: путь к файлу (str) или None, если ошибка.
    """
    if not video_url:
        return None

    # Генерируем временные имена файлов
    timestamp = int(time.time())
    raw_path = Path(f"temp_raw_{timestamp}.mp4")
    final_path = Path(f"video_{timestamp}.mp4")

    try:
        # 1. Скачиваем (360p)
        if not _download_loader_to(video_url, raw_path):
            return None

        # 2. Накладываем вотермарку
        if not _add_watermark(raw_path, watermark_path, final_path):
            if raw_path.exists(): raw_path.unlink()
            return None

        # Убираем исходник
        if raw_path.exists(): raw_path.unlink()
        
        return str(final_path)

    except Exception as e:
        logger.error(f"Global Video Error: {e}")
        # Чистим мусор при ошибке
        if raw_path.exists(): raw_path.unlink()
        if final_path.exists(): final_path.unlink()
        return None

def _download_loader_to(video_url, output_path):
    session = cffi_requests.Session(impersonate="chrome120")
    api_url = "https://loader.to/ajax/download.php"
    
    # Просим 360p для скорости
    params = {"format": "360", "url": video_url}
    
    try:
        # Старт задачи
        resp = session.get(api_url, params=params, timeout=15)
        data = resp.json()
        if not data.get("success"):
            return False
        task_id = data.get("id")
        
        # Ждем готовности (до 60 сек)
        download_url = None
        for _ in range(20):
            time.sleep(3)
            try:
                check = session.get("https://loader.to/ajax/progress.php", params={"id": task_id}, timeout=10)
                status = check.json()
                if status.get("success") == 1:
                    download_url = status.get("download_url")
                    break
            except:
                pass
        
        if not download_url:
            return False

        # Скачивание файла
        with session.get(download_url, stream=True, timeout=120) as r:
            r.raise_for_status()
            with open(output_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        
        # Проверка на битый файл
        if output_path.stat().st_size < 10000:
            return False
            
        return True
    except Exception as e:
        logger.error(f"Download Error: {e}")
        return False

def _add_watermark(input_path, watermark_path, output_path):
    if not os.path.exists(watermark_path):
        logger.warning("Watermark file not found! Skipping watermark.")
        # Если вотермарки нет, просто переименовываем файл (отдаем без нее)
        os.rename(input_path, output_path)
        return True

    # Настройки: 35% ширины, правый верхний угол (отступ 10px), 360p пресет
    cmd = [
        "ffmpeg", "-y", "-i", str(input_path), "-i", str(watermark_path),
        "-filter_complex", "[1:v][0:v]scale2ref=iw*0.35:-1[wm][vid];[vid][wm]overlay=W-w-10:10",
        "-c:v", "libx264", "-preset", "superfast", "-crf", "28", "-c:a", "copy",
        str(output_path)
    ]
    
    try:
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"FFmpeg Error: {e.stderr.decode()}")
        return False
