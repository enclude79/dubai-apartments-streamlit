import os
import sys
import json
import logging
import subprocess
import time
from datetime import datetime
import schedule

# Настройка логирования
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = os.path.join(log_dir, "scheduler.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

SCHEDULE_CONFIG = "schedule_config.json"

# Загрузка расписания

def load_schedule_config():
    if not os.path.exists(SCHEDULE_CONFIG):
        logger.error(f"Файл {SCHEDULE_CONFIG} не найден!")
        return []
    with open(SCHEDULE_CONFIG, encoding="utf-8") as f:
        data = json.load(f)
    return data.get("publications", [])

def run_script(script_name):
    logger.info(f"Запуск скрипта: {script_name}")
    try:
        result = subprocess.run([sys.executable, script_name], capture_output=True, text=True, timeout=3600)
        logger.info(f"Скрипт {script_name} завершён с кодом {result.returncode}")
        if result.stdout:
            logger.info(f"STDOUT {script_name}:\n{result.stdout}")
        if result.stderr:
            logger.warning(f"STDERR {script_name}:\n{result.stderr}")
    except Exception as e:
        logger.error(f"Ошибка при запуске {script_name}: {e}")

def schedule_jobs():
    publications = load_schedule_config()
    for pub in publications:
        script = pub["script_name"]
        days = pub["days"]
        time_str = pub["time"]
        for day in days:
            day = day.lower()
            if day == "понедельник":
                schedule.every().monday.at(time_str).do(run_script, script)
            elif day == "вторник":
                schedule.every().tuesday.at(time_str).do(run_script, script)
            elif day == "среда":
                schedule.every().wednesday.at(time_str).do(run_script, script)
            elif day == "четверг":
                schedule.every().thursday.at(time_str).do(run_script, script)
            elif day == "пятница":
                schedule.every().friday.at(time_str).do(run_script, script)
            elif day == "суббота":
                schedule.every().saturday.at(time_str).do(run_script, script)
            elif day == "воскресенье":
                schedule.every().sunday.at(time_str).do(run_script, script)
            else:
                logger.warning(f"Неизвестный день недели: {day} для скрипта {script}")
        logger.info(f"Добавлено расписание: {script} на {days} в {time_str}")

def main():
    logger.info("Запуск планировщика публикаций...")
    schedule_jobs()
    logger.info("Планировщик запущен. Ожидание задач...")
    try:
        while True:
            schedule.run_pending()
            time.sleep(10)
    except KeyboardInterrupt:
        logger.info("Планировщик остановлен пользователем.")

if __name__ == "__main__":
    main() 