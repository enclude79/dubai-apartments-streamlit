import schedule
import time
import os
import subprocess
import logging
import argparse
from datetime import datetime
import sys
import git
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("schedule_export.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("schedule_export")

# Настройки Git репозитория
GIT_REPO_PATH = os.getenv("GIT_REPO_PATH", os.getcwd())
GIT_USERNAME = os.getenv("GIT_USERNAME", "")
GIT_EMAIL = os.getenv("GIT_EMAIL", "")
GIT_TOKEN = os.getenv("GIT_TOKEN", "")  # Персональный токен доступа GitHub

def run_export_script():
    """Запускает скрипт экспорта данных"""
    try:
        logger.info("Запуск скрипта экспорта данных из PostgreSQL в SQLite")
        
        # Запускаем скрипт экспорта
        result = subprocess.run(
            [sys.executable, "postgres_to_sqlite.py"],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            logger.info("Скрипт экспорта выполнен успешно")
            logger.debug(f"Вывод: {result.stdout}")
            return True
        else:
            logger.error(f"Ошибка при выполнении скрипта экспорта: {result.stderr}")
            return False
    except Exception as e:
        logger.error(f"Ошибка при запуске скрипта экспорта: {e}")
        return False

def push_to_github():
    """Отправляет изменения в GitHub репозиторий"""
    try:
        logger.info("Отправка изменений в GitHub")
        
        # Инициализируем репозиторий
        repo = git.Repo(GIT_REPO_PATH)
        
        # Проверяем, есть ли изменения для коммита
        if not repo.is_dirty() and len(repo.untracked_files) == 0:
            logger.info("Нет изменений для коммита")
            return True
        
        # Добавляем новые файлы
        repo.git.add("dubai_properties.db")
        
        # Создаем коммит
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        commit_message = f"Автоматический экспорт данных из PostgreSQL в SQLite ({timestamp})"
        repo.git.commit("-m", commit_message)
        
        # Настраиваем удаленный репозиторий с токеном, если он предоставлен
        if GIT_TOKEN:
            origin_url = repo.remotes.origin.url
            if "https://" in origin_url:
                # Заменяем URL на версию с токеном
                new_url = origin_url.replace(
                    "https://", f"https://{GIT_TOKEN}@"
                )
                repo.git.remote("set-url", "origin", new_url)
        
        # Отправляем изменения
        repo.git.push("origin", repo.active_branch.name)
        
        logger.info("Изменения успешно отправлены в GitHub")
        return True
    except Exception as e:
        logger.error(f"Ошибка при отправке изменений в GitHub: {e}")
        return False

def export_and_push():
    """Экспортирует данные и отправляет их в GitHub"""
    logger.info("Начало процесса экспорта и отправки данных")
    
    # Запускаем экспорт
    if run_export_script():
        # Если экспорт успешен, отправляем в GitHub
        if push_to_github():
            logger.info("Процесс экспорта и отправки данных успешно завершен")
        else:
            logger.error("Не удалось отправить данные в GitHub")
    else:
        logger.error("Не удалось выполнить экспорт данных")

def setup_git_config():
    """Настраивает конфигурацию Git, если она не настроена"""
    try:
        if GIT_USERNAME and GIT_EMAIL:
            repo = git.Repo(GIT_REPO_PATH)
            with repo.config_writer() as git_config:
                git_config.set_value('user', 'name', GIT_USERNAME)
                git_config.set_value('user', 'email', GIT_EMAIL)
            logger.info(f"Git настроен для пользователя {GIT_USERNAME} <{GIT_EMAIL}>")
        return True
    except Exception as e:
        logger.error(f"Ошибка при настройке Git: {e}")
        return False

def start_scheduler(interval_hours=12):
    """Запускает планировщик с указанным интервалом"""
    logger.info(f"Запуск планировщика с интервалом {interval_hours} часов")
    
    # Настраиваем Git, если предоставлены учетные данные
    setup_git_config()
    
    # Выполняем первоначальный экспорт и отправку
    export_and_push()
    
    # Настраиваем расписание
    schedule.every(interval_hours).hours.do(export_and_push)
    
    # Основной цикл планировщика
    while True:
        schedule.run_pending()
        time.sleep(60)  # Проверяем каждую минуту

def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(description="Планировщик экспорта данных из PostgreSQL в SQLite и отправки в GitHub")
    
    parser.add_argument(
        "--run-once",
        action="store_true",
        help="Выполнить экспорт и отправку один раз, без запуска планировщика"
    )
    
    parser.add_argument(
        "--interval",
        type=int,
        default=12,
        help="Интервал выполнения экспорта в часах (по умолчанию: 12)"
    )
    
    args = parser.parse_args()
    
    if args.run_once:
        logger.info("Запуск одноразового экспорта и отправки данных")
        export_and_push()
    else:
        start_scheduler(args.interval)

if __name__ == "__main__":
    main() 