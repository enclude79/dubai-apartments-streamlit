import schedule
import time
import logging
import subprocess
import os
import sys
from datetime import datetime, timedelta

# Настройка логирования
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = f'{log_dir}/scheduler_{datetime.now().strftime("%Y%m%d")}.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_api_to_csv():
    """Запускает скрипт загрузки данных из API в CSV"""
    logger.info("Запуск загрузки данных из API в CSV")
    
    try:
        # Получаем путь к интерпретатору Python
        python_path = sys.executable
        
        # Определяем команду для запуска скрипта
        script_path = os.path.join(os.getcwd(), "api_to_csv.py")
        
        # Запускаем скрипт в отдельном процессе
        process = subprocess.Popen(
            [python_path, script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Получаем результат выполнения
        stdout, stderr = process.communicate(timeout=3600)  # Максимальное время выполнения - 1 час
        
        if process.returncode == 0:
            logger.info("Загрузка данных из API в CSV успешно завершена")
            
            # Ищем путь к CSV файлу в выводе
            csv_path = None
            for line in stdout.splitlines():
                if line.startswith("CSV_PATH:"):
                    csv_path = line.strip().replace("CSV_PATH:", "")
                    break
            
            if csv_path:
                logger.info(f"Найден путь к CSV файлу: {csv_path}")
                return csv_path
            else:
                logger.warning("Путь к CSV файлу не найден в выводе скрипта")
                return None
        else:
            logger.error(f"Ошибка при выполнении скрипта (код {process.returncode})")
            logger.error(f"Ошибка: {stderr}")
            return None
            
    except subprocess.TimeoutExpired:
        logger.error("Превышено время выполнения скрипта (1 час)")
        return None
    except Exception as e:
        logger.error(f"Ошибка при запуске скрипта: {e}")
        return None

def run_csv_to_sql(csv_path):
    """Запускает скрипт загрузки данных из CSV в SQL"""
    if not csv_path:
        logger.error("Не указан путь к CSV файлу для загрузки в SQL")
        return False
        
    logger.info(f"Запуск загрузки данных из CSV в SQL: {csv_path}")
    
    try:
        # Получаем путь к интерпретатору Python
        python_path = sys.executable
        
        # Определяем команду для запуска скрипта
        script_path = os.path.join(os.getcwd(), "csv_to_sql.py")
        
        # Запускаем скрипт в отдельном процессе
        process = subprocess.Popen(
            [python_path, script_path, csv_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Получаем результат выполнения
        stdout, stderr = process.communicate(timeout=3600)  # Максимальное время выполнения - 1 час
        
        if process.returncode == 0:
            logger.info("Загрузка данных из CSV в SQL успешно завершена")
            return True
        else:
            logger.error(f"Ошибка при выполнении скрипта (код {process.returncode})")
            logger.error(f"Ошибка: {stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error("Превышено время выполнения скрипта (1 час)")
        return False
    except Exception as e:
        logger.error(f"Ошибка при запуске скрипта: {e}")
        return False

def run_property_loader():
    """Запускает полный процесс загрузки данных о недвижимости"""
    logger.info("Запуск ежедневной загрузки данных о недвижимости")
    
    try:
        # Шаг 1: Загрузка данных из API в CSV
        csv_path = run_api_to_csv()
        
        if not csv_path:
            logger.error("Ошибка при загрузке данных из API в CSV. Процесс загрузки прерван.")
            return False
            
        # Шаг 2: Загрузка данных из CSV в SQL
        success = run_csv_to_sql(csv_path)
        
        if success:
            logger.info("Полный процесс загрузки данных успешно завершен")
            return True
        else:
            logger.error("Ошибка при загрузке данных из CSV в SQL")
            return False
            
    except Exception as e:
        logger.error(f"Ошибка при выполнении процесса загрузки: {e}")
        return False

def run_daily(hour=3, minute=0):
    """Запускает api_to_csv.py каждый день в заданное время (по умолчанию 03:00)"""
    while True:
        now = datetime.now()
        next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if next_run <= now:
            next_run += timedelta(days=1)
        wait_seconds = (next_run - now).total_seconds()
        print(f"Следующий запуск: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
        time.sleep(wait_seconds)
        print(f"Запуск скрипта api_to_csv.py в {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        subprocess.run(["python", "api_to_csv.py"])

def main():
    """Основная функция скрипта планировщика"""
    # Получаем время для ежедневного запуска (по умолчанию 2:00 ночи)
    execution_time = os.environ.get("DAILY_LOAD_TIME", "02:00")
    
    logger.info(f"Планировщик запущен. Время ежедневного запуска: {execution_time}")
    
    # Планируем ежедневное выполнение
    schedule.every().day.at(execution_time).do(run_property_loader)
    
    # Если скрипт запущен с параметром --now, выполняем загрузку немедленно
    if len(sys.argv) > 1 and sys.argv[1] == "--now":
        logger.info("Запуск немедленной загрузки данных")
        run_property_loader()
    
    # Основной цикл планировщика
    while True:
        schedule.run_pending()
        time.sleep(60)  # Проверка каждую минуту

if __name__ == "__main__":
    # Обрабатываем случай, когда запуск с параметром --daemon
    if len(sys.argv) > 1 and sys.argv[1] == "--daemon":
        try:
            main()
        except KeyboardInterrupt:
            logger.info("Планировщик остановлен пользователем")
    else:
        # Выводим информацию о параметрах запуска
        print("Использование:")
        print("  python schedule_daily_load.py          - Выводит эту подсказку")
        print("  python schedule_daily_load.py --now    - Запускает загрузку немедленно")
        print("  python schedule_daily_load.py --daemon - Запускает планировщик в фоновом режиме")
        
        # Если нет параметров, запускаем загрузку сразу
        if len(sys.argv) == 1:
            print("\nЗапуск загрузки данных...")
            run_property_loader()

    run_daily(hour=3, minute=0)  # Запуск каждый день в 03:00 