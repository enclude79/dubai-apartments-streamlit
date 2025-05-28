import os
import psycopg2
import logging
from dotenv import load_dotenv
from datetime import datetime

# Настройка логирования
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = f'{log_dir}/db_count_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Загружаем переменные окружения
load_dotenv()

# Параметры базы данных
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASSWORD', 'Enclude79'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432')
}

def check_db_count():
    """Проверяет количество записей в базе данных и логирует результаты"""
    try:
        # Подключаемся к базе данных
        logger.info("Подключение к базе данных...")
        conn = psycopg2.connect(
            dbname=DB_CONFIG['dbname'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port']
        )
        
        with conn.cursor() as cursor:
            # Получаем количество записей в таблице bayut_properties
            cursor.execute("SELECT COUNT(*) FROM bayut_properties")
            count = cursor.fetchone()[0]
            logger.info(f"Количество записей в таблице bayut_properties: {count}")
            print(f"Количество записей в таблице bayut_properties: {count}")
            
            # Получаем последнюю дату обновления
            cursor.execute("SELECT updated_at FROM bayut_properties ORDER BY updated_at DESC LIMIT 1")
            last_update = cursor.fetchone()
            if last_update:
                logger.info(f"Последняя дата обновления в таблице bayut_properties: {last_update[0]}")
                print(f"Последняя дата обновления в таблице bayut_properties: {last_update[0]}")
            
            # Получаем информацию о последнем запуске
            cursor.execute("SELECT last_run_date, last_updated_at FROM last_run_info WHERE script_name = 'api_to_sql'")
            last_run = cursor.fetchone()
            if last_run:
                logger.info(f"Последний запуск api_to_sql: {last_run[0]}")
                logger.info(f"Последняя сохраненная дата обновления: {last_run[1]}")
                print(f"Последний запуск api_to_sql: {last_run[0]}")
                print(f"Последняя сохраненная дата обновления: {last_run[1]}")
        
        conn.close()
        logger.info("Проверка завершена успешно")
        return True
    
    except Exception as e:
        logger.error(f"Ошибка при проверке базы данных: {e}")
        print(f"Ошибка при проверке базы данных: {e}")
        return False

if __name__ == "__main__":
    print("Проверка количества записей в базе данных...")
    check_db_count()
    print("Проверка завершена. Результаты записаны в лог.") 