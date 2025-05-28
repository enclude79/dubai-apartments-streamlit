import os
import psycopg2
import logging
from datetime import datetime
from load_env import load_environment_variables

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/recreate_table_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Загружаем переменные окружения
load_environment_variables()

# Параметры подключения к базе данных
DB_PARAMS = {
    'dbname': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASSWORD', 'Enclude79'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432')
}

def recreate_table():
    """Удаляет существующую таблицу и создает новую с правильной структурой"""
    conn = None
    try:
        # Подключаемся к базе данных
        logger.info("Подключение к базе данных...")
        conn = psycopg2.connect(**DB_PARAMS)
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Проверяем существование таблицы
        cursor.execute("SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = 'bayut_properties')")
        table_exists = cursor.fetchone()[0]
        
        if table_exists:
            # Удаляем существующую таблицу
            logger.info("Удаление существующей таблицы...")
            cursor.execute("DROP TABLE IF EXISTS bayut_properties")
            logger.info("Таблица bayut_properties удалена")
        
        # Создаем новую таблицу с правильными именами полей
        logger.info("Создание новой таблицы с правильными именами полей...")
        cursor.execute("""
            CREATE TABLE bayut_properties (
                id INTEGER PRIMARY KEY,
                title TEXT,
                price NUMERIC,
                rooms INTEGER,
                baths INTEGER,
                area NUMERIC,
                rent_frequency TEXT,
                location TEXT,  /* Изменено с JSON на TEXT для избежания проблем с форматом */
                cover_photo_url TEXT,
                property_url TEXT,
                category TEXT,
                property_type TEXT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                furnishing_status TEXT,
                completion_status TEXT,
                amenities TEXT,
                agency_name TEXT,
                contact_info TEXT,
                geography TEXT,
                agency_logo_url TEXT,
                proxy_mobile TEXT,
                keywords TEXT,
                is_verified BOOLEAN,
                purpose TEXT,
                floor_number INTEGER,
                city_level_score INTEGER,
                score INTEGER,
                agency_licenses TEXT,
                agency_rating NUMERIC
            )
        """)
        logger.info("Новая таблица создана успешно")
        
        return True
    except Exception as e:
        logger.error(f"Ошибка при пересоздании таблицы: {e}")
        return False
    finally:
        if conn:
            conn.close()

def main():
    """Основная функция скрипта"""
    logger.info("Запуск процесса пересоздания таблицы...")
    if recreate_table():
        logger.info("Таблица успешно пересоздана")
        print("Таблица успешно пересоздана")
    else:
        logger.error("Ошибка при пересоздании таблицы")
        print("Ошибка при пересоздании таблицы")

if __name__ == "__main__":
    main() 