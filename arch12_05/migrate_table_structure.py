import os
import psycopg2
import logging
from datetime import datetime
from load_env import load_environment_variables

# Настройка логирования
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = f'{log_dir}/migration_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

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
load_environment_variables()

# Параметры подключения к базе данных
DB_PARAMS = {
    'dbname': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASSWORD', 'Enclude79'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432')
}

def create_connection():
    """Создает подключение к базе данных"""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        conn.autocommit = True
        return conn
    except Exception as e:
        logger.error(f"Ошибка при подключении к базе данных: {e}")
        return None

def migrate_database_structure():
    """Миграция структуры базы данных"""
    conn = create_connection()
    if not conn:
        return False
    
    cursor = conn.cursor()
    
    try:
        # Создаем резервную копию существующей таблицы
        logger.info("Создание резервной копии существующей таблицы...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bayut_properties_backup AS 
            SELECT * FROM bayut_properties;
        """)
        logger.info("Резервная копия создана успешно")
        
        # Подсчитываем количество записей в резервной таблице
        cursor.execute("SELECT COUNT(*) FROM bayut_properties_backup")
        backup_count = cursor.fetchone()[0]
        logger.info(f"В резервной таблице сохранено {backup_count} записей")
        
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
                location JSON,
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
        
        # Восстанавливаем данные из резервной копии с преобразованием имен полей
        logger.info("Восстановление данных из резервной копии...")
        cursor.execute("""
            INSERT INTO bayut_properties 
            SELECT 
                id,
                "Unnamed: 1" AS title,
                "Unnamed: 2" AS price,
                "Unnamed: 3" AS rooms,
                "Unnamed: 4" AS baths,
                "Unnamed: 5" AS area,
                "Unnamed: 6" AS rent_frequency,
                CAST("Unnamed: 7" AS JSON) AS location,
                "Unnamed: 8" AS cover_photo_url,
                "Unnamed: 9" AS property_url,
                "Unnamed: 10" AS category,
                "Unnamed: 11" AS property_type,
                to_timestamp("Unnamed: 12", 'YYYY-MM-DD HH24:MI:SS') AS created_at,
                to_timestamp("Unnamed: 13", 'YYYY-MM-DD HH24:MI:SS') AS updated_at,
                "Unnamed: 14" AS furnishing_status,
                "Unnamed: 15" AS completion_status,
                "Unnamed: 16" AS amenities,
                "Unnamed: 18" AS agency_name,
                "Unnamed: 19" AS contact_info,
                "Unnamed: 20" AS geography,
                "Unnamed: 22" AS agency_logo_url,
                "Unnamed: 23" AS proxy_mobile,
                "Unnamed: 24" AS keywords,
                CAST("Unnamed: 25" AS BOOLEAN) AS is_verified,
                "Unnamed: 26" AS purpose,
                "Unnamed: 27" AS floor_number,
                "Unnamed: 28" AS city_level_score,
                "Unnamed: 29" AS score,
                "Unnamed: 30" AS agency_licenses,
                "Unnamed: 31" AS agency_rating
            FROM bayut_properties_backup
        """)
        logger.info("Данные успешно восстановлены")
        
        # Подсчитываем количество записей в новой таблице
        cursor.execute("SELECT COUNT(*) FROM bayut_properties")
        new_count = cursor.fetchone()[0]
        logger.info(f"В новой таблице {new_count} записей")
        
        if backup_count == new_count:
            logger.info("Миграция завершена успешно! Количество записей совпадает.")
        else:
            logger.warning(f"Внимание! Количество записей не совпадает: было {backup_count}, стало {new_count}")
            
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при миграции структуры базы данных: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

def main():
    """Основная функция скрипта"""
    logger.info("Запуск миграции структуры базы данных...")
    if migrate_database_structure():
        logger.info("Миграция структуры базы данных завершена успешно")
        print("Миграция структуры базы данных завершена успешно!")
    else:
        logger.error("Ошибка при миграции структуры базы данных")
        print("Произошла ошибка при миграции структуры базы данных. См. лог файл для подробностей.")

if __name__ == "__main__":
    main() 