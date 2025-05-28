import os
import pandas as pd
import psycopg2
import logging
from datetime import datetime
import numpy as np

# Настройка логирования
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = f'{log_dir}/recreate_simple_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Параметры базы данных
DB_CONFIG = {
    'dbname': 'postgres',
    'user': 'admin',
    'password': 'Enclude79',
    'host': 'localhost',
    'port': '5432'
}

# Пути к файлам
CSV_DIR = "Api_Bayat"

def recreate_table():
    """Пересоздаёт таблицу bayut_properties без первичного ключа"""
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Удаляем существующую таблицу, если она есть
        cursor.execute("DROP TABLE IF EXISTS bayut_properties")
        logger.info("Таблица bayut_properties удалена")
        print("Таблица bayut_properties удалена")
        
        # Создаем новую таблицу без PRIMARY KEY для id
        cursor.execute("""
            CREATE TABLE bayut_properties (
                id BIGINT,
                title TEXT,
                price NUMERIC,
                rooms INTEGER,
                baths INTEGER,
                area NUMERIC,
                rent_frequency TEXT,
                location TEXT,
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
        
        # Создаем индекс для ускорения поиска
        cursor.execute("CREATE INDEX idx_bayut_properties_id ON bayut_properties(id)")
        
        logger.info("Таблица bayut_properties создана заново (без первичного ключа)")
        print("Таблица bayut_properties создана заново (без первичного ключа)")
        
        cursor.close()
        return True
    except Exception as e:
        logger.error(f"Ошибка при пересоздании таблицы: {e}")
        print(f"Ошибка при пересоздании таблицы: {e}")
        return False
    finally:
        if conn:
            conn.close()

def load_data_from_csv(csv_file):
    """Загружает данные из CSV в базу данных"""
    conn = None
    cursor = None
    try:
        # Проверяем существование файла
        if not os.path.exists(csv_file):
            logger.error(f"Файл не найден: {csv_file}")
            print(f"Файл не найден: {csv_file}")
            return False
        
        # Загружаем данные из CSV напрямую
        logger.info(f"Загрузка данных из CSV файла: {csv_file}")
        print(f"Загрузка данных из CSV файла: {csv_file}")
        
        # Подключаемся к базе данных
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Создаем временную таблицу для COPY
        cursor.execute("""
            CREATE TEMP TABLE temp_csv_import (LIKE bayut_properties)
        """)
        
        # Загружаем файл с помощью COPY
        print("Загрузка CSV напрямую через COPY...")
        with open(csv_file, 'r', encoding='utf-8-sig') as f:
            # Пропускаем заголовок
            next(f)
            # Копируем данные напрямую
            cursor.copy_expert("""
                COPY temp_csv_import FROM STDIN WITH (FORMAT CSV, HEADER false, ENCODING 'UTF8')
            """, f)
        
        # Подсчитываем количество загруженных строк
        cursor.execute("SELECT COUNT(*) FROM temp_csv_import")
        loaded_count = cursor.fetchone()[0]
        print(f"Загружено {loaded_count} строк из CSV")
        
        # Переносим данные из временной таблицы в основную, выполняя необходимую конвертацию типов
        cursor.execute("""
            INSERT INTO bayut_properties
            SELECT 
                (CASE WHEN trim(id) ~ '^[0-9]+$' THEN id::BIGINT ELSE NULL END),
                title,
                (CASE WHEN trim(price) ~ '^[0-9.]+$' THEN price::NUMERIC ELSE NULL END),
                (CASE WHEN trim(rooms) ~ '^[0-9.]+$' THEN rooms::INTEGER ELSE NULL END),
                (CASE WHEN trim(baths) ~ '^[0-9.]+$' THEN baths::INTEGER ELSE NULL END),
                (CASE WHEN trim(area) ~ '^[0-9.]+$' THEN area::NUMERIC ELSE NULL END),
                rent_frequency,
                location,
                cover_photo_url,
                property_url,
                category,
                property_type,
                (CASE WHEN created_at ~ '[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN created_at::TIMESTAMP ELSE NULL END),
                (CASE WHEN updated_at ~ '[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN updated_at::TIMESTAMP ELSE NULL END),
                furnishing_status,
                completion_status,
                amenities,
                agency_name,
                contact_info,
                geography,
                agency_logo_url,
                proxy_mobile,
                keywords,
                (CASE 
                    WHEN lower(is_verified) IN ('true', 't', 'yes', 'y', '1') THEN TRUE
                    WHEN lower(is_verified) IN ('false', 'f', 'no', 'n', '0') THEN FALSE
                    ELSE NULL
                END),
                purpose,
                (CASE WHEN trim(floor_number) ~ '^[0-9-]+$' THEN floor_number::INTEGER ELSE NULL END),
                (CASE WHEN trim(city_level_score) ~ '^[0-9-]+$' THEN city_level_score::INTEGER ELSE NULL END),
                (CASE WHEN trim(score) ~ '^[0-9-]+$' THEN score::INTEGER ELSE NULL END),
                agency_licenses,
                (CASE WHEN trim(agency_rating) ~ '^[0-9.]+$' THEN agency_rating::NUMERIC ELSE NULL END)
            FROM temp_csv_import
            WHERE id IS NOT NULL
        """)
        
        # Получаем статистику
        cursor.execute("SELECT COUNT(*) FROM bayut_properties")
        total_count = cursor.fetchone()[0]
        
        # Фиксируем изменения
        conn.commit()
        print(f"Транзакция успешно завершена. Добавлено {total_count} записей в базу данных")
        
        return True
    except Exception as e:
        if conn and not conn.closed:
            conn.rollback()
        logger.error(f"Ошибка при загрузке данных: {e}")
        print(f"Ошибка при загрузке данных: {e}")
        return False
    finally:
        if cursor:
            cursor.close()
        if conn and not conn.closed:
            conn.close()

def main():
    """Основная функция скрипта"""
    print("Запуск процесса пересоздания таблицы и загрузки данных")
    
    # Пересоздаем таблицу
    if not recreate_table():
        print("Ошибка при пересоздании таблицы")
        return 1
    
    # Находим самый новый CSV файл
    csv_files = []
    for root, _, files in os.walk(CSV_DIR):
        for file in files:
            if file.endswith('.csv'):
                csv_files.append(os.path.join(root, file))
    
    if not csv_files:
        logger.error("CSV файлы не найдены")
        print("CSV файлы не найдены")
        return 1
    
    # Сортируем по дате изменения (самый новый первый)
    csv_files.sort(key=os.path.getmtime, reverse=True)
    csv_file = csv_files[0]
    logger.info(f"Выбран самый новый CSV файл: {csv_file}")
    print(f"Выбран самый новый CSV файл: {csv_file}")
    
    # Загружаем данные
    if load_data_from_csv(csv_file):
        print("Процесс успешно завершен")
        return 0
    else:
        print("Процесс завершен с ошибками")
        return 1

if __name__ == "__main__":
    main() 