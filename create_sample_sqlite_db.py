import sqlite3
import pandas as pd
import numpy as np
import os
from datetime import datetime
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("create_sample_db.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("create_sample_db")

# Путь к SQLite базе данных
SQLITE_DB_PATH = "dubai_properties.db"

def create_sqlite_connection():
    """Создает соединение с SQLite"""
    try:
        conn = sqlite3.connect(SQLITE_DB_PATH)
        return conn
    except Exception as e:
        logger.error(f"Ошибка создания базы SQLite: {e}")
        return None

def create_sample_data():
    """Создает примерные данные для базы данных"""
    # Список районов Дубая
    areas = [
        "Dubai Marina", "Palm Jumeirah", "Downtown Dubai", "Jumeirah Beach Residence",
        "Business Bay", "Dubai Hills Estate", "Emirates Hills", "Jumeirah Lake Towers",
        "Arabian Ranches", "Jumeirah Islands", "Jumeirah Park", "Dubai Creek Harbour"
    ]
    
    # Типы недвижимости
    property_types = [
        "Apartment", "Villa", "Penthouse", "Townhouse", "Duplex", 
        "Studio", "Loft", "Mansion"
    ]
    
    # Генерируем данные
    num_records = 500
    
    data = {
        "id": np.arange(1, num_records + 1),
        "title": [f"Beautiful {np.random.choice(property_types)} in {np.random.choice(areas)}" for _ in range(num_records)],
        "description": [f"Luxurious property with amazing views and premium amenities." for _ in range(num_records)],
        "price": np.random.randint(500000, 10000000, num_records),
        "area": np.random.choice(areas, num_records),
        "property_type": np.random.choice(property_types, num_records),
        "bedrooms": np.random.choice([1, 2, 3, 4, 5, 6], num_records),
        "bathrooms": np.random.choice([1, 2, 3, 4, 5], num_records),
        "size": np.random.randint(50, 1000, num_records),
        "latitude": np.random.uniform(25.0, 25.3, num_records),
        "longitude": np.random.uniform(55.1, 55.4, num_records),
        "status": np.random.choice(["For Sale", "For Rent", "Sold"], num_records),
        "created_at": [datetime.now().strftime("%Y-%m-%d %H:%M:%S") for _ in range(num_records)]
    }
    
    return pd.DataFrame(data)

def create_tables(conn):
    """Создает таблицы в базе данных"""
    try:
        cursor = conn.cursor()
        
        # Таблица properties
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS properties (
            id INTEGER PRIMARY KEY,
            title TEXT,
            description TEXT,
            price REAL,
            area TEXT,
            property_type TEXT,
            bedrooms INTEGER,
            bathrooms INTEGER,
            size REAL,
            latitude REAL,
            longitude REAL,
            status TEXT,
            created_at TEXT
        )
        """)
        
        conn.commit()
        logger.info("Таблицы успешно созданы")
        return True
    except Exception as e:
        logger.error(f"Ошибка создания таблиц: {e}")
        return False

def insert_sample_data(conn, df):
    """Вставляет примерные данные в базу данных"""
    try:
        # Очищаем таблицу
        cursor = conn.cursor()
        cursor.execute("DELETE FROM properties")
        conn.commit()
        
        # Вставляем данные
        df.to_sql("properties", conn, if_exists="append", index=False)
        
        logger.info(f"Вставлено {len(df)} записей в таблицу properties")
        return True
    except Exception as e:
        logger.error(f"Ошибка вставки данных: {e}")
        return False

def main():
    """Основная функция"""
    logger.info("Начало создания примерной базы данных SQLite")
    
    # Проверяем, существует ли файл базы данных
    if os.path.exists(SQLITE_DB_PATH):
        logger.warning(f"Файл базы данных {SQLITE_DB_PATH} уже существует и будет перезаписан")
    
    # Создаем соединение
    conn = create_sqlite_connection()
    if not conn:
        logger.error("Не удалось создать соединение с базой данных")
        return False
    
    success = True
    
    try:
        # Создаем таблицы
        if not create_tables(conn):
            logger.error("Не удалось создать таблицы")
            success = False
        
        # Создаем примерные данные
        sample_data = create_sample_data()
        
        # Вставляем данные
        if not insert_sample_data(conn, sample_data):
            logger.error("Не удалось вставить примерные данные")
            success = False
        
    except Exception as e:
        logger.error(f"Необработанная ошибка: {e}")
        success = False
    finally:
        conn.close()
    
    if success:
        logger.info(f"Примерная база данных успешно создана: {SQLITE_DB_PATH}")
    else:
        logger.error("Ошибка при создании примерной базы данных")
    
    return success

if __name__ == "__main__":
    if main():
        print(f"База данных {SQLITE_DB_PATH} успешно создана с примерными данными")
    else:
        print("Ошибка при создании базы данных. Смотрите лог для подробностей.") 