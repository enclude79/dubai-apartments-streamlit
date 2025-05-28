#!/usr/bin/env python
"""
Скрипт для экспорта данных из PostgreSQL в SQLite.
Используется для создания файла SQLite, который можно загрузить в Git
и использовать в Streamlit Cloud.

Использование:
    python postgres_to_sqlite.py
"""

import os
import pandas as pd
import psycopg2
import sqlite3
from dotenv import load_dotenv
import time
import logging
from datetime import datetime
import json

# Настройка логирования
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = f'{log_dir}/postgres_to_sqlite_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

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

# Параметры PostgreSQL
PG_CONFIG = {
    'dbname': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASSWORD', 'Enclude79'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432')
}

# Путь к файлу SQLite
SQLITE_DB_PATH = "dubai_properties.db"

def get_postgres_schema():
    """Получает схему таблицы bayut_properties из PostgreSQL"""
    try:
        # Подключаемся к PostgreSQL
        conn = psycopg2.connect(
            dbname=PG_CONFIG['dbname'],
            user=PG_CONFIG['user'],
            password=PG_CONFIG['password'],
            host=PG_CONFIG['host'],
            port=PG_CONFIG['port'],
            connect_timeout=10
        )
        cursor = conn.cursor()
        
        # Получаем список колонок и их типов
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length
            FROM information_schema.columns
            WHERE table_name = 'bayut_properties'
            ORDER BY ordinal_position;
        """)
        columns = cursor.fetchall()
        
        # Получаем информацию о первичном ключе
        cursor.execute("""
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = 'bayut_properties'::regclass AND i.indisprimary;
        """)
        primary_keys = cursor.fetchall()
        primary_key_columns = [pk[0] for pk in primary_keys]
        
        cursor.close()
        conn.close()
        
        return {
            'columns': columns,
            'primary_keys': primary_key_columns
        }
    except Exception as e:
        logger.error(f"Ошибка при получении схемы PostgreSQL: {e}")
        return None

def create_sqlite_schema(schema):
    """Создает схему таблицы в SQLite на основе схемы PostgreSQL"""
    if not schema:
        logger.error("Не удалось получить схему PostgreSQL")
        return False
    
    # Преобразование типов данных PostgreSQL в типы SQLite
    type_mapping = {
        'integer': 'INTEGER',
        'bigint': 'INTEGER',
        'smallint': 'INTEGER',
        'character varying': 'TEXT',
        'text': 'TEXT',
        'boolean': 'INTEGER',  # SQLite не имеет булевого типа
        'double precision': 'REAL',
        'real': 'REAL',
        'numeric': 'REAL',
        'timestamp without time zone': 'TEXT',
        'timestamp with time zone': 'TEXT',
        'date': 'TEXT',
        'jsonb': 'TEXT',  # JSON хранится как текст в SQLite
        'json': 'TEXT'
    }
    
    try:
        # Подключаемся к SQLite
        conn = sqlite3.connect(SQLITE_DB_PATH)
        cursor = conn.cursor()
        
        # Создаем таблицу properties
        create_table_sql = "CREATE TABLE IF NOT EXISTS properties (\n"
        
        for column in schema['columns']:
            column_name = column[0]
            data_type = column[1]
            max_length = column[2]
            
            # Определяем тип SQLite
            sqlite_type = type_mapping.get(data_type, 'TEXT')
            
            # Добавляем определение колонки
            create_table_sql += f"    {column_name} {sqlite_type}"
            
            # Добавляем PRIMARY KEY для первичных ключей
            if column_name in schema['primary_keys']:
                create_table_sql += " PRIMARY KEY"
            
            create_table_sql += ",\n"
        
        # Удаляем последнюю запятую и закрываем скобку
        create_table_sql = create_table_sql.rstrip(",\n") + "\n);"
        
        # Удаляем существующую таблицу, если она есть
        cursor.execute("DROP TABLE IF EXISTS properties;")
        
        # Создаем новую таблицу
        cursor.execute(create_table_sql)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info("Схема SQLite успешно создана")
        return True
    except Exception as e:
        logger.error(f"Ошибка при создании схемы SQLite: {e}")
        return False

def export_data(batch_size=1000):
    """Экспортирует данные из PostgreSQL в SQLite по частям"""
    try:
        # Подключаемся к PostgreSQL
        pg_conn = psycopg2.connect(
            dbname=PG_CONFIG['dbname'],
            user=PG_CONFIG['user'],
            password=PG_CONFIG['password'],
            host=PG_CONFIG['host'],
            port=PG_CONFIG['port'],
            connect_timeout=10
        )
        pg_cursor = pg_conn.cursor()
        
        # Получаем список колонок и их имен
        pg_cursor.execute("SELECT * FROM bayut_properties LIMIT 0;")
        
        # Получаем имена колонок и сохраняем их позицию
        column_info = {desc[0]: i for i, desc in enumerate(pg_cursor.description)}
        column_names = list(column_info.keys()) # Упорядоченный список имен колонок
        
        columns_str = ", ".join(column_names)
        placeholders = ", ".join(['?'] * len(column_names))
        
        # Получаем общее количество записей
        pg_cursor.execute("SELECT COUNT(*) FROM bayut_properties;")
        total_records = pg_cursor.fetchone()[0]
        logger.info(f"Всего записей для экспорта: {total_records}")
        
        # Подключаемся к SQLite
        sqlite_conn = sqlite3.connect(SQLITE_DB_PATH)
        sqlite_cursor = sqlite_conn.cursor()
        
        # Начинаем экспорт
        start_time = time.time()
        processed_records = 0
        
        for offset in range(0, total_records, batch_size):
            # Получаем партию данных из PostgreSQL
            pg_cursor.execute(f"SELECT * FROM bayut_properties LIMIT {batch_size} OFFSET {offset};")
            batch_data = pg_cursor.fetchall()
            
            if not batch_data:
                break
            
            # Конвертируем типы, несовместимые с SQLite
            converted_batch = []
            for row in batch_data:
                converted_row = []
                for i, value in enumerate(row):
                    col_name = column_names[i] # Получаем имя текущей колонки

                    if col_name == 'geography':
                        if isinstance(value, dict): # Если это уже словарь (из JSON/JSONB)
                            # Убедимся, что есть ключи lat и lng, иначе пишем NULL
                            if 'lat' in value and 'lng' in value and value['lat'] is not None and value['lng'] is not None:
                                converted_row.append(json.dumps(value))
                            else:
                                converted_row.append(None) # Неполные или отсутствующие координаты
                        elif isinstance(value, str): # Если это строка
                            try:
                                # Попытка распарсить и проверить структуру
                                parsed_json = json.loads(value)
                                if isinstance(parsed_json, dict) and 'lat' in parsed_json and 'lng' in parsed_json and parsed_json['lat'] is not None and parsed_json['lng'] is not None:
                                    converted_row.append(value) # Строка уже валидный JSON с нужными полями
                                else:
                                    converted_row.append(None) # Валидный JSON, но не той структуры
                            except json.JSONDecodeError:
                                converted_row.append(None) # Невалидный JSON
                        elif value is None:
                            converted_row.append(None)
                        else: # Другие типы для geography - маловероятно, но на всякий случай
                            converted_row.append(None)
                    elif value is None:
                        converted_row.append(None)
                    elif isinstance(value, (int, str, float, bool)):
                        converted_row.append(value)
                    elif hasattr(value, 'isoformat'):  # Даты и время
                        converted_row.append(value.isoformat())
                    else:  # Другие типы (включая Decimal)
                        converted_row.append(str(value))
                converted_batch.append(tuple(converted_row))
            
            # Вставляем данные в SQLite
            sqlite_cursor.executemany(
                f"INSERT INTO properties ({columns_str}) VALUES ({placeholders});", 
                converted_batch
            )
            sqlite_conn.commit()
            
            processed_records += len(batch_data)
            elapsed_time = time.time() - start_time
            rate = processed_records / elapsed_time if elapsed_time > 0 else 0
            
            logger.info(f"Прогресс: {processed_records}/{total_records} записей ({processed_records/total_records*100:.1f}%), "
                      f"скорость: {rate:.1f} записей/сек")
        
        # Закрываем соединения
        pg_cursor.close()
        pg_conn.close()
        sqlite_cursor.close()
        sqlite_conn.close()
        
        total_time = time.time() - start_time
        logger.info(f"Экспорт завершен. Всего экспортировано {processed_records} записей за {total_time:.1f} секунд.")
        return True
    except Exception as e:
        logger.error(f"Ошибка при экспорте данных: {e}")
        return False

def optimize_sqlite():
    """Оптимизирует базу данных SQLite"""
    try:
        conn = sqlite3.connect(SQLITE_DB_PATH)
        cursor = conn.cursor()
        
        logger.info("Оптимизация SQLite...")
        
        # VACUUM
        logger.info("Выполняем VACUUM для оптимизации размера файла...")
        cursor.execute("VACUUM;")
        conn.commit()
        
        # ANALYZE
        logger.info("Выполняем ANALYZE для оптимизации запросов...")
        cursor.execute("ANALYZE;")
        conn.commit()
        
        # Создаем индексы (если их нет)
        logger.info("Создаем индексы...")
        
        # Пример создания индекса для часто используемых колонок:
        # Индекс для колонки 'area' (ранее было 'size')
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_area ON properties (area);")
        # Индекс для колонки 'price'
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_price ON properties (price);")
        # Индекс для колонки 'property_type'
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_property_type ON properties (property_type);")
        # Индекс для географических координат (если они часто используются в WHERE)
        # Поскольку json_extract используется в приложении, прямой индекс на lat/lng создать сложно.
        # Вместо этого, индексируем поле geography целиком, если по нему идет фильтрация.
        # cursor.execute("CREATE INDEX IF NOT EXISTS idx_geography ON properties (geography);")
        
        conn.commit()
        logger.info("Индексы созданы/проверены.")
        
        cursor.close()
        conn.close()
        logger.info("Оптимизация SQLite завершена.")
        return True
    except Exception as e:
        logger.error(f"Ошибка при оптимизации SQLite: {e}")
        return False

def main():
    """Основная функция для выполнения экспорта"""
    logger.info("Начало экспорта данных из PostgreSQL в SQLite")
    
    # Получаем схему PostgreSQL
    logger.info("Получение схемы PostgreSQL...")
    schema = get_postgres_schema()
    if not schema:
        logger.error("Не удалось получить схему PostgreSQL. Экспорт отменен.")
        return False
    
    # Создаем схему SQLite
    logger.info("Создание схемы SQLite...")
    if not create_sqlite_schema(schema):
        logger.error("Не удалось создать схему SQLite. Экспорт отменен.")
        return False
    
    # Экспортируем данные
    logger.info("Экспорт данных...")
    if not export_data():
        logger.error("Не удалось экспортировать данные. Экспорт отменен.")
        return False
    
    # Оптимизируем SQLite
    logger.info("Оптимизация SQLite...")
    if not optimize_sqlite():
        logger.warning("Не удалось оптимизировать SQLite. Экспорт завершен, но база данных может быть не оптимальной.")
    
    logger.info("Экспорт успешно завершен!")
    return True

if __name__ == "__main__":
    main() 