import os
import psycopg2
import sqlite3
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("postgres_to_sqlite.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("postgres_to_sqlite")

# Загрузка переменных окружения
load_dotenv()

# Параметры подключения к PostgreSQL
PG_CONFIG = {
    'host': os.getenv("DB_HOST", "localhost"),
    'port': os.getenv("DB_PORT", "5432"),
    'database': os.getenv("DB_NAME", "postgres"),
    'user': os.getenv("DB_USER", "postgres"),
    'password': os.getenv("DB_PASSWORD", "")
}

# Путь к SQLite базе данных
SQLITE_DB_PATH = "dubai_properties.db"

# Таблицы для экспорта
TABLES_TO_EXPORT = [
    "properties",
]

def connect_to_postgres():
    """Устанавливает соединение с PostgreSQL"""
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        return conn
    except Exception as e:
        logger.error(f"Ошибка подключения к PostgreSQL: {e}")
        return None

def create_sqlite_connection():
    """Создает соединение с SQLite"""
    try:
        conn = sqlite3.connect(SQLITE_DB_PATH)
        return conn
    except Exception as e:
        logger.error(f"Ошибка создания базы SQLite: {e}")
        return None

def get_postgres_table_schema(pg_conn, table_name):
    """Получает схему таблицы PostgreSQL"""
    try:
        query = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position;
        """
        
        return pd.read_sql_query(query, pg_conn)
    except Exception as e:
        logger.error(f"Ошибка получения схемы таблицы {table_name}: {e}")
        return None

def create_sqlite_table(sqlite_conn, table_name, schema_df):
    """Создает таблицу в SQLite на основе схемы PostgreSQL"""
    try:
        # Создаем SQL-запрос для создания таблицы
        columns = []
        for _, row in schema_df.iterrows():
            col_name = row['column_name']
            data_type = row['data_type']
            
            # Преобразование типов данных PostgreSQL в SQLite
            if data_type in ('integer', 'bigint', 'smallint'):
                sqlite_type = 'INTEGER'
            elif data_type in ('numeric', 'decimal', 'real', 'double precision'):
                sqlite_type = 'REAL'
            elif data_type in ('timestamp without time zone', 'timestamp with time zone', 'date', 'time'):
                sqlite_type = 'TEXT'
            else:
                sqlite_type = 'TEXT'
                
            columns.append(f"{col_name} {sqlite_type}")
            
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)});"
        
        # Создаем таблицу
        cursor = sqlite_conn.cursor()
        cursor.execute(create_table_sql)
        sqlite_conn.commit()
        logger.info(f"Таблица {table_name} создана или уже существует в SQLite")
        return True
    except Exception as e:
        logger.error(f"Ошибка создания таблицы {table_name} в SQLite: {e}")
        return False

def export_table_data(pg_conn, sqlite_conn, table_name):
    """Экспортирует данные из таблицы PostgreSQL в SQLite"""
    try:
        # Получаем данные из PostgreSQL
        logger.info(f"Экспорт данных из таблицы {table_name}")
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", pg_conn)
        
        if df.empty:
            logger.warning(f"Таблица {table_name} не содержит данных")
            return False
        
        # Очищаем таблицу в SQLite перед вставкой новых данных
        cursor = sqlite_conn.cursor()
        cursor.execute(f"DELETE FROM {table_name}")
        sqlite_conn.commit()
        
        # Записываем данные в SQLite
        df.to_sql(table_name, sqlite_conn, if_exists='append', index=False)
        
        logger.info(f"Экспорт таблицы {table_name} завершен. Экспортировано {len(df)} записей.")
        return True
    except Exception as e:
        logger.error(f"Ошибка экспорта данных для таблицы {table_name}: {e}")
        return False

def main():
    """Основная функция экспорта данных"""
    start_time = datetime.now()
    logger.info(f"Начало экспорта данных из PostgreSQL в SQLite в {start_time}")
    
    # Подключение к базам данных
    pg_conn = connect_to_postgres()
    if not pg_conn:
        logger.error("Не удалось подключиться к PostgreSQL. Экспорт отменен.")
        return False
    
    sqlite_conn = create_sqlite_connection()
    if not sqlite_conn:
        logger.error("Не удалось создать базу SQLite. Экспорт отменен.")
        pg_conn.close()
        return False
    
    success = True
    
    try:
        # Обрабатываем каждую таблицу
        for table_name in TABLES_TO_EXPORT:
            # Получаем схему таблицы
            schema_df = get_postgres_table_schema(pg_conn, table_name)
            if schema_df is None:
                logger.error(f"Не удалось получить схему таблицы {table_name}. Пропускаем.")
                success = False
                continue
            
            # Создаем таблицу в SQLite
            if not create_sqlite_table(sqlite_conn, table_name, schema_df):
                logger.error(f"Не удалось создать таблицу {table_name} в SQLite. Пропускаем.")
                success = False
                continue
            
            # Экспортируем данные
            if not export_table_data(pg_conn, sqlite_conn, table_name):
                logger.error(f"Не удалось экспортировать данные таблицы {table_name}. Пропускаем.")
                success = False
                continue
                
    except Exception as e:
        logger.error(f"Необработанная ошибка при экспорте данных: {e}")
        success = False
    finally:
        # Закрываем соединения
        pg_conn.close()
        sqlite_conn.close()
    
    end_time = datetime.now()
    duration = end_time - start_time
    logger.info(f"Экспорт данных завершен в {end_time}. Длительность: {duration}")
    
    return success

if __name__ == "__main__":
    if main():
        print("Экспорт данных успешно завершен")
    else:
        print("Экспорт данных завершен с ошибками. Смотрите лог для подробностей.") 