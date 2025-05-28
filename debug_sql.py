import os
import requests
import json
import psycopg2
import psycopg2.extras
import argparse
import time
import logging
import traceback
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Настройка логирования
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = f'{log_dir}/debug_sql_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

logging.basicConfig(
    level=logging.DEBUG,  # Устанавливаем DEBUG уровень для более подробных логов
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

def debug_insert(property_id=8514211):
    """Тестовая функция для отладки вставки в БД с фиксированными данными"""
    logger.info(f"Начало тестовой вставки для ID {property_id}")
    print(f"Начало тестовой вставки для ID {property_id}")
    
    # Создаем тестовые данные
    test_data = {
        'id': property_id,
        'title': f'Тестовый объект {property_id}',
        'price': 1000000,
        'rooms': 2,
        'baths': 2,
        'area': 100,
        'location': 'Test Location',
        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    connection = None
    cursor = None
    
    try:
        logger.debug("Попытка подключения к базе данных...")
        print("Попытка подключения к базе данных...")
        start_time = time.time()
        
        connection = psycopg2.connect(**DB_CONFIG, connect_timeout=5)
        
        conn_time = time.time() - start_time
        logger.debug(f"Соединение установлено за {conn_time:.2f} секунд")
        print(f"Соединение установлено за {conn_time:.2f} секунд")
        
        connection.autocommit = False
        cursor = connection.cursor()
        
        # Получаем ключи и значения для вставки
        keys = test_data.keys()
        values = [test_data[key] for key in keys]
        
        # Создаем строку колонок и плейсхолдеров
        columns = ', '.join(keys)
        placeholders = ', '.join(['%s'] * len(keys))
        
        # Простой базовый запрос без ON CONFLICT
        query = f"INSERT INTO bayut_properties ({columns}) VALUES ({placeholders}) ON CONFLICT (id) DO NOTHING"
        
        logger.debug(f"Подготовлен SQL-запрос: {query}")
        print(f"Подготовлен SQL-запрос (длина: {len(query)} символов)")
        
        # Засекаем время выполнения запроса
        query_start = time.time()
        
        # Выполняем запрос
        logger.debug("Выполнение запроса...")
        print("Выполнение запроса...")
        cursor.execute(query, values)
        
        query_time = time.time() - query_start
        logger.debug(f"Запрос выполнен за {query_time:.2f} секунд")
        print(f"Запрос выполнен за {query_time:.2f} секунд")
        
        # Засекаем время коммита
        commit_start = time.time()
        
        # Подтверждаем транзакцию
        logger.debug("Выполнение COMMIT...")
        print("Выполнение COMMIT...")
        connection.commit()
        
        commit_time = time.time() - commit_start
        logger.debug(f"COMMIT выполнен за {commit_time:.2f} секунд")
        print(f"COMMIT выполнен за {commit_time:.2f} секунд")
        
        total_time = time.time() - start_time
        logger.info(f"Запись с ID {property_id} успешно вставлена за {total_time:.2f} секунд")
        print(f"Запись с ID {property_id} успешно вставлена за {total_time:.2f} секунд")
        
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при вставке записи: {e}")
        logger.error(traceback.format_exc())
        print(f"Ошибка при вставке записи: {e}")
        if connection:
            connection.rollback()
        return False
        
    finally:
        logger.debug("Закрытие соединения...")
        print("Закрытие соединения...")
        close_start = time.time()
        
        if cursor:
            cursor.close()
        if connection:
            connection.close()
            
        close_time = time.time() - close_start
        logger.debug(f"Соединение закрыто за {close_time:.2f} секунд")
        print(f"Соединение закрыто за {close_time:.2f} секунд")
        logger.info("Тестовая вставка завершена")
        print("Тестовая вставка завершена")

def test_connection():
    """Проверка соединения с базой данных"""
    logger.info("Проверка соединения с базой данных")
    print("Проверка соединения с базой данных")
    
    try:
        start_time = time.time()
        connection = psycopg2.connect(**DB_CONFIG, connect_timeout=5)
        
        conn_time = time.time() - start_time
        logger.info(f"Соединение установлено за {conn_time:.2f} секунд")
        print(f"Соединение установлено за {conn_time:.2f} секунд")
        
        cursor = connection.cursor()
        
        # Выполняем простой запрос
        logger.debug("Выполнение SELECT 1...")
        print("Выполнение SELECT 1...")
        
        query_start = time.time()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        
        query_time = time.time() - query_start
        logger.info(f"Запрос выполнен за {query_time:.2f} секунд, результат: {result}")
        print(f"Запрос выполнен за {query_time:.2f} секунд, результат: {result}")
        
        # Проверяем таблицу
        logger.debug("Проверка таблицы bayut_properties...")
        print("Проверка таблицы bayut_properties...")
        
        cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'bayut_properties');")
        table_exists = cursor.fetchone()[0]
        
        logger.info(f"Таблица bayut_properties существует: {table_exists}")
        print(f"Таблица bayut_properties существует: {table_exists}")
        
        if table_exists:
            # Проверяем количество записей
            cursor.execute("SELECT COUNT(*) FROM bayut_properties")
            count = cursor.fetchone()[0]
            logger.info(f"Количество записей в таблице: {count}")
            print(f"Количество записей в таблице: {count}")
        
        cursor.close()
        connection.close()
        logger.info("Проверка соединения завершена успешно")
        print("Проверка соединения завершена успешно")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при проверке соединения: {e}")
        logger.error(traceback.format_exc())
        print(f"Ошибка при проверке соединения: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Отладочный скрипт для PostgreSQL")
    parser.add_argument('--test-connection', action='store_true', help='Проверить соединение с базой данных')
    parser.add_argument('--test-insert', action='store_true', help='Протестировать вставку в базу данных')
    parser.add_argument('--id', type=int, default=8514211, help='ID для тестовой вставки')
    args = parser.parse_args()
    
    if args.test_connection:
        test_connection()
    
    if args.test_insert:
        debug_insert(args.id)
    
    if not args.test_connection and not args.test_insert:
        # Если не указаны параметры, выполняем обе проверки
        test_connection()
        debug_insert()

if __name__ == "__main__":
    main() 