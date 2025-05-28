import os
import psycopg2
import time
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Параметры подключения с оптимальными настройками
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASSWORD', 'Enclude79'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'options': '-c statement_timeout=5000'  # Таймаут 5 секунд
}

def test_insert():
    """Тестовая вставка данных с оптимальными настройками"""
    print("Начало тестовой вставки...")
    start_time = time.time()
    
    try:
        # ВАЖНО: устанавливаем autocommit=True при создании соединения
        connection = psycopg2.connect(**DB_CONFIG)
        connection.autocommit = True  # Автоматический коммит каждой операции
        
        cursor = connection.cursor()
        
        # Простой тестовый запрос без сложной логики
        test_id = int(time.time())
        query = "INSERT INTO bayut_properties (id, title, price) VALUES (%s, %s, %s)"
        data = (test_id, f"Test {test_id}", 1000000)
        
        print(f"Выполняю запрос...")
        cursor.execute(query, data)
        print(f"Запрос выполнен за {time.time() - start_time:.2f} секунд")
        
        # Проверяем, что запись существует
        cursor.execute("SELECT COUNT(*) FROM bayut_properties WHERE id = %s", (test_id,))
        count = cursor.fetchone()[0]
        print(f"Найдено записей с ID {test_id}: {count}")
        
        # Проверяем общее количество записей
        cursor.execute("SELECT COUNT(*) FROM bayut_properties")
        total = cursor.fetchone()[0]
        print(f"Всего записей в таблице: {total}")
        
        return True
    except Exception as e:
        print(f"Ошибка: {e}")
        return False
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection:
            connection.close()
        print(f"Тест завершен за {time.time() - start_time:.2f} секунд")

if __name__ == "__main__":
    test_insert() 