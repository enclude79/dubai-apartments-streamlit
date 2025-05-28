import os
import psycopg2
import time
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Упрощенные параметры подключения без лишних опций
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASSWORD', 'Enclude79'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432')
}

def fast_insert():
    """Максимально быстрая вставка данных"""
    print("Начало супер-быстрой вставки...")
    start_time = time.time()
    
    # Создаем соединение с автокоммитом
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()
    
    # Генерируем уникальный ID
    test_id = int(time.time()) + 1000
    
    try:
        # Самый простой INSERT без сложной логики
        print(f"Вставка записи с ID {test_id}...")
        cur.execute(
            "INSERT INTO bayut_properties (id, title, price) VALUES (%s, %s, %s)",
            (test_id, f"Супер-тест {test_id}", 5000000)
        )
        
        # Проверяем, что запись действительно добавлена
        cur.execute("SELECT COUNT(*) FROM bayut_properties")
        total = cur.fetchone()[0]
        
        print(f"Запись вставлена за {time.time() - start_time:.2f} секунд")
        print(f"Всего записей в таблице: {total}")
        
        return True
    except Exception as e:
        print(f"Ошибка: {e}")
        return False
    finally:
        # Закрываем соединение
        cur.close()
        conn.close()
        print(f"Тест завершен за {time.time() - start_time:.2f} секунд")

if __name__ == "__main__":
    print("Начало выполнения супер-быстрого теста...")
    fast_insert()
    print("Тест завершен!") 