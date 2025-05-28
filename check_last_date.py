import os
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

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

def check_last_dates():
    """Проверяет последние даты в базе данных"""
    try:
        # Подключаемся к базе данных
        conn = psycopg2.connect(
            dbname=DB_CONFIG['dbname'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port']
        )
        cur = conn.cursor()
        
        # Получаем последнюю дату обновления
        print("\n--- Последняя дата обновления (updated_at) ---")
        cur.execute("SELECT MAX(updated_at) FROM bayut_properties;")
        last_updated = cur.fetchone()[0]
        print(f"MAX(updated_at): {last_updated}")

        # Получаем последнюю дату создания
        print("\n--- Последняя дата создания (created_at) ---")
        cur.execute("SELECT MAX(created_at) FROM bayut_properties;")
        last_created = cur.fetchone()[0]
        print(f"MAX(created_at): {last_created}")
        
        # Получаем записи с самыми новыми датами
        print("\n--- 5 самых недавно обновленных записей ---")
        cur.execute("SELECT id, title, updated_at, created_at FROM bayut_properties ORDER BY updated_at DESC LIMIT 5;")
        recent_records = cur.fetchall()
        for record in recent_records:
            print(f"ID: {record[0]}, Название: {record[1][:30]}..., Обновлено: {record[2]}, Создано: {record[3]}")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"Ошибка при проверке дат: {e}")

if __name__ == "__main__":
    check_last_dates() 