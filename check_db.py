import os
import psycopg2
from dotenv import load_dotenv
import time

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

def check_database():
    """Проверяет базу данных на наличие данных и выводит статистику"""
    print("Проверка базы данных...")
    
    try:
        # Подключаемся к базе данных
        print("Подключение к базе данных...")
        conn = psycopg2.connect(
            dbname=DB_CONFIG['dbname'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port']
        )
        print("Подключение успешно!")
        
        cur = conn.cursor()
        
        # Проверка таблицы bayut_properties
        print("\n=== Проверка таблицы bayut_properties ===")
        cur.execute("SELECT COUNT(*) FROM bayut_properties")
        count = cur.fetchone()[0]
        print(f"Общее количество записей в таблице: {count}")
        
        # Проверка таблицы last_run_info
        print("\n=== Проверка таблицы last_run_info ===")
        cur.execute("SELECT script_name, last_run, last_updated_date FROM last_run_info")
        rows = cur.fetchall()
        for row in rows:
            print(f"Скрипт: {row[0]}, Последний запуск: {row[1]}, Последняя дата обновления: {row[2]}")
        
        # Проверка последних 5 записей
        print("\n=== Последние 5 записей в таблице bayut_properties ===")
        cur.execute("SELECT id, title, price, updated_at FROM bayut_properties ORDER BY updated_at DESC LIMIT 5")
        rows = cur.fetchall()
        for row in rows:
            print(f"ID: {row[0]}, Обновлено: {row[3]}, Цена: {row[2]}, Название: {row[1]}")
        
        cur.close()
        conn.close()
        print("\nПроверка завершена!")
        
    except Exception as e:
        print(f"Ошибка при проверке базы данных: {e}")
    
    # Ожидание ввода пользователя, чтобы окно не закрывалось
    input("\nНажмите Enter для выхода...")

if __name__ == "__main__":
    check_database() 