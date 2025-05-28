import os
import psycopg2
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Параметры базы данных
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASSWORD', 'Enclude79'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
}

def check_dates():
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
        cur = conn.cursor()
        
        # Проверяем максимальную дату в bayut_properties
        print("\n--- Проверка максимальной даты в таблице bayut_properties ---")
        cur.execute("SELECT MAX(updated_at) FROM bayut_properties;")
        max_date = cur.fetchone()[0]
        print(f"Максимальная дата обновления в bayut_properties: {max_date}")
        
        # Проверяем существование таблицы last_run_info
        print("\n--- Проверка таблицы last_run_info ---")
        cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'last_run_info');")
        table_exists = cur.fetchone()[0]
        print(f"Таблица last_run_info существует: {table_exists}")
        
        if table_exists:
            # Проверяем записи в last_run_info
            cur.execute("SELECT script_name, last_run, last_updated_date, status FROM last_run_info;")
            rows = cur.fetchall()
            if rows:
                print("\nЗаписи в таблице last_run_info:")
                for row in rows:
                    print(f"Скрипт: {row[0]}, Последний запуск: {row[1]}, Последняя дата обновления: {row[2]}, Статус: {row[3]}")
            else:
                print("Таблица last_run_info пуста")
        
        # Получаем 5 последних записей из bayut_properties
        print("\n--- 5 последних добавленных/обновленных записей в bayut_properties ---")
        cur.execute("SELECT id, title, updated_at FROM bayut_properties ORDER BY updated_at DESC LIMIT 5;")
        latest_records = cur.fetchall()
        for record in latest_records:
            print(f"ID: {record[0]}, Обновлено: {record[2]}, Заголовок: {record[1]}")
        
        cur.close()
        conn.close()
        print("\nПроверка завершена.")
        
    except Exception as e:
        print(f"Ошибка при проверке: {e}")

if __name__ == "__main__":
    check_dates() 