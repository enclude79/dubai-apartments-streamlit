import os
import psycopg2
from dotenv import load_dotenv
from datetime import datetime, timedelta

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

def reset_last_run_date():
    """Сбрасывает дату последнего обновления на неделю назад"""
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
        conn.autocommit = True
        print("Подключение успешно!")
        
        # Устанавливаем дату неделю назад
        week_ago = datetime.now() - timedelta(days=7)
        
        cur = conn.cursor()
        
        # Проверяем существование таблицы last_run_info
        cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'last_run_info');")
        table_exists = cur.fetchone()[0]
        
        if not table_exists:
            print("Таблица last_run_info не существует! Создаем...")
            cur.execute("""
                CREATE TABLE last_run_info (
                    id SERIAL PRIMARY KEY,
                    script_name VARCHAR(255) NOT NULL,
                    last_run TIMESTAMP,
                    last_updated_date TIMESTAMP,
                    status VARCHAR(50),
                    records_processed INTEGER,
                    UNIQUE(script_name)
                );
            """)
            print("Таблица last_run_info создана")
        
        # Получаем текущую дату
        cur.execute("SELECT last_updated_date FROM last_run_info WHERE script_name = 'api_to_sql';")
        result = cur.fetchone()
        
        if result:
            current_date = result[0]
            print(f"Текущая дата последнего обновления: {current_date}")
            
            # Обновляем дату на неделю назад
            cur.execute("""
                UPDATE last_run_info 
                SET last_updated_date = %s
                WHERE script_name = 'api_to_sql';
            """, (week_ago,))
            print(f"Дата обновлена на: {week_ago}")
        else:
            print("Запись api_to_sql не найдена в таблице last_run_info. Создаем новую...")
            cur.execute("""
                INSERT INTO last_run_info (script_name, last_run, last_updated_date, status, records_processed)
                VALUES ('api_to_sql', NOW(), %s, 'RESET', 0);
            """, (week_ago,))
            print(f"Создана новая запись с датой: {week_ago}")
        
        # Проверяем обновление
        cur.execute("SELECT last_updated_date FROM last_run_info WHERE script_name = 'api_to_sql';")
        new_date = cur.fetchone()[0]
        print(f"Проверка: новая дата последнего обновления: {new_date}")
        
        cur.close()
        conn.close()
        print("Операция успешно завершена!")
        
    except Exception as e:
        print(f"Ошибка: {e}")
    
    input("\nНажмите Enter для выхода...")

if __name__ == "__main__":
    reset_last_run_date() 