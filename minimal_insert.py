import os
import psycopg2
import time
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Параметры подключения к БД
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASSWORD', 'Enclude79'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'options': '-c statement_timeout=5000'  # Таймаут 5 секунд
}

def kill_all_hanging_queries():
    """Убивает все зависшие запросы INSERT в базе данных"""
    print("Поиск и завершение зависших запросов INSERT...")
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cur = conn.cursor()
        
        # Находим все активные запросы INSERT
        cur.execute("""
            SELECT pid FROM pg_stat_activity 
            WHERE query ILIKE '%INSERT INTO bayut_properties%' 
            AND state = 'active' 
            AND pid <> pg_backend_pid()
        """)
        
        pids = [row[0] for row in cur.fetchall()]
        
        if not pids:
            print("Зависших INSERT запросов не найдено")
            return 0
            
        print(f"Найдено {len(pids)} зависших INSERT запросов")
        
        # Завершаем каждый запрос
        for pid in pids:
            print(f"Завершение запроса с PID {pid}")
            cur.execute(f"SELECT pg_terminate_backend({pid})")
        
        return len(pids)
        
    except Exception as e:
        print(f"Ошибка при завершении зависших запросов: {e}")
        return 0
    finally:
        if 'cur' in locals() and cur:
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()

def minimal_insert():
    """Вставляет одну запись с жестко заданными значениями"""
    print("Вставка одной записи в базу данных...")
    
    # Жестко заданные значения для вставки
    test_id = int(time.time())  # Используем текущее время как уникальный ID
    test_title = "Test Property"
    test_price = 1000000
    
    conn = None
    cur = None
    
    try:
        # Завершаем все зависшие запросы перед выполнением
        kill_all_hanging_queries()
        
        print("Подключение к БД...")
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True  # Важно: включаем автокоммит
        cur = conn.cursor()
        print("Подключение к БД успешно.")
        
        # Простой запрос INSERT с минимумом данных
        query = "INSERT INTO bayut_properties (id, title, price) VALUES (%s, %s, %s)"
        values = (test_id, test_title, test_price)
        
        print(f"Выполнение INSERT: id={test_id}, title='{test_title}', price={test_price}")
        start_time = time.time()
        
        cur.execute(query, values)
        
        end_time = time.time()
        print(f"Запрос выполнен за {end_time - start_time:.2f} секунд")
        
        # Проверяем, что запись была вставлена
        cur.execute("SELECT * FROM bayut_properties WHERE id = %s", (test_id,))
        result = cur.fetchone()
        
        if result:
            print(f"Запись успешно вставлена: {result}")
            return True
        else:
            print("Запись не найдена после вставки")
            return False
        
    except Exception as e:
        print(f"Ошибка при вставке записи: {e}")
        return False
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        print("Соединение с БД закрыто")

if __name__ == "__main__":
    print("Запуск минимального теста вставки...")
    success = minimal_insert()
    print(f"Результат: {'успешно' if success else 'неудачно'}") 