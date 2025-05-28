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
    'port': os.getenv('DB_PORT', '5432')
}

def batch_test(num_records=5):
    """Выполняет тестовую вставку нескольких записей подряд"""
    print(f"Тестирование вставки {num_records} записей...")
    
    conn = None
    cur = None
    
    try:
        print("Подключение к БД...")
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True  # Важно: включаем автокоммит
        cur = conn.cursor()
        print("Подключение к БД успешно.")
        
        # Проверяем количество записей до теста
        cur.execute("SELECT COUNT(*) FROM bayut_properties")
        count_before = cur.fetchone()[0]
        print(f"Количество записей до теста: {count_before}")
        
        # Выполняем несколько вставок подряд
        total_time = 0
        for i in range(num_records):
            # Генерируем уникальный ID
            test_id = int(time.time()) + i + 1000
            test_title = f"TEST_BATCH_{i+1}"
            test_price = 1000000 + (i * 10000)
            
            start_time = time.time()
            print(f"Вставка записи {i+1}/{num_records}: id={test_id}, title='{test_title}', price={test_price}")
            
            cur.execute(
                "INSERT INTO bayut_properties (id, title, price) VALUES (%s, %s, %s)",
                (test_id, test_title, test_price)
            )
            
            end_time = time.time()
            duration = end_time - start_time
            total_time += duration
            print(f"  → Запись {i+1} вставлена за {duration:.4f} секунд")
            
            # Проверяем, что запись существует
            cur.execute("SELECT COUNT(*) FROM bayut_properties WHERE id = %s", (test_id,))
            count = cur.fetchone()[0]
            if count > 0:
                print(f"  ✓ Запись с ID {test_id} подтверждена в БД")
            else:
                print(f"  ✗ Запись с ID {test_id} не найдена в БД!")
            
            # Небольшая задержка между вставками для наблюдения
            time.sleep(0.2)
        
        # Проверяем количество записей после теста
        cur.execute("SELECT COUNT(*) FROM bayut_properties")
        count_after = cur.fetchone()[0]
        
        print(f"\nИТОГИ ТЕСТА:")
        print(f"Количество записей до: {count_before}, после: {count_after}")
        print(f"Добавлено записей: {count_after - count_before}")
        print(f"Общее время вставки: {total_time:.4f} секунд")
        print(f"Среднее время на запись: {total_time/num_records:.4f} секунд")
        print(f"Производительность: {num_records/total_time:.2f} записей/секунду")
        
        return True
    
    except Exception as e:
        print(f"Ошибка при выполнении теста: {e}")
        return False
    
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        print("Соединение с БД закрыто")

if __name__ == "__main__":
    batch_test(10)  # Тестируем вставку 10 записей 