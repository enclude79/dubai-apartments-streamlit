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

def test_db_minimal():
    """Минимальный тест для проверки записи в базу данных"""
    print("Начинаем минимальный тест базы данных")

    # ТЕСТ 1: Подключение и проверка таблиц
    print("\n=== ТЕСТ 1: Подключение к базе данных и проверка таблиц ===")
    try:
        # Подключаемся к базе данных
        print("Подключение к базе данных...")
        conn = psycopg2.connect(
            dbname=DB_CONFIG['dbname'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            connect_timeout=10
        )
        print("Подключение успешно!")

        # Создаем курсор
        cur = conn.cursor()
        
        # Проверяем существование таблицы bayut_properties
        print("\nПроверка существования таблицы bayut_properties...")
        try:
            cur.execute("SELECT COUNT(*) FROM bayut_properties LIMIT 1")
            count = cur.fetchone()[0]
            print(f"Количество записей в таблице bayut_properties: {count}")
        except Exception as e:
            print(f"Ошибка при проверке таблицы: {e}")
        
        # Проверяем существование таблицы last_run_info
        print("\nПроверка существования таблицы last_run_info...")
        try:
            cur.execute("SELECT COUNT(*) FROM last_run_info")
            count = cur.fetchone()[0]
            print(f"Количество записей в таблице last_run_info: {count}")
        except Exception as e:
            print(f"Ошибка при проверке таблицы last_run_info: {e}")
        
        # Закрываем первое соединение
        cur.close()
        conn.close()
        print("\nПервое соединение закрыто.")
    except Exception as e:
        print(f"Ошибка при подключении к базе данных: {e}")
    
    # ТЕСТ 2: Создание тестовой таблицы, вставка и удаление
    print("\n=== ТЕСТ 2: Создание тестовой таблицы и вставка данных ===")
    try:
        conn2 = psycopg2.connect(
            dbname=DB_CONFIG['dbname'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            connect_timeout=10
        )
        conn2.autocommit = True  # Устанавливаем autocommit сразу
        
        cur2 = conn2.cursor()
        
        # Создаем тестовую таблицу
        print("Создание тестовой таблицы...")
        try:
            cur2.execute("DROP TABLE IF EXISTS test_table")
            cur2.execute("CREATE TABLE test_table (id INT PRIMARY KEY, value TEXT)")
            print("Тестовая таблица создана успешно")
            
            # Вставляем данные
            print("Вставка данных в тестовую таблицу...")
            cur2.execute("INSERT INTO test_table VALUES (1, 'test_value')")
            print("Данные успешно вставлены")
            
            # Проверяем вставку
            cur2.execute("SELECT * FROM test_table")
            result = cur2.fetchone()
            print(f"Прочитанные данные: {result}")
            
            # Удаляем тестовую таблицу
            cur2.execute("DROP TABLE IF EXISTS test_table")
            print("Тестовая таблица удалена")
        except Exception as e:
            print(f"Ошибка при работе с тестовой таблицей: {e}")
        
        cur2.close()
        conn2.close()
        print("Второе соединение закрыто.")
    except Exception as e:
        print(f"Ошибка при втором подключении: {e}")
    
    # ТЕСТ 3: Вставка в bayut_properties с новым соединением и autocommit=True
    print("\n=== ТЕСТ 3: Вставка в bayut_properties ===")
    try:
        # Новое соединение с autocommit=True
        conn3 = psycopg2.connect(
            dbname=DB_CONFIG['dbname'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            connect_timeout=10
        )
        conn3.autocommit = True  # Устанавливаем autocommit сразу
        
        cur3 = conn3.cursor()
        
        # Проверяем поля таблицы
        print("Проверка структуры таблицы bayut_properties...")
        cur3.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'bayut_properties'
            LIMIT 5
        """)
        columns = cur3.fetchall()
        print(f"Первые 5 столбцов таблицы: {columns}")
        
        # Пробуем вставить тестовые данные
        print("\nВставка тестовых данных в bayut_properties...")
        try:
            test_id = 99999999  # Используем очень большой ID, который не должен существовать
            
            # Проверяем существование записи
            cur3.execute("SELECT COUNT(*) FROM bayut_properties WHERE id = %s", (test_id,))
            count = cur3.fetchone()[0]
            if count > 0:
                print(f"Запись с ID {test_id} уже существует, удаляем её перед тестом")
                cur3.execute("DELETE FROM bayut_properties WHERE id = %s", (test_id,))
            
            # Вставляем тестовую запись
            cur3.execute("""
                INSERT INTO bayut_properties (id, title, price) 
                VALUES (%s, 'Test Property', 100000)
            """, (test_id,))
            print("Тестовая запись успешно вставлена")
            
            # Проверяем вставку
            cur3.execute("SELECT id, title, price FROM bayut_properties WHERE id = %s", (test_id,))
            result = cur3.fetchone()
            print(f"Прочитанные данные: {result}")
            
            # Обновляем запись
            cur3.execute("""
                UPDATE bayut_properties 
                SET title = 'Test Property Updated', price = 200000
                WHERE id = %s
            """, (test_id,))
            print("Тестовая запись успешно обновлена")
            
            # Проверяем обновление
            cur3.execute("SELECT id, title, price FROM bayut_properties WHERE id = %s", (test_id,))
            result = cur3.fetchone()
            print(f"Обновлённые данные: {result}")
            
            # Удаляем тестовую запись
            cur3.execute("DELETE FROM bayut_properties WHERE id = %s", (test_id,))
            print("Тестовая запись удалена")
        except Exception as e:
            print(f"Ошибка при работе с bayut_properties: {e}")
        
        cur3.close()
        conn3.close()
        print("Третье соединение закрыто.")
    except Exception as e:
        print(f"Ошибка при третьем подключении: {e}")
    
    print("\n=== Тесты базы данных завершены ===")

if __name__ == "__main__":
    test_db_minimal() 