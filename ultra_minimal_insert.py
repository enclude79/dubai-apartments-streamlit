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
    'options': '-c statement_timeout=10000'  # Таймаут 10 секунд на запрос
}

def one_shot_insert():
    """Вставляет одну единственную запись с максимальным логированием"""
    print("=== НАЧАЛО УЛЬТРА-МИНИМАЛЬНОЙ ВСТАВКИ ===")
    
    conn = None
    cur = None
    
    # Жестко заданные значения для вставки
    # Используем timestamp + случайное число, чтобы ID был почти всегда уникальным
    test_id = int(time.time() * 1000) + int(time.time() % 1000) 
    test_title = f"ULTRA_TEST_{test_id}"
    test_price = 555555
    
    try:
        print(f"[DB_CONFIG]: {DB_CONFIG}")
        print("1. Попытка подключения к PostgreSQL...")
        conn = psycopg2.connect(**DB_CONFIG)
        print("2. Соединение установлено.")
        
        conn.autocommit = True
        print("3. Autocommit включен.")
        
        cur = conn.cursor()
        print("4. Курсор создан.")
        
        print("5. Получение количества записей ДО вставки...")
        cur.execute("SELECT COUNT(*) FROM bayut_properties")
        count_before = cur.fetchone()[0]
        print(f"   [INFO] Записей ДО: {count_before}")
        
        query = "INSERT INTO bayut_properties (id, title, price) VALUES (%s, %s, %s)"
        values = (test_id, test_title, test_price)
        
        print(f"6. Подготовка к выполнению запроса: {query} с значениями {values}")
        
        start_time_execute = time.time()
        print("7. Выполнение cur.execute()...")
        cur.execute(query, values)
        print("8. cur.execute() ЗАВЕРШЕН.")
        duration_execute = time.time() - start_time_execute
        print(f"   [INFO] Время выполнения cur.execute(): {duration_execute:.4f} сек.")
        
        # Проверка rowcount
        print(f"9. Проверка cur.rowcount: {cur.rowcount}")
        if cur.rowcount > 0:
            print("   [SUCCESS] Запись УСПЕШНО добавлена (судя по rowcount).")
        else:
            print("   [WARNING] Запись НЕ добавлена (rowcount = 0). Возможно, конфликт ID или другая проблема.")

        print("10. Получение количества записей ПОСЛЕ вставки...")
        cur.execute("SELECT COUNT(*) FROM bayut_properties")
        count_after = cur.fetchone()[0]
        print(f"   [INFO] Записей ПОСЛЕ: {count_after}")
        
        print("11. Проверка фактического наличия записи в БД...")
        cur.execute("SELECT id FROM bayut_properties WHERE id = %s", (test_id,))
        result_check = cur.fetchone()
        
        if result_check:
            print(f"   [SUCCESS] Запись с ID {test_id} НАЙДЕНА в БД!")
        else:
            print(f"   [ERROR] Запись с ID {test_id} НЕ НАЙДЕНА в БД после попытки вставки!")
            
        if count_after > count_before:
            print("[ИТОГ]: УСПЕХ! Количество записей увеличилось.")
            return True
        else:
            print("[ИТОГ]: НЕУДАЧА! Количество записей не изменилось или уменьшилось.")
            return False
            
    except psycopg2.Error as db_err:
        print(f"[DATABASE ERROR]: {db_err}")
        if conn:
            print(f"   [DB_STATUS] Статус соединения: {conn.status}")
            print(f"   [DB_DSN] DSN параметры: {conn.dsn}")
        return False
    except Exception as e:
        print(f"[UNEXPECTED ERROR]: {e}")
        return False
    finally:
        print("12. Блок finally. Закрытие курсора и соединения...")
        if cur:
            cur.close()
            print("   [INFO] Курсор закрыт.")
        if conn:
            conn.close()
            print("   [INFO] Соединение закрыто.")
        print("=== ЗАВЕРШЕНИЕ УЛЬТРА-МИНИМАЛЬНОЙ ВСТАВКИ ===")

if __name__ == "__main__":
    success = one_shot_insert()
    print(f"\nФинальный результат скрипта: {'УСПЕШНО' if success else 'НЕУДАЧНО'}") 