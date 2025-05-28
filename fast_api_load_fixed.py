import os
import requests
import psycopg2
import time
import argparse
from datetime import datetime
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Параметры API
RAPIDAPI_KEY = os.getenv('RAPIDAPI_KEY')
API_CONFIG = {
    "url": "https://bayut.p.rapidapi.com/properties/list",
    "headers": {
        "X-RapidAPI-Key": RAPIDAPI_KEY,
        "X-RapidAPI-Host": "bayut.p.rapidapi.com"
    },
    "params": {
        "locationExternalIDs": "5002,6020",
        "purpose": "for-sale",
        "hitsPerPage": "25",
        "sort": "date-desc",
        "categoryExternalID": "4"
    }
}

# Параметры подключения к БД с таймаутом
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASSWORD', 'Enclude79'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'options': '-c statement_timeout=10000'  # Установка таймаута запросов в 10 секунд
}

def kill_all_hanging_queries():
    """Убивает все зависшие запросы в базе данных"""
    print("Поиск и завершение зависших запросов INSERT...")
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cur = conn.cursor()
        
        # Находим все активные запросы INSERT, которые висят более 1 минуты
        cur.execute("""
            SELECT pid, query_start, query 
            FROM pg_stat_activity 
            WHERE state = 'active' 
            AND query_start < NOW() - INTERVAL '1 minute'
            AND query ILIKE '%INSERT INTO bayut_properties%'
            AND pid <> pg_backend_pid()
        """)
        
        hanging_queries = cur.fetchall()
        killed_count = 0
        
        print(f"Найдено {len(hanging_queries)} зависших запросов INSERT")
        
        # Завершаем каждый зависший запрос
        for pid, start_time, query in hanging_queries:
            print(f"Завершение запроса (PID: {pid}, запущен: {start_time})")
            print(f"Запрос: {query[:100]}...")
            
            # Пытаемся сначала просто отменить запрос
            cur.execute(f"SELECT pg_cancel_backend({pid})")
            time.sleep(0.5)  # Даем немного времени на отмену
            
            # Проверяем, все еще активен ли запрос
            cur.execute(f"SELECT 1 FROM pg_stat_activity WHERE pid = {pid} AND state = 'active'")
            if cur.fetchone():
                # Если запрос все еще активен, применяем жесткое завершение
                print(f"Запрос не удалось отменить, выполняем принудительное завершение (PID: {pid})")
                cur.execute(f"SELECT pg_terminate_backend({pid})")
            
            killed_count += 1
        
        print(f"Завершено {killed_count} зависших запросов")
        
        # Также удаляем все запросы, которые могут быть в состоянии idle in transaction
        cur.execute("""
            SELECT pid, query_start, query 
            FROM pg_stat_activity 
            WHERE state = 'idle in transaction' 
            AND query_start < NOW() - INTERVAL '1 minute'
            AND query ILIKE '%INSERT INTO bayut_properties%'
            AND pid <> pg_backend_pid()
        """)
        
        idle_transactions = cur.fetchall()
        idle_killed = 0
        
        print(f"Найдено {len(idle_transactions)} зависших транзакций")
        
        for pid, start_time, query in idle_transactions:
            print(f"Завершение транзакции (PID: {pid}, запущена: {start_time})")
            cur.execute(f"SELECT pg_terminate_backend({pid})")
            idle_killed += 1
        
        print(f"Завершено {idle_killed} зависших транзакций")
        
        return killed_count + idle_killed
        
    except Exception as e:
        print(f"Ошибка при попытке завершить зависшие запросы: {e}")
        return 0
    finally:
        if 'cur' in locals() and cur:
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()

def get_api_data(limit=10):
    """Загружает данные из API Bayut (только id)"""
    print(f"Загрузка данных из API (лимит: {limit} записей)")
    
    properties = []
    page = 1
    
    while len(properties) < limit:
        print(f"Загрузка страницы {page} из API")
        
        params = API_CONFIG["params"].copy()
        params["page"] = str(page)
        
        try:
            response = requests.get(
                API_CONFIG["url"],
                headers=API_CONFIG["headers"],
                params=params,
                timeout=30
            )
            response.raise_for_status()
            
            data = response.json()
            hits = data.get('hits', [])
            
            if not hits:
                print("Данные не найдены в API")
                break
            
            for item in hits:
                # Извлекаем ТОЛЬКО id из API, остальные данные будут фиксированными
                property_id = item.get('id')
                if property_id is not None:
                    properties.append({'id': property_id})
                    if len(properties) >= limit:
                        break
            
            print(f"Загружено {len(properties)} записей из API")
            
            if len(properties) >= limit:
                break
                
            page += 1
            time.sleep(1)  # Задержка между запросами
            
        except Exception as e:
            print(f"Ошибка при загрузке данных из API: {e}")
            break
    
    return properties

def super_simple_insert(properties):
    """Максимально простая вставка данных с хардкодированными значениями кроме id"""
    if not properties:
        print("Нет данных для вставки")
        return 0
    
    print(f"Начало вставки {len(properties)} записей в базу данных")
    start_time = time.time()
    
    conn = None
    cur = None
    
    inserted_count = 0
    
    try:
        print("Подключение к БД...")
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True  # Автоматический коммит
        cur = conn.cursor()
        print("Подключение к БД успешно.")
        
        # Проверяем общее количество записей в таблице
        cur.execute("SELECT COUNT(*) FROM bayut_properties")
        total_before = cur.fetchone()[0]
        print(f"Исходное количество записей в таблице: {total_before}")
        
        # Вставляем каждую запись с ФИКСИРОВАННЫМИ ДАННЫМИ (кроме id)
        for i, prop in enumerate(properties, 1):
            prop_id = prop.get('id')
            
            if prop_id is None:
                print(f"Пропуск записи {i} - отсутствует ID")
                continue
                
            try:
                # Используем максимально простой запрос с ФИКСИРОВАННЫМИ значениями для title и price
                query = "INSERT INTO bayut_properties (id, title, price) VALUES (%s, %s, %s) ON CONFLICT (id) DO NOTHING"
                values = (prop_id, f"Test Property {i}", 1000000)
                
                print(f"Вставка записи {i}/{len(properties)} (ID: {prop_id})...")
                cur.execute(query, values)
                
                if cur.rowcount > 0:
                    inserted_count += 1
                    print(f"Запись {i} успешно вставлена.")
                else:
                    print(f"Запись {i} не вставлена (возможно, уже существует).")
                    
            except Exception as e:
                print(f"Ошибка при вставке записи {i}: {e}")
        
        # Проверяем новое количество записей
        cur.execute("SELECT COUNT(*) FROM bayut_properties")
        total_after = cur.fetchone()[0]
        
        total_time = time.time() - start_time
        print(f"Вставка завершена за {total_time:.2f} секунд.")
        print(f"Количество записей до: {total_before}, после: {total_after}")
        print(f"Успешно вставлено: {inserted_count} записей")
        
        return inserted_count
        
    except Exception as e:
        print(f"Критическая ошибка при работе с БД: {e}")
        return 0
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        print("Соединение с БД закрыто")

def kill_all_hanging_inserts():
    """Завершает все висящие INSERT запросы"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cur = conn.cursor()
        
        # Находим все INSERT запросы, которые висят
        cur.execute("""
            SELECT pid FROM pg_stat_activity 
            WHERE query ILIKE '%INSERT%bayut_properties%' 
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

def main():
    parser = argparse.ArgumentParser(description="Максимально упрощенная вставка данных в PostgreSQL")
    parser.add_argument('--limit', type=int, default=2, help='Количество записей для загрузки из API')
    parser.add_argument('--check-db', action='store_true', help='Только проверить состояние БД')
    parser.add_argument('--kill-pid', type=int, help='Завершить запрос с указанным PID')
    parser.add_argument('--kill-all', action='store_true', help='Завершить все зависшие запросы')
    args = parser.parse_args()
    
    if args.check_db:
        print("Проверка состояния БД...")
        killed = kill_all_hanging_queries()
        return
        
    if args.kill_pid:
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(f"SELECT pg_terminate_backend({args.kill_pid})")
            result = cur.fetchone()[0]
            print(f"Завершение запроса с PID {args.kill_pid}: {'успешно' if result else 'не удалось'}")
            cur.close()
            conn.close()
        except Exception as e:
            print(f"Ошибка при завершении запроса: {e}")
        return
        
    if args.kill_all:
        print("Завершение всех зависших запросов...")
        killed = kill_all_hanging_queries()
        print(f"Всего завершено {killed} зависших запросов")
        return
    
    print(f"Запуск с лимитом: {args.limit}")
    
    # Завершаем все зависшие запросы перед выполнением
    print("Очистка зависших запросов перед выполнением...")
    killed = kill_all_hanging_queries()
    print(f"Завершено {killed} зависших запросов")
    
    # Получаем данные из API
    properties = get_api_data(args.limit)
    
    if not properties:
        print("Не удалось получить данные из API")
        return
    
    # Вставляем данные в базу
    inserted = super_simple_insert(properties)
    
    print(f"Итого: успешно вставлено {inserted} из {len(properties)} записей")

if __name__ == "__main__":
    main() 