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

# Параметры подключения к БД
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASSWORD', 'Enclude79'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432')
}

def get_api_data(limit=10):
    """Загружает данные из API Bayut (только id, title, price)"""
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
                print("Данные не найдены в API на странице {page}")
                break
            
            for item in hits:
                # Извлекаем только id, title, price
                title = item.get('title', '')
                # Убедимся, что title это строка и обрежем для безопасности
                title_str = str(title) if title is not None else ''
                
                property_data = {
                    'id': item.get('id'),
                    'title': title_str[:250], # Ограничиваем длину title
                    'price': item.get('price', 0)
                }
                
                # Проверка на None для id, так как это первичный ключ
                if property_data['id'] is None:
                    print(f"Пропущена запись из-за отсутствия ID: {item}")
                    continue

                properties.append(property_data)
                if len(properties) >= limit:
                    break
            
            print(f"Загружено {len(properties)} записей из API (всего {len(hits)} на странице {page})")
            
            if len(properties) >= limit or not hits: # Выходим, если достигли лимита или нет больше данных
                break
                
            page += 1
            time.sleep(1)  # Задержка между запросами
            
        except Exception as e:
            print(f"Ошибка при загрузке данных из API: {e}")
            break
    
    return properties

def insert_to_database(properties):
    """Вставляет данные в базу (только id, title, price)"""
    if not properties:
        print("Нет данных для вставки")
        return 0
    
    print(f"Начало вставки {len(properties)} записей в базу данных (только id, title, price)")
    start_time = time.time()
    
    conn = None
    cur = None
    
    try:
        print("Попытка подключения к БД...")
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cur = conn.cursor()
        print("Подключение к БД успешно.")

        # Проверяем наличие таблицы
        cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'bayut_properties');")
        if not cur.fetchone()[0]:
            print("Таблица bayut_properties не существует")
            return 0
        print("Таблица bayut_properties найдена.")
        
        inserted_count = 0
        for i, prop_data in enumerate(properties, 1):
            prop_id = prop_data.get('id')
            prop_title = prop_data.get('title')
            prop_price = prop_data.get('price')

            # Дополнительная проверка перед вставкой
            if prop_id is None:
                print(f"Пропуск записи {i} из-за отсутствия ID в подготовленных данных.")
                continue

            print(f"Подготовка к вставке записи {i}/{len(properties)}: ID={prop_id}, Title='{prop_title}', Price={prop_price}")
            
            try:
                query = "INSERT INTO bayut_properties (id, title, price) VALUES (%s, %s, %s) ON CONFLICT (id) DO NOTHING"
                values = (prop_id, prop_title, prop_price)
                
                print(f"Выполнение SQL: {query} с данными: {values}")
                cur.execute(query, values)
                
                # Проверим, была ли запись действительно вставлена или произошел конфликт
                if cur.rowcount > 0:
                    inserted_count += 1
                    print(f"Успешно вставлена запись {i} (ID: {prop_id}). Всего вставлено: {inserted_count}")
                else:
                    print(f"Запись {i} (ID: {prop_id}) уже существует или не вставлена (rowcount: {cur.rowcount}).")

            except Exception as e_insert:
                print(f"Ошибка при вставке записи {i} (ID: {prop_id}): {e_insert}")
        
        print("Проверка общего количества записей после цикла вставки...")
        cur.execute("SELECT COUNT(*) FROM bayut_properties")
        total_records = cur.fetchone()[0]
        
        total_time_taken = time.time() - start_time
        print(f"Вставка завершена: {inserted_count} новых записей за {total_time_taken:.2f} секунд.")
        print(f"Всего записей в таблице: {total_records}")
        
        return inserted_count
        
    except Exception as e_main:
        print(f"Критическая ошибка при работе с БД: {e_main}")
        return 0
    finally:
        if cur:
            print("Закрытие курсора...")
            cur.close()
        if conn:
            print("Закрытие соединения с БД...")
            conn.close()
        print("Функция insert_to_database завершена.")

def main():
    parser = argparse.ArgumentParser(description="Быстрая загрузка данных из API Bayut в PostgreSQL (упрощенная версия)")
    parser.add_argument('--limit', type=int, default=2, help='Количество записей для загрузки из API')
    args = parser.parse_args()
    
    print(f"Запуск скрипта с лимитом: {args.limit}")
    
    properties_data = get_api_data(args.limit)
    
    if not properties_data:
        print("Не удалось получить данные из API или список пуст.")
        return
    
    print(f"Получено {len(properties_data)} записей из API. Начало вставки в БД...")
    inserted_records = insert_to_database(properties_data)
    
    print(f"Итого: успешно вставлено {inserted_records} из {len(properties_data)} полученных записей.")

if __name__ == "__main__":
    main() 