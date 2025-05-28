import os
import requests
import json
import psycopg2
import psycopg2.extras
import argparse
import time
import logging
import re
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Настройка логирования
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = f'{log_dir}/fix_api_final_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Загружаем переменные окружения
load_dotenv()

# Проверяем наличие API ключа
RAPIDAPI_KEY = os.getenv('RAPIDAPI_KEY')
if not RAPIDAPI_KEY:
    logger.error("RAPIDAPI_KEY не найден в переменных окружения")
    print("Ошибка: RAPIDAPI_KEY не найден в переменных окружения")
    exit(1)
else:
    logger.info(f"API ключ загружен: {RAPIDAPI_KEY[:10]}...")
    print(f"API ключ загружен: {RAPIDAPI_KEY[:10]}...")

# Параметры API
API_CONFIG = {
    "url": "https://bayut.p.rapidapi.com/properties/list",
    "headers": {
        "X-RapidAPI-Key": RAPIDAPI_KEY,
        "X-RapidAPI-Host": "bayut.p.rapidapi.com"
    },
    "params": {
        "locationExternalIDs": "5002,6020",  # Дубай
        "purpose": "for-sale",
        "hitsPerPage": "25",
        "sort": "date-desc",
        "categoryExternalID": "4",  # Квартиры
        "isDeveloper": "true",  # Только от застройщиков
        "completionStatus": ["off-plan", "under-construction"]  # Строящиеся объекты
    }
}

# Параметры базы данных с оптимальными настройками
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASSWORD', 'Enclude79'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'options': '-c statement_timeout=30000'  # Таймаут 30 секунд
}

def clean_text(text):
    """Очищает текст от HTML и специальных символов"""
    if text is None:
        return ""
    # Добавляем импорт re в функцию
    import re
    # Удаляем HTML теги
    text = re.sub(r'<[^>]+>', '', text)
    # Заменяем специальные символы
    text = text.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
    return text

def fetch_properties_from_api(max_records=10):
    """Получает данные из API Bayut"""
    logger.info(f"Загрузка данных из API (лимит: {max_records} записей)")
    print(f"Загрузка данных из API (лимит: {max_records} записей)")
    
    properties = []
    page = 1
    page_size = min(25, max_records)  # Максимум 25 записей на страницу
    
    while len(properties) < max_records:
        logger.info(f"Загрузка страницы {page} из API")
        print(f"Загрузка страницы {page} из API")
        
        querystring = API_CONFIG["params"].copy()
        querystring["page"] = str(page)
        querystring["hitsPerPage"] = str(page_size)
        
        try:
            # Делаем запрос к API
            response = requests.get(
                API_CONFIG["url"],
                headers=API_CONFIG["headers"],
                params=querystring,
                timeout=30  # Добавляем таймаут запроса
            )
            response.raise_for_status()
            
            data = response.json()
            hits = data.get('hits', [])
            
            if not hits:
                logger.info("Данные не найдены в API")
                break
            
            for item in hits:
                properties.append(extract_property_data(item))
                if len(properties) >= max_records:
                    break
            
            logger.info(f"Загружено {len(properties)} записей из API")
            print(f"Загружено {len(properties)} записей из API")
            
            if len(properties) >= max_records:
                break
                
            page += 1
            time.sleep(1)  # Небольшая задержка между запросами
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных из API: {e}")
            print(f"Ошибка при загрузке данных из API: {e}")
            break
    
    return properties

def extract_property_data(property_item):
    """Извлекает необходимые данные из ответа API"""
    created_at = datetime.fromtimestamp(property_item.get('createdAt', 0))
    updated_at = datetime.fromtimestamp(property_item.get('updatedAt', 0))
    
    # Извлекаем данные из сложной структуры JSON
    location = ""
    if property_item.get('location'):
        locations = property_item.get('location', [])
        # Берем название места с самым высоким уровнем детализации
        for loc in locations:
            if loc.get('level') == 2 and loc.get('name'):
                location = loc.get('name')
                break
        # Если не найдено, берем любой доступный уровень
        if not location and len(locations) > 0:
            location = locations[-1].get('name', '')
    
    # Формируем итоговый словарь данных
    data = {
        'id': property_item.get('id'),
        'title': clean_text(property_item.get('title', '')),
        'price': property_item.get('price', 0),
        'rooms': property_item.get('rooms', 0),
        'baths': property_item.get('baths', 0),
        'area': property_item.get('area', 0),
        'location': clean_text(location),
        'property_type': property_item.get('propertyType', ''),
        'created_at': created_at.strftime('%Y-%m-%d %H:%M:%S'),
        'updated_at': updated_at.strftime('%Y-%m-%d %H:%M:%S')
    }
    
    return data

def insert_properties_to_db(properties):
    """Вставляет данные о недвижимости в базу данных"""
    if not properties:
        logger.warning("Нет данных для вставки в БД")
        return 0
    
    # Параметры для отслеживания прогресса
    total = len(properties)
    inserted = 0
    errors = 0
    
    logger.info(f"Начало вставки {total} записей в базу данных")
    print(f"Начало вставки {total} записей в базу данных")
    
    # Устанавливаем соединение с автокоммитом
    try:
        start_time = time.time()
        connection = psycopg2.connect(**DB_CONFIG)
        connection.autocommit = True  # Автоматический коммит
        cursor = connection.cursor()
        
        # Проверяем наличие таблицы
        cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'bayut_properties');")
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            logger.error("Таблица bayut_properties не существует")
            print("Ошибка: Таблица bayut_properties не существует")
            return 0
        
        # Вставляем каждую запись отдельно для надежности
        for i, property_data in enumerate(properties, 1):
            try:
                # Получаем ключи и значения
                keys = property_data.keys()
                values = [property_data[key] for key in keys]
                
                # Формируем простой SQL запрос с обработкой конфликтов
                columns = ', '.join(keys)
                placeholders = ', '.join(['%s'] * len(keys))
                
                # Базовый запрос
                query = f"""
                INSERT INTO bayut_properties ({columns}) 
                VALUES ({placeholders})
                ON CONFLICT (id) DO UPDATE SET 
                """
                
                # Добавляем UPDATE часть
                update_parts = []
                for key in keys:
                    if key != 'id':  # id не обновляем
                        update_parts.append(f"{key} = EXCLUDED.{key}")
                
                query += ", ".join(update_parts)
                
                # Выполняем запрос с таймаутом
                cursor.execute(query, values)
                
                # Успешная вставка
                inserted += 1
                logger.info(f"Вставлена запись {i} из {total} (ID: {property_data.get('id')})")
                print(f"Вставлена запись {i} из {total} (ID: {property_data.get('id')})")
                
            except Exception as e:
                errors += 1
                logger.error(f"Ошибка при вставке записи {i}: {e}")
                print(f"Ошибка при вставке записи {i}: {e}")
                # Продолжаем со следующей записью
        
        # Закрываем соединение
        cursor.close()
        connection.close()
        
        total_time = time.time() - start_time
        logger.info(f"Вставка завершена: {inserted} успешно, {errors} с ошибками за {total_time:.2f} секунд")
        print(f"Вставка завершена: {inserted} успешно, {errors} с ошибками за {total_time:.2f} секунд")
        
        return inserted
        
    except Exception as e:
        logger.error(f"Критическая ошибка при работе с БД: {e}")
        print(f"Критическая ошибка при работе с БД: {e}")
        return 0

def main():
    parser = argparse.ArgumentParser(description="Загрузка данных из API Bayut в PostgreSQL")
    parser.add_argument('--limit', type=int, default=10, help='Количество записей для загрузки из API')
    args = parser.parse_args()
    
    # Загружаем данные из API
    properties = fetch_properties_from_api(args.limit)
    
    if not properties:
        logger.error("Не удалось получить данные из API")
        print("Не удалось получить данные из API")
        return
    
    # Вставляем данные в базу
    inserted = insert_properties_to_db(properties)
    
    # Выводим итоговую статистику
    logger.info(f"Итого: успешно вставлено {inserted} из {len(properties)} записей")
    print(f"Итого: успешно вставлено {inserted} из {len(properties)} записей")

if __name__ == "__main__":
    main() 