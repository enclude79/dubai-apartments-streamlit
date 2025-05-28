import os
import requests
import json
import psycopg2
import psycopg2.extras
import argparse
import time
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Настройка логирования
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = f'{log_dir}/simple_api_to_sql_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

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
if not os.getenv('RAPIDAPI_KEY'):
    logger.error("RAPIDAPI_KEY не найден в переменных окружения")
    print("Ошибка: RAPIDAPI_KEY не найден в переменных окружения")
    exit(1)
else:
    print(f"API ключ загружен: {os.getenv('RAPIDAPI_KEY')[:10]}...")

# Параметры API
API_CONFIG = {
    "url": "https://bayut.p.rapidapi.com/properties/list",
    "headers": {
        "X-RapidAPI-Key": os.getenv('RAPIDAPI_KEY'),
        "X-RapidAPI-Host": "bayut.p.rapidapi.com"
    },
    "params": {
        "locationExternalIDs": "5002,6020",  # Дубай
        "purpose": "for-sale",
        "hitsPerPage": "25",
        "sort": "date-desc",  # Сортировка по дате (сначала новые)
        "categoryExternalID": "4",  # Квартиры
        "isDeveloper": "true",  # Только от застройщиков
        "completionStatus": ["off-plan", "under-construction"]  # Строящиеся объекты
    }
}

# Параметры базы данных
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASSWORD', 'Enclude79'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432')
}

def fetch_properties_from_api(max_records=5):
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
                params=querystring
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
            time.sleep(2)  # Задержка между запросами
            
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
        # Берем название места с самым высоким уровнем детализации (обычно уровень 2 - район)
        for loc in locations:
            if loc.get('level') == 2 and loc.get('name'):
                location = loc.get('name')
                break
        # Если не найдено, берем любой доступный уровень
        if not location and len(locations) > 0:
            location = locations[-1].get('name', '')
    
    # Получаем список удобств
    amenities_list = []
    for amenity in property_item.get('amenities', []):
        if isinstance(amenity, dict) and amenity.get('text'):
            amenities_list.append(amenity.get('text'))
    amenities_str = ', '.join(amenities_list)
    
    # Формируем контактную информацию
    phone_number = property_item.get('phoneNumber', {})
    contact_info = f"Тел: {phone_number.get('mobile', '?')}; WhatsApp: {phone_number.get('whatsapp', '?')}"
    
    # Формируем географические данные
    geography = property_item.get('geography', {})
    geo_info = f"Широта: {geography.get('lat')}, Долгота: {geography.get('lng')}"
    
    # Категория (берем первую, если есть)
    category_name = ""
    categories = property_item.get('category', [])
    if categories and isinstance(categories, list) and len(categories) > 0:
        if isinstance(categories[0], dict):
            category_name = categories[0].get('name', '')
    
    # Формируем итоговый словарь данных
    data = {
        'id': property_item.get('id'),
        'title': property_item.get('title', ''),
        'price': property_item.get('price', 0),
        'rooms': property_item.get('rooms', 0),
        'baths': property_item.get('baths', 0),
        'area': property_item.get('area', 0),
        'rent_frequency': property_item.get('rentFrequency', ''),
        'location': location,
        'cover_photo_url': property_item.get('coverPhoto', {}).get('url', ''),
        'property_url': f"https://www.bayut.com/property/details-{property_item.get('externalID')}.html",
        'category': category_name,
        'property_type': property_item.get('propertyType', ''),
        'created_at': created_at.strftime('%Y-%m-%d %H:%M:%S'),
        'updated_at': updated_at.strftime('%Y-%m-%d %H:%M:%S'),
        'furnishing_status': property_item.get('furnishingStatus', ''),
        'completion_status': property_item.get('completionStatus', ''),
        'amenities': amenities_str,
        'agency_name': property_item.get('agency', {}).get('name', ''),
        'contact_info': contact_info,
        'geography': geo_info,
        'agency_logo_url': property_item.get('agency', {}).get('logo', {}).get('url', ''),
        'proxy_mobile': phone_number.get('proxyMobile', ''),
        'keywords': json.dumps(property_item.get('keywords', [])),
        'is_verified': property_item.get('isVerified', False),
        'purpose': property_item.get('purpose', ''),
        'floor_number': property_item.get('floorNumber', 0),
        'city_level_score': property_item.get('cityLevelScore', 0),
        'score': property_item.get('score', 0),
        'agency_licenses': json.dumps(property_item.get('agency', {}).get('licenses', [])),
        'agency_rating': property_item.get('agency', {}).get('rating', 0)
    }
    
    return data

def insert_property(property_data):
    """Вставляет одну запись в базу данных"""
    if not property_data:
        return False
    
    connection = None
    cursor = None
    
    try:
        # Устанавливаем соединение с базой данных
        connection = psycopg2.connect(**DB_CONFIG, connect_timeout=10)
        connection.autocommit = False  # Важно для транзакций
        cursor = connection.cursor()
        
        # Получаем ключи и значения для вставки
        keys = property_data.keys()
        values = [property_data[key] for key in keys]
        
        # Создаем строку колонок и плейсхолдеров
        columns = ', '.join(keys)
        placeholders = ', '.join(['%s'] * len(keys))
        
        # Формируем базовый запрос
        base_query = f"INSERT INTO bayut_properties ({columns}) VALUES ({placeholders})"
        
        # Создаем части для ON CONFLICT
        update_parts = []
        for key in keys:
            if key != 'id':  # id не обновляем
                update_parts.append(f"{key} = EXCLUDED.{key}")
        
        # Финальный запрос
        query = f"{base_query} ON CONFLICT (id) DO UPDATE SET {', '.join(update_parts)}"
        
        # Выполняем запрос
        cursor.execute(query, values)
        
        # Подтверждаем транзакцию
        connection.commit()
        logger.info(f"Запись с ID {property_data.get('id')} успешно вставлена/обновлена")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при вставке записи: {e}")
        if connection:
            connection.rollback()
        return False
        
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def simple_insert_many(properties, max_attempts=3):
    """Вставляет несколько записей по одной"""
    if not properties:
        logger.warning("Нет данных для вставки")
        return 0
    
    inserted_count = 0
    errors_count = 0
    
    for i, property_data in enumerate(properties, 1):
        logger.info(f"Вставка записи {i} из {len(properties)} (ID: {property_data.get('id')})")
        print(f"Вставка записи {i} из {len(properties)} (ID: {property_data.get('id')})")
        
        attempt = 0
        success = False
        
        while attempt < max_attempts and not success:
            attempt += 1
            success = insert_property(property_data)
            
            if success:
                inserted_count += 1
                logger.info(f"Запись {i} успешно вставлена")
            else:
                logger.warning(f"Ошибка при вставке записи {i} (попытка {attempt}/{max_attempts})")
                if attempt < max_attempts:
                    time.sleep(1)  # Ждем секунду перед повторной попыткой
        
        if not success:
            errors_count += 1
            logger.error(f"Не удалось вставить запись {i} после {max_attempts} попыток")
            
    logger.info(f"Обработка завершена. Вставлено {inserted_count} записей, ошибок: {errors_count}")
    print(f"Обработка завершена. Вставлено {inserted_count} записей, ошибок: {errors_count}")
    
    return inserted_count

def main():
    parser = argparse.ArgumentParser(description="Простая загрузка данных из API Bayut в PostgreSQL")
    parser.add_argument('--limit', type=int, default=5, help='Количество записей для загрузки из API')
    args = parser.parse_args()
    
    # Проверяем наличие таблицы bayut_properties
    try:
        connection = psycopg2.connect(**DB_CONFIG)
        cursor = connection.cursor()
        cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'bayut_properties');")
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            logger.error("Таблица bayut_properties не существует в базе данных!")
            print("Таблица bayut_properties не существует в базе данных!")
            cursor.close()
            connection.close()
            return
            
        cursor.close()
        connection.close()
        
    except Exception as e:
        logger.error(f"Ошибка при подключении к базе данных: {e}")
        print(f"Ошибка при подключении к базе данных: {e}")
        return
    
    # Загружаем данные из API
    properties = fetch_properties_from_api(args.limit)
    
    if not properties:
        logger.error("Не удалось получить данные из API")
        print("Не удалось получить данные из API")
        return
    
    # Вставляем данные в базу данных
    inserted_count = simple_insert_many(properties)
    
    logger.info(f"Итого: успешно вставлено {inserted_count} из {len(properties)} записей")
    print(f"Итого: успешно вставлено {inserted_count} из {len(properties)} записей")

if __name__ == "__main__":
    main() 