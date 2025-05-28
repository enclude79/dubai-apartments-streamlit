import os
import requests
import json
import csv
import time
from datetime import datetime
import logging
import argparse
import psycopg2
from dotenv import load_dotenv

# Настройка логирования
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = f'{log_dir}/api_to_csv_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

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

# Пути к файлам
CSV_DIR = "Api_Bayat"
os.makedirs(CSV_DIR, exist_ok=True)

load_dotenv()

# Проверяем наличие API ключа
if not os.getenv('RAPIDAPI_KEY'):
    logger.error("RAPIDAPI_KEY не найден в переменных окружения")
    print("Ошибка: RAPIDAPI_KEY не найден в переменных окружения")
    print("Пожалуйста, создайте файл .env и добавьте в него:")
    print("RAPIDAPI_KEY=ваш_ключ_api")
    exit(1)

DB_PARAMS = {
    'dbname': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'Admin'),
    'password': os.getenv('DB_PASSWORD', 'Enclude79'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432')
}

def get_last_created_at():
    """Получает максимальное значение created_at из базы данных"""
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    cur.execute("SELECT MAX(created_at) FROM bayut_properties;")
    result = cur.fetchone()[0]
    cur.close()
    conn.close()
    return result  # None если записей нет

def fetch_properties(max_pages=None, max_records=10):
    """Загружает последние 10 объектов через API Bayut"""
    logger.info(f"Начало загрузки последних {max_records} записей через API Bayut")
    all_properties = []
    page = 1
    MAX_RECORDS = max_records
    
    # Настройки для соблюдения rate limit
    REQUESTS_PER_MINUTE = 12  # ~1 запрос в 5 секунд
    DELAY_BETWEEN_REQUESTS = 5  # секунды между запросами
    
    querystring = API_CONFIG["params"].copy()
    querystring["page"] = str(page)
    querystring["hitsPerPage"] = str(max_records)  # Запрашиваем только нужное количество записей
    
    try:
        # Делаем запрос
        response = requests.get(
            API_CONFIG["url"],
            headers=API_CONFIG["headers"],
            params=querystring
        )
        
        # Проверяем заголовки rate limit
        rate_limit = response.headers.get('X-RapidAPI-RateLimit-Limit', '')
        rate_remaining = response.headers.get('X-RapidAPI-RateLimit-Remaining', '')
        if rate_limit and rate_remaining:
            logger.info(f"Rate limit: {rate_remaining}/{rate_limit} запросов осталось")
        
        response.raise_for_status()
        data = response.json()
        
        hits = data.get('hits', [])
        if not hits:
            logger.info("Данные не найдены")
            return []
            
        for item in hits:
            all_properties.append(extract_property_data(item))
            if len(all_properties) >= MAX_RECORDS:
                break
            
        logger.info(f"Получено {len(all_properties)} объектов")
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка при запросе к API: {e}")
        return []
    except Exception as e:
        logger.error(f"Неожиданная ошибка: {e}")
        return []
    
    # Обрезаем до max_records, если вдруг получили больше
    all_properties = all_properties[:MAX_RECORDS]
    logger.info(f"Загрузка завершена. Всего загружено объектов: {len(all_properties)}")
    return all_properties

def extract_property_data(property_item):
    """Извлекает данные из объекта API в формат для сохранения"""
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
    return {
        'id': property_item.get('id'),
        'title': property_item.get('title'),
        'price': property_item.get('price'),
        'rooms': property_item.get('rooms'),
        'baths': property_item.get('baths'),
        'area': property_item.get('area'),
        'rent_frequency': property_item.get('rentFrequency'),
        'location': location,
        'cover_photo_url': property_item.get('coverPhoto', {}).get('url'),
        'property_url': f"https://www.bayut.com/property/details-{property_item.get('externalID')}.html",
        'category': category_name,
        'property_type': property_item.get('propertyType'),
        'created_at': created_at.strftime('%Y-%m-%d %H:%M:%S'),
        'updated_at': updated_at.strftime('%Y-%m-%d %H:%M:%S'),
        'furnishing_status': property_item.get('furnishingStatus'),
        'completion_status': property_item.get('completionStatus'),
        'amenities': amenities_str,
        'agency_name': property_item.get('agency', {}).get('name'),
        'contact_info': contact_info,
        'geography': geo_info,
        'agency_logo_url': property_item.get('agency', {}).get('logo', {}).get('url'),
        'proxy_mobile': phone_number.get('proxyMobile'),
        'keywords': json.dumps(property_item.get('keywords', [])),
        'is_verified': property_item.get('isVerified'),
        'purpose': property_item.get('purpose'),
        'floor_number': property_item.get('floorNumber'),
        'city_level_score': property_item.get('cityLevelScore'),
        'score': property_item.get('score'),
        'agency_licenses': json.dumps(property_item.get('agency', {}).get('licenses', [])),
        'agency_rating': property_item.get('agency', {}).get('rating')
    }

def save_to_csv(properties_data):
    """Сохраняет данные о недвижимости в CSV файл"""
    if not properties_data:
        logger.warning("Нет данных для сохранения")
        print("Нет данных для сохранения")
        return None
    
    # Формируем имя файла с текущей датой
    current_date = datetime.now().strftime("%Y%m%d")
    output_file = os.path.join(CSV_DIR, f'bayut_properties_sale_{current_date}.csv')
    
    # Определяем заголовки CSV из ключей первого объекта
    fieldnames = list(properties_data[0].keys())
    
    # Сохраняем в CSV с кодировкой UTF-8
    with open(output_file, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(properties_data)
    
    logger.info(f"Данные сохранены в файл: {output_file}")
    print(f"Данные сохранены в файл: {output_file}")
    print(f"CSV_PATH:{output_file}")  # Метка для автоматического извлечения пути
    
    return output_file

def filter_by_created_at(properties, since_date):
    if not since_date:
        return properties
    since_dt = None
    if isinstance(since_date, str):
        try:
            since_dt = datetime.strptime(since_date, "%Y-%m-%d")
        except Exception:
            try:
                since_dt = datetime.strptime(since_date, "%Y-%m-%d %H:%M:%S")
            except Exception:
                print(f"Не удалось распарсить дату отсечки: {since_date}")
                return properties
    else:
        since_dt = since_date
    filtered = []
    for prop in properties:
        created_at = prop.get('created_at')
        if created_at:
            try:
                prop_dt = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
                if prop_dt > since_dt:
                    filtered.append(prop)
            except Exception:
                continue
    return filtered

def filter_by_date_range(properties, since_date, until_date):
    """Оставляет объекты с since_date < created_at <= until_date"""
    since_dt = datetime.min
    until_dt = datetime.now()
    if since_date:
        since_dt = datetime.strptime(since_date, "%Y-%m-%d")
    if until_date:
        until_dt = datetime.strptime(until_date, "%Y-%m-%d")
    filtered = []
    for prop in properties:
        created_at = prop.get('created_at')
        if not created_at:
            continue
        prop_dt = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
        if since_dt < prop_dt <= until_dt:
            filtered.append(prop)
    return filtered

def main():
    parser = argparse.ArgumentParser(description="Загрузка данных из API в CSV с поддержкой диапазона дат")
    parser.add_argument('--since', type=str, default=None, help='Дата начала (YYYY-MM-DD).')
    parser.add_argument('--until', type=str, default=None, help='Дата окончания (YYYY-MM-DD), по умолчанию — сегодня.')
    parser.add_argument('--max-pages', type=int, default=None, help='Максимальное число страниц (по умолчанию — все).')
    parser.add_argument('--limit', type=int, default=4000, help='Сколько записей загрузить (по умолчанию 4000).')
    args = parser.parse_args()

    since_date = args.since
    until_date = args.until
    limit = args.limit

    if since_date:
        print(f"Дата начала (от пользователя): {since_date}")
    else:
        since_date = None
        print("Нижняя граница не используется, будут загружены все данные до даты окончания.")

    if until_date:
        print(f"Дата окончания (от пользователя): {until_date}")
    else:
        until_date = datetime.now().strftime("%Y-%m-%d")
        print(f"Дата окончания (по умолчанию): {until_date}")

    try:
        logger.info("Запуск процесса загрузки данных из API Bayut")
        print("Запуск процесса загрузки данных из API Bayut")
        
        # Получаем данные из API
        properties_data = fetch_properties(max_pages=args.max_pages, max_records=limit)
        
        logger.info(f"Получено объектов: {len(properties_data)}")
        print(f"Получено объектов: {len(properties_data)}")
        
        # Сохраняем данные в CSV
        if properties_data:
            csv_path = save_to_csv(properties_data)
            if csv_path:
                logger.info(f"Процесс загрузки данных завершен успешно. Всего объектов: {len(properties_data)}")
                print(f"Процесс загрузки данных завершен успешно. Всего объектов: {len(properties_data)}")
                print(f"\nДанные сохранены в файл: {csv_path}")
                print(f"Всего получено объектов: {len(properties_data)}")
                return csv_path
            else:
                logger.error("Не удалось сохранить данные в CSV")
                print("Не удалось сохранить данные в CSV")
        else:
            logger.error("Нет данных для сохранения")
            print("Нет данных для сохранения")
        
        return None
    except Exception as e:
        logger.error(f"Ошибка при выполнении процесса: {e}")
        print(f"Ошибка при выполнении процесса: {e}")
        return None

if __name__ == "__main__":
    main() 