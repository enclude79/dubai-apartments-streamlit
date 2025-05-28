import os
import pandas as pd
import psycopg2
import logging
from datetime import datetime
import chardet

# Настройка логирования
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = f'{log_dir}/rename_columns_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Параметры базы данных
DB_CONFIG = {
    'dbname': 'postgres',
    'user': 'admin',
    'password': 'Enclude79',
    'host': 'localhost',
    'port': '5432'
}

# Пути к файлам
CSV_DIR = "Api_Bayat"

# Соответствие имен полей в API и в текущей базе данных
API_FIELD_MAPPING = {
    'id': 'id',  # ID в API -> ID в нашей базе
    'title': 'title',  # Название в API -> title в нашей базе
    'price': 'price',  # Цена в API -> price в нашей базе
    'rooms': 'rooms',  # Комнаты в API -> rooms в нашей базе
    'baths': 'baths',  # Ванные в API -> baths в нашей базе
    'area': 'area',  # Площадь в API -> area в нашей базе
    'rentFrequency': 'rent_frequency',  # Частота аренды в API -> rent_frequency в нашей базе
    'location': 'location',  # Местоположение в API -> location в нашей базе
    'coverPhoto.url': 'cover_photo_url',  # URL фото в API -> cover_photo_url в нашей базе
    'externalURL': 'property_url',  # URL объекта в API -> property_url в нашей базе
    'category': 'category',  # Категория в API -> category в нашей базе
    'propertyType': 'property_type',  # Тип недвижимости в API -> property_type в нашей базе
    'createdAt': 'created_at',  # Дата создания в API -> created_at в нашей базе
    'updatedAt': 'updated_at',  # Дата обновления в API -> updated_at в нашей базе
    'furnishingStatus': 'furnishing_status',  # Статус мебели в API -> furnishing_status в нашей базе
    'completionStatus': 'completion_status',  # Статус завершения в API -> completion_status в нашей базе
    'amenities': 'amenities',  # Удобства в API -> amenities в нашей базе
    'agency.name': 'agency_name',  # Название агентства в API -> agency_name в нашей базе
    'phoneNumber': 'contact_info',  # Контактная информация в API -> contact_info в нашей базе
    'geography': 'geography',  # География в API -> geography в нашей базе
    'agency.logo.url': 'agency_logo_url',  # URL логотипа агентства в API -> agency_logo_url в нашей базе
    'phoneNumber.proxyMobile': 'proxy_mobile',  # Прокси-мобильный в API -> proxy_mobile в нашей базе
    'keywords': 'keywords',  # Ключевые слова в API -> keywords в нашей базе
    'isVerified': 'is_verified',  # Проверено в API -> is_verified в нашей базе
    'purpose': 'purpose',  # Назначение в API -> purpose в нашей базе
    'floorNumber': 'floor_number',  # Номер этажа в API -> floor_number в нашей базе
    'cityLevelScore': 'city_level_score',  # Оценка уровня города в API -> city_level_score в нашей базе
    'score': 'score',  # Оценка в API -> score в нашей базе
    'agency.licenses': 'agency_licenses',  # Лицензии агентства в API -> agency_licenses в нашей базе
    'agency.rating': 'agency_rating'  # Рейтинг агентства в API -> agency_rating в нашей базе
}

def detect_encoding(file_path):
    """Определяет кодировку файла"""
    with open(file_path, 'rb') as f:
        raw_data = f.read(10000)  # Читаем первые 10000 байт для определения кодировки
        result = chardet.detect(raw_data)
        return result['encoding']

def rename_columns_in_table():
    """Переименовывает колонки в SQL таблице для соответствия API"""
    conn = None
    try:
        # Подключаемся к базе данных
        conn = psycopg2.connect(**DB_CONFIG)
        conn.set_client_encoding('UTF8')
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Проверяем существующие колонки в таблице
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'bayut_properties'
            ORDER BY ordinal_position
        """)
        existing_columns = [row[0] for row in cursor.fetchall()]
        
        # Инвертируем словарь соответствия полей (API -> наша база) для проверки
        inverted_mapping = {v: k for k, v in API_FIELD_MAPPING.items()}
        
        print("\n===== Текущие колонки и их соответствие API =====")
        for i, col in enumerate(existing_columns):
            api_field = inverted_mapping.get(col, "Нет соответствия")
            print(f"{i+1}. {col} -> {api_field}")
        
        # Предлагаем переименования
        print("\n===== Предлагаемые переименования для соответствия API =====")
        for i, col in enumerate(existing_columns):
            api_field = inverted_mapping.get(col, col)
            if api_field != col and api_field != "Нет соответствия":
                print(f"{col} -> {api_field}")
                
                # Спрашиваем пользователя о переименовании
                rename = input(f"Переименовать {col} в {api_field}? (y/n): ")
                if rename.lower() == 'y':
                    try:
                        # Переименовываем колонку
                        cursor.execute(f"""
                            ALTER TABLE bayut_properties
                            RENAME COLUMN "{col}" TO "{api_field}"
                        """)
                        logger.info(f"Колонка {col} переименована в {api_field}")
                        print(f"Колонка {col} успешно переименована в {api_field}")
                    except Exception as e:
                        logger.error(f"Ошибка при переименовании колонки {col} в {api_field}: {e}")
                        print(f"Ошибка при переименовании колонки: {e}")
        
        # Выводим обновленную структуру таблицы
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'bayut_properties'
            ORDER BY ordinal_position
        """)
        updated_columns = [row[0] for row in cursor.fetchall()]
        
        print("\n===== Обновленная структура таблицы =====")
        for i, col in enumerate(updated_columns):
            print(f"{i+1}. {col}")
        
        cursor.close()
        
    except Exception as e:
        logger.error(f"Ошибка при переименовании колонок: {e}")
        print(f"Ошибка при переименовании колонок: {e}")
    finally:
        if conn:
            conn.close()

def create_view_with_api_names():
    """Создает представление (view) с именами полей, соответствующими API"""
    conn = None
    try:
        # Подключаемся к базе данных
        conn = psycopg2.connect(**DB_CONFIG)
        conn.set_client_encoding('UTF8')
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Проверяем существующие колонки в таблице
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'bayut_properties'
            ORDER BY ordinal_position
        """)
        existing_columns = [row[0] for row in cursor.fetchall()]
        
        # Инвертируем словарь соответствия полей (API -> наша база) для проверки
        inverted_mapping = {v: k for k, v in API_FIELD_MAPPING.items()}
        
        # Формируем SQL для создания представления
        view_columns = []
        for col in existing_columns:
            api_field = inverted_mapping.get(col, col)
            if api_field != col and api_field != "Нет соответствия":
                view_columns.append(f'"{col}" AS "{api_field}"')
            else:
                view_columns.append(f'"{col}"')
        
        view_sql = f"""
        DROP VIEW IF EXISTS api_properties;
        CREATE VIEW api_properties AS
        SELECT {', '.join(view_columns)}
        FROM bayut_properties
        """
        
        # Создаем представление
        cursor.execute(view_sql)
        logger.info("Представление api_properties успешно создано")
        print("Представление api_properties успешно создано с именами полей, соответствующими API")
        
        # Выводим структуру представления
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'api_properties'
            ORDER BY ordinal_position
        """)
        view_columns = [row[0] for row in cursor.fetchall()]
        
        print("\n===== Структура представления api_properties =====")
        for i, col in enumerate(view_columns):
            print(f"{i+1}. {col}")
        
        cursor.close()
        
    except Exception as e:
        logger.error(f"Ошибка при создании представления: {e}")
        print(f"Ошибка при создании представления: {e}")
    finally:
        if conn:
            conn.close()

def show_common_api_structures():
    """Показывает типичные структуры данных из API Bayut"""
    print("\n===== Типичная структура API Bayut =====")
    print("""
{
  "id": 1234567,
  "title": "Luxury Apartment in Downtown Dubai",
  "price": 1500000,
  "rooms": 3,
  "baths": 2,
  "area": 150,
  "rentFrequency": null,
  "location": [
    { "name": "UAE", "level": 0 },
    { "name": "Dubai", "level": 1 },
    { "name": "Downtown Dubai", "level": 2 }
  ],
  "coverPhoto": {
    "url": "https://example.com/photos/1234567.jpg"
  },
  "externalID": "123-abc",
  "category": [
    { "name": "Residential", "id": 1 }
  ],
  "propertyType": "Apartment",
  "createdAt": 1589180346,
  "updatedAt": 1611240746,
  "furnishingStatus": "furnished",
  "completionStatus": "under-construction",
  "amenities": [
    { "name": "Swimming Pool", "id": 1 },
    { "name": "Gym", "id": 2 }
  ],
  "agency": {
    "name": "Example Agency",
    "logo": {
      "url": "https://example.com/agency_logo.jpg"
    },
    "licenses": [
      { "number": "ABC123" }
    ],
    "rating": 4.5
  },
  "phoneNumber": {
    "mobile": "+971501234567",
    "whatsapp": "+971501234567",
    "proxyMobile": "+971501234567"
  },
  "geography": {
    "lat": 25.1234,
    "lng": 55.5678
  },
  "keywords": ["luxury", "downtown", "new"],
  "isVerified": true,
  "purpose": "for-sale",
  "floorNumber": 10,
  "cityLevelScore": 85,
  "score": 90
}
    """)
    
    print("\n===== Типичное извлечение данных из API =====")
    print("""
property_data = {
    'id': property_item.get('id'),
    'title': property_item.get('title'),
    'price': property_item.get('price'),
    'rooms': property_item.get('rooms'),
    'baths': property_item.get('baths'),
    'area': property_item.get('area'),
    'rent_frequency': property_item.get('rentFrequency'),
    'location': json.dumps(property_item.get('location', [])),
    'cover_photo_url': property_item.get('coverPhoto', {}).get('url'),
    'property_url': f"https://www.bayut.com/property/details-{property_item.get('externalID')}.html",
    'category': property_item.get('category', [{}])[0].get('name'),
    'property_type': property_item.get('type'),
    'created_at': created_at.strftime('%Y-%m-%d %H:%M:%S'),
    'updated_at': updated_at.strftime('%Y-%m-%d %H:%M:%S'),
    'furnishing_status': property_item.get('furnishingStatus'),
    'completion_status': property_item.get('completionStatus'),
    'amenities': ', '.join(property_item.get('amenities', [])),
    'agency_name': property_item.get('agency', {}).get('name'),
    'contact_info': f"Тел: {property_item.get('phoneNumber', {}).get('mobile')}; WhatsApp: {property_item.get('phoneNumber', {}).get('whatsapp')}",
    'geography': f"Широта: {property_item.get('geography', {}).get('lat')}, Долгота: {property_item.get('geography', {}).get('lng')}",
    'agency_logo_url': property_item.get('agency', {}).get('logo', {}).get('url'),
    'proxy_mobile': property_item.get('phoneNumber', {}).get('proxyMobile'),
    'keywords': json.dumps(property_item.get('keywords', [])),
    'is_verified': property_item.get('isVerified'),
    'purpose': property_item.get('purpose'),
    'floor_number': property_item.get('floorNumber'),
    'city_level_score': property_item.get('cityLevelScore'),
    'score': property_item.get('score'),
    'agency_licenses': json.dumps(property_item.get('agency', {}).get('licenses', [])),
    'agency_rating': property_item.get('agency', {}).get('rating')
}
    """)

if __name__ == "__main__":
    show_common_api_structures()
    print("\nВыберите действие:")
    print("1. Переименовать колонки в таблице для соответствия API")
    print("2. Создать представление (view) с именами полей, соответствующими API")
    print("3. Выход")
    
    choice = input("Ваш выбор (1-3): ")
    
    if choice == "1":
        rename_columns_in_table()
    elif choice == "2":
        create_view_with_api_names()
    else:
        print("Выход из программы") 