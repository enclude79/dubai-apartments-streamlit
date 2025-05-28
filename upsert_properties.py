import os
import pandas as pd
import psycopg2
import logging
from datetime import datetime

# Настройка логирования
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = f'{log_dir}/upsert_properties_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

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
    'port': '5432',
    'table': 'bayut_properties'
}

def clean_text(text):
    """Очищает текст от символов, не поддерживаемых WIN1251 (cp1251)"""
    if not isinstance(text, str):
        return text
    try:
        # Перекодируем в cp1251, заменяя неподдерживаемые символы на '?'
        return text.encode('cp1251', errors='replace').decode('cp1251')
    except Exception:
        return text.encode('ascii', 'ignore').decode('ascii')

def upsert_data_from_csv(csv_file):
    """Загружает данные из CSV с обновлением существующих записей"""
    if not os.path.exists(csv_file):
        logger.error(f"Файл не найден: {csv_file}")
        print(f"Файл не найден: {csv_file}")
        return False
    
    # Загружаем данные из CSV
    logger.info(f"Загрузка данных из CSV файла: {csv_file}")
    print(f"Загрузка данных из CSV файла: {csv_file}")
    
    try:
        df = pd.read_csv(csv_file, encoding='utf-8-sig')
    except Exception as e:
        logger.error(f"Ошибка при чтении CSV с UTF-8: {e}")
        try:
            df = pd.read_csv(csv_file, encoding='latin1')
        except Exception as e:
            logger.error(f"Ошибка при чтении CSV: {e}")
            return False
    
    # Проверяем наличие данных
    if df.empty:
        logger.warning("CSV файл не содержит данных")
        print("CSV файл не содержит данных")
        return False
    
    logger.info(f"Загружено {len(df)} строк из CSV файла")
    print(f"Загружено {len(df)} строк из CSV файла")
    
    # Очищаем текстовые данные от проблемных символов
    for col in df.columns:
        if df[col].dtype == 'object':  # Для строковых столбцов
            df[col] = df[col].apply(clean_text)
    
    # Подключаемся к базе данных
    try:
        conn = psycopg2.connect(
            dbname=DB_CONFIG['dbname'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port']
        )
        conn.set_client_encoding('UTF8')
        conn.autocommit = False
        logger.info("Успешное подключение к базе данных")
        print("Успешное подключение к базе данных")
    except Exception as e:
        logger.error(f"Ошибка при подключении к базе данных: {e}")
        print(f"Ошибка при подключении к базе данных: {e}")
        return False
    
    cursor = conn.cursor()
    new_records = 0
    updated_records = 0
    errors = 0
    
    try:
        for _, row in df.iterrows():
            try:
                # Преобразуем is_verified в boolean
                is_verified = row['is_verified']
                if isinstance(is_verified, str):
                    is_verified = is_verified.lower() in ('true', 't', 'yes', 'y', '1')
                
                # Преобразуем числовые значения
                price = float(row['price']) if pd.notna(row['price']) else None
                floor_number = int(float(row['floor_number'])) if pd.notna(row['floor_number']) else None
                city_level_score = int(float(row['city_level_score'])) if pd.notna(row['city_level_score']) else None
                score = int(float(row['score'])) if pd.notna(row['score']) else None
                agency_rating = float(row['agency_rating']) if pd.notna(row['agency_rating']) else None
                
                # UPSERT - INSERT с обновлением при конфликте
                cursor.execute("""
                    INSERT INTO bayut_properties 
                    (id, title, price, rooms, baths, area, rent_frequency, location, 
                    cover_photo_url, property_url, category, property_type, created_at, 
                    updated_at, furnishing_status, completion_status, amenities, agency_name, 
                    contact_info, geography, agency_logo_url, proxy_mobile, keywords, 
                    is_verified, purpose, floor_number, city_level_score, score, 
                    agency_licenses, agency_rating)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                           to_timestamp(%s, 'YYYY-MM-DD HH24:MI:SS'), 
                           to_timestamp(%s, 'YYYY-MM-DD HH24:MI:SS'), 
                           %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id, updated_at, created_at) DO UPDATE SET
                    title = EXCLUDED.title,
                    price = EXCLUDED.price,
                    rooms = EXCLUDED.rooms,
                    baths = EXCLUDED.baths,
                    area = EXCLUDED.area,
                    rent_frequency = EXCLUDED.rent_frequency,
                    location = EXCLUDED.location,
                    cover_photo_url = EXCLUDED.cover_photo_url,
                    property_url = EXCLUDED.property_url,
                    category = EXCLUDED.category,
                    property_type = EXCLUDED.property_type,
                    furnishing_status = EXCLUDED.furnishing_status,
                    completion_status = EXCLUDED.completion_status,
                    amenities = EXCLUDED.amenities,
                    agency_name = EXCLUDED.agency_name,
                    contact_info = EXCLUDED.contact_info,
                    geography = EXCLUDED.geography,
                    agency_logo_url = EXCLUDED.agency_logo_url,
                    proxy_mobile = EXCLUDED.proxy_mobile,
                    keywords = EXCLUDED.keywords,
                    is_verified = EXCLUDED.is_verified,
                    purpose = EXCLUDED.purpose,
                    floor_number = EXCLUDED.floor_number,
                    city_level_score = EXCLUDED.city_level_score,
                    score = EXCLUDED.score,
                    agency_licenses = EXCLUDED.agency_licenses,
                    agency_rating = EXCLUDED.agency_rating
                    RETURNING (xmax = 0) AS inserted
                """, (
                    row['id'], row['title'], price, row['rooms'], row['baths'], 
                    row['area'], row['rent_frequency'], row['location'], row['cover_photo_url'], 
                    row['property_url'], row['category'], row['property_type'], 
                    row['created_at'], row['updated_at'], row['furnishing_status'], 
                    row['completion_status'], row['amenities'], row['agency_name'], 
                    row['contact_info'], row['geography'], row['agency_logo_url'], 
                    row['proxy_mobile'], row['keywords'], is_verified, row['purpose'], 
                    floor_number, city_level_score, score, 
                    row['agency_licenses'], agency_rating
                ))
                
                # Определяем, была ли вставка новой записи или обновление существующей
                is_inserted = cursor.fetchone()[0]
                if is_inserted:
                    new_records += 1
                else:
                    updated_records += 1
                    
            except Exception as e:
                errors += 1
                logger.error(f"Ошибка при обработке записи {row['id']}: {e}")
                print(f"Ошибка при обработке записи {row['id']}: {e}")
        
        # Фиксируем изменения
        conn.commit()
        logger.info(f"Обработка данных завершена. Добавлено: {new_records}, Обновлено: {updated_records}, Ошибок: {errors}")
        print(f"Обработка данных завершена. Добавлено: {new_records}, Обновлено: {updated_records}, Ошибок: {errors}")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Произошла ошибка при обработке данных: {e}")
        print(f"Произошла ошибка при обработке данных: {e}")
        return False
    finally:
        cursor.close()
        conn.close()
        logger.info("Подключение к базе данных закрыто")
        print("Подключение к базе данных закрыто")
    
    return True

def main():
    import sys
    
    if len(sys.argv) < 2:
        print("Укажите путь к CSV файлу в качестве аргумента")
        print("Пример: python upsert_properties.py Api_Bayat/bayut_properties_sale_20250514.csv")
        return
    
    csv_file = sys.argv[1]
    result = upsert_data_from_csv(csv_file)
    
    if result:
        print("Данные успешно обработаны и загружены в базу данных")
    else:
        print("Произошла ошибка при обработке данных")

if __name__ == "__main__":
    main() 