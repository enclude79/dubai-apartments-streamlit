import os
import pandas as pd
import psycopg2
import logging
from datetime import datetime
import json
import numpy as np

# Настройка логирования
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = f'{log_dir}/recreate_table_and_load_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

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

def clean_text(text):
    """Очищает текст от проблемных символов"""
    if not isinstance(text, str):
        return str(text) if text is not None else None
    
    try:
        # Удаляем BOM и другие специальные символы
        text = text.replace('\ufeff', '')
        # Экранируем управляющие символы
        for c in ('\u0000', '\u0001', '\u0002', '\u0003', '\u0004', '\u0005', '\u0006', '\u0007',
                 '\u0008', '\u000b', '\u000c', '\u000e', '\u000f', '\u0010', '\u0011', '\u0012',
                 '\u0013', '\u0014', '\u0015', '\u0016', '\u0017', '\u0018', '\u0019', '\u001a',
                 '\u001b', '\u001c', '\u001d', '\u001e', '\u001f'):
            text = text.replace(c, ' ')
        
        # Безопасное преобразование
        return text.encode('ascii', 'ignore').decode('ascii')
    except:
        # В случае ошибки возвращаем пустую строку
        return ''

def recreate_table():
    """Пересоздаёт таблицу bayut_properties"""
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Удаляем существующую таблицу, если она есть
        cursor.execute("DROP TABLE IF EXISTS bayut_properties")
        logger.info("Таблица bayut_properties удалена")
        print("Таблица bayut_properties удалена")
        
        # Создаем новую таблицу
        cursor.execute("""
            CREATE TABLE bayut_properties (
                id BIGINT PRIMARY KEY,
                title TEXT,
                price NUMERIC,
                rooms INTEGER,
                baths INTEGER,
                area NUMERIC,
                rent_frequency TEXT,
                location TEXT,
                cover_photo_url TEXT,
                property_url TEXT,
                category TEXT,
                property_type TEXT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                furnishing_status TEXT,
                completion_status TEXT,
                amenities TEXT,
                agency_name TEXT,
                contact_info TEXT,
                geography TEXT,
                agency_logo_url TEXT,
                proxy_mobile TEXT,
                keywords TEXT,
                is_verified BOOLEAN,
                purpose TEXT,
                floor_number INTEGER,
                city_level_score INTEGER,
                score INTEGER,
                agency_licenses TEXT,
                agency_rating NUMERIC
            )
        """)
        logger.info("Таблица bayut_properties создана заново")
        print("Таблица bayut_properties создана заново")
        
        cursor.close()
        return True
    except Exception as e:
        logger.error(f"Ошибка при пересоздании таблицы: {e}")
        print(f"Ошибка при пересоздании таблицы: {e}")
        return False
    finally:
        if conn:
            conn.close()

def load_data_from_csv(csv_file):
    """Загружает данные из CSV в базу данных"""
    conn = None
    cursor = None
    try:
        # Проверяем существование файла
        if not os.path.exists(csv_file):
            logger.error(f"Файл не найден: {csv_file}")
            print(f"Файл не найден: {csv_file}")
            return False
        
        # Загружаем данные из CSV
        logger.info(f"Загрузка данных из CSV файла: {csv_file}")
        print(f"Загрузка данных из CSV файла: {csv_file}")
        
        try:
            df = pd.read_csv(csv_file, encoding='utf-8-sig')
            print(f"Файл успешно прочитан с кодировкой UTF-8")
        except Exception as e:
            logger.error(f"Ошибка при чтении CSV с UTF-8: {e}")
            print(f"Ошибка при чтении CSV с UTF-8: {e}")
            print("Пробуем альтернативную кодировку...")
            df = pd.read_csv(csv_file, encoding='latin1')
            print(f"Файл успешно прочитан с кодировкой Latin-1")
        
        # Проверяем наличие данных
        if df.empty:
            logger.warning("CSV файл не содержит данных")
            print("CSV файл не содержит данных")
            return False
        
        logger.info(f"Загружено {len(df)} строк из CSV файла")
        print(f"Загружено {len(df)} строк из CSV файла")
        
        # Очищаем данные от проблемных значений
        # Заменяем NaN на None для корректной работы с базой данных
        df = df.replace({np.nan: None})
        
        # Очистка текстовых данных
        for col in df.columns:
            if df[col].dtype == 'object':  # Строковый тип
                df[col] = df[col].apply(lambda x: clean_text(x) if x is not None else None)
        
        # Удаляем дубликаты по ID
        duplicate_count = len(df) - len(df.drop_duplicates(subset=['id']))
        if duplicate_count > 0:
            print(f"Обнаружено {duplicate_count} дубликатов по ID, они будут удалены")
            df = df.drop_duplicates(subset=['id'])
        
        # Подключаемся к базе данных
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False  # Начинаем транзакцию
        cursor = conn.cursor()
        
        # Загружаем данные
        success_count = 0
        error_count = 0
        
        for idx, row in df.iterrows():
            try:
                # Проверка обязательных полей
                if pd.isna(row['id']) or not row['id']:
                    logger.warning(f"Пропуск строки {idx+1}: отсутствует ID")
                    error_count += 1
                    continue
                
                # Приводим типы данных с обработкой исключений
                try:
                    row_id = int(row['id']) if row['id'] is not None else None
                except (ValueError, TypeError):
                    logger.warning(f"Ошибка преобразования ID в строке {idx+1}, значение: {row['id']}")
                    error_count += 1
                    continue
                
                try:
                    price = float(row['price']) if row['price'] is not None else None
                except (ValueError, TypeError):
                    price = None
                
                try:
                    rooms = int(float(row['rooms'])) if row['rooms'] is not None else None
                except (ValueError, TypeError):
                    rooms = None
                
                try:
                    baths = int(float(row['baths'])) if row['baths'] is not None else None
                except (ValueError, TypeError):
                    baths = None
                
                try:
                    area = float(row['area']) if row['area'] is not None else None
                except (ValueError, TypeError):
                    area = None
                
                # Приводим булевы значения
                is_verified = None
                if row['is_verified'] is not None:
                    if isinstance(row['is_verified'], bool):
                        is_verified = row['is_verified']
                    elif isinstance(row['is_verified'], str):
                        is_verified = row['is_verified'].lower() in ('true', 't', 'yes', 'y', '1')
                
                # Числовые поля, которые могут быть None
                try:
                    floor_number = int(float(row['floor_number'])) if row['floor_number'] is not None else None
                except (ValueError, TypeError):
                    floor_number = None
                
                try:
                    city_level_score = int(float(row['city_level_score'])) if row['city_level_score'] is not None else None
                except (ValueError, TypeError):
                    city_level_score = None
                
                try:
                    score = int(float(row['score'])) if row['score'] is not None else None
                except (ValueError, TypeError):
                    score = None
                
                try:
                    agency_rating = float(row['agency_rating']) if row['agency_rating'] is not None else None
                except (ValueError, TypeError):
                    agency_rating = None
                
                # Вставляем запись в таблицу с безопасным приведением типов времени и текста
                cursor.execute("""
                    INSERT INTO bayut_properties (
                        id, title, price, rooms, baths, area, rent_frequency,
                        location, cover_photo_url, property_url, category, property_type,
                        created_at, updated_at, furnishing_status, completion_status,
                        amenities, agency_name, contact_info, geography, agency_logo_url,
                        proxy_mobile, keywords, is_verified, purpose, floor_number,
                        city_level_score, score, agency_licenses, agency_rating
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s
                    )
                """, (
                    row_id, row['title'], price, rooms, baths, 
                    area, row['rent_frequency'], row['location'], row['cover_photo_url'], 
                    row['property_url'], row['category'], row['property_type'], 
                    row['created_at'], row['updated_at'], row['furnishing_status'], 
                    row['completion_status'], row['amenities'], row['agency_name'], 
                    row['contact_info'], row['geography'], row['agency_logo_url'], 
                    row['proxy_mobile'], row['keywords'], is_verified, row['purpose'], 
                    floor_number, city_level_score, score, 
                    row['agency_licenses'], agency_rating
                ))
                success_count += 1
                
                # Фиксируем каждые 100 записей, чтобы избежать длинных транзакций
                if success_count % 100 == 0:
                    conn.commit()
                    print(f"Обработано {success_count} записей...")
                
            except Exception as e:
                error_count += 1
                # Ограничиваем вывод ошибок для экономии места в логе
                if error_count <= 10:
                    logger.warning(f"Ошибка в строке {idx+1}, ID={row.get('id', 'None')}: {e}")
                    print(f"Ошибка в строке {idx+1}, ID={row.get('id', 'None')}: {e}")
                # Если ошибка связана с дубликатом ключа, делаем коммит и продолжаем
                if "duplicate key value violates unique constraint" in str(e):
                    conn.commit()
                    continue
        
        # Фиксируем итоговые изменения
        conn.commit()
        logger.info(f"Транзакция зафиксирована: {success_count} записей добавлено, {error_count} ошибок")
        print(f"Транзакция зафиксирована: {success_count} записей добавлено, {error_count} ошибок")
        
        return success_count > 0
            
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных: {e}")
        print(f"Ошибка при загрузке данных: {e}")
        if conn and not conn.closed:
            try:
                conn.rollback()
            except:
                pass
        return False
    finally:
        if cursor:
            try:
                cursor.close()
            except:
                pass
        if conn and not conn.closed:
            try:
                conn.close()
            except:
                pass

def main():
    """Основная функция скрипта"""
    print("Запуск процесса пересоздания таблицы и загрузки данных")
    
    # Пересоздаем таблицу
    if not recreate_table():
        print("Ошибка при пересоздании таблицы")
        return 1
    
    # Находим самый новый CSV файл
    csv_files = []
    for root, _, files in os.walk(CSV_DIR):
        for file in files:
            if file.endswith('.csv'):
                csv_files.append(os.path.join(root, file))
    
    if not csv_files:
        logger.error("CSV файлы не найдены")
        print("CSV файлы не найдены")
        return 1
    
    # Сортируем по дате изменения (самый новый первый)
    csv_files.sort(key=os.path.getmtime, reverse=True)
    csv_file = csv_files[0]
    logger.info(f"Выбран самый новый CSV файл: {csv_file}")
    print(f"Выбран самый новый CSV файл: {csv_file}")
    
    # Загружаем данные
    if load_data_from_csv(csv_file):
        print("Процесс успешно завершен")
        return 0
    else:
        print("Процесс завершен с ошибками")
        return 1

if __name__ == "__main__":
    main() 