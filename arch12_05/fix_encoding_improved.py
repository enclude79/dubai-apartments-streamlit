import os
import pandas as pd
import psycopg2
import psycopg2.extras
import logging
import chardet
import numpy as np
import re
import csv
import sys
import traceback
from datetime import datetime

# Настройка логирования
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = f'{log_dir}/fix_encoding_improved_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

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

def detect_encoding(file_path):
    """Определяет кодировку файла"""
    with open(file_path, 'rb') as f:
        raw_data = f.read(10000)  # Читаем первые 10000 байт для определения кодировки
        result = chardet.detect(raw_data)
        logger.info(f"Определена кодировка файла: {result['encoding']} с уверенностью {result['confidence']}")
        return result['encoding']

def normalize_column_name(col_name):
    """Нормализует название столбца для использования в SQL-запросах"""
    if not isinstance(col_name, str):
        return f"column_{str(col_name).lower()}"
    # Заменяем пробелы и специальные символы на подчеркивания
    normalized = re.sub(r'[^a-zA-Z0-9_]', '_', col_name)
    # Убираем множественные подчеркивания
    normalized = re.sub(r'_+', '_', normalized)
    # Убираем подчеркивания в начале и конце
    normalized = normalized.strip('_')
    return normalized.lower()

def preprocess_csv_file(csv_file):
    """Предобработка CSV файла с исправлением кодировки"""
    try:
        # Определение кодировки
        detected_encoding = detect_encoding(csv_file)
        logger.info(f"Определена кодировка файла: {detected_encoding}")
        print(f"Определена кодировка файла: {detected_encoding}")
        
        # Пробуем различные кодировки, если автоопределение не сработало
        encodings_to_try = [detected_encoding, 'utf-8', 'utf-8-sig', 'cp1251', 'latin1', 'windows-1252']
        
        df = None
        for encoding in encodings_to_try:
            try:
                logger.info(f"Попытка чтения файла с кодировкой {encoding}")
                print(f"Попытка чтения файла с кодировкой {encoding}")
                df = pd.read_csv(csv_file, encoding=encoding, low_memory=False, on_bad_lines='skip')
                logger.info(f"Успешно прочитан файл с кодировкой {encoding}")
                print(f"Успешно прочитан файл с кодировкой {encoding}")
                break
            except Exception as e:
                logger.warning(f"Не удалось прочитать файл с кодировкой {encoding}: {e}")
                print(f"Не удалось прочитать файл с кодировкой {encoding}: {e}")
        
        if df is None:
            logger.error("Не удалось прочитать файл ни с одной из кодировок")
            print("Не удалось прочитать файл ни с одной из кодировок")
            return None
            
        # Нормализуем названия столбцов для совместимости с SQL
        original_columns = df.columns.tolist()
        df.columns = [normalize_column_name(col) for col in df.columns]
        logger.info(f"Нормализованы имена столбцов: {list(zip(original_columns, df.columns))}")
        
        # Очистка данных
        logger.info("Начинаем предобработку данных...")
        print("Начинаем предобработку данных...")
        
        # 1. Заменяем NaN на None для корректной конвертации в SQL
        df = df.replace({np.nan: None})
        
        # 2. Очистка строковых полей от проблемных символов
        string_columns = df.select_dtypes(include=['object']).columns
        for col in string_columns:
            if col in df.columns:
                # Заменяем None на пустую строку только для обработки и обратно
                df[col] = df[col].apply(lambda x: '' if pd.isna(x) else x)
                # Используем двойное преобразование для исправления кодировки
                df[col] = df[col].apply(lambda x: x.encode('cp1251', 'ignore').decode('cp1251', 'ignore').encode('utf-8', 'ignore').decode('utf-8') if isinstance(x, str) else x)
                # Заменяем пустые строки обратно на None
                df[col] = df[col].apply(lambda x: None if x == '' else x)
        
        # 3. Обработка числовых полей: преобразуем строковые значения в числовые, где это возможно
        numeric_cols = ['price', 'rooms', 'baths', 'area', 'floor_number', 'city_level_score', 'score', 'agency_rating',
                       'price', 'id', 'bathrooms', 'floor']
        for col in df.columns:
            if col in numeric_cols or any(num_col in col for num_col in ['price', 'id', 'rooms', 'baths', 'area', 'floor', 'score', 'rating']):
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                except:
                    logger.warning(f"Не удалось преобразовать столбец {col} в числовой")
        
        # 4. Преобразуем даты в правильный формат
        date_cols = ['created_at', 'updated_at', 'creation_date', 'update_date']
        for col in df.columns:
            if col in date_cols or any(date_col in col for date_col in ['date', 'time']):
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                except:
                    logger.warning(f"Не удалось преобразовать столбец {col} в дату")
        
        # 5. Обработка булевых полей
        bool_cols = ['is_verified', 'verified', 'is_active']
        for col in df.columns:
            if col in bool_cols or any(bool_col in col for bool_col in ['is_', 'has_', 'can_']):
                df[col] = df[col].map({'True': True, 'False': False, 'true': True, 'false': False, 
                                      '1': True, '0': False, 1: True, 0: False}, na_action='ignore')
        
        return df
            
    except Exception as e:
        logger.error(f"Ошибка при предобработке CSV файла: {e}")
        print(f"Ошибка при предобработке CSV файла: {e}")
        # Выводим полный стек ошибки
        logger.error(traceback.format_exc())
        return None

def recreate_table():
    """Пересоздаёт таблицу bayut_properties без первичного ключа"""
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        # Установка кодировки соединения
        conn.set_client_encoding('UTF8')
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Проверяем текущую кодировку клиента
        cursor.execute("SHOW client_encoding;")
        client_encoding = cursor.fetchone()[0]
        logger.info(f"Текущая кодировка клиента: {client_encoding}")
        print(f"Текущая кодировка клиента: {client_encoding}")
        
        # Проверяем кодировку сервера
        cursor.execute("SHOW server_encoding;")
        server_encoding = cursor.fetchone()[0]
        logger.info(f"Кодировка сервера: {server_encoding}")
        print(f"Кодировка сервера: {server_encoding}")
        
        # Удаляем существующую таблицу, если она есть
        cursor.execute("DROP TABLE IF EXISTS bayut_properties")
        logger.info("Таблица bayut_properties удалена")
        print("Таблица bayut_properties удалена")
        
        # Создаем новую таблицу без PRIMARY KEY для id
        cursor.execute("""
            CREATE TABLE bayut_properties (
                id BIGINT,
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
        
        # Создаем индекс для ускорения поиска
        cursor.execute("CREATE INDEX idx_bayut_properties_id ON bayut_properties(id)")
        
        logger.info("Таблица bayut_properties создана заново (без первичного ключа)")
        print("Таблица bayut_properties создана заново (без первичного ключа)")
        
        cursor.close()
        return True
    except Exception as e:
        logger.error(f"Ошибка при пересоздании таблицы: {e}")
        print(f"Ошибка при пересоздании таблицы: {e}")
        # Выводим полный стек ошибки
        logger.error(traceback.format_exc())
        return False
    finally:
        if conn:
            conn.close()

def load_data_to_db(df):
    """Загружает предобработанные данные в базу данных"""
    conn = None
    cursor = None
    try:
        # Подключаемся к базе данных
        conn = psycopg2.connect(**DB_CONFIG)
        # Установка кодировки соединения
        conn.set_client_encoding('UTF8')
        conn.autocommit = False  # Используем явные транзакции
        cursor = conn.cursor()
        
        # Проверяем кодировку клиента
        cursor.execute("SHOW client_encoding;")
        logger.info(f"Кодировка клиента при загрузке: {cursor.fetchone()[0]}")
        
        logger.info(f"Загружаем {len(df)} строк в БД")
        print(f"Загружаем {len(df)} строк в БД")
        
        # Получаем список колонок в правильном порядке
        cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'bayut_properties' ORDER BY ordinal_position")
        db_columns = [row[0] for row in cursor.fetchall()]
        
        # Создаем маппинг между колонками DataFrame и колонками БД
        df_columns = df.columns.tolist()
        column_mapping = {}
        
        for db_col in db_columns:
            # Проверяем точное совпадение
            if db_col in df_columns:
                column_mapping[db_col] = db_col
            else:
                # Проверяем совпадение по нормализованным именам
                normalized_db_col = normalize_column_name(db_col)
                for df_col in df_columns:
                    normalized_df_col = normalize_column_name(df_col)
                    if normalized_db_col == normalized_df_col:
                        column_mapping[db_col] = df_col
                        break
        
        logger.info(f"Найдено соответствие колонок: {column_mapping}")
        
        # Проверяем наличие всех колонок в DataFrame
        missing_columns = [col for col in db_columns if col not in column_mapping]
        if missing_columns:
            logger.warning(f"В DataFrame отсутствуют следующие колонки: {missing_columns}")
            print(f"В DataFrame отсутствуют следующие колонки: {missing_columns}")
        
        # Создаем DataFrame с нужными колонками в правильном порядке
        # Для отсутствующих колонок устанавливаем None
        new_df = pd.DataFrame()
        for db_col in db_columns:
            if db_col in column_mapping:
                new_df[db_col] = df[column_mapping[db_col]]
            else:
                new_df[db_col] = None
        
        # Обрабатываем данные пакетами по 100 строк
        batch_size = 100
        total_rows = len(new_df)
        processed_rows = 0
        
        for i in range(0, total_rows, batch_size):
            batch_df = new_df.iloc[i:i+batch_size]
            values = []
            
            # Формируем список значений для вставки
            for _, row in batch_df.iterrows():
                row_values = []
                for val in row:
                    if pd.isna(val):  # Обработка NaN и None
                        row_values.append(None)
                    elif isinstance(val, str):
                        # Удаляем невалидные символы UTF-8
                        clean_val = val.encode('utf-8', 'ignore').decode('utf-8')
                        row_values.append(clean_val)
                    else:
                        row_values.append(val)
                values.append(row_values)
            
            # Формируем SQL запрос для вставки с placeholders
            placeholders = ",".join(["%s"] * len(db_columns))
            column_names = ",".join([f'"{col}"' for col in db_columns])
            insert_query = f'INSERT INTO bayut_properties ({column_names}) VALUES ({placeholders})'
            
            # Выполняем вставку с помощью execute_batch для оптимизации
            psycopg2.extras.execute_batch(cursor, insert_query, values)
            
            processed_rows += len(batch_df)
            logger.info(f"Обработано {processed_rows} из {total_rows} строк ({processed_rows/total_rows*100:.2f}%)")
            print(f"Обработано {processed_rows} из {total_rows} строк ({processed_rows/total_rows*100:.2f}%)")
        
        # Фиксируем изменения
        conn.commit()
        
        # Проверяем количество вставленных строк
        cursor.execute("SELECT COUNT(*) FROM bayut_properties")
        row_count = cursor.fetchone()[0]
        
        logger.info(f"Данные успешно загружены в БД. Всего строк: {row_count}")
        print(f"Данные успешно загружены в БД. Всего строк: {row_count}")
        
        # Выводим первые 3 записи для проверки
        if row_count > 0:
            cursor.execute("""
                SELECT id, title, price, rooms, baths, area, location
                FROM bayut_properties
                LIMIT 3
            """)
            
            rows = cursor.fetchall()
            logger.info("Примеры загруженных данных (первые 3 записи):")
            print("\nПримеры загруженных данных (первые 3 записи):")
            for row in rows:
                logger.info(f"ID: {row[0]}, Название: {row[1]}")
                logger.info(f"Цена: {row[2]}, Комнат: {row[3]}, Ванных: {row[4]}, Площадь: {row[5]}")
                logger.info(f"Расположение: {row[6]}")
                logger.info("---")
                
                print(f"ID: {row[0]}, Название: {row[1]}")
                print(f"Цена: {row[2]}, Комнат: {row[3]}, Ванных: {row[4]}, Площадь: {row[5]}")
                print(f"Расположение: {row[6]}")
                print("---")
        
        return True
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Ошибка при загрузке данных в БД: {e}")
        print(f"Ошибка при загрузке данных в БД: {e}")
        # Выводим полную информацию об ошибке для отладки
        logger.error(traceback.format_exc())
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            logger.info("Соединение с базой данных закрыто")
            print("Соединение с базой данных закрыто")

def main():
    """Основная функция скрипта"""
    print("Запуск процесса исправления кодировки и загрузки данных")
    
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
    
    # Предобработка CSV файла
    df = preprocess_csv_file(csv_file)
    if df is None:
        logger.error("Не удалось выполнить предобработку CSV файла")
        print("Не удалось выполнить предобработку CSV файла")
        return 1
    
    # Загрузка данных в БД
    if load_data_to_db(df):
        print("Процесс успешно завершен")
        return 0
    else:
        print("Процесс завершен с ошибками")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 