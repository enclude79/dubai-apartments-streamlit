import os
import pandas as pd
import psycopg2
import logging
from datetime import datetime
import chardet

# Настройка логирования
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = f'{log_dir}/analyze_fields_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

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
        return result['encoding']

def get_csv_columns(csv_file):
    """Получает названия колонок из CSV файла"""
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
                df = pd.read_csv(csv_file, encoding=encoding, low_memory=False, nrows=1)
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
            
        return list(df.columns)
    except Exception as e:
        logger.error(f"Ошибка при получении колонок из CSV файла: {e}")
        print(f"Ошибка при получении колонок из CSV файла: {e}")
        return None

def get_sql_columns():
    """Получает названия колонок из SQL таблицы"""
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        # Установка кодировки соединения
        conn.set_client_encoding('UTF8')
        cursor = conn.cursor()
        
        # Получаем список колонок
        cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'bayut_properties'
            ORDER BY ordinal_position
        """)
        
        columns = [(row[0], row[1]) for row in cursor.fetchall()]
        
        cursor.close()
        return columns
    except Exception as e:
        logger.error(f"Ошибка при получении колонок из SQL таблицы: {e}")
        print(f"Ошибка при получении колонок из SQL таблицы: {e}")
        return None
    finally:
        if conn:
            conn.close()

def analyze_sql_data():
    """Анализирует данные в SQL таблице"""
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        # Установка кодировки соединения
        conn.set_client_encoding('UTF8')
        conn.autocommit = True
        cursor = conn.cursor()
        
        # 1. Общее количество записей
        cursor.execute("SELECT COUNT(*) FROM bayut_properties")
        total_count = cursor.fetchone()[0]
        print(f"\n===== Анализ данных в таблице bayut_properties =====")
        print(f"Общее количество записей: {total_count}")
        
        # 2. Распределение по районам (топ-10)
        print(f"\n----- Топ-10 районов по количеству объектов -----")
        cursor.execute("""
            SELECT location, COUNT(*) as count
            FROM bayut_properties
            GROUP BY location
            ORDER BY count DESC
            LIMIT 10
        """)
        for row in cursor.fetchall():
            print(f"{row[0]}: {row[1]} объектов")
        
        # 3. Средняя цена по районам (топ-10)
        print(f"\n----- Средняя цена по районам (топ-10 самых дорогих) -----")
        cursor.execute("""
            SELECT location, ROUND(AVG(price)::numeric, 2) as avg_price, COUNT(*) as count
            FROM bayut_properties
            GROUP BY location
            HAVING COUNT(*) >= 5
            ORDER BY avg_price DESC
            LIMIT 10
        """)
        for row in cursor.fetchall():
            print(f"{row[0]}: {row[1]} AED (на основе {row[2]} объектов)")
        
        # 4. Распределение по количеству комнат
        print(f"\n----- Распределение по количеству комнат -----")
        cursor.execute("""
            SELECT rooms, COUNT(*) as count
            FROM bayut_properties
            WHERE rooms IS NOT NULL
            GROUP BY rooms
            ORDER BY rooms
        """)
        for row in cursor.fetchall():
            print(f"{row[0]} комнат: {row[1]} объектов")
        
        # 5. Средняя площадь по количеству комнат
        print(f"\n----- Средняя площадь по количеству комнат -----")
        cursor.execute("""
            SELECT rooms, ROUND(AVG(area)::numeric, 2) as avg_area
            FROM bayut_properties
            WHERE rooms IS NOT NULL AND area IS NOT NULL
            GROUP BY rooms
            ORDER BY rooms
        """)
        for row in cursor.fetchall():
            print(f"{row[0]} комнат: {row[1]} кв.м.")
        
        # 6. Топ-10 самых дешевых квартир
        print(f"\n----- Топ-10 самых дешевых квартир -----")
        cursor.execute("""
            SELECT id, title, location, price, rooms, area
            FROM bayut_properties
            WHERE price > 0
            ORDER BY price
            LIMIT 10
        """)
        for row in cursor.fetchall():
            print(f"ID: {row[0]}, {row[1]}")
            print(f"Район: {row[2]}, Цена: {row[3]} AED, Комнат: {row[4]}, Площадь: {row[5]} кв.м.")
            print("---")
        
        # 7. Статистика по ценам
        print(f"\n----- Статистика по ценам -----")
        cursor.execute("""
            SELECT 
                MIN(price) as min_price,
                MAX(price) as max_price,
                ROUND(AVG(price)::numeric, 2) as avg_price,
                ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)::numeric, 2) as median_price
            FROM bayut_properties
            WHERE price > 0
        """)
        row = cursor.fetchone()
        print(f"Минимальная цена: {row[0]} AED")
        print(f"Максимальная цена: {row[1]} AED")
        print(f"Средняя цена: {row[2]} AED")
        print(f"Медианная цена: {row[3]} AED")
        
        # 8. Объекты по типу недвижимости
        print(f"\n----- Распределение по типу недвижимости -----")
        cursor.execute("""
            SELECT property_type, COUNT(*) as count
            FROM bayut_properties
            GROUP BY property_type
            ORDER BY count DESC
        """)
        for row in cursor.fetchall():
            print(f"{row[0]}: {row[1]} объектов")
        
        cursor.close()
        
    except Exception as e:
        logger.error(f"Ошибка при анализе данных: {e}")
        print(f"Ошибка при анализе данных: {e}")
    finally:
        if conn:
            conn.close()

def compare_fields():
    """Сравнивает поля в CSV и SQL"""
    # Находим самый новый CSV файл
    csv_files = []
    for root, _, files in os.walk(CSV_DIR):
        for file in files:
            if file.endswith('.csv'):
                csv_files.append(os.path.join(root, file))
    
    if not csv_files:
        logger.error("CSV файлы не найдены")
        print("CSV файлы не найдены")
        return
    
    # Сортируем по дате изменения (самый новый первый)
    csv_files.sort(key=os.path.getmtime, reverse=True)
    csv_file = csv_files[0]
    logger.info(f"Выбран самый новый CSV файл: {csv_file}")
    print(f"Выбран самый новый CSV файл: {csv_file}")
    
    # Получаем колонки из CSV
    csv_columns = get_csv_columns(csv_file)
    if not csv_columns:
        return
    
    # Получаем колонки из SQL
    sql_columns = get_sql_columns()
    if not sql_columns:
        return
    
    # Выводим информацию о колонках
    print("\n===== Колонки в CSV файле =====")
    for i, col in enumerate(csv_columns):
        print(f"{i+1}. {col}")
    
    print("\n===== Колонки в SQL таблице =====")
    for i, (col, data_type) in enumerate(sql_columns):
        print(f"{i+1}. {col} ({data_type})")
    
    # Сравниваем колонки
    sql_col_names = [col[0] for col in sql_columns]
    
    # Находим колонки, которые есть в CSV, но отсутствуют в SQL
    missing_in_sql = [col for col in csv_columns if col.lower() not in [c.lower() for c in sql_col_names]]
    if missing_in_sql:
        print("\n===== Колонки, которые есть в CSV, но отсутствуют в SQL =====")
        for col in missing_in_sql:
            print(f"- {col}")
    
    # Находим колонки, которые есть в SQL, но отсутствуют в CSV
    missing_in_csv = [col for col in sql_col_names if col.lower() not in [c.lower() for c in csv_columns]]
    if missing_in_csv:
        print("\n===== Колонки, которые есть в SQL, но отсутствуют в CSV =====")
        for col in missing_in_csv:
            print(f"- {col}")
    
    # Анализируем соответствие колонок по смыслу
    original_names = {
        'id': 'ID объекта',
        'title': 'Название объекта',
        'price': 'Цена',
        'rooms': 'Количество комнат',
        'baths': 'Количество ванных комнат',
        'area': 'Площадь',
        'rent_frequency': 'Частота аренды',
        'location': 'Местоположение',
        'cover_photo_url': 'URL обложки',
        'property_url': 'URL объекта',
        'category': 'Категория',
        'property_type': 'Тип недвижимости',
        'created_at': 'Дата создания',
        'updated_at': 'Дата обновления',
        'furnishing_status': 'Статус мебели',
        'completion_status': 'Статус завершения',
        'amenities': 'Удобства',
        'agency_name': 'Название агентства',
        'contact_info': 'Контактная информация',
        'geography': 'География',
        'agency_logo_url': 'URL логотипа агентства',
        'proxy_mobile': 'Прокси-мобильный',
        'keywords': 'Ключевые слова',
        'is_verified': 'Проверено',
        'purpose': 'Назначение',
        'floor_number': 'Номер этажа',
        'city_level_score': 'Оценка уровня города',
        'score': 'Оценка',
        'agency_licenses': 'Лицензии агентства',
        'agency_rating': 'Рейтинг агентства'
    }
    
    print("\n===== Соответствие полей API =====")
    for i, (col, data_type) in enumerate(sql_columns):
        original_name = original_names.get(col, "Неизвестно")
        print(f"{i+1}. {col} - {original_name} ({data_type})")
    
    # Проводим анализ данных в SQL
    analyze_sql_data()

if __name__ == "__main__":
    compare_fields() 