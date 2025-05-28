import os
import pandas as pd
import psycopg2
import logging
from datetime import datetime
import json

# Настройка логирования
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = f'{log_dir}/csv_to_sql_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

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

class DatabaseConnection:
    """Класс для работы с подключением к базе данных"""
    
    def __init__(self, db_config):
        self.db_config = db_config
        self.connection = None
    
    def connect(self):
        """Подключается к базе данных"""
        try:
            self.connection = psycopg2.connect(
                dbname=self.db_config['dbname'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                host=self.db_config['host'],
                port=self.db_config['port']
            )
            self.connection.set_client_encoding('UTF8')
            self.connection.autocommit = True
            logger.info("Успешное подключение к базе данных")
            return True
        except Exception as e:
            logger.error(f"Ошибка при подключении к базе данных: {e}")
            return False
    
    def close(self):
        """Закрывает подключение к базе данных"""
        if self.connection:
            self.connection.close()
            logger.info("Подключение к базе данных закрыто")
    
    def execute_query(self, query, params=None):
        """Выполняет SQL запрос"""
        if not self.connection:
            logger.error("Нет активного подключения к базе данных")
            return False
        
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, params)
            logger.info("SQL запрос выполнен успешно")
            return True
        except Exception as e:
            logger.error(f"Ошибка при выполнении SQL запроса: {e}")
            return False
        finally:
            cursor.close()

class CsvToSqlLoader:
    """Класс для загрузки данных из CSV в SQL"""
    
    def __init__(self, db_config):
        self.db_config = db_config
        self.db = DatabaseConnection(db_config)
    
    def clean_text(self, text):
        """Очищает текст от символов, не поддерживаемых WIN1251 (cp1251)"""
        if not isinstance(text, str):
            return text
        try:
            # Перекодируем в cp1251, заменяя неподдерживаемые символы на '?'
            return text.encode('cp1251', errors='replace').decode('cp1251')
        except Exception:
            return text.encode('ascii', 'ignore').decode('ascii')
    
    def run(self, csv_file):
        """Запускает процесс загрузки данных из CSV в SQL"""
        if not os.path.exists(csv_file):
            logger.error(f"Файл не найден: {csv_file}")
            print(f"Файл не найден: {csv_file}")
            return False
        
        # Подключаемся к базе данных
        if not self.db.connect():
            return False
        
        try:
            # Загружаем данные из CSV
            logger.info(f"Загрузка данных из CSV файла: {csv_file}")
            print(f"Загрузка данных из CSV файла: {csv_file}")
            
            try:
                df = pd.read_csv(csv_file, encoding='utf-8-sig')
            except Exception as e:
                logger.error(f"Ошибка при чтении CSV с UTF-8: {e}")
                print(f"Ошибка при чтении CSV с UTF-8: {e}")
                print("Пробуем альтернативную кодировку...")
                df = pd.read_csv(csv_file, encoding='latin1')
            
            # Проверяем наличие данных
            if df.empty:
                logger.warning("CSV файл не содержит данных")
                print("CSV файл не содержит данных")
                return False
            
            logger.info(f"Загружено {len(df)} строк из CSV файла")
            print(f"Загружено {len(df)} строк из CSV файла")
            
            # Печатаем названия столбцов
            print("Названия столбцов в CSV:")
            for col in df.columns:
                print(f"  - {col}")
            
            # Очищаем текстовые данные от проблемных символов
            for col in df.columns:
                if df[col].dtype == 'object':  # Для строковых столбцов
                    df[col] = df[col].apply(self.clean_text)
            
            # Преобразуем boolean поля в строки
            if 'is_verified' in df.columns:
                df['is_verified'] = df['is_verified'].astype(str)
                print("Поле is_verified преобразовано в строковый тип")
            
            # Проверяем наличие всех необходимых столбцов
            required_columns = [
                'id', 'title', 'price', 'rooms', 'baths', 'area', 'rent_frequency',
                'location', 'cover_photo_url', 'property_url', 'category', 'property_type',
                'created_at', 'updated_at', 'furnishing_status', 'completion_status',
                'amenities', 'agency_name', 'contact_info', 'geography', 'agency_logo_url',
                'proxy_mobile', 'keywords', 'is_verified', 'purpose', 'floor_number',
                'city_level_score', 'score', 'agency_licenses', 'agency_rating'
            ]
            
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logger.error(f"Отсутствуют необходимые столбцы: {', '.join(missing_columns)}")
                print(f"Отсутствуют необходимые столбцы: {', '.join(missing_columns)}")
                return False
            
            # Удаляем дубликаты по (id, updated_at)
            df = df.drop_duplicates(subset=['id', 'updated_at'])
            
            # Добавляем данные в базу
            new_records = 0
            updated_records = 0
            errors = 0
            
            # Отключаем автокоммит для транзакции
            self.db.connection.autocommit = False
            cursor = self.db.connection.cursor()
            
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
                        logger.error(f"Ошибка при вставке данных: {e}")
                        print(f"Ошибка при вставке данных: {e}")
                
                # Фиксируем изменения в БД
                self.db.connection.commit()
                
                logger.info(f"Всего обработано записей: {len(df)}")
                logger.info(f"Добавлено новых записей: {new_records}")
                logger.info(f"Обновлено существующих записей: {updated_records}")
                
                print(f"Успешно добавлено: {new_records}, обновлено: {updated_records}, ошибок: {errors}")
                
                return True
                
            except Exception as e:
                self.db.connection.rollback()
                logger.error(f"Ошибка при загрузке данных из CSV: {e}")
                print(f"Ошибка при загрузке данных из CSV: {e}")
                return False
                
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных из CSV: {e}")
            print(f"Ошибка при загрузке данных из CSV: {e}")
            return False
            
        finally:
            self.db.close()

def main(csv_file=None):
    """Основная функция скрипта"""
    # Если файл не указан, ищем самый новый CSV файл
    if not csv_file:
        csv_dir = "Api_Bayat"
        if not os.path.exists(csv_dir):
            logger.error(f"Каталог не найден: {csv_dir}")
            print(f"Каталог не найден: {csv_dir}")
            return False
        
        # Ищем самый новый CSV файл
        csv_files = [f for f in os.listdir(csv_dir) if f.endswith('.csv')]
        if not csv_files:
            logger.error(f"В каталоге {csv_dir} не найдены CSV файлы")
            print(f"В каталоге {csv_dir} не найдены CSV файлы")
            return False
        
        csv_files.sort(reverse=True)  # Сортируем по имени (с датой)
        csv_file = os.path.join(csv_dir, csv_files[0])
        logger.info(f"Выбран самый новый CSV файл: {csv_file}")
        print(f"Выбран самый новый CSV файл: {csv_file}")
    
    # Проверяем существование файла
    if not os.path.exists(csv_file):
        logger.error(f"Файл не найден: {csv_file}")
        print(f"Файл не найден: {csv_file}")
        return False
    
    print(f"Загрузка файла: {csv_file}")
    
    # Запускаем загрузку данных
    loader = CsvToSqlLoader(DB_CONFIG)
    result = loader.run(csv_file)
    
    return result

if __name__ == "__main__":
    import sys
    # Если передан аргумент командной строки, используем его как путь к CSV файлу
    csv_file = sys.argv[1] if len(sys.argv) > 1 else None
    if main(csv_file):
        print("Данные успешно загружены в базу данных")
    else:
        print("Ошибка при загрузке данных в базу данных") 