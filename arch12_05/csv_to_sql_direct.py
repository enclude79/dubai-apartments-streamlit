import os
import pandas as pd
import psycopg2
import logging
from datetime import datetime
import json

# Настройка логирования
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = f'{log_dir}/csv_to_sql_direct_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

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
            self.connection.autocommit = True
            logger.info("Успешное подключение к базе данных")
            print("Успешное подключение к базе данных")
            return True
        except Exception as e:
            logger.error(f"Ошибка при подключении к базе данных: {e}")
            print(f"Ошибка при подключении к базе данных: {e}")
            return False
    
    def close(self):
        """Закрывает подключение к базе данных"""
        if self.connection:
            self.connection.close()
            logger.info("Подключение к базе данных закрыто")

class CsvToSqlLoader:
    """Класс для загрузки данных из CSV в SQL"""
    
    def __init__(self, db_config):
        self.db_config = db_config
        self.db = DatabaseConnection(db_config)
    
    def clean_text(self, text):
        """Очищает текст от проблемных символов"""
        if not isinstance(text, str):
            return str(text) if text is not None else None
        
        try:
            # Удаляем BOM и другие специальные символы
            text = text.replace('\ufeff', '')
            # Заменяем символы Unicode выше 0xFFFF на пробелы
            cleaned_text = ''.join(c if ord(c) < 65536 else ' ' for c in text)
            return cleaned_text
        except:
            # В случае ошибки просто удаляем все не-ASCII символы
            return str(text).encode('ascii', 'ignore').decode('ascii')
    
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
            
            # Печатаем названия столбцов
            print("Названия столбцов в CSV:")
            for col in df.columns:
                print(f"  - {col}")
            
            # Проверяем наличие необходимых полей
            required_fields = ['id']
            missing_fields = [field for field in required_fields if field not in df.columns]
            if missing_fields:
                logger.error(f"В CSV файле отсутствуют обязательные поля: {', '.join(missing_fields)}")
                print(f"В CSV файле отсутствуют обязательные поля: {', '.join(missing_fields)}")
                return False
            
            # Очистка текстовых данных
            for col in df.columns:
                if df[col].dtype == 'object':  # Строковый тип
                    df[col] = df[col].apply(self.clean_text)
            
            # Проверяем существование таблицы и создаем при необходимости
            cursor = self.db.connection.cursor()
            
            try:
                # Проверяем существование таблицы
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'bayut_properties'
                    )
                """)
                table_exists = cursor.fetchone()[0]
                
                if not table_exists:
                    # Создаем таблицу, если она не существует
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
                    print("Таблица bayut_properties создана")
            except Exception as e:
                logger.error(f"Ошибка при проверке/создании таблицы: {e}")
                print(f"Ошибка при проверке/создании таблицы: {e}")
                return False
            finally:
                cursor.close()
            
            # Открываем новую транзакцию для загрузки данных
            self.db.connection.autocommit = False
            cursor = self.db.connection.cursor()
            
            try:
                # Создаем временную таблицу
                cursor.execute("""
                    CREATE TEMP TABLE temp_properties (
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
                print("Временная таблица создана")
                
                # Загружаем данные во временную таблицу
                success_count = 0
                error_count = 0
                
                for idx, row in df.iterrows():
                    try:
                        # Безопасно преобразуем типы данных
                        row_id = row.get('id')
                        if row_id is None:
                            # Пропускаем строки без ID
                            continue
                            
                        # Преобразуем строковые значения timestamp в правильный формат
                        created_at = row.get('created_at')
                        updated_at = row.get('updated_at')
                        
                        try:
                            # Преобразуем is_verified из строки в булево значение
                            is_verified = row.get('is_verified')
                            if isinstance(is_verified, str):
                                is_verified = is_verified.lower() in ('true', 't', 'yes', 'y', '1')
                            elif not isinstance(is_verified, bool):
                                is_verified = None
                        except:
                            is_verified = None
                            
                        # Вставляем запись во временную таблицу
                        cursor.execute("""
                            INSERT INTO temp_properties (
                                id, title, price, rooms, baths, area, rent_frequency,
                                location, cover_photo_url, property_url, category, property_type,
                                created_at, updated_at, furnishing_status, completion_status,
                                amenities, agency_name, contact_info, geography, agency_logo_url,
                                proxy_mobile, keywords, is_verified, purpose, floor_number,
                                city_level_score, score, agency_licenses, agency_rating
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, 
                                %s, %s, %s, %s, %s, 
                                NULLIF(%s,'')::timestamp, NULLIF(%s,'')::timestamp, %s, %s, 
                                %s, %s, %s, %s, %s, 
                                %s, %s, %s, %s, NULLIF(%s,'')::integer, 
                                NULLIF(%s,'')::integer, NULLIF(%s,'')::integer, %s, NULLIF(%s,'')::numeric
                            )
                        """, (
                            row_id, row.get('title'), row.get('price'), row.get('rooms'), row.get('baths'), 
                            row.get('area'), row.get('rent_frequency'), row.get('location'), row.get('cover_photo_url'), 
                            row.get('property_url'), row.get('category'), row.get('property_type'), 
                            created_at, updated_at, row.get('furnishing_status'), 
                            row.get('completion_status'), row.get('amenities'), row.get('agency_name'), 
                            row.get('contact_info'), row.get('geography'), row.get('agency_logo_url'), 
                            row.get('proxy_mobile'), row.get('keywords'), is_verified, row.get('purpose'), 
                            row.get('floor_number'), row.get('city_level_score'), row.get('score'), 
                            row.get('agency_licenses'), row.get('agency_rating')
                        ))
                        success_count += 1
                    except Exception as e:
                        error_count += 1
                        error_msg = str(e)
                        # Ограничиваем вывод ошибок
                        if error_count <= 5:
                            logger.warning(f"Ошибка в строке {idx+1}, ID={row.get('id', 'None')}: {error_msg}")
                            print(f"Ошибка в строке {idx+1}, ID={row.get('id', 'None')}: {error_msg}")
                
                print(f"Обработано строк: {success_count} успешно, {error_count} с ошибками")
                
                if success_count == 0:
                    logger.error("Не удалось добавить ни одной записи")
                    print("Не удалось добавить ни одной записи")
                    self.db.connection.rollback()
                    return False
                
                # Выполняем вставку или обновление данных
                cursor.execute("""
                    INSERT INTO bayut_properties
                    SELECT * FROM temp_properties
                    ON CONFLICT (id) DO UPDATE SET
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
                        created_at = EXCLUDED.created_at,
                        updated_at = EXCLUDED.updated_at,
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
                """)
                
                # Фиксируем транзакцию
                self.db.connection.commit()
                print("Данные успешно загружены в базу данных")
                
                # Получаем статистику
                cursor.execute("SELECT COUNT(*) FROM bayut_properties")
                total_count = cursor.fetchone()[0]
                print(f"Всего записей в базе: {total_count}")
                
                return True
                
            except Exception as e:
                self.db.connection.rollback()
                logger.error(f"Ошибка при загрузке данных в базу: {e}")
                print(f"Ошибка при загрузке данных в базу: {e}")
                return False
            finally:
                # Удаляем временную таблицу и закрываем курсор
                try:
                    cursor.execute("DROP TABLE IF EXISTS temp_properties")
                    cursor.close()
                except:
                    pass
                # Восстанавливаем режим автокоммита
                self.db.connection.autocommit = True
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных из CSV: {e}")
            print(f"Ошибка при загрузке данных из CSV: {e}")
            return False
        finally:
            # Закрываем подключение к базе
            self.db.close()

def main(csv_file=None):
    """Основная функция скрипта"""
    # Если файл не указан, ищем самый новый CSV файл
    if not csv_file:
        csv_dir = "Api_Bayat"
        if not os.path.exists(csv_dir):
            csv_dir = "."  # Если директория не найдена, ищем в текущей
        
        # Ищем CSV-файлы
        csv_files = []
        for root, _, files in os.walk(csv_dir):
            for file in files:
                if file.endswith('.csv'):
                    csv_files.append(os.path.join(root, file))
        
        if not csv_files:
            logger.error("CSV файлы не найдены")
            print("CSV файлы не найдены")
            return False
        
        # Сортируем по дате изменения (самый новый первый)
        csv_files.sort(key=os.path.getmtime, reverse=True)
        csv_file = csv_files[0]
        logger.info(f"Выбран самый новый CSV файл: {csv_file}")
        print(f"Выбран самый новый CSV файл: {csv_file}")
    
    # Запускаем загрузку данных
    loader = CsvToSqlLoader(DB_CONFIG)
    result = loader.run(csv_file)
    
    if result:
        logger.info("Загрузка данных успешно завершена")
        print("Загрузка данных успешно завершена")
    else:
        logger.error("Загрузка данных завершилась с ошибками")
        print("Загрузка данных завершилась с ошибками")
    
    return result

if __name__ == "__main__":
    import sys
    # Если передан аргумент командной строки, используем его как путь к CSV файлу
    csv_file = sys.argv[1] if len(sys.argv) > 1 else None
    main(csv_file) 