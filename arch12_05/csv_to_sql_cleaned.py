import os
import pandas as pd
import psycopg2
import logging
from datetime import datetime
import json
import re

# Настройка логирования
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = f'{log_dir}/csv_to_sql_cleaned_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

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

class DataCleaner:
    """Класс для очистки и преобразования данных"""
    
    @staticmethod
    def clean_text(text):
        """Очищает текст от проблемных символов"""
        if not isinstance(text, str):
            return str(text) if text is not None else None
        
        # Заменяем эмодзи и другие проблемные символы на пробелы
        try:
            # Удаляем BOM и другие специальные символы
            text = text.replace('\ufeff', '')
            # Заменяем символы Unicode выше 0xFFFF на пробелы
            cleaned_text = ''.join(c if ord(c) < 65536 else ' ' for c in text)
            return cleaned_text
        except:
            # В случае ошибки просто удаляем все не-ASCII символы
            return str(text).encode('ascii', 'ignore').decode('ascii')
    
    @staticmethod
    def clean_json(json_str):
        """Очищает и исправляет строку JSON"""
        if not isinstance(json_str, str):
            return "[]" if json_str is None else str(json_str)
        
        try:
            # Заменяем одинарные кавычки на двойные
            json_str = json_str.replace("'", '"')
            # Проверяем, что JSON валидный
            json.loads(json_str)
            return json_str
        except:
            # Если не можем исправить, возвращаем пустой массив
            return "[]"
    
    @staticmethod
    def safe_int(value):
        """Безопасно преобразует значение в целое число"""
        if pd.isna(value) or value is None:
            return None
        try:
            return int(float(value))
        except:
            return None
    
    @staticmethod
    def safe_float(value):
        """Безопасно преобразует значение в число с плавающей точкой"""
        if pd.isna(value) or value is None:
            return None
        try:
            return float(value)
        except:
            return None
    
    @staticmethod
    def safe_bool(value):
        """Безопасно преобразует значение в логическое"""
        if pd.isna(value) or value is None:
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            return value.lower() in ('true', 't', 'yes', 'y', '1')
        return None

class CsvToSqlLoader:
    """Класс для загрузки данных из CSV в SQL"""
    
    def __init__(self, db_config):
        self.db_config = db_config
        self.db = DatabaseConnection(db_config)
        self.cleaner = DataCleaner()
        
        # Сопоставление русских названий столбцов с английскими
        self.column_mapping = {
            'ID': 'id',
            'Название': 'title',
            'Цена': 'price',
            'Комнат': 'rooms',
            'Ванных': 'baths',
            'Площадь': 'area',
            'Регион': 'rent_frequency',
            'Локация': 'location',
            'Фото': 'cover_photo_url',
            'Ссылка на объявление': 'property_url',
            'Категория': 'category',
            'Тип недвижимости': 'property_type',
            'Дата публикации': 'created_at',
            'Последнее обновление': 'updated_at',
            'Количество парковочных мест': 'furnishing_status',
            'Статус строительства': 'completion_status',
            'Особенности': 'amenities',
            'Описание': 'description',
            'Застройщик': 'agency_name',
            'Контакты': 'contact_info',
            'Координаты': 'geography',
            'Ссылка на застройщика': 'proxy_mobile',
            'Логотип застройщика': 'agency_logo_url',
            'Статус верификации': 'is_verified',
            'Ключевые слова': 'keywords',
            'Счетчик просмотров': 'purpose',
            'Счетчик фото': 'city_level_score',
            'Счетчик видео': 'score',
            'Счетчик панорам': 'property_count',
            'Счетчик этажей': 'floor_number',
            'Лицензии застройщика': 'agency_licenses',
            'Рейтинг застройщика': 'agency_rating'
        }
    
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
            
            # Определяем, используются ли русские названия столбцов
            using_russian = any(col in self.column_mapping for col in df.columns)
            print(f"Используются {'русские' if using_russian else 'английские'} названия столбцов")
            
            # Если используются русские названия, переименовываем столбцы
            if using_russian:
                # Создаем словарь переименования только для существующих столбцов
                rename_dict = {col: self.column_mapping[col] for col in df.columns if col in self.column_mapping}
                # Переименовываем столбцы
                df = df.rename(columns=rename_dict)
                print(f"Столбцы переименованы: {rename_dict}")
                # Печатаем новые названия столбцов
                print("Названия столбцов после переименования:")
                for col in df.columns:
                    print(f"  - {col}")
            
            # Очищаем и преобразуем данные
            print("Очистка и преобразование данных...")
            for col in df.columns:
                if df[col].dtype == 'object':  # Для строковых столбцов
                    # Очищаем текстовые данные
                    df[col] = df[col].apply(self.cleaner.clean_text)
            
            # Специально обрабатываем поля JSON
            if 'location' in df.columns:
                df['location'] = df['location'].apply(self.cleaner.clean_json)
            if 'keywords' in df.columns:
                df['keywords'] = df['keywords'].apply(self.cleaner.clean_json)
            if 'agency_licenses' in df.columns:
                df['agency_licenses'] = df['agency_licenses'].apply(self.cleaner.clean_json)
            
            # Специальные поля для преобразования
            if 'is_verified' in df.columns:
                df['is_verified'] = df['is_verified'].apply(self.cleaner.safe_bool)
                print("Поле is_verified преобразовано в логический тип")
            
            print("Данные очищены и подготовлены")
            
            # Проверяем наличие обязательных полей
            required_fields = ['id', 'title', 'price']
            missing_fields = [field for field in required_fields if field not in df.columns]
            if missing_fields:
                logger.error(f"В CSV файле отсутствуют обязательные поля: {', '.join(missing_fields)}")
                print(f"В CSV файле отсутствуют обязательные поля: {', '.join(missing_fields)}")
                print("Пожалуйста, убедитесь, что CSV файл имеет все необходимые поля.")
                return False
            
            # Добавляем данные в базу
            # Начинаем транзакцию
            self.db.connection.autocommit = False
            cursor = self.db.connection.cursor()
            
            try:
                # Создаем временную таблицу
                cursor.execute("""
                    CREATE TEMP TABLE temp_properties (
                        id INTEGER PRIMARY KEY,
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
                
                for _, row in df.iterrows():
                    try:
                        # Создаем словарь с данными для вставки, заполняя отсутствующие поля значениями по умолчанию
                        data = {
                            'id': self.cleaner.safe_int(row.get('id')),
                            'title': row.get('title', ''),
                            'price': self.cleaner.safe_float(row.get('price')),
                            'rooms': self.cleaner.safe_int(row.get('rooms')),
                            'baths': self.cleaner.safe_int(row.get('baths')),
                            'area': self.cleaner.safe_float(row.get('area')),
                            'rent_frequency': row.get('rent_frequency', ''),
                            'location': row.get('location', '[]'),
                            'cover_photo_url': row.get('cover_photo_url', ''),
                            'property_url': row.get('property_url', ''),
                            'category': row.get('category', ''),
                            'property_type': row.get('property_type', ''),
                            'created_at': row.get('created_at'),
                            'updated_at': row.get('updated_at'),
                            'furnishing_status': row.get('furnishing_status', ''),
                            'completion_status': row.get('completion_status', ''),
                            'amenities': row.get('amenities', ''),
                            'agency_name': row.get('agency_name', ''),
                            'contact_info': row.get('contact_info', ''),
                            'geography': row.get('geography', ''),
                            'agency_logo_url': row.get('agency_logo_url', ''),
                            'proxy_mobile': row.get('proxy_mobile', ''),
                            'keywords': row.get('keywords', '[]'),
                            'is_verified': self.cleaner.safe_bool(row.get('is_verified')),
                            'purpose': row.get('purpose', ''),
                            'floor_number': self.cleaner.safe_int(row.get('floor_number')),
                            'city_level_score': self.cleaner.safe_int(row.get('city_level_score')),
                            'score': self.cleaner.safe_int(row.get('score')),
                            'agency_licenses': row.get('agency_licenses', '[]'),
                            'agency_rating': self.cleaner.safe_float(row.get('agency_rating'))
                        }
                        
                        # Пропускаем строки без ID
                        if data['id'] is None:
                            logger.warning(f"Пропуск строки без ID")
                            error_count += 1
                            continue
                        
                        # Преобразование метки времени
                        created_at = data['created_at'] if isinstance(data['created_at'], str) else None
                        updated_at = data['updated_at'] if isinstance(data['updated_at'], str) else None
                        
                        cursor.execute("""
                            INSERT INTO temp_properties 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                   to_timestamp(%s, 'YYYY-MM-DD HH24:MI:SS'), 
                                   to_timestamp(%s, 'YYYY-MM-DD HH24:MI:SS'), 
                                   %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            data['id'], data['title'], data['price'], data['rooms'], data['baths'], 
                            data['area'], data['rent_frequency'], data['location'], data['cover_photo_url'], 
                            data['property_url'], data['category'], data['property_type'], 
                            created_at, updated_at, data['furnishing_status'], 
                            data['completion_status'], data['amenities'], data['agency_name'], 
                            data['contact_info'], data['geography'], data['agency_logo_url'], 
                            data['proxy_mobile'], data['keywords'], data['is_verified'], data['purpose'], 
                            data['floor_number'], data['city_level_score'], data['score'], 
                            data['agency_licenses'], data['agency_rating']
                        ))
                        success_count += 1
                    except Exception as e:
                        error_count += 1
                        if error_count < 10:  # Выводим только первые 10 ошибок
                            logger.warning(f"Ошибка при добавлении строки {row.get('id', 'Unknown')}: {e}")
                            print(f"Ошибка при добавлении строки {row.get('id', 'Unknown')}: {e}")
                
                print(f"Обработка строк: успешно {success_count}, ошибок {error_count}")
                
                if success_count == 0:
                    logger.error("Не удалось добавить ни одной записи")
                    print("Не удалось добавить ни одной записи")
                    self.db.connection.rollback()
                    return False
                
                # Выполняем слияние данных (уппсерт)
                try:
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
                    
                    print("Данные успешно добавлены в основную таблицу")
                except Exception as e:
                    logger.error(f"Ошибка при слиянии данных: {e}")
                    print(f"Ошибка при слиянии данных: {e}")
                    self.db.connection.rollback()
                    return False
                
                # Получаем статистику по вставке и обновлению
                cursor.execute("""
                    SELECT 
                        (SELECT COUNT(*) FROM temp_properties) as total,
                        (SELECT COUNT(*) FROM temp_properties t 
                         WHERE NOT EXISTS (SELECT 1 FROM bayut_properties WHERE id = t.id)) as inserted,
                        (SELECT COUNT(*) FROM temp_properties t 
                         WHERE EXISTS (SELECT 1 FROM bayut_properties WHERE id = t.id)) as updated
                """)
                
                stats = cursor.fetchone()
                total_count = stats[0]
                inserted_count = stats[1]
                updated_count = stats[2]
                
                # Фиксируем транзакцию
                self.db.connection.commit()
                logger.info(f"Всего обработано записей: {total_count}")
                logger.info(f"Добавлено новых записей: {inserted_count}")
                logger.info(f"Обновлено существующих записей: {updated_count}")
                
                print(f"Всего обработано записей: {total_count}")
                print(f"Добавлено новых записей: {inserted_count}")
                print(f"Обновлено существующих записей: {updated_count}")
                
            except Exception as e:
                # Откатываем транзакцию в случае ошибки
                self.db.connection.rollback()
                logger.error(f"Ошибка при добавлении данных в базу: {e}")
                print(f"Ошибка при добавлении данных в базу: {e}")
                return False
            finally:
                # Удаляем временную таблицу
                try:
                    cursor.execute("DROP TABLE IF EXISTS temp_properties")
                    cursor.close()
                except:
                    pass
                # Восстанавливаем режим автокоммита
                self.db.connection.autocommit = True
            
            # Перемещаем файл в архив
            try:
                archive_dir = os.path.join(os.path.dirname(csv_file), "archive")
                os.makedirs(archive_dir, exist_ok=True)
                
                filename = os.path.basename(csv_file)
                base, ext = os.path.splitext(filename)
                archive_path = os.path.join(archive_dir, f"{base}_{datetime.now().strftime('%Y%m%d_%H%M%S')}{ext}")
                
                # Копируем файл в архив и удаляем оригинал
                import shutil
                shutil.copy2(csv_file, archive_path)
                
                logger.info(f"Файл {csv_file} перемещен в архив: {archive_path}")
                print(f"Файл {csv_file} перемещен в архив: {archive_path}")
            except Exception as e:
                logger.warning(f"Не удалось переместить файл в архив: {e}")
                print(f"Не удалось переместить файл в архив: {e}")
            
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных из CSV: {e}")
            print(f"Ошибка при загрузке данных из CSV: {e}")
            return False
        finally:
            # Закрываем подключение к базе данных
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
    
    print(f"Запуск загрузки данных из {csv_file} в базу данных")
    
    # Запускаем загрузку данных
    loader = CsvToSqlLoader(DB_CONFIG)
    result = loader.run(csv_file)
    
    if result:
        print("Данные успешно загружены в базу данных")
    else:
        print("Ошибка при загрузке данных в базу данных")
    
    return result

if __name__ == "__main__":
    import sys
    # Если передан аргумент командной строки, используем его как путь к CSV файлу
    csv_file = sys.argv[1] if len(sys.argv) > 1 else None
    main(csv_file) 