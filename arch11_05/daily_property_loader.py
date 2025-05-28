import os
import requests
import pandas as pd
import psycopg2
import logging
import shutil
import time
from datetime import datetime, timedelta
import chardet
import csv
import json

# Настройка логирования
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = f'{log_dir}/property_loader_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Константы
ARCHIVE_DIR = "Api_Bayat/archive"
CSV_DIR = "Api_Bayat"
os.makedirs(ARCHIVE_DIR, exist_ok=True)

# Параметры базы данных
DB_CONFIG = {
    'user': 'admin',
    'password': 'Enclude79',
    'host': 'localhost',
    'port': '5432',
    'database': 'postgres',
    'table': 'bayut_properties'
}

# Параметры API
API_CONFIG = {
    "url": "https://bayut.p.rapidapi.com/properties/list",
    "headers": {
        "X-RapidAPI-Key": "86b3cfbc80msh3cd99bad2e7126dp18c722jsnc5b7ca0b0d3d",
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

class DateUtils:
    """Утилиты для работы с датами"""
    
    @staticmethod
    def get_last_updated_date(conn, table_name):
        """Получает дату последнего обновления из базы данных"""
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT MAX(to_timestamp("Unnamed: 13", 'YYYY-MM-DD HH24:MI:SS')) 
                    FROM {table_name}
                """)
                last_date = cursor.fetchone()[0]
                
                if last_date:
                    logger.info(f"Последнее обновление в базе: {last_date}")
                    # Конвертируем в формат timestamp с учетом UTC
                    return last_date.timestamp()
                else:
                    # Если нет данных, берем дату на 30 дней назад
                    default_date = datetime.now() - timedelta(days=30)
                    logger.info(f"В базе нет данных, используем дату: {default_date}")
                    return default_date.timestamp()
        except Exception as e:
            logger.error(f"Ошибка при получении даты последнего обновления: {e}")
            # Возвращаем дату на 7 дней назад в случае ошибки
            default_date = datetime.now() - timedelta(days=7)
            return default_date.timestamp()

class DatabaseManager:
    """Класс для работы с базой данных"""
    
    def __init__(self, db_config):
        self.db_config = db_config
    
    def get_connection(self):
        """Создает и возвращает соединение с базой данных"""
        return psycopg2.connect(
            dbname=self.db_config['database'],
            user=self.db_config['user'],
            password=self.db_config['password'],
            host=self.db_config['host'],
            port=self.db_config['port']
        )
    
    def get_last_updated_date(self):
        """Получает дату последнего обновления данных в базе"""
        conn = self.get_connection()
        try:
            date = DateUtils.get_last_updated_date(conn, self.db_config['table'])
            return date
        finally:
            conn.close()
    
    def load_data_from_csv(self, csv_path):
        """Загружает данные из CSV в базу данных"""
        logger.info(f"Загрузка данных из {csv_path} в базу данных")
        
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                # Проверяем существование таблицы
                cursor.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{self.db_config['table']}')")
                table_exists = cursor.fetchone()[0]
                
                if not table_exists:
                    logger.error(f"Таблица {self.db_config['table']} не существует")
                    return False
                
                # Создаем временную таблицу для новых данных
                temp_table = f"{self.db_config['table']}_temp"
                cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
                cursor.execute(f"CREATE TABLE {temp_table} (LIKE {self.db_config['table']} INCLUDING ALL)")
                
                # Используем pandas для загрузки данных из CSV
                df = pd.read_csv(csv_path, encoding='utf-8-sig')
                
                # Преобразуем DataFrame в список кортежей для вставки
                columns = df.columns.tolist()
                tuples = [tuple(x) for x in df.to_numpy()]
                
                # Формируем строку с плейсхолдерами для SQL запроса
                placeholders = ','.join(['%s'] * len(columns))
                
                # Формируем SQL запрос для вставки данных
                columns_str = ','.join([f'"{col}"' for col in columns])
                insert_query = f'INSERT INTO {temp_table} ({columns_str}) VALUES ({placeholders})'
                
                # Выполняем массовую вставку данных
                cursor.executemany(insert_query, tuples)
                logger.info(f"Вставлено {len(tuples)} записей во временную таблицу")
                
                # Определяем последний день данных
                cursor.execute(f"""
                    SELECT DATE(to_timestamp("Unnamed: 12", 'YYYY-MM-DD HH24:MI:SS')) as pub_date
                    FROM {temp_table}
                    ORDER BY pub_date DESC
                    LIMIT 1
                """)
                latest_date = cursor.fetchone()[0]
                
                if latest_date:
                    # Удаляем данные за последний день из основной таблицы
                    cursor.execute(f"""
                        DELETE FROM {self.db_config['table']}
                        WHERE DATE(to_timestamp("Unnamed: 12", 'YYYY-MM-DD HH24:MI:SS')) = %s
                    """, (latest_date,))
                    
                    # Переносим данные из временной таблицы в основную
                    cursor.execute(f"""
                        INSERT INTO {self.db_config['table']}
                        SELECT * FROM {temp_table}
                    """)
                    
                    rows_inserted = cursor.rowcount
                    logger.info(f"Загружено {rows_inserted} новых записей в базу данных")
                    
                    # Удаляем временную таблицу
                    cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
                    
                    conn.commit()
                    return True
                else:
                    logger.warning("В загружаемых данных нет даты публикации")
                    return False
                    
        except Exception as e:
            conn.rollback()
            logger.error(f"Ошибка при загрузке данных в базу: {e}")
            return False
        finally:
            conn.close()

class ApiClient:
    """Класс для работы с API Bayut"""
    
    def __init__(self, api_config):
        self.api_config = api_config
    
    def fetch_properties(self, from_timestamp):
        """Загружает данные о недвижимости через API начиная с указанной даты"""
        logger.info(f"Загрузка данных через API начиная с {datetime.fromtimestamp(from_timestamp)}")
        all_properties = []
        page = 1
        max_pages = 50  # Ограничиваем количество страниц
        
        from_date = datetime.fromtimestamp(from_timestamp)
        
        while page <= max_pages:
            # Обновляем номер страницы в параметрах запроса
            querystring = self.api_config["params"].copy()
            querystring["page"] = str(page)
            
            try:
                logger.info(f"Запрос страницы {page}...")
                response = requests.get(
                    self.api_config["url"], 
                    headers=self.api_config["headers"], 
                    params=querystring
                )
                response.raise_for_status()
                data = response.json()
                
                if not data.get('hits'):
                    logger.info("Больше данных не найдено")
                    break
                
                # Обрабатываем полученные объекты
                new_properties = 0
                for property_item in data['hits']:
                    # Проверяем дату публикации
                    created_at = datetime.fromtimestamp(property_item.get('createdAt', 0))
                    
                    # Добавляем только объекты с датой публикации после последнего обновления
                    if created_at >= from_date:
                        property_data = self._extract_property_data(property_item)
                        all_properties.append(property_data)
                        new_properties += 1
                
                logger.info(f"Получено {new_properties} новых объектов на странице {page}")
                
                # Если на текущей странице нет новых объектов, значит мы достигли старых данных
                if new_properties == 0:
                    logger.info("Достигнуты старые данные, завершаем загрузку")
                    break
                
                page += 1
                time.sleep(1.5)  # Пауза между запросами
                
            except Exception as e:
                logger.error(f"Ошибка при получении данных через API: {e}")
                break
        
        logger.info(f"Всего загружено {len(all_properties)} объектов")
        return all_properties
    
    def _extract_property_data(self, property_item):
        """Извлекает данные из объекта API в формат для сохранения"""
        created_at = datetime.fromtimestamp(property_item.get('createdAt', 0))
        updated_at = datetime.fromtimestamp(property_item.get('updatedAt', 0))
        
        return {
            'id': property_item.get('id'),
            'Unnamed: 1': property_item.get('title'),  # Название
            'Unnamed: 2': property_item.get('price'),  # Цена
            'Unnamed: 3': property_item.get('rooms'),  # Комнат
            'Unnamed: 4': property_item.get('baths'),  # Ванных
            'Unnamed: 5': property_item.get('area'),   # Площадь
            'Unnamed: 6': property_item.get('rentFrequency'),  # Частота аренды
            'Unnamed: 7': json.dumps(property_item.get('location', [])),  # Локация
            'Unnamed: 8': property_item.get('coverPhoto', {}).get('url'),  # Фото
            'Unnamed: 9': f"https://www.bayut.com/property/details-{property_item.get('externalID')}.html",  # Ссылка
            'Unnamed: 10': property_item.get('category', [{}])[0].get('name'),  # Категория
            'Unnamed: 11': property_item.get('type'),  # Тип
            'Unnamed: 12': created_at.strftime('%Y-%m-%d %H:%M:%S'),  # Дата публикации
            'Unnamed: 13': updated_at.strftime('%Y-%m-%d %H:%M:%S'),  # Дата обновления
            'Unnamed: 14': property_item.get('furnishingStatus'),  # Статус мебели
            'Unnamed: 15': property_item.get('completionStatus'),  # Статус завершения
            'Unnamed: 16': ', '.join(property_item.get('amenities', [])),  # Удобства
            'Unnamed: 17': property_item.get('rentFrequency'),  # Частота аренды (дубликат)
            'Unnamed: 18': property_item.get('agency', {}).get('name'),  # Агентство
            'Unnamed: 19': f"Тел: {property_item.get('phoneNumber', {}).get('mobile')}; WhatsApp: {property_item.get('phoneNumber', {}).get('whatsapp')}",  # Контакты
            'Unnamed: 20': f"Широта: {property_item.get('geography', {}).get('lat')}, Долгота: {property_item.get('geography', {}).get('lng')}",  # Координаты
            'Unnamed: 21': property_item.get('agency', {}).get('name'),  # Название агентства
            'Unnamed: 22': property_item.get('agency', {}).get('logo', {}).get('url'),  # Логотип
            'Unnamed: 23': property_item.get('phoneNumber', {}).get('proxyMobile'),  # Прокси-телефон
            'Unnamed: 24': json.dumps(property_item.get('keywords', [])),  # Ключевые слова
            'Unnamed: 25': property_item.get('isVerified'),  # Верифицирован
            'Unnamed: 26': property_item.get('purpose'),  # Цель
            'Unnamed: 27': property_item.get('floorNumber'),  # Этаж
            'Unnamed: 28': property_item.get('cityLevelScore'),  # Оценка города
            'Unnamed: 29': property_item.get('score'),  # Оценка
            'Unnamed: 30': json.dumps(property_item.get('agency', {}).get('licenses', [])),  # Лицензии
            'Unnamed: 31': property_item.get('agency', {}).get('rating'),  # Рейтинг
        }

class CsvManager:
    """Класс для работы с CSV файлами"""
    
    @staticmethod
    def save_to_csv(properties_data, output_dir=CSV_DIR):
        """Сохраняет данные о недвижимости в CSV файл"""
        if not properties_data:
            logger.warning("Нет данных для сохранения в CSV")
            return None
            
        # Создаем DataFrame
        df = pd.DataFrame(properties_data)
        
        # Формируем имя файла с текущей датой
        current_date = datetime.now().strftime("%Y%m%d")
        output_file = os.path.join(output_dir, f'bayut_properties_sale_{current_date}.csv')
        
        # Сохраняем в CSV
        df.to_csv(output_file, index=False, encoding='utf-8-sig', quoting=csv.QUOTE_MINIMAL)
        logger.info(f"Данные сохранены в файл: {output_file}")
        
        return output_file
    
    @staticmethod
    def move_to_archive(csv_path, archive_dir=ARCHIVE_DIR):
        """Перемещает CSV файл в архивную директорию"""
        if not os.path.exists(csv_path):
            logger.warning(f"Файл {csv_path} не существует")
            return False
            
        # Получаем имя файла из пути
        filename = os.path.basename(csv_path)
        
        # Добавляем метку времени к имени файла в архиве
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archive_filename = f"{os.path.splitext(filename)[0]}_{timestamp}{os.path.splitext(filename)[1]}"
        archive_path = os.path.join(archive_dir, archive_filename)
        
        try:
            # Перемещаем файл в архив
            shutil.move(csv_path, archive_path)
            logger.info(f"Файл {csv_path} перемещен в архив: {archive_path}")
            return True
        except Exception as e:
            logger.error(f"Ошибка при перемещении файла в архив: {e}")
            return False

class PropertyLoader:
    """Основной класс для загрузки данных о недвижимости"""
    
    def __init__(self, db_config, api_config):
        self.db_manager = DatabaseManager(db_config)
        self.api_client = ApiClient(api_config)
        
    def run(self):
        """Запускает процесс загрузки данных"""
        try:
            logger.info("Запуск процесса загрузки данных о недвижимости")
            
            # Получаем дату последнего обновления
            last_updated = self.db_manager.get_last_updated_date()
            
            # Загружаем данные через API
            properties_data = self.api_client.fetch_properties(last_updated)
            
            if not properties_data:
                logger.info("Нет новых данных для загрузки")
                return
            
            # Сохраняем данные в CSV
            csv_path = CsvManager.save_to_csv(properties_data)
            if not csv_path:
                logger.error("Ошибка при сохранении данных в CSV")
                return
            
            # Загружаем данные из CSV в базу данных
            success = self.db_manager.load_data_from_csv(csv_path)
            
            if success:
                # Перемещаем файл в архив
                CsvManager.move_to_archive(csv_path)
                logger.info("Процесс загрузки данных успешно завершен")
            else:
                logger.error("Ошибка при загрузке данных в базу данных")
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных: {e}")

def main():
    """Основная функция скрипта"""
    try:
        # Инициализируем загрузчик данных
        loader = PropertyLoader(DB_CONFIG, API_CONFIG)
        
        # Запускаем процесс загрузки
        loader.run()
        
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")

if __name__ == "__main__":
    main() 