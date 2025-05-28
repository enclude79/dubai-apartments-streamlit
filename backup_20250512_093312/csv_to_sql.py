import os
import psycopg2
import pandas as pd
import logging
import shutil
from datetime import datetime
import sys
import csv
import unicodedata
import re

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

# Константы
ARCHIVE_DIR = "Api_Bayat/archive"
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

class DataCleaner:
    """Класс для очистки и нормализации данных перед загрузкой в базу"""
    
    @staticmethod
    def clean_text(text):
        """Очищает текст от проблемных символов Unicode"""
        if not isinstance(text, str):
            return text
        
        # Нормализация Unicode (преобразование составных символов в их базовую форму)
        text = unicodedata.normalize('NFKD', text)
        
        # Заменяем специальные символы на их ASCII-эквиваленты
        replacements = {
            '✦': '*',  # Звездочка Unicode на обычную звездочку
            '✓': 'v',  # Галочка на букву v
            '✔': 'v',  # Другая галочка на букву v
            '→': '->',  # Стрелка на ASCII-эквивалент
            '←': '<-',  # Стрелка на ASCII-эквивалент
            '©': '(c)',  # Знак копирайта
            '®': '(r)',  # Зарегистрированная торговая марка
            '™': '(tm)',  # Торговая марка
            '°': ' degrees',  # Градус
            '±': '+/-',  # Плюс-минус
            '×': 'x',  # Умножение
            '÷': '/',  # Деление
            '≤': '<=',  # Меньше или равно
            '≥': '>=',  # Больше или равно
            '≠': '!=',  # Не равно
            '∞': 'infinity',  # Бесконечность
            # Добавьте другие символы по необходимости
        }
        
        for old, new in replacements.items():
            text = text.replace(old, new)
        
        # Удаляем все оставшиеся символы, которые могут вызвать проблемы с кодировкой
        # Оставляем только ASCII-символы (коды с 32 по 126) и базовые знаки препинания
        return re.sub(r'[^\x20-\x7E\.,;:!?\-\(\)]', '', text)
    
    @staticmethod
    def clean_dataframe(df):
        """Очищает все строковые данные в DataFrame и преобразует типы данных"""
        # Копируем DataFrame, чтобы не изменять оригинал
        df_clean = df.copy()
        
        # Обрабатываем все строковые колонки
        for col in df_clean.columns:
            if df_clean[col].dtype == 'object':  # Если колонка строкового типа
                df_clean[col] = df_clean[col].apply(DataCleaner.clean_text)
            elif df_clean[col].dtype == 'bool':  # Если колонка логического типа
                # Преобразуем boolean в строки '1' и '0'
                df_clean[col] = df_clean[col].map({True: '1', False: '0'})
                logger.info(f"Колонка {col} преобразована из boolean в строковый тип")
        
        # Если в колонке "Unnamed: 25" есть значения True/False, преобразуем их в строки
        if 'Unnamed: 25' in df_clean.columns:
            if df_clean['Unnamed: 25'].dtype == 'bool':
                df_clean['Unnamed: 25'] = df_clean['Unnamed: 25'].map({True: '1', False: '0'})
            else:
                # Попытка преобразовать значения, если они не boolean, но имеют такой смысл
                df_clean['Unnamed: 25'] = df_clean['Unnamed: 25'].apply(
                    lambda x: '1' if (x is True or x == 'True' or x == 'true' or x == 1) 
                            else ('0' if (x is False or x == 'False' or x == 'false' or x == 0) 
                                 else str(x))
                )
            logger.info("Колонка Unnamed: 25 преобразована в строковый формат")
        
        logger.info(f"Данные очищены: обработано {len(df_clean.columns)} колонок")
        return df_clean

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
                
                # Очищаем и нормализуем данные перед загрузкой
                df_clean = DataCleaner.clean_dataframe(df)
                
                # Преобразуем DataFrame в список кортежей для вставки
                columns = df_clean.columns.tolist()
                tuples = [tuple(x) for x in df_clean.to_numpy()]
                
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

class ArchiveManager:
    """Класс для работы с архивом файлов"""
    
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

class CsvToSqlLoader:
    """Основной класс для загрузки данных из CSV в SQL"""
    
    def __init__(self, db_config):
        self.db_manager = DatabaseManager(db_config)
    
    def run(self, csv_path):
        """Запускает процесс загрузки данных из CSV в базу данных"""
        try:
            logger.info(f"Запуск процесса загрузки данных из CSV в SQL: {csv_path}")
            
            if not os.path.exists(csv_path):
                logger.error(f"Файл CSV не существует: {csv_path}")
                return False
            
            # Загружаем данные из CSV в базу данных
            success = self.db_manager.load_data_from_csv(csv_path)
            
            if success:
                # Перемещаем файл в архив
                ArchiveManager.move_to_archive(csv_path)
                logger.info("Процесс загрузки данных из CSV в SQL успешно завершен")
                return True
            else:
                logger.error("Ошибка при загрузке данных в базу данных")
                return False
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных: {e}")
            return False

def main():
    """Основная функция скрипта"""
    try:
        # Проверяем наличие аргумента с путем к файлу CSV
        if len(sys.argv) > 1:
            csv_path = sys.argv[1]
        else:
            # Если аргумент не указан, проверяем стандартный ввод
            for line in sys.stdin:
                if line.startswith("CSV_PATH:"):
                    csv_path = line.strip().replace("CSV_PATH:", "")
                    break
            else:
                logger.error("Путь к CSV файлу не указан")
                print("Использование: python csv_to_sql.py <путь_к_csv_файлу>")
                return
        
        # Инициализируем загрузчик данных
        loader = CsvToSqlLoader(DB_CONFIG)
        
        # Запускаем процесс загрузки
        success = loader.run(csv_path)
        
        # Возвращаем статус выполнения
        if success:
            logger.info(f"Успешная загрузка данных из {csv_path} в базу данных")
            return 0
        else:
            logger.error(f"Ошибка при загрузке данных из {csv_path} в базу данных")
            return 1
            
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 