import pandas as pd
import psycopg2
import logging
from datetime import datetime
import chardet
import os
import io
import csv
import tempfile

# Настройка логирования
log_filename = f'csv_loader_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

def detect_encoding(file_path):
    """Определение кодировки файла"""
    print(f"Определение кодировки файла {file_path}...")
    with open(file_path, 'rb') as file:
        raw_data = file.read(1000000)
        result = chardet.detect(raw_data)
        encoding = result['encoding']
        if encoding is None:
            encoding = 'utf-8-sig'  # Используем безопасную кодировку по умолчанию
        print(f"Определена кодировка: {encoding}")
        return encoding

def read_csv_safely(file_path, encoding='utf-8'):
    """Безопасное чтение CSV файла с обработкой ошибок кодировки"""
    try:
        # Сначала попробуем прочитать напрямую
        print(f"Попытка чтения файла с кодировкой {encoding}...")
        return pd.read_csv(file_path, encoding=encoding)
    except UnicodeDecodeError:
        print(f"Ошибка декодирования с {encoding}, пробуем latin1...")
        try:
            # Если не получилось, пробуем latin1 (поддерживает все байты)
            return pd.read_csv(file_path, encoding='latin1')
        except Exception as e:
            print(f"Ошибка при чтении файла с latin1: {e}")
            # Последняя попытка - прочитать через байтовый поток
            with open(file_path, 'rb') as f:
                content = f.read()
                # Декодируем с заменой проблемных символов
                text_content = content.decode('latin1', errors='replace')
                # Создаем StringIO объект для pandas
                csv_buffer = io.StringIO(text_content)
                print("Используем StringIO с latin1 и replace")
                return pd.read_csv(csv_buffer)

def create_clean_csv_with_psql_copy(df, temp_dir=None):
    """Создает временный CSV файл, очищенный от проблемных символов, для COPY"""
    # Получаем текущую директорию, если temp_dir не указан
    if temp_dir is None:
        temp_dir = os.getcwd()
    
    # Создаем временный CSV файл
    temp_csv_path = os.path.join(temp_dir, f"temp_clean_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
    print(f"Создание временного CSV файла для загрузки: {temp_csv_path}")
    
    # Очищаем данные от проблемных символов и записываем в CSV
    with open(temp_csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        # Записываем заголовки
        writer.writerow(df.columns)
        # Записываем данные, заменяя NULL и проблемные символы
        for _, row in df.iterrows():
            clean_row = []
            for val in row:
                if pd.isna(val):
                    clean_row.append('')
                elif isinstance(val, str):
                    # Удаляем проблемные символы
                    clean_val = ''.join(c for c in val if ord(c) < 128)
                    clean_row.append(clean_val)
                else:
                    clean_row.append(str(val))
            writer.writerow(clean_row)
    
    return temp_csv_path

def insert_data_to_table(csv_path, db_params, table_name):
    """Загрузка данных в существующую таблицу через psycopg2 COPY"""
    print(f"Загрузка данных в существующую таблицу {table_name} через psycopg2 COPY...")
    
    # Создаем строку подключения
    conn = psycopg2.connect(
        dbname=db_params['database'],
        user=db_params['user'],
        password=db_params['password'],
        host=db_params['host'],
        port=db_params['port']
    )
    
    try:
        with conn.cursor() as cur:
            # Проверяем существование таблицы
            cur.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}')")
            table_exists = cur.fetchone()[0]
            
            if not table_exists:
                print(f"Ошибка: Таблица {table_name} не существует!")
                print("Пожалуйста, сначала создайте таблицу с помощью скрипта create_bayut_table.py")
                return False
                
            # Очищаем таблицу перед загрузкой новых данных
            print(f"Очистка данных в таблице {table_name}...")
            cur.execute(f"TRUNCATE TABLE {table_name}")
            
            # Загружаем данные через COPY
            with open(csv_path, 'r', encoding='utf-8') as f:
                next(f)  # Пропускаем заголовок
                print("Копирование данных...")
                cur.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV", f)
            
            conn.commit()
            print("Данные успешно загружены!")
            return True
    except Exception as e:
        conn.rollback()
        print(f"Ошибка при загрузке данных: {e}")
        return False
    finally:
        conn.close()

def main():
    # Названия всех возможных файлов
    csv_files = [
        'Api_Bayat/bayut_properties_sale_6m_20250401_fixed.csv',
        'Api_Bayat/bayut_properties_sale_6m_20250401_utf8.csv',
        'Api_Bayat/bayut_properties_sale_6m_20250401.csv'
    ]
    
    # Выбираем первый доступный файл
    csv_file_path = None
    for file in csv_files:
        if os.path.exists(file):
            csv_file_path = file
            print(f"Выбран файл: {csv_file_path}")
            break
    
    if csv_file_path is None:
        print("Не найден ни один CSV файл!")
        return
    
    # Параметры подключения к базе данных
    db_params = {
        'user': 'admin',
        'password': 'Enclude79',
        'host': 'localhost',
        'port': '5432',
        'database': 'postgres'
    }
    table_name = 'bayut_properties'

    print(f"Запуск загрузки CSV в PostgreSQL. Лог сохраняется в {os.path.abspath(log_filename)}")

    try:
        # Определяем кодировку файла
        encoding = detect_encoding(csv_file_path)
        logger.info(f"Определена кодировка файла: {encoding}")

        # Читаем CSV с обработкой ошибок кодировки
        print(f"Чтение CSV файла: {csv_file_path}...")
        logger.info(f"Чтение CSV файла: {csv_file_path}")
        
        # Используем безопасную функцию чтения
        df = read_csv_safely(csv_file_path, encoding)
            
        print(f"Файл прочитан, количество строк: {len(df)}")
        logger.info(f"Файл прочитан, количество строк: {len(df)}")

        # Создаем очищенный CSV файл для загрузки
        temp_csv_path = create_clean_csv_with_psql_copy(df)
        logger.info(f"Создан временный CSV файл: {temp_csv_path}")
        
        # Загружаем данные в существующую таблицу
        success = insert_data_to_table(temp_csv_path, db_params, table_name)
        
        if success:
            print("Данные успешно загружены в базу данных!")
            logger.info("Данные успешно загружены в базу данных!")
        else:
            print("Ошибка при загрузке данных в базу данных.")
            logger.error("Ошибка при загрузке данных в базу данных.")
            
        # Удаляем временный файл
        if os.path.exists(temp_csv_path):
            os.remove(temp_csv_path)
            print(f"Временный файл {temp_csv_path} удален")

    except Exception as e:
        print(f"Критическая ошибка: {e}")
        logger.error(f"Критическая ошибка: {e}")

if __name__ == "__main__":
    main() 