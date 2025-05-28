import psycopg2
import pandas as pd
import chardet
import csv
import os

def detect_encoding(file_path):
    """Определение кодировки файла"""
    print(f"Определение кодировки файла {file_path}...")
    with open(file_path, 'rb') as file:
        raw_data = file.read(1000000)
        result = chardet.detect(raw_data)
        encoding = result['encoding']
        if encoding is None:
            encoding = 'utf-8-sig'
        print(f"Определена кодировка: {encoding}")
        return encoding

def read_csv_safely(file_path, encoding='utf-8'):
    """Безопасное чтение CSV файла с обработкой ошибок кодировки"""
    try:
        print(f"Попытка чтения файла с кодировкой {encoding}...")
        return pd.read_csv(file_path, encoding=encoding)
    except UnicodeDecodeError:
        print(f"Ошибка декодирования с {encoding}, пробуем latin1...")
        try:
            return pd.read_csv(file_path, encoding='latin1')
        except Exception as e:
            print(f"Ошибка при чтении файла с latin1: {e}")
            with open(file_path, 'rb') as f:
                content = f.read()
                text_content = content.decode('latin1', errors='replace')
                csv_buffer = pd.io.StringIO(text_content)
                print("Используем StringIO с latin1 и replace")
                return pd.read_csv(csv_buffer)

def create_table(db_params, table_name, csv_file_path):
    """Создает таблицу на основе структуры CSV файла"""
    
    # Определяем кодировку и читаем CSV
    encoding = detect_encoding(csv_file_path)
    df = read_csv_safely(csv_file_path, encoding)
    print(f"Файл прочитан, количество строк: {len(df)}")
    
    # Подключаемся к базе данных
    conn = psycopg2.connect(
        dbname=db_params['database'],
        user=db_params['user'],
        password=db_params['password'],
        host=db_params['host'],
        port=db_params['port']
    )
    
    try:
        with conn.cursor() as cur:
            # Удаляем таблицу, если она существует
            print(f"Удаление существующей таблицы {table_name}...")
            cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
            
            # Создаем новую таблицу с оптимальными типами данных
            print("Создание новой таблицы со следующими колонками:")
            columns = []
            for column in df.columns:
                # Определяем тип данных для каждой колонки
                dtype = df[column].dtype
                
                if pd.api.types.is_integer_dtype(dtype):
                    sql_type = "INTEGER"
                elif pd.api.types.is_float_dtype(dtype):
                    sql_type = "NUMERIC"
                elif pd.api.types.is_bool_dtype(dtype):
                    sql_type = "BOOLEAN"
                elif pd.api.types.is_datetime64_dtype(dtype):
                    sql_type = "TIMESTAMP"
                else:
                    sql_type = "TEXT"
                
                columns.append(f'"{column}" {sql_type}')
                print(f"  - {column}: {sql_type}")
            
            # Создаем таблицу
            create_table_sql = f"CREATE TABLE {table_name} (\n    {',\n    '.join(columns)}\n)"
            cur.execute(create_table_sql)
            conn.commit()
            
            print(f"Таблица {table_name} успешно создана!")
            
    except Exception as e:
        conn.rollback()
        print(f"Ошибка при создании таблицы: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
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
        exit(1)
    
    # Параметры подключения к базе данных
    db_params = {
        'user': 'admin',
        'password': 'Enclude79',
        'host': 'localhost',
        'port': '5432',
        'database': 'postgres'
    }
    table_name = 'bayut_properties'
    
    # Создаем таблицу
    create_table(db_params, table_name, csv_file_path) 