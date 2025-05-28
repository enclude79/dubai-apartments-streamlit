import psycopg2
import os
import traceback
import pandas as pd
import csv
from io import StringIO
import re

# Параметры подключения
conn_params = {
    'dbname': 'postgres',
    'user': 'Admin',
    'password': 'Enclude79',
    'host': 'localhost',
    'port': '5432',
    # Не указываем кодировку здесь, будем устанавливать её после подключения
}

# Путь к CSV файлу
csv_path = r"C:\WealthCompas\Api_Bayat\bayut_properties_sale_6m_20250401.csv"

def detect_encoding(file_path):
    """Определяет кодировку файла"""
    import chardet
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read(10000))
    return result['encoding']

def normalize_column_name(col_name):
    """Нормализует название столбца для использования в SQL-запросах"""
    # Заменяем пробелы и специальные символы на подчеркивания
    normalized = re.sub(r'[^a-zA-Z0-9_]', '_', col_name)
    # Убираем множественные подчеркивания
    normalized = re.sub(r'_+', '_', normalized)
    # Убираем подчеркивания в начале и конце
    normalized = normalized.strip('_')
    return normalized.lower()

def preprocess_csv(input_path, output_path=None):
    """
    Предобработка CSV-файла для исправления проблем с кодировкой.
    Если output_path не указан, возвращает данные в памяти.
    """
    try:
        # Определяем кодировку исходного файла
        encoding = detect_encoding(input_path)
        print(f"Определена кодировка исходного файла: {encoding}")
        
        # Читаем файл с определенной кодировкой
        df = pd.read_csv(input_path, encoding=encoding, low_memory=False)
        
        # Нормализуем названия столбцов
        df.columns = [normalize_column_name(col) for col in df.columns]
        print(f"Нормализованные названия столбцов: {list(df.columns)}")
        
        # Обрабатываем текстовые столбцы для обеспечения совместимости
        for col in df.columns:
            if pd.api.types.is_string_dtype(df[col]) or pd.api.types.is_object_dtype(df[col]):
                df[col] = df[col].apply(lambda x: str(x).encode('cp1251', 'ignore').decode('cp1251', 'ignore') if isinstance(x, str) else x)
        
        # Сохраняем в формате, совместимом с базой данных
        if output_path:
            df.to_csv(output_path, encoding='utf-8', index=False, quoting=csv.QUOTE_ALL)
            return output_path
        else:
            # Создаем буфер в памяти
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, encoding='utf-8', index=False, quoting=csv.QUOTE_ALL)
            csv_buffer.seek(0)
            return csv_buffer
    except Exception as e:
        print(f"Ошибка при предобработке CSV: {e}")
        traceback.print_exc()
        raise

try:
    # Проверка существования файла
    print(f"Проверка существования файла: {csv_path}")
    if not os.path.exists(csv_path):
        print(f"ОШИБКА: Файл не найден: {csv_path}")
        exit(1)
    
    print(f"Файл найден: {csv_path}")
    file_size = os.path.getsize(csv_path) / (1024 * 1024)  # Размер в МБ
    print(f"Размер файла: {file_size:.2f} МБ")
    
    # Предобработка CSV файла
    print("Выполняется предобработка CSV для исправления кодировки...")
    try:
        # Пробуем найти модуль chardet для определения кодировки
        import chardet
    except ImportError:
        print("Модуль chardet не установлен, выполняется установка...")
        import subprocess
        subprocess.check_call(["pip", "install", "chardet"])
        import chardet
    
    # Создаем временный файл с исправленной кодировкой
    fixed_csv_path = csv_path.replace('.csv', '_fixed.csv')
    preprocessed_csv = preprocess_csv(csv_path, fixed_csv_path)
    print(f"Создан файл с исправленной кодировкой: {fixed_csv_path}")
    
    # Подключение к базе данных
    print("Подключение к базе данных...")
    conn = psycopg2.connect(**conn_params)
    
    # Проверяем текущую кодировку клиента
    cur = conn.cursor()
    cur.execute("SHOW client_encoding;")
    print(f"Текущая кодировка клиента: {cur.fetchone()[0]}")
    
    # Проверяем кодировку сервера
    cur.execute("SHOW server_encoding;")
    print(f"Кодировка сервера: {cur.fetchone()[0]}")
    
    # Установка подходящей кодировки для клиента
    server_encoding = "WIN1251"  # используем кодировку, совместимую с сервером
    print(f"Устанавливаем кодировку клиента: {server_encoding}")
    conn.set_client_encoding(server_encoding)
    
    # Проверка существования таблицы
    print("Проверка существования таблицы temp_properties...")
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'temp_properties'
    """)
    
    if not cur.fetchone():
        print("ОШИБКА: Таблица temp_properties не существует!")
        print("Сначала запустите update_schema.py для создания таблицы.")
        exit(1)
    
    # Проверяем структуру таблицы temp_properties
    print("Получение структуры таблицы temp_properties...")
    cur.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'temp_properties' 
        ORDER BY ordinal_position
    """)
    db_columns = {row[0]: row[1] for row in cur.fetchall()}
    print(f"Столбцы в базе данных: {list(db_columns.keys())}")
    
    # Очистка таблицы перед загрузкой
    print("Очистка таблицы перед загрузкой...")
    cur.execute("TRUNCATE TABLE temp_properties;")
    conn.commit()
    
    # Загрузка данных из CSV
    print("Альтернативный метод загрузки - по записям...")
    # Читаем предобработанный CSV файл
    clean_df = pd.read_csv(fixed_csv_path, encoding='utf-8')
    
    # Создаем соответствие между столбцами CSV и столбцами базы данных
    print("Сопоставление столбцов CSV и базы данных...")
    csv_columns = list(clean_df.columns)
    print(f"Столбцы в CSV: {csv_columns}")
    
    # Находим соответствия между столбцами
    column_mapping = {}
    for csv_col in csv_columns:
        # Ищем соответствующий столбец в базе данных, пробуем точное и нормализованное совпадение
        if csv_col in db_columns:
            column_mapping[csv_col] = csv_col
        else:
            # Если точного совпадения нет, ищем по нормализованным именам
            normalized_csv_col = normalize_column_name(csv_col)
            for db_col in db_columns:
                normalized_db_col = normalize_column_name(db_col)
                if normalized_csv_col == normalized_db_col:
                    column_mapping[csv_col] = db_col
                    break
    
    print(f"Сопоставление столбцов: {column_mapping}")
    
    # Оставляем только те столбцы, которые есть в базе данных
    valid_csv_columns = [col for col in csv_columns if col in column_mapping]
    db_columns_to_use = [column_mapping[col] for col in valid_csv_columns]
    
    print(f"Используемые столбцы CSV: {valid_csv_columns}")
    print(f"Соответствующие столбцы БД: {db_columns_to_use}")
    
    # Фильтруем данные только по нужным столбцам
    clean_df = clean_df[valid_csv_columns]
    
    rows_imported = 0
    total_rows = len(clean_df)
    
    placeholders = ', '.join(['%s'] * len(valid_csv_columns))
    columns_str = ', '.join([f'"{col}"' for col in db_columns_to_use])  # добавляем кавычки для избежания SQL-инъекций
    
    # Вставляем данные по батчам
    batch_size = 100
    for i in range(0, total_rows, batch_size):
        batch = clean_df.iloc[i:i+batch_size]
        batch_tuples = [tuple(row) for row in batch.values.tolist()]
        
        # Собираем запрос
        insert_query = f'INSERT INTO temp_properties ({columns_str}) VALUES ({placeholders})'
        
        # Выполняем вставку
        cur.executemany(insert_query, batch_tuples)
        conn.commit()
        
        rows_imported += len(batch)
        print(f"Импортировано {rows_imported} из {total_rows} строк ({rows_imported/total_rows*100:.2f}%)")
    
    # Проверка количества загруженных записей
    cur.execute("SELECT COUNT(*) FROM temp_properties")
    count = cur.fetchone()[0]
    print(f"Данные успешно загружены! Количество загруженных записей: {count}")
    
    # Вывести первые несколько записей для проверки
    if count > 0:
        print("\nПримеры загруженных данных (первые 3 записи):")
        cur.execute("""
            SELECT id, title, price, rooms, bathrooms, area, region, developer
            FROM temp_properties
            LIMIT 3
        """)
        
        rows = cur.fetchall()
        for row in rows:
            print(f"ID: {row[0]}, Название: {row[1]}")
            print(f"Цена: {row[2]}, Комнат: {row[3]}, Ванных: {row[4]}, Площадь: {row[5]}")
            print(f"Регион: {row[6]}, Застройщик: {row[7]}")
            print("---")
    
except Exception as e:
    print(f"Ошибка при загрузке данных: {e}")
    print("Детали ошибки:")
    traceback.print_exc()
finally:
    if 'cur' in locals() and cur:
        cur.close()
    if 'conn' in locals() and conn:
        conn.close()
    print("Соединение с базой данных закрыто.") 