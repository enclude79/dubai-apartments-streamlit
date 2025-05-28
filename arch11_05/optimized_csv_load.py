import os
import pandas as pd
import psycopg2
import chardet
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Параметры подключения к базе данных
DB_PARAMS = {
    'dbname': 'postgres',
    'user': 'Admin',
    'password': 'Enclude79',
    'host': 'localhost',
    'port': '5432',
    'client_encoding': 'utf8'  # Явно указываем кодировку для соединения
}

def detect_encoding(file_path):
    """Определение кодировки файла"""
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read(10000))
    
    print(f"Обнаружена кодировка: {result['encoding']} (уверенность: {result['confidence']})")
    return result['encoding']

def load_csv_with_optimal_types(file_path, exclude_columns=None):
    """Загрузка CSV файла с правильными типами данных и исключением проблемных колонок"""
    try:
        # Определение кодировки
        encoding = detect_encoding(file_path)
        
        # Читаем только заголовки для получения списка колонок
        df_header = pd.read_csv(file_path, nrows=0, encoding=encoding)
        all_columns = list(df_header.columns)
        
        print(f"Все колонки в CSV файле ({len(all_columns)}):")
        for i, col in enumerate(all_columns):
            print(f"{i+1}. {col}")
        
        # Исключаем ненужные колонки
        if exclude_columns:
            use_columns = [col for col in all_columns if col not in exclude_columns]
            print(f"\nИсключаем {len(exclude_columns)} колонки: {', '.join(exclude_columns)}")
        else:
            use_columns = all_columns
        
        # Загрузка данных с исключением проблемных колонок
        print(f"\nЗагрузка CSV с использованием {len(use_columns)} колонок...")
        df = pd.read_csv(file_path, usecols=use_columns, encoding=encoding, dtype=str, low_memory=False)
        
        # Заменяем пустые строки на None
        df = df.replace('', None)
        
        # Латинизация имен колонок для избежания проблем с кодировкой
        columns_map = {}
        for col in df.columns:
            # Транслитерация имен колонок
            latin_col = transliterate_text(col)
            columns_map[col] = latin_col
        
        # Переименовываем колонки
        df = df.rename(columns=columns_map)
        print("Колонки транслитерированы для избежания проблем с кодировкой")
        
        # Очистка данных от проблемных символов
        for col in df.columns:
            df[col] = df[col].apply(lambda x: clean_text(x) if x is not None else x)
        
        print(f"Загружено {len(df)} строк и {len(df.columns)} колонок")
        return df
    
    except Exception as e:
        logger.error(f"Ошибка при загрузке CSV: {e}")
        return None

def clean_text(text):
    """Очистка текста от проблемных символов"""
    if not isinstance(text, str):
        return text
    
    # Заменяем непечатаемые и проблемные символы
    try:
        # Сначала пробуем преобразовать в ASCII, удаляя или заменяя непонятные символы
        text = text.encode('ascii', 'replace').decode('ascii')
    except:
        # Если не получается, используем более агрессивную замену
        try:
            text = text.encode('utf-8', 'ignore').decode('utf-8')
        except:
            # В крайнем случае просто удаляем все не-ASCII символы
            text = ''.join(c for c in text if ord(c) < 128)
    
    return text

def transliterate_text(text):
    """Транслитерация текста с кириллицы на латиницу"""
    if not isinstance(text, str):
        return text
    
    # Словарь соответствия кириллических и латинских символов
    transliteration = {
        'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'e',
        'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm',
        'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
        'ф': 'f', 'х': 'h', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'sch', 'ъ': '',
        'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya', ' ': '_', '-': '_'
    }
    
    # Преобразование в нижний регистр
    text = text.lower()
    
    # Транслитерация
    result = ''
    for char in text:
        if char in transliteration:
            result += transliteration[char]
        elif char.isalnum() or char == '_':
            # Оставляем цифры, латинские буквы и подчеркивания
            result += char
        else:
            # Заменяем другие символы на подчеркивание
            result += '_'
    
    return result

def create_table_from_df(conn, table_name, df, drop_if_exists=False):
    """Создание таблицы на основе DataFrame с оптимальными типами данных"""
    try:
        cursor = conn.cursor()
        
        # Удаление таблицы, если она существует и указан флаг drop_if_exists
        if drop_if_exists:
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.commit()
            print(f"Таблица {table_name} удалена")
        
        # Подготовка SQL для создания таблицы с базовыми типами
        columns = []
        for col in df.columns:
            # Используем все поля как текстовые для избежания проблем с конвертацией типов
            if col.lower() in ('id'):
                col_type = "character varying(255)"
            elif col.lower() in ('osobennosti', 'features', 'opisanie', 'description'):
                col_type = "text"
            else:
                col_type = "character varying(255)"
            
            columns.append(f"\"{col}\" {col_type}")
        
        # Создание таблицы
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
        create_table_sql += ",\n".join(columns)
        create_table_sql += "\n)"
        
        print(f"\nСоздание таблицы {table_name} со следующей структурой:")
        print(create_table_sql)
        
        cursor.execute(create_table_sql)
        conn.commit()
        print(f"Таблица {table_name} успешно создана")
        
        return True
    
    except Exception as e:
        logger.error(f"Ошибка при создании таблицы: {e}")
        conn.rollback()
        return False

def insert_data_to_table(conn, table_name, df, batch_size=50):
    """Вставка данных в таблицу с обработкой ошибок кодировки"""
    try:
        cursor = conn.cursor()
        
        # Получение списка колонок
        columns = list(df.columns)
        
        # Подготовка строки с колонками для SQL запроса
        columns_str = ", ".join([f"\"{col}\"" for col in columns])
        
        # Подготовка строки с плейсхолдерами
        placeholders = ", ".join(["%s"] * len(columns))
        
        # SQL запрос для вставки
        insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        
        # Вставка данных пачками
        total_rows = len(df)
        print(f"\nВставка {total_rows} строк в таблицу {table_name}...")
        
        successful_inserts = 0
        for i in range(0, total_rows, batch_size):
            batch_df = df.iloc[i:i+batch_size]
            batch_values = []
            
            # Подготовка данных
            for _, row in batch_df.iterrows():
                row_values = []
                for val in row:
                    row_values.append(val)
                batch_values.append(tuple(row_values))
            
            # Выполнение вставки для пачки
            try:
                cursor.executemany(insert_sql, batch_values)
                conn.commit()
                successful_inserts += len(batch_values)
                print(f"Вставлено {successful_inserts}/{total_rows} строк ({successful_inserts/total_rows*100:.1f}%)")
            except Exception as e:
                logger.error(f"Ошибка при вставке пачки данных: {e}")
                conn.rollback()
                
                # Попробуем вставлять по одной строке для отладки
                print("Пробуем вставлять по одной строке...")
                for row_index, row in batch_df.iterrows():
                    try:
                        values = tuple(row)
                        cursor.execute(insert_sql, values)
                        conn.commit()
                        successful_inserts += 1
                        if successful_inserts % 10 == 0:
                            print(f"Вставлено {successful_inserts}/{total_rows} строк")
                    except Exception as row_error:
                        logger.error(f"Ошибка при вставке строки {row_index}: {row_error}")
                        conn.rollback()
        
        print(f"\nУспешно вставлено {successful_inserts} из {total_rows} строк")
        return successful_inserts
    
    except Exception as e:
        logger.error(f"Ошибка при вставке данных: {e}")
        conn.rollback()
        return 0

def optimized_csv_import(csv_path, table_name="properties_optimized", exclude_columns=None):
    """Основная функция для оптимизированного импорта CSV в PostgreSQL"""
    try:
        print(f"Начало оптимизированного импорта {csv_path} в таблицу {table_name}")
        
        # Загрузка CSV файла с правильной кодировкой и без проблемных колонок
        df = load_csv_with_optimal_types(csv_path, exclude_columns)
        if df is None or len(df) == 0:
            print("Не удалось загрузить данные из CSV файла")
            return False
        
        # Подключение к базе данных
        print("\nПодключение к базе данных...")
        conn = psycopg2.connect(**DB_PARAMS)
        
        # Явно устанавливаем кодировку для соединения
        conn.set_client_encoding('UTF8')
        
        # Создание таблицы с оптимальными типами данных
        if not create_table_from_df(conn, table_name, df, drop_if_exists=True):
            print("Не удалось создать таблицу")
            conn.close()
            return False
        
        # Вставка данных в таблицу
        inserted_rows = insert_data_to_table(conn, table_name, df)
        
        # Закрытие соединения
        conn.close()
        
        if inserted_rows > 0:
            print(f"\nИмпорт успешно завершен! Вставлено {inserted_rows} строк")
            return True
        else:
            print("\nИмпорт завершен с ошибками")
            return False
        
    except Exception as e:
        logger.error(f"Ошибка при импорте CSV: {e}")
        return False

if __name__ == "__main__":
    # Путь к CSV файлу
    csv_file = os.path.join("Api_Bayat", "bayut_properties_sale_6m_20250401_utf8.csv")
    
    # Колонки для исключения
    exclude_cols = ["Координаты", "Регион", "Локация"]
    
    # Запуск оптимизированного импорта
    optimized_csv_import(csv_file, "properties_optimized", exclude_cols) 