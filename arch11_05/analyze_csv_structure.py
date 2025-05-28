import pandas as pd
import psycopg2
import logging
from psycopg2 import sql

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Параметры базы данных
DB_CONFIG = {
    'user': 'admin',
    'password': 'Enclude79',
    'host': 'localhost',
    'port': '5432',
    'database': 'postgres',
    'table': 'bayut_properties'
}

def get_csv_structure(csv_path):
    """Анализирует структуру CSV-файла и типы данных в каждой колонке"""
    logger.info(f"Анализ структуры CSV-файла: {csv_path}")
    
    # Загружаем CSV в DataFrame
    df = pd.read_csv(csv_path, encoding='utf-8-sig')
    
    # Получаем информацию о колонках и типах данных
    columns_info = []
    for col in df.columns:
        column_type = df[col].dtype
        sample_values = df[col].dropna().head(3).tolist()
        sample_values_str = str(sample_values)[:100] + '...' if len(str(sample_values)) > 100 else str(sample_values)
        
        columns_info.append({
            'name': col,
            'type': str(column_type),
            'sample_values': sample_values_str,
            'suggested_sql_type': suggest_sql_type(column_type, sample_values),
            'null_count': df[col].isna().sum(),
            'unique_count': df[col].nunique()
        })
    
    return columns_info

def suggest_sql_type(pandas_type, sample_values):
    """Предлагает SQL тип данных на основе типа данных в pandas и примеров значений"""
    if 'int' in str(pandas_type):
        if str(pandas_type) == 'int64':
            return 'INTEGER'
        else:
            return 'BIGINT'
    elif 'float' in str(pandas_type):
        return 'NUMERIC'
    elif 'bool' in str(pandas_type):
        return 'BOOLEAN'
    elif 'datetime' in str(pandas_type):
        return 'TIMESTAMP'
    elif 'object' in str(pandas_type):
        # Для строковых данных проверяем длину и содержимое
        max_length = 0
        is_json = False
        
        for val in sample_values:
            if val is None:
                continue
            if isinstance(val, str):
                if val.startswith('[') and val.endswith(']'):
                    is_json = True
                if val.startswith('{') and val.endswith('}'):
                    is_json = True
                max_length = max(max_length, len(str(val)))
        
        if is_json:
            return 'JSONB'
        elif max_length > 255:
            return 'TEXT'
        else:
            return f'VARCHAR({max(255, max_length + 50)})'
    else:
        return 'TEXT'

def get_table_structure(db_config):
    """Получает структуру таблицы из базы данных"""
    logger.info(f"Получение структуры таблицы {db_config['table']} из базы данных")
    
    conn = None
    try:
        # Подключаемся к базе данных
        conn = psycopg2.connect(
            dbname=db_config['database'],
            user=db_config['user'],
            password=db_config['password'],
            host=db_config['host'],
            port=db_config['port']
        )
        
        with conn.cursor() as cursor:
            # Запрос для получения информации о колонках таблицы
            query = sql.SQL("""
                SELECT column_name, data_type, character_maximum_length
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position
            """)
            
            cursor.execute(query, (db_config['table'],))
            columns = cursor.fetchall()
            
            columns_info = []
            for col in columns:
                column_name = col[0]
                data_type = col[1]
                max_length = col[2]
                
                if max_length:
                    data_type = f"{data_type}({max_length})"
                
                columns_info.append({
                    'name': column_name,
                    'sql_type': data_type
                })
            
            return columns_info
            
    except Exception as e:
        logger.error(f"Ошибка при получении структуры таблицы: {e}")
        return []
    finally:
        if conn:
            conn.close()

def compare_structures(csv_structure, table_structure):
    """Сравнивает структуру CSV и таблицы SQL, выявляет несоответствия типов данных"""
    logger.info("Сравнение структуры CSV и таблицы SQL")
    
    # Создаем словарь колонок таблицы для быстрого доступа
    table_columns = {col['name']: col for col in table_structure}
    
    # Выявляем несоответствия
    mismatches = []
    for csv_col in csv_structure:
        if csv_col['name'] in table_columns:
            table_col = table_columns[csv_col['name']]
            csv_type = csv_col['suggested_sql_type']
            table_type = table_col['sql_type']
            
            # Проверяем, совместимы ли типы данных
            if not are_types_compatible(csv_type, table_type):
                mismatches.append({
                    'column': csv_col['name'],
                    'csv_type': csv_type,
                    'table_type': table_type,
                    'sample_values': csv_col['sample_values']
                })
    
    return mismatches

def are_types_compatible(csv_type, table_type):
    """Проверяет, совместимы ли типы данных из CSV и таблицы SQL"""
    # Преобразуем типы для сравнения
    csv_type = csv_type.upper()
    table_type = table_type.upper()
    
    # Числовые типы
    if 'INT' in csv_type and 'INT' in table_type:
        return True
    if ('NUMERIC' in csv_type or 'FLOAT' in csv_type) and ('NUMERIC' in table_type or 'FLOAT' in table_type or 'DOUBLE' in table_type):
        return True
    
    # Строковые типы
    if 'VARCHAR' in csv_type and 'VARCHAR' in table_type:
        return True
    if 'TEXT' in csv_type and ('TEXT' in table_type or 'VARCHAR' in table_type):
        return True
    if 'JSONB' in csv_type and ('JSONB' in table_type or 'JSON' in table_type or 'TEXT' in table_type):
        return True
    
    # Логические типы
    if 'BOOL' in csv_type and 'BOOL' in table_type:
        return True
    
    # Датавременные типы
    if 'TIME' in csv_type and 'TIME' in table_type:
        return True
    
    # Считаем TEXT совместимым с любым типом
    if 'TEXT' in table_type:
        return True
    
    return False

def generate_alter_table_script(mismatches, table_name):
    """Генерирует SQL-скрипт для изменения типов данных колонок таблицы"""
    if not mismatches:
        return "-- Нет необходимости в изменении структуры таблицы"
    
    script = f"-- Скрипт изменения типов данных в таблице {table_name}\n\n"
    
    for mismatch in mismatches:
        column = mismatch['column']
        suggested_type = mismatch['csv_type']
        
        script += f"""-- Колонка: {column}, текущий тип: {mismatch['table_type']}, рекомендуемый тип: {suggested_type}
-- Примеры значений: {mismatch['sample_values']}
ALTER TABLE {table_name} 
ALTER COLUMN "{column}" TYPE {suggested_type} USING "{column}"::{suggested_type};\n\n"""
    
    return script

def main(csv_path):
    """Основная функция анализа и сравнения структур"""
    try:
        # Анализируем структуру CSV
        csv_structure = get_csv_structure(csv_path)
        
        # Выводим информацию о колонках CSV
        print("\n=== СТРУКТУРА CSV ===")
        for col in csv_structure:
            print(f"Колонка: {col['name']}")
            print(f"  Тип данных: {col['type']}")
            print(f"  Примеры значений: {col['sample_values']}")
            print(f"  Рекомендуемый SQL тип: {col['suggested_sql_type']}")
            print(f"  Количество NULL: {col['null_count']}")
            print(f"  Количество уникальных значений: {col['unique_count']}")
            print()
        
        # Получаем структуру таблицы SQL
        table_structure = get_table_structure(DB_CONFIG)
        
        # Выводим информацию о колонках таблицы
        print("\n=== СТРУКТУРА ТАБЛИЦЫ SQL ===")
        for col in table_structure:
            print(f"Колонка: {col['name']}")
            print(f"  Тип данных: {col['sql_type']}")
            print()
        
        # Сравниваем структуры и выявляем несоответствия
        mismatches = compare_structures(csv_structure, table_structure)
        
        # Выводим информацию о несоответствиях
        print("\n=== НЕСООТВЕТСТВИЯ ТИПОВ ДАННЫХ ===")
        if not mismatches:
            print("Несоответствий не выявлено. Структуры CSV и таблицы SQL совместимы.")
        else:
            for mismatch in mismatches:
                print(f"Колонка: {mismatch['column']}")
                print(f"  Тип в CSV: {mismatch['csv_type']}")
                print(f"  Тип в таблице: {mismatch['table_type']}")
                print(f"  Примеры значений: {mismatch['sample_values']}")
                print()
        
        # Генерируем скрипт изменения типов данных
        script = generate_alter_table_script(mismatches, DB_CONFIG['table'])
        
        # Выводим скрипт
        print("\n=== SQL СКРИПТ ДЛЯ ИЗМЕНЕНИЯ ТИПОВ ДАННЫХ ===")
        print(script)
        
        # Сохраняем скрипт в файл
        with open('alter_table_script.sql', 'w') as f:
            f.write(script)
        print("\nСкрипт сохранен в файл alter_table_script.sql")
        
    except Exception as e:
        logger.error(f"Ошибка при анализе структур: {e}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        csv_path = sys.argv[1]
    else:
        csv_path = "Api_Bayat/bayut_properties_sale_20250511.csv"
    
    main(csv_path) 