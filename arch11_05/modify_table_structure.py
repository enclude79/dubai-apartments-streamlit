import psycopg2
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

def connect_to_db():
    """Подключение к базе данных"""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        conn.set_client_encoding('UTF8')
        print("Подключение к базе данных успешно!")
        return conn
    except Exception as e:
        logger.error(f"Ошибка при подключении к базе данных: {e}")
        return None

def get_table_structure(conn, table_name):
    """Получение структуры таблицы"""
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT column_name, data_type, character_maximum_length 
            FROM information_schema.columns 
            WHERE table_name = '{table_name}' 
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        
        print(f"\nТекущая структура таблицы {table_name}:")
        for col in columns:
            col_name = col[0]
            data_type = col[1]
            max_length = col[2]
            
            if max_length:
                print(f"  {col_name}: {data_type}({max_length})")
            else:
                print(f"  {col_name}: {data_type}")
                
        cursor.close()
        return columns
    
    except Exception as e:
        logger.error(f"Ошибка при получении структуры таблицы: {e}")
        return None

def modify_column_type(conn, table_name, column_name, new_type):
    """Изменение типа данных колонки"""
    try:
        cursor = conn.cursor()
        sql = f'ALTER TABLE {table_name} ALTER COLUMN "{column_name}" TYPE {new_type}'
        
        print(f"Изменение типа колонки {column_name} на {new_type}...")
        cursor.execute(sql)
        conn.commit()
        
        print(f"Тип колонки {column_name} успешно изменен на {new_type}")
        cursor.close()
        return True
    
    except Exception as e:
        logger.error(f"Ошибка при изменении типа колонки: {e}")
        conn.rollback()
        return False

def convert_column_to_numeric(conn, table_name, column_name):
    """Конвертация строковой колонки в числовой тип с обработкой ошибок"""
    try:
        cursor = conn.cursor()
        
        # Сначала создаем временную колонку
        temp_column = f"{column_name}_numeric"
        add_column(conn, table_name, temp_column, "numeric")
        
        # Заполняем временную колонку числовыми значениями
        cursor.execute(f"""
            UPDATE {table_name}
            SET "{temp_column}" = 
                CASE 
                    WHEN "{column_name}" ~ '^[0-9]+(\.[0-9]+)?$' THEN "{column_name}"::numeric 
                    ELSE NULL 
                END
        """)
        conn.commit()
        
        # Удаляем оригинальную колонку
        drop_column(conn, table_name, column_name)
        
        # Переименовываем временную колонку в оригинальное имя
        rename_column(conn, table_name, temp_column, column_name)
        
        print(f"Колонка {column_name} успешно конвертирована в числовой тип")
        cursor.close()
        return True
    
    except Exception as e:
        logger.error(f"Ошибка при конвертации колонки {column_name} в числовой тип: {e}")
        conn.rollback()
        return False

def convert_column_to_integer(conn, table_name, column_name):
    """Конвертация строковой колонки в целочисленный тип с обработкой ошибок"""
    try:
        cursor = conn.cursor()
        
        # Сначала создаем временную колонку
        temp_column = f"{column_name}_int"
        add_column(conn, table_name, temp_column, "integer")
        
        # Заполняем временную колонку целочисленными значениями
        cursor.execute(f"""
            UPDATE {table_name}
            SET "{temp_column}" = 
                CASE 
                    WHEN "{column_name}" ~ '^[0-9]+$' THEN "{column_name}"::integer 
                    ELSE NULL 
                END
        """)
        conn.commit()
        
        # Удаляем оригинальную колонку
        drop_column(conn, table_name, column_name)
        
        # Переименовываем временную колонку в оригинальное имя
        rename_column(conn, table_name, temp_column, column_name)
        
        print(f"Колонка {column_name} успешно конвертирована в целочисленный тип")
        cursor.close()
        return True
    
    except Exception as e:
        logger.error(f"Ошибка при конвертации колонки {column_name} в целочисленный тип: {e}")
        conn.rollback()
        return False

def add_column(conn, table_name, column_name, column_type):
    """Добавление новой колонки в таблицу"""
    try:
        cursor = conn.cursor()
        sql = f'ALTER TABLE {table_name} ADD COLUMN "{column_name}" {column_type}'
        
        print(f"Добавление колонки {column_name} с типом {column_type}...")
        cursor.execute(sql)
        conn.commit()
        
        print(f"Колонка {column_name} успешно добавлена")
        cursor.close()
        return True
    
    except Exception as e:
        logger.error(f"Ошибка при добавлении колонки: {e}")
        conn.rollback()
        return False

def rename_column(conn, table_name, old_name, new_name):
    """Переименование колонки"""
    try:
        cursor = conn.cursor()
        sql = f'ALTER TABLE {table_name} RENAME COLUMN "{old_name}" TO "{new_name}"'
        
        print(f"Переименование колонки {old_name} в {new_name}...")
        cursor.execute(sql)
        conn.commit()
        
        print(f"Колонка {old_name} успешно переименована в {new_name}")
        cursor.close()
        return True
    
    except Exception as e:
        logger.error(f"Ошибка при переименовании колонки: {e}")
        conn.rollback()
        return False

def create_index(conn, table_name, column_name, index_type="btree"):
    """Создание индекса для ускорения поиска"""
    try:
        cursor = conn.cursor()
        index_name = f"idx_{table_name}_{column_name}"
        sql = f'CREATE INDEX {index_name} ON {table_name} USING {index_type} ("{column_name}")'
        
        print(f"Создание индекса {index_name} для колонки {column_name}...")
        cursor.execute(sql)
        conn.commit()
        
        print(f"Индекс {index_name} успешно создан")
        cursor.close()
        return True
    
    except Exception as e:
        logger.error(f"Ошибка при создании индекса: {e}")
        conn.rollback()
        return False

def drop_column(conn, table_name, column_name):
    """Удаление колонки из таблицы"""
    try:
        cursor = conn.cursor()
        sql = f'ALTER TABLE {table_name} DROP COLUMN "{column_name}"'
        
        print(f"Удаление колонки {column_name}...")
        cursor.execute(sql)
        conn.commit()
        
        print(f"Колонка {column_name} успешно удалена")
        cursor.close()
        return True
    
    except Exception as e:
        logger.error(f"Ошибка при удалении колонки: {e}")
        conn.rollback()
        return False

def optimize_property_table(table_name="properties_optimized"):
    """Оптимизация таблицы с недвижимостью для аналитики"""
    try:
        conn = connect_to_db()
        if not conn:
            return False
        
        # Получаем текущую структуру таблицы
        columns = get_table_structure(conn, table_name)
        if not columns:
            conn.close()
            return False
        
        # Добавление новых колонок для аналитики
        add_column(conn, table_name, "price_per_sqm", "numeric")
        add_column(conn, table_name, "import_date", "timestamp DEFAULT CURRENT_TIMESTAMP")
        add_column(conn, table_name, "is_analyzed", "boolean DEFAULT false")
        
        # Преобразование числовых колонок из строковых в соответствующие типы
        convert_column_to_numeric(conn, table_name, "tsena")
        convert_column_to_numeric(conn, table_name, "ploschad")
        convert_column_to_integer(conn, table_name, "komnat")
        
        # Создание индексов для ускорения поиска
        create_index(conn, table_name, "id")
        create_index(conn, table_name, "tsena")
        create_index(conn, table_name, "ploschad")
        create_index(conn, table_name, "tip_nedvizhimosti")
        
        # Получаем обновленную структуру таблицы после преобразования типов
        columns = get_table_structure(conn, table_name)
        
        # Вычисление стоимости за квадратный метр для непустых значений
        cursor = conn.cursor()
        cursor.execute(f"""
            UPDATE {table_name}
            SET price_per_sqm = tsena / ploschad
            WHERE tsena IS NOT NULL AND ploschad IS NOT NULL AND ploschad > 0
        """)
        conn.commit()
        print("Рассчитано значение price_per_sqm")
        
        # Добавление тегов для простого фильтра по цене
        add_column(conn, table_name, "price_category", "character varying(50)")
        
        cursor.execute(f"""
            UPDATE {table_name}
            SET price_category = 
                CASE 
                    WHEN tsena IS NULL THEN 'Unknown'
                    WHEN tsena < 1000000 THEN 'Budget'
                    WHEN tsena < 5000000 THEN 'Mid-range'
                    WHEN tsena < 10000000 THEN 'Premium'
                    ELSE 'Luxury'
                END
        """)
        conn.commit()
        print("Добавлены категории цен для удобного фильтра")
        
        # Получаем обновленную структуру таблицы
        print("\nОбновленная структура таблицы:")
        get_table_structure(conn, table_name)
        
        # Подсчет количества записей
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        print(f"\nВсего записей в таблице {table_name}: {count}")
        
        # Статистика по категориям цен
        cursor.execute(f"""
            SELECT price_category, COUNT(*) as count
            FROM {table_name}
            GROUP BY price_category
            ORDER BY COUNT(*) DESC
        """)
        
        price_categories = cursor.fetchall()
        print("\nРаспределение по категориям цен:")
        for category in price_categories:
            print(f"  {category[0]}: {category[1]} объектов")
        
        # Закрытие соединения
        cursor.close()
        conn.close()
        
        print("\nОптимизация таблицы успешно завершена!")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при оптимизации таблицы: {e}")
        if conn:
            conn.close()
        return False

if __name__ == "__main__":
    # Вызов функции оптимизации
    optimize_property_table() 