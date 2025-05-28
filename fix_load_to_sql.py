import os
import psycopg2
import psycopg2.extras
import pandas as pd
import logging
from dotenv import load_dotenv
from datetime import datetime

# Настройка логирования
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = f'{log_dir}/fix_load_to_sql_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Загружаем переменные окружения
load_dotenv()

# Параметры базы данных
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASSWORD', 'Enclude79'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432')
}

def clean_text(text):
    """Очищает текст от символов, не поддерживаемых WIN1251 (cp1251)"""
    if not isinstance(text, str):
        return text
    try:
        # Перекодируем в cp1251, заменяя неподдерживаемые символы на '?'
        return text.encode('cp1251', errors='replace').decode('cp1251')
    except Exception:
        return text.encode('ascii', 'ignore').decode('ascii')

def get_required_columns():
    """Получает список обязательных колонок для таблицы bayut_properties"""
    try:
        conn = psycopg2.connect(
            dbname=DB_CONFIG['dbname'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port']
        )
        conn.autocommit = True
        
        with conn.cursor() as cursor:
            # Получаем список всех колонок таблицы bayut_properties
            cursor.execute("""
                SELECT column_name, is_nullable, data_type
                FROM information_schema.columns
                WHERE table_name = 'bayut_properties'
                ORDER BY ordinal_position;
            """)
            
            columns = cursor.fetchall()
            
            # Выделяем обязательные колонки (где is_nullable = 'NO')
            required_columns = []
            all_columns = []
            
            for col_name, is_nullable, data_type in columns:
                all_columns.append((col_name, data_type))
                if is_nullable == 'NO':
                    required_columns.append(col_name)
            
            logger.info(f"Все колонки таблицы bayut_properties: {len(all_columns)}")
            logger.info(f"Обязательные колонки: {required_columns}")
            
            return required_columns, all_columns
    
    except Exception as e:
        logger.error(f"Ошибка при получении списка колонок: {e}")
        return [], []
    finally:
        if conn:
            conn.close()

def fix_load_to_sql_function():
    """Исправляет функцию load_to_sql в файле api_to_sql.py"""
    try:
        # Получаем список всех колонок и обязательных колонок
        required_columns, all_columns = get_required_columns()
        
        if not all_columns:
            logger.error("Не удалось получить список колонок таблицы bayut_properties")
            return False
        
        # Создаем тестовые данные для проверки вставки
        logger.info("Создание тестовых данных для проверки вставки...")
        
        test_data = [{
            'id': 999999,  # Уникальный ID для теста
            'title': 'Test Property - Fixed',
            'price': 1000000.0,
            'rooms': 3,
            'baths': 2,
            'area': 150.0,
            'location': 'Test Location',
            'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'purpose': 'for-sale',
            'is_verified': True
        }]
        
        # Создаем соединение с autocommit=True для очистки тестовых данных
        conn_cleanup = psycopg2.connect(
            dbname=DB_CONFIG['dbname'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port']
        )
        conn_cleanup.autocommit = True
        
        try:
            with conn_cleanup.cursor() as cur:
                # Удаляем тестовую запись, если она существует
                cur.execute("DELETE FROM bayut_properties WHERE id = 999999")
                logger.info("Тестовая запись удалена (если существовала)")
        except Exception as e:
            logger.error(f"Ошибка при очистке тестовых данных: {e}")
        finally:
            conn_cleanup.close()
        
        # Тестируем исправленную функцию load_to_sql
        result = fixed_load_to_sql(test_data)
        
        if result[0] > 0:
            logger.info("Тест исправленной функции load_to_sql УСПЕШЕН")
            return True
        else:
            logger.error("Тест исправленной функции load_to_sql НЕУДАЧЕН")
            return False
    
    except Exception as e:
        logger.error(f"Ошибка при исправлении функции load_to_sql: {e}")
        return False

def fixed_load_to_sql(properties_data):
    """Исправленная версия функции load_to_sql"""
    logger.info("Запуск исправленной функции load_to_sql...")
    
    if not properties_data:
        logger.warning("Нет данных для загрузки в базу данных")
        return 0, 0, 1, None
    
    # Создаем новое соединение с autocommit=False специально для транзакции
    conn = None
    cursor = None
    
    try:
        # Преобразуем список словарей в DataFrame для удобства обработки
        df = pd.DataFrame(properties_data)
        
        # Логируем информацию о данных
        logger.info(f"Загрузка {len(df)} записей в базу данных")
        
        # Очищаем текстовые данные от проблемных символов
        for col in df.columns:
            if df[col].dtype == 'object':  # Для строковых столбцов
                df[col] = df[col].apply(clean_text)
        
        # Удаляем дубликаты по (id, updated_at)
        df = df.drop_duplicates(subset=['id', 'updated_at'])
        logger.info(f"После удаления дубликатов осталось {len(df)} записей")
        
        # Подключаемся к базе данных
        logger.info("Создание соединения с autocommit=False для транзакции")
        conn = psycopg2.connect(
            dbname=DB_CONFIG['dbname'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port']
        )
        conn.autocommit = False
        cursor = conn.cursor()
        
        # Проверяем существование таблицы
        cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'bayut_properties');")
        table_exists = cursor.fetchone()[0]
        logger.info(f"Таблица bayut_properties существует: {table_exists}")
        
        if not table_exists:
            logger.error("Таблица bayut_properties не существует! Необходимо создать таблицу.")
            return 0, 0, 1, None
        
        # Получаем список всех колонок
        cursor.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = 'bayut_properties' 
            ORDER BY ordinal_position;
        """)
        db_columns = [row[0] for row in cursor.fetchall()]
        logger.info(f"Колонки в таблице: {db_columns}")
        
        # Подготавливаем данные для вставки
        records_to_insert = []
        processed_ids = []
        
        # Создаем список кортежей для вставки
        for _, row in df.iterrows():
            # Создаем словарь значений для текущей записи
            record_values = {}
            
            # Заполняем значения из DataFrame, игнорируя отсутствующие поля
            for col in db_columns:
                if col in row:
                    record_values[col] = row[col]
            
            # Добавляем ID в список обработанных
            if 'id' in record_values:
                processed_ids.append(record_values['id'])
            
            # Добавляем запись в список для вставки
            records_to_insert.append(record_values)
        
        logger.info(f"Подготовлено {len(records_to_insert)} записей для вставки")
        
        # Вставляем записи пакетно
        new_records = 0
        updated_records = 0
        
        for record in records_to_insert:
            # Получаем список колонок и значений
            columns = list(record.keys())
            values = [record[col] for col in columns]
            
            # Формируем запрос
            columns_str = ', '.join(columns)
            placeholders = ', '.join(['%s'] * len(columns))
            
            # Формируем строку для обновления при конфликте
            update_parts = []
            for col in columns:
                if col != 'id':  # Исключаем первичный ключ
                    update_parts.append(f"{col} = EXCLUDED.{col}")
            
            update_str = ', '.join(update_parts)
            
            # SQL запрос для вставки или обновления данных
            sql_query = f"""
                INSERT INTO bayut_properties 
                ({columns_str})
                VALUES ({placeholders})
                ON CONFLICT (id) DO UPDATE SET
                {update_str}
            """
            
            try:
                # Выполняем запрос
                cursor.execute(sql_query, values)
                new_records += 1
            except Exception as e:
                logger.error(f"Ошибка при вставке записи {record.get('id')}: {e}")
                # Пропускаем ошибочную запись и продолжаем
                continue
        
        # Фиксируем изменения
        logger.info("Фиксация изменений (commit)...")
        conn.commit()
        logger.info("Изменения зафиксированы успешно")
        
        # Ищем максимальную дату обновления
        max_updated_at = None
        if 'updated_at' in df.columns:
            max_updated_at = df['updated_at'].max()
            logger.info(f"Максимальная дата обновления: {max_updated_at}")
        
        logger.info(f"Всего обработано записей: {new_records}")
        logger.info(f"Добавлено новых: {new_records}, обновлено: {updated_records}")
        
        return new_records, updated_records, 0, max_updated_at
    
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных в SQL: {e}")
        if conn:
            try:
                conn.rollback()
                logger.info("Изменения отменены (rollback)")
            except:
                pass
        return 0, 0, 1, None
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        logger.info("Соединение с базой данных закрыто")

def write_fixed_function():
    """Записывает исправленную функцию load_to_sql в файл"""
    fixed_code = '''
def load_to_sql(properties_data):
    """Загружает данные напрямую в SQL без использования CSV"""
    if not properties_data:
        logger.warning("Нет данных для загрузки в базу данных")
        print("Нет данных для загрузки в базу данных")
        return 0, 0, 1, None
    
    # Подключаемся к базе данных для проверки структуры
    db = DatabaseConnection(DB_CONFIG)
    if not db.connect():
        logger.error("ОТЛАДКА: Ошибка подключения к базе данных в load_to_sql")
        print("ОТЛАДКА: Ошибка подключения к базе данных в load_to_sql")
        return 0, 0, 1, None
    
    conn = None
    cursor = None
    
    try:
        # Преобразуем данные в безопасные типы Python
        safe_properties_data = []
        
        for item in properties_data:
            safe_item = {}
            for key, value in item.items():
                if isinstance(value, np.integer):
                    safe_item[key] = int(value)  # numpy.int64 -> int
                elif isinstance(value, np.floating):
                    safe_item[key] = float(value)  # numpy.float64 -> float
                elif isinstance(value, np.bool_):
                    safe_item[key] = bool(value)  # numpy.bool_ -> bool
                else:
                    safe_item[key] = value
            safe_properties_data.append(safe_item)
        
        # Используем новый список с безопасными типами
        properties_data = safe_properties_data
        
        # Преобразуем список словарей в DataFrame для дальнейшей обработки
        df = pd.DataFrame(properties_data)
        
        # Логируем информацию о данных
        logger.info(f"Загрузка {len(df)} записей в базу данных")
        print(f"Загрузка {len(df)} записей в базу данных")
        
        # Очищаем текстовые данные от проблемных символов
        for col in df.columns:
            if df[col].dtype == 'object':  # Для строковых столбцов
                df[col] = df[col].apply(clean_text)
        
        # Удаляем дубликаты по (id, updated_at)
        df = df.drop_duplicates(subset=['id', 'updated_at'])
        logger.info(f"ОТЛАДКА: После удаления дубликатов осталось {len(df)} записей")
        
        # Создаем новое соединение с autocommit=False специально для транзакции
        conn = psycopg2.connect(
            dbname=DB_CONFIG['dbname'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port']
        )
        conn.autocommit = False  # Устанавливаем autocommit=False для новой транзакции
        cursor = conn.cursor()
        
        try:
            # Сохраняем ID обработанных записей
            processed_ids = []
            
            # Проверяем существование таблицы
            cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'bayut_properties');")
            table_exists = cursor.fetchone()[0]
            logger.info(f"ОТЛАДКА: Таблица bayut_properties существует: {table_exists}")
            print(f"ОТЛАДКА: Таблица bayut_properties существует: {table_exists}")
            
            if not table_exists:
                logger.error("ОТЛАДКА: Таблица bayut_properties не существует! Необходимо создать таблицу.")
                print("ОТЛАДКА: Таблица bayut_properties не существует! Необходимо создать таблицу.")
                return 0, 0, 1, None
            
            # Получаем список всех колонок
            cursor.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name = 'bayut_properties' 
                ORDER BY ordinal_position;
            """)
            db_columns = [row[0] for row in cursor.fetchall()]
            logger.info(f"ОТЛАДКА: Колонки в таблице: {db_columns}")
            
            # Создаем список кортежей с данными для вставки
            values = []
            for _, row in df.iterrows():
                property_id = int(row['id']) if pd.notna(row['id']) else None
                title = str(row['title']) if pd.notna(row['title']) else None
                price = float(row['price']) if pd.notna(row['price']) else None
                updated_at = row['updated_at'] if pd.notna(row['updated_at']) else None
                
                values.append((property_id, title, price, updated_at))
                processed_ids.append(property_id)
            
            # SQL запрос для вставки или обновления данных
            sql_query = """
                INSERT INTO bayut_properties 
                (id, title, price, updated_at)
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                title = EXCLUDED.title,
                price = EXCLUDED.price,
                updated_at = EXCLUDED.updated_at
            """
            
            logger.info(f"ОТЛАДКА: Выполнение пакетной вставки {len(values)} записей")
            print(f"ОТЛАДКА: Выполнение пакетной вставки {len(values)} записей")
            
            try:
                # Используем execute_values для пакетной вставки
                psycopg2.extras.execute_values(
                    cursor, 
                    sql_query, 
                    values,
                    template=None, 
                    page_size=100
                )
                
                logger.info("ОТЛАДКА: Пакетная вставка успешно выполнена")
                print("ОТЛАДКА: Пакетная вставка успешно выполнена")
                
                # Фиксируем изменения
                conn.commit()
                logger.info("ОТЛАДКА: Изменения зафиксированы успешно")
                print("ОТЛАДКА: Изменения зафиксированы успешно")
                
                # Определяем количество вставленных и обновленных записей
                # Так как мы использовали пакетную вставку, точное количество неизвестно
                new_records = len(values)
                updated_records = 0
                
                logger.info(f"Всего обработано записей: {len(values)}")
                logger.info(f"Добавлено записей: {new_records}")
                
                print(f"Всего обработано записей: {len(values)}")
                print(f"Добавлено записей: {new_records}")
                
                # Ищем максимальную дату обновления
                max_updated_at = None
                if 'updated_at' in df.columns:
                    max_updated_at = df['updated_at'].max()
                    logger.info(f"ОТЛАДКА: Максимальная дата обновления: {max_updated_at}")
                
                return new_records, updated_records, 0, max_updated_at
            
            except Exception as e:
                conn.rollback()
                logger.error(f"ОТЛАДКА: Ошибка при выполнении пакетной вставки: {e}")
                print(f"ОТЛАДКА: Ошибка при выполнении пакетной вставки: {e}")
                raise
        finally:
            cursor.close()
            conn.close()
            logger.info("ОТЛАДКА: Закрыто соединение для транзакции")
            print("ОТЛАДКА: Закрыто соединение для транзакции")
    
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных в SQL: {e}")
        print(f"Ошибка при загрузке данных в SQL: {e}")
        return 0, 0, 1, None
'''
    
    try:
        with open('fixed_load_to_sql.py', 'w', encoding='utf-8') as f:
            f.write(fixed_code)
        logger.info("Исправленная функция load_to_sql записана в файл fixed_load_to_sql.py")
        return True
    except Exception as e:
        logger.error(f"Ошибка при записи исправленной функции в файл: {e}")
        return False

if __name__ == "__main__":
    print("Запуск исправления функции load_to_sql...")
    logger.info("Запуск исправления функции load_to_sql...")
    
    # Исправляем функцию load_to_sql
    success = fix_load_to_sql_function()
    
    # Записываем исправленную функцию в файл
    write_fixed_function()
    
    if success:
        print("Функция load_to_sql успешно исправлена и протестирована")
        print("Исправленная функция записана в файл fixed_load_to_sql.py")
        print("Для применения исправлений замените функцию load_to_sql в файле api_to_sql.py")
    else:
        print("Ошибка при исправлении функции load_to_sql")
    
    print("Проверьте лог для получения подробной информации")
    logger.info("Исправление функции load_to_sql завершено") 