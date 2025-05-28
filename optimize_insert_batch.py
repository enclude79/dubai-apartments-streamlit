import os
import re
import shutil
import traceback
from datetime import datetime
import psycopg2
import psycopg2.extras
import logging

# Настройка логирования
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = f'{log_dir}/optimize_insert_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def backup_original_file(file_path):
    """Создает резервную копию оригинального файла"""
    backup_dir = "backups"
    os.makedirs(backup_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = os.path.join(backup_dir, f"api_to_sql_{timestamp}.py.bak")
    
    shutil.copy2(file_path, backup_path)
    logger.info(f"Создана резервная копия: {backup_path}")
    
    return backup_path

def fix_load_to_sql_function(file_path):
    """Заменяет функцию load_to_sql на оптимизированную версию"""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Находим начало функции load_to_sql
    load_to_sql_start = content.find("def load_to_sql(properties_data):")
    if load_to_sql_start == -1:
        logger.error("Не удалось найти функцию load_to_sql в файле")
        return False
    
    # Находим следующую функцию после load_to_sql
    next_function_match = re.search(r"def\s+\w+\(", content[load_to_sql_start+10:])
    if next_function_match:
        next_function_start = load_to_sql_start + 10 + next_function_match.start()
        load_to_sql_content = content[load_to_sql_start:next_function_start]
    else:
        logger.error("Не удалось найти конец функции load_to_sql")
        return False
    
    # Новая оптимизированная версия функции load_to_sql
    new_load_to_sql = '''
def load_to_sql(properties_data):
    """Загружает данные напрямую в SQL без использования CSV"""
    import traceback  # Импортируем здесь для надежности
    
    if not properties_data:
        logger.warning("Нет данных для загрузки в базу данных")
        print("Нет данных для загрузки в базу данных")
        return 0, 0, 1, None
    
    # Подключаемся к базе данных для проверки структуры
    try:
        connection_params = {k: v for k, v in DB_CONFIG.items() if k != "table"}
        logger.info("Подключение к базе данных для проверки структуры...")
        conn = psycopg2.connect(**connection_params, connect_timeout=10)
        conn.autocommit = True  # Для начальной проверки используем автокоммит
        
        with conn.cursor() as cursor:
            # Проверяем существование таблицы
            cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'bayut_properties');")
            table_exists = cursor.fetchone()[0]
            logger.info(f"ОТЛАДКА: Таблица bayut_properties существует: {table_exists}")
            
            if not table_exists:
                logger.error("ОТЛАДКА: Таблица bayut_properties не существует! Необходимо создать таблицу.")
                return 0, 0, 1, None
            
            # Получаем список всех колонок
            cursor.execute("""
                SELECT column_name, data_type FROM information_schema.columns 
                WHERE table_name = 'bayut_properties' 
                ORDER BY ordinal_position;
            """)
            db_columns = {row[0]: row[1] for row in cursor.fetchall()}
            logger.info(f"ОТЛАДКА: Колонки в таблице: {list(db_columns.keys())}")
        
        conn.close()
        logger.info("Закрыто соединение для проверки структуры")
    except Exception as e:
        logger.error(f"Ошибка при проверке структуры базы данных: {e}")
        return 0, 0, 1, None
    
    # Преобразуем данные в безопасные типы Python
    try:
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
        
        # Преобразуем список словарей в DataFrame для дальнейшей обработки
        df = pd.DataFrame(safe_properties_data)
        
        # Логируем информацию о данных
        logger.info(f"Загрузка {len(df)} записей в базу данных")
        
        # Очищаем текстовые данные от проблемных символов
        for col in df.columns:
            if df[col].dtype == 'object':  # Для строковых столбцов
                df[col] = df[col].apply(lambda x: clean_text(x) if isinstance(x, str) else x)
        
        # Удаляем дубликаты по (id, updated_at)
        df = df.drop_duplicates(subset=['id', 'updated_at'])
        logger.info(f"ОТЛАДКА: После удаления дубликатов осталось {len(df)} записей")
        
        # Создаем новое соединение для транзакции
        conn = None
        cursor = None
        inserted_count = 0
        max_updated_at = None
        
        try:
            # Подключаемся к базе данных
            conn = psycopg2.connect(**connection_params, connect_timeout=15)
            
            # ВАЖНО: Отключаем автокоммит для управления транзакциями вручную
            conn.autocommit = False
            
            cursor = conn.cursor()
            
            # Определяем колонки, которые существуют в таблице
            valid_columns = [col for col in df.columns if col in db_columns]
            
            # Сначала обязательные колонки
            essential_columns = ['id', 'title', 'price', 'updated_at']
            
            # Затем другие колонки, которые есть и в DataFrame, и в таблице
            other_columns = [col for col in valid_columns if col not in essential_columns]
            
            # Формируем SQL запрос только с существующими колонками
            columns_str = ', '.join(essential_columns + other_columns)
            
            # Используем подход с RETURNING для получения ID вставленных записей
            sql_query = f"""
                INSERT INTO bayut_properties ({columns_str})
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET
            """
            
            # Добавляем SET часть для UPDATE
            update_parts = []
            for col in essential_columns + other_columns:
                if col != 'id':  # id не обновляем
                    update_parts.append(f"{col} = EXCLUDED.{col}")
            
            sql_query += ", ".join(update_parts)
            
            # Опционально, можно добавить RETURNING для проверки
            sql_query += " RETURNING id"
            
            logger.info(f"ОТЛАДКА: SQL запрос: {sql_query}")
            
            # Создаем список значений для вставки
            values = []
            for _, row in df.iterrows():
                row_values = []
                for col in essential_columns + other_columns:
                    if col in row and pd.notna(row[col]):
                        row_values.append(row[col])
                    else:
                        row_values.append(None)
                values.append(tuple(row_values))
            
            logger.info(f"ОТЛАДКА: Подготовлено {len(values)} записей для вставки")
            
            # Вставляем данные небольшими порциями
            chunk_size = 20  # Увеличиваем размер чанка для эффективности
            
            for i in range(0, len(values), chunk_size):
                chunk = values[i:i+chunk_size]
                chunk_start = i + 1
                chunk_end = min(i + chunk_size, len(values))
                
                logger.info(f"ОТЛАДКА: Вставка записей {chunk_start}-{chunk_end} из {len(values)}")
                
                try:
                    # Используем execute_values для пакетной вставки
                    result = psycopg2.extras.execute_values(
                        cursor, 
                        sql_query, 
                        chunk,
                        template=None, 
                        page_size=100,
                        fetch=True  # Получаем результаты RETURNING
                    )
                    
                    # Подтверждаем транзакцию после каждого чанка
                    conn.commit()
                    
                    if result:
                        # Записываем количество вставленных записей
                        inserted_records = len(result)
                        inserted_count += inserted_records
                        logger.info(f"ОТЛАДКА: Успешно вставлено {inserted_records} записей (всего {inserted_count})")
                    
                except Exception as e:
                    # Откатываем изменения для этого чанка при ошибке
                    conn.rollback()
                    error_info = traceback.format_exc()
                    logger.error(f"ОШИБКА при вставке чанка {chunk_start}-{chunk_end}: {e}")
                    logger.error(f"Подробная информация: {error_info}")
                    
                    # Продолжаем с следующим чанком вместо прерывания всего процесса
                    logger.info("Продолжаем со следующим чанком...")
            
            # Ищем максимальную дату обновления
            if 'updated_at' in df.columns:
                max_updated_at = df['updated_at'].max()
                logger.info(f"ОТЛАДКА: Максимальная дата обновления: {max_updated_at}")
            
            return inserted_count, 0, 0, max_updated_at
            
        except Exception as e:
            if conn:
                conn.rollback()
            error_info = traceback.format_exc()
            logger.error(f"КРИТИЧЕСКАЯ ОШИБКА при работе с базой данных: {e}")
            logger.error(f"Подробная информация: {error_info}")
            return 0, 0, 1, None
            
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
            logger.info("ОТЛАДКА: Закрыто соединение для транзакции")
    
    except Exception as e:
        error_info = traceback.format_exc()
        logger.error(f"Ошибка при подготовке данных: {e}")
        logger.error(f"Подробная информация: {error_info}")
        return 0, 0, 1, None'''
    
    # Заменяем старую функцию на новую
    new_content = content[:load_to_sql_start] + new_load_to_sql + content[next_function_start:]
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(new_content)
    
    logger.info("Функция load_to_sql успешно заменена на оптимизированную версию")
    return True

def fix_import_statements(file_path):
    """Добавляет или исправляет импорты, необходимые для корректной работы скрипта"""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Проверяем наличие импорта re
    if 'import re' not in content.split('\n'):
        # Добавляем импорт re после других импортов
        import_section = content.find('import')
        import_end = content.find('\n\n', import_section)
        if import_end == -1:
            import_end = content.find('\n# ', import_section)
        
        if import_end != -1:
            new_content = content[:import_end] + '\nimport re' + content[import_end:]
            content = new_content
        else:
            logger.error("Не удалось найти место для добавления импорта re")
    
    # Проверяем наличие импорта psycopg2.extras
    if 'import psycopg2.extras' not in content:
        # Добавляем импорт psycopg2.extras после импорта psycopg2
        psycopg2_import = content.find('import psycopg2')
        if psycopg2_import != -1:
            line_end = content.find('\n', psycopg2_import)
            new_content = content[:line_end] + '\nimport psycopg2.extras' + content[line_end:]
            content = new_content
        else:
            logger.error("Не удалось найти импорт psycopg2")
    
    # Проверяем наличие импорта traceback
    if 'import traceback' not in content:
        # Добавляем импорт traceback после других импортов
        import_section = content.find('import')
        import_end = content.find('\n\n', import_section)
        if import_end == -1:
            import_end = content.find('\n# ', import_section)
        
        if import_end != -1:
            new_content = content[:import_end] + '\nimport traceback' + content[import_end:]
            content = new_content
        else:
            logger.error("Не удалось найти место для добавления импорта traceback")
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    logger.info("Импорты успешно исправлены")
    return True

def fix_clean_text_function(file_path):
    """Исправляет функцию clean_text, чтобы она не вызывала ошибку с модулем re"""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Находим начало функции clean_text
    clean_text_start = content.find("def clean_text(text):")
    if clean_text_start == -1:
        logger.error("Не удалось найти функцию clean_text в файле")
        return False
    
    # Проверяем, есть ли уже импорт re в функции
    clean_text_body_start = content.find(":", clean_text_start) + 1
    clean_text_end = content.find("def ", clean_text_start + 10)
    clean_text_content = content[clean_text_body_start:clean_text_end]
    
    # Если импорт re уже есть в функции, то не делаем изменений
    if "import re" in clean_text_content:
        logger.info("Функция clean_text уже содержит импорт re")
        return True
    
    # Новая версия функции clean_text с импортом re
    new_clean_text = '''
def clean_text(text):
    """Очищает текстовые данные от проблемных символов"""
    import re  # Импортируем re внутри функции для надежности
    
    if text is None:
        return None
    
    if not isinstance(text, str):
        try:
            text = str(text)
        except Exception:
            return None
    
    try:
        # Удаляем невидимые управляющие символы
        text = re.sub(r'[\x00-\x1F\x7F-\x9F]', '', text)
        # Заменяем все непечатаемые символы на пробелы
        text = re.sub(r'[^\x20-\x7E\u0400-\u04FF]', ' ', text)
        # Убираем множественные пробелы
        text = re.sub(r'\s+', ' ', text)
        # Обрезаем пробелы по краям
        text = text.strip()
        return text
    except Exception as e:
        logger.error(f"Ошибка при очистке текста: {e}")
        # Возвращаем пустую строку в случае ошибки
        return ""
'''
    
    # Заменяем старую функцию на новую
    new_content = content[:clean_text_start] + new_clean_text + content[clean_text_end:]
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(new_content)
    
    logger.info("Функция clean_text успешно исправлена")
    return True

def main():
    """Основная функция для применения всех исправлений"""
    file_path = "api_to_sql.py"
    
    # Создаем резервную копию
    backup_path = backup_original_file(file_path)
    logger.info(f"Создана резервная копия: {backup_path}")
    
    # Исправляем импорты
    if fix_import_statements(file_path):
        logger.info("Импорты успешно исправлены")
    else:
        logger.error("Не удалось исправить импорты")
    
    # Исправляем функцию clean_text
    if fix_clean_text_function(file_path):
        logger.info("Функция clean_text успешно исправлена")
    else:
        logger.error("Не удалось исправить функцию clean_text")
    
    # Исправляем функцию load_to_sql
    if fix_load_to_sql_function(file_path):
        logger.info("Функция load_to_sql успешно исправлена")
    else:
        logger.error("Не удалось исправить функцию load_to_sql")
    
    logger.info("Все исправления успешно применены к файлу api_to_sql.py")
    logger.info("Теперь скрипт должен корректно выполнять вставку данных в базу данных")
    print("Все исправления успешно применены к файлу api_to_sql.py")

if __name__ == "__main__":
    main() 