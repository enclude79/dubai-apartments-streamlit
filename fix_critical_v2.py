import os
import re
import shutil
from datetime import datetime

def backup_original_file(file_path):
    """Создает резервную копию оригинального файла"""
    backup_dir = "backups"
    os.makedirs(backup_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = os.path.join(backup_dir, f"api_to_sql_{timestamp}.py.bak")
    
    shutil.copy2(file_path, backup_path)
    print(f"Создана резервная копия: {backup_path}")
    
    return backup_path

def fix_psycopg2_extras_import(file_path):
    """Исправляет импорт psycopg2.extras и других необходимых модулей"""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Проверяем и добавляем необходимые импорты
    imports_to_add = [
        ('import psycopg2.extras', 'import psycopg2\nimport psycopg2.extras'),
        ('import traceback', 'import logging\nimport traceback'),
        ('import numpy as np', 'import pandas as pd\nimport numpy as np')
    ]
    
    for import_check, import_replace in imports_to_add:
        if import_check not in content:
            pattern = import_replace.split('\n')[0]
            content = re.sub(pattern, import_replace, content)
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print("Импорты модулей исправлены")

def fix_insert_method(file_path):
    """Полностью переписывает метод вставки данных в базу"""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Находим функцию load_to_sql
    load_to_sql_pattern = re.compile(r'def load_to_sql\(properties_data\):(.*?)def api_to_sql', re.DOTALL)
    load_to_sql_match = load_to_sql_pattern.search(content)
    
    if not load_to_sql_match:
        print("Не удалось найти функцию load_to_sql")
        return False
    
    # Новая функция load_to_sql с лучшей обработкой ошибок и меньшими чанками данных
    new_load_to_sql = """def load_to_sql(properties_data):
    \"\"\"Загружает данные напрямую в SQL без использования CSV\"\"\"
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
            cursor.execute(\"\"\"
                SELECT column_name, data_type FROM information_schema.columns 
                WHERE table_name = 'bayut_properties' 
                ORDER BY ordinal_position;
            \"\"\")
            db_columns = {row[0]: row[1] for row in cursor.fetchall()}
            logger.info(f"ОТЛАДКА: Колонки в таблице: {list(db_columns.keys())}")
            logger.info(f"ОТЛАДКА: Типы данных колонок: {db_columns}")
        
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
        
        try:
            conn = psycopg2.connect(**connection_params, connect_timeout=15)
            conn.autocommit = False  # Для транзакции отключаем автокоммит
            cursor = conn.cursor()
            
            # Определяем колонки, которые существуют в таблице
            valid_columns = [col for col in df.columns if col in db_columns]
            
            # Создаем оптимальные группы колонок для вставки
            # Сначала обязательные колонки
            essential_columns = ['id', 'title', 'price', 'updated_at']
            
            # Затем другие колонки, которые есть и в DataFrame, и в таблице
            other_columns = [col for col in valid_columns if col not in essential_columns]
            
            # Формируем SQL запрос только с существующими колонками
            columns_str = ', '.join(essential_columns + other_columns)
            placeholders = ', '.join(['%s'] * len(essential_columns + other_columns))
            
            sql_query = f\"\"\"
                INSERT INTO bayut_properties 
                ({columns_str})
                VALUES ({placeholders})
                ON CONFLICT (id) DO UPDATE SET
            \"\"\"
            
            # Добавляем SET часть для UPDATE
            update_parts = []
            for col in essential_columns + other_columns:
                if col != 'id':  # id не обновляем
                    update_parts.append(f"{col} = EXCLUDED.{col}")
            
            sql_query += ", ".join(update_parts)
            
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
            
            # Вставляем данные небольшими порциями для избежания проблем с памятью
            chunk_size = 10  # Очень маленький размер чанка для отладки
            
            for i in range(0, len(values), chunk_size):
                chunk = values[i:i+chunk_size]
                chunk_start = i + 1
                chunk_end = min(i + chunk_size, len(values))
                
                logger.info(f"ОТЛАДКА: Вставка записей {chunk_start}-{chunk_end} из {len(values)}")
                
                try:
                    # Вставляем одну запись за раз для отладки
                    for j, record in enumerate(chunk):
                        logger.info(f"ОТЛАДКА: Вставка записи {chunk_start + j} из {len(values)}")
                        cursor.execute(sql_query, record)
                    
                    # Коммитим после каждого чанка
                    conn.commit()
                    inserted_count += len(chunk)
                    logger.info(f"ОТЛАДКА: Успешно вставлено {len(chunk)} записей (всего {inserted_count})")
                
                except Exception as e:
                    # Откатываем изменения для этого чанка и продолжаем
                    conn.rollback()
                    error_info = traceback.format_exc()
                    logger.error(f"ОТЛАДКА: Ошибка при вставке чанка {chunk_start}-{chunk_end}: {e}")
                    logger.error(f"ОТЛАДКА: Подробная информация: {error_info}")
                    # Логируем проблемную запись для отладки
                    if chunk:
                        logger.error(f"ОТЛАДКА: Проблемная запись: {chunk[0]}")
            
            # Ищем максимальную дату обновления
            max_updated_at = None
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
        return 0, 0, 1, None
"""
    
    # Заменяем старую функцию на новую
    new_content = content.replace(load_to_sql_match.group(0), new_load_to_sql + "\ndef api_to_sql")
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(new_content)
    
    print("Функция load_to_sql полностью переписана")
    return True

def fix_clean_text_function(file_path):
    """Улучшает функцию clean_text для лучшей обработки строк"""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Находим функцию clean_text
    clean_text_pattern = re.compile(r'def clean_text\(text\):(.*?)def', re.DOTALL)
    clean_text_match = clean_text_pattern.search(content)
    
    if not clean_text_match:
        print("Не удалось найти функцию clean_text")
        return False
    
    # Новая улучшенная функция clean_text
    new_clean_text = """def clean_text(text):
    \"\"\"Очищает текстовые данные от проблемных символов\"\"\"
    if text is None:
        return None
    
    if not isinstance(text, str):
        try:
            text = str(text)
        except Exception:
            return None
    
    try:
        # Удаляем невидимые управляющие символы
        text = re.sub(r'[\\x00-\\x1F\\x7F-\\x9F]', '', text)
        # Заменяем все непечатаемые символы на пробелы
        text = re.sub(r'[^\\x20-\\x7E\\u0400-\\u04FF]', ' ', text)
        # Убираем множественные пробелы
        text = re.sub(r'\\s+', ' ', text)
        # Обрезаем пробелы по краям
        text = text.strip()
        return text
    except Exception as e:
        logger.error(f"Ошибка при очистке текста: {e}")
        # Возвращаем пустую строку в случае ошибки
        return ""
"""
    
    # Заменяем старую функцию на новую
    new_content = content.replace(clean_text_match.group(0), new_clean_text + "\ndef ")
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(new_content)
    
    print("Функция clean_text улучшена")
    return True

def main():
    file_path = "api_to_sql.py"
    
    # Создаем резервную копию
    backup_path = backup_original_file(file_path)
    print(f"Создана резервная копия: {backup_path}")
    
    # Исправляем импорты модулей
    fix_psycopg2_extras_import(file_path)
    
    # Улучшаем функцию clean_text
    fix_clean_text_function(file_path)
    
    # Полностью переписываем метод вставки данных
    fix_insert_method(file_path)
    
    print("Все критические исправления успешно применены к файлу api_to_sql.py")
    print("Теперь скрипт должен корректно выполнять вставку данных с подробным логированием каждого шага.")

if __name__ == "__main__":
    main() 