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

def fix_load_to_sql_function(file_path):
    """Исправляет функцию load_to_sql для правильной вставки данных в базу"""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Исправляем SQL-запрос в функции load_to_sql
    old_query = """
                INSERT INTO bayut_properties 
                (id, title, price, updated_at)
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                title = EXCLUDED.title,
                price = EXCLUDED.price,
                updated_at = EXCLUDED.updated_at
            """
    
    new_query = """
                INSERT INTO bayut_properties 
                (id, title, price, rooms, baths, area, location, created_at, updated_at)
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                title = EXCLUDED.title,
                price = EXCLUDED.price,
                rooms = EXCLUDED.rooms,
                baths = EXCLUDED.baths,
                area = EXCLUDED.area,
                location = EXCLUDED.location,
                updated_at = EXCLUDED.updated_at
            """
    
    # Заменяем SQL-запрос
    content = content.replace(old_query, new_query)
    
    # Исправляем создание values для вставки
    old_values_code = """
            # Создаем список кортежей с данными для вставки
            values = []
            for _, row in df.iterrows():
                property_id = int(row['id']) if pd.notna(row['id']) else None
                title = str(row['title']) if pd.notna(row['title']) else None
                price = float(row['price']) if pd.notna(row['price']) else None
                updated_at = row['updated_at'] if pd.notna(row['updated_at']) else None
                
                values.append((property_id, title, price, updated_at))
                processed_ids.append(property_id)
            """
    
    new_values_code = """
            # Создаем список кортежей с данными для вставки
            values = []
            for _, row in df.iterrows():
                property_id = int(row['id']) if pd.notna(row['id']) else None
                title = str(row['title']) if pd.notna(row['title']) else None
                price = float(row['price']) if pd.notna(row['price']) else None
                rooms = int(row['rooms']) if pd.notna(row.get('rooms')) else None
                baths = int(row['baths']) if pd.notna(row.get('baths')) else None
                area = float(row['area']) if pd.notna(row.get('area')) else None
                location = str(row['location']) if pd.notna(row.get('location')) else None
                created_at = row['created_at'] if pd.notna(row.get('created_at')) else datetime.now()
                updated_at = row['updated_at'] if pd.notna(row['updated_at']) else datetime.now()
                
                values.append((property_id, title, price, rooms, baths, area, location, created_at, updated_at))
                processed_ids.append(property_id)
            """
    
    content = content.replace(old_values_code, new_values_code)
    
    # Убедимся, что закрытие соединения происходит только в блоке finally
    old_finally_block = """
        finally:
            cursor.close()
            conn.close()
            logger.info("ОТЛАДКА: Закрыто соединение для транзакции")
            print("ОТЛАДКА: Закрыто соединение для транзакции")
    """
    
    new_finally_block = """
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
            logger.info("ОТЛАДКА: Закрыто соединение для транзакции")
            print("ОТЛАДКА: Закрыто соединение для транзакции")
    """
    
    content = content.replace(old_finally_block, new_finally_block)
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print("Функция load_to_sql исправлена")

def fix_db_connection_in_get_count(file_path):
    """Исправляет подключение к базе данных в функции логирования количества записей"""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Находим и исправляем проблему с проверкой количества записей
    old_connection = """
        with psycopg2.connect(**{k: v for k, v in DB_CONFIG.items() if k != "table"}) as conn:
"""
    
    new_connection = """
        # Используем правильный способ подключения без параметра table
        connection_params = {k: v for k, v in DB_CONFIG.items() if k != "table"}
        with psycopg2.connect(**connection_params) as conn:
"""
    
    content = content.replace(old_connection, new_connection)
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print("Исправлено подключение к базе данных в функции логирования количества записей")

def main():
    file_path = "api_to_sql.py"
    
    # Создаем резервную копию
    backup_path = backup_original_file(file_path)
    print(f"Создана резервная копия: {backup_path}")
    
    # Исправляем функцию load_to_sql
    fix_load_to_sql_function(file_path)
    
    # Исправляем подключение к базе данных
    fix_db_connection_in_get_count(file_path)
    
    print("Все исправления успешно применены к файлу api_to_sql.py")

if __name__ == "__main__":
    main() 