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

def fix_db_connection(file_path):
    """Исправляет проблему с подключением к базе данных - удаляет параметр 'table' из DB_CONFIG при подключении"""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Ищем места, где создается подключение к базе данных напрямую через psycopg2.connect
    # и передается весь DB_CONFIG, который содержит параметр 'table'
    connection_pattern = re.compile(r'psycopg2\.connect\(\*\*DB_CONFIG\)')
    modified_content = connection_pattern.sub(r'psycopg2.connect(**{k: v for k, v in DB_CONFIG.items() if k != "table"})', content)
    
    # Также исправим другие места, где могут передаваться все параметры DB_CONFIG
    # Например, при создании объекта DatabaseConnection
    db_connection_pattern = re.compile(r'DatabaseConnection\(DB_CONFIG\)')
    modified_content = db_connection_pattern.sub(r'DatabaseConnection({k: v for k, v in DB_CONFIG.items() if k != "table"})', modified_content)
    
    # Записываем изменения в файл
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(modified_content)
    
    print("Исправлено использование DB_CONFIG при подключении к базе данных")
    return True

if __name__ == "__main__":
    file_path = "api_to_sql.py"
    
    if not os.path.exists(file_path):
        print(f"Ошибка: файл {file_path} не найден")
        exit(1)
    
    # Создаем резервную копию
    backup_path = backup_original_file(file_path)
    
    # Исправляем проблему с подключением
    success = fix_db_connection(file_path)
    
    if success:
        print("Файл успешно обновлен, исправлена проблема с подключением к базе данных")
        print(f"Резервная копия сохранена в {backup_path}")
    else:
        print("Произошла ошибка при обновлении файла")
        print(f"Оригинальный файл доступен в {backup_path}") 