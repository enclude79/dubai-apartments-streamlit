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

def add_count_logging_code(file_path):
    """Добавляет код для логирования количества записей в базе данных в конце скрипта"""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Ищем функцию main
    main_pattern = re.compile(r'def\s+main\s*\(.*?\).*?return.*?\n', re.DOTALL)
    main_match = main_pattern.search(content)
    
    if not main_match:
        print("Ошибка: функция main не найдена в файле api_to_sql.py")
        return False
    
    # Получаем текст функции main
    main_function = main_match.group(0)
    
    # Ищем последний return в функции main
    return_pattern = re.compile(r'(\s+return\s+.*?)$', re.MULTILINE)
    return_match = return_pattern.search(main_function)
    
    if not return_match:
        print("Ошибка: оператор return не найден в функции main")
        return False
    
    # Код для логирования количества записей в базе данных
    log_count_code = """
    # Логируем количество записей в базе данных после загрузки
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM bayut_properties")
                count = cursor.fetchone()[0]
                logger.info(f"Количество записей в таблице bayut_properties после загрузки: {count}")
                print(f"Количество записей в таблице bayut_properties после загрузки: {count}")
                
                cursor.execute("SELECT updated_at FROM bayut_properties ORDER BY updated_at DESC LIMIT 1")
                last_update = cursor.fetchone()
                if last_update:
                    logger.info(f"Последняя дата обновления в таблице bayut_properties: {last_update[0]}")
    except Exception as e:
        logger.error(f"Ошибка при проверке количества записей: {e}")
    """
    
    # Добавляем код для логирования перед return
    indent = return_match.group(1).split('return')[0]  # Получаем отступ
    log_count_code = '\n'.join([indent + line for line in log_count_code.strip().split('\n')])
    
    modified_main = main_function.replace(return_match.group(0), log_count_code + "\n" + return_match.group(0))
    modified_content = content.replace(main_function, modified_main)
    
    # Записываем измененное содержимое в файл
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(modified_content)
    
    print(f"Код для логирования количества записей добавлен в файл {file_path}")
    return True

if __name__ == "__main__":
    file_path = "api_to_sql.py"
    
    if not os.path.exists(file_path):
        print(f"Ошибка: файл {file_path} не найден")
        exit(1)
    
    print(f"Модификация файла {file_path}...")
    backup_path = backup_original_file(file_path)
    
    success = add_count_logging_code(file_path)
    
    if success:
        print("Модификация успешно выполнена!")
        print("Теперь скрипт будет логировать количество записей в базе данных после загрузки")
    else:
        print("Ошибка при модификации файла")
        print(f"Оригинальный файл сохранен в {backup_path}") 