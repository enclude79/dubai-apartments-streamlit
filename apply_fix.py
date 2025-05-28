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

def find_function_in_file(file_path, function_name):
    """Находит функцию в файле и возвращает ее начало и конец"""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Ищем определение функции
    pattern = re.compile(r'def\s+' + function_name + r'\s*\([^)]*\):.*?(?=\n\S|$)', re.DOTALL)
    match = pattern.search(content)
    
    if not match:
        return None, None, None
    
    start_pos = match.start()
    end_pos = match.end()
    
    # Получаем текст до и после функции
    text_before = content[:start_pos]
    text_after = content[end_pos:]
    
    return text_before, match.group(0), text_after

def read_fixed_function(file_path):
    """Читает исправленную функцию из файла"""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    return content.strip()

def apply_fix(original_file, fixed_function_file):
    """Применяет исправление к оригинальному файлу"""
    # Создаем резервную копию
    backup_path = backup_original_file(original_file)
    
    # Находим функцию load_to_sql в оригинальном файле
    text_before, old_function, text_after = find_function_in_file(original_file, "load_to_sql")
    
    if not old_function:
        print("Ошибка: функция load_to_sql не найдена в файле api_to_sql.py")
        return False
    
    # Читаем исправленную функцию
    fixed_function = read_fixed_function(fixed_function_file)
    
    if not fixed_function:
        print("Ошибка: не удалось прочитать исправленную функцию из файла fixed_load_to_sql.py")
        return False
    
    # Создаем новое содержимое файла
    new_content = text_before + fixed_function + text_after
    
    # Записываем новое содержимое в файл
    try:
        with open(original_file, 'w', encoding='utf-8') as f:
            f.write(new_content)
        
        print(f"Исправление успешно применено к файлу {original_file}")
        print(f"Функция load_to_sql заменена на исправленную версию")
        return True
    except Exception as e:
        print(f"Ошибка при записи в файл: {e}")
        return False

if __name__ == "__main__":
    original_file = "api_to_sql.py"
    fixed_function_file = "fixed_load_to_sql.py"
    
    if not os.path.exists(original_file):
        print(f"Ошибка: файл {original_file} не найден")
        exit(1)
    
    if not os.path.exists(fixed_function_file):
        print(f"Ошибка: файл {fixed_function_file} не найден")
        exit(1)
    
    print(f"Применение исправления к файлу {original_file}...")
    success = apply_fix(original_file, fixed_function_file)
    
    if success:
        print("Исправление успешно применено!")
        print("Теперь вы можете запустить api_to_sql.py для проверки")
    else:
        print("Ошибка при применении исправления") 