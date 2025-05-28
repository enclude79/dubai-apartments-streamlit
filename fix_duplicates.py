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

def remove_duplicate_logging_code(file_path):
    """Удаляет дублирующийся код логирования в функции main"""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Находим функцию main
    main_pattern = re.compile(r'def\s+main\s*\([^)]*\):(.*?)(?=\n\S|\Z)', re.DOTALL)
    main_match = main_pattern.search(content)
    
    if not main_match:
        print("Ошибка: функция main не найдена")
        return False
    
    main_body = main_match.group(1)
    
    # Шаблон для поиска дублирующегося кода логирования
    duplicate_pattern = re.compile(r'(\s+# Логируем количество записей в базе данных после загрузки.*?except Exception as e:.*?logger\.error\(f"Ошибка при проверке количества записей: \{e\}"\))', re.DOTALL)
    
    # Ищем все вхождения кода логирования
    logging_blocks = duplicate_pattern.findall(main_body)
    
    if len(logging_blocks) <= 1:
        print("Дублирующийся код не найден или только одно вхождение")
        return False
    
    print(f"Найдено {len(logging_blocks)} блоков кода логирования")
    
    # Оставляем только первое и последнее вхождение (в начале и в конце функции)
    # Удаляем промежуточные дубликаты
    modified_main = main_body
    
    # Заменяем второй и последующие блоки пустой строкой, кроме последнего
    for i in range(1, len(logging_blocks) - 1):
        modified_main = modified_main.replace(logging_blocks[i], "", 1)
    
    # Заменяем оригинальное тело функции main модифицированным
    modified_content = content.replace(main_body, modified_main)
    
    # Записываем изменения в файл
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(modified_content)
    
    print(f"Удалены {len(logging_blocks) - 2} дублирующихся блока кода логирования")
    return True

if __name__ == "__main__":
    file_path = "api_to_sql.py"
    
    if not os.path.exists(file_path):
        print(f"Ошибка: файл {file_path} не найден")
        exit(1)
    
    # Создаем резервную копию
    backup_path = backup_original_file(file_path)
    
    # Удаляем дублирующийся код
    success = remove_duplicate_logging_code(file_path)
    
    if success:
        print("Файл успешно очищен от дублирующегося кода")
        print(f"Резервная копия сохранена в {backup_path}")
    else:
        print("Изменения не внесены или произошла ошибка")
        print(f"Оригинальный файл доступен в {backup_path}") 