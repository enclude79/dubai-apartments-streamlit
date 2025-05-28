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

def add_re_import(file_path):
    """Добавляет импорт модуля re в начало файла, если его нет"""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Проверяем, есть ли уже импорт re
    if 'import re' not in content:
        # Добавляем импорт re после других импортов
        import_pattern = re.compile(r'(import[^\n]+\n)', re.MULTILINE)
        matches = list(import_pattern.finditer(content))
        
        if matches:
            # Находим последний импорт и добавляем после него
            last_import = matches[-1]
            insert_position = last_import.end()
            new_content = content[:insert_position] + 'import re\n' + content[insert_position:]
            
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(new_content)
            
            print("Добавлен импорт модуля re в начало файла")
            return True
        else:
            print("Не найдены импорты в файле")
            return False
    else:
        print("Импорт модуля re уже присутствует в файле")
        return True

def fix_clean_text_function(file_path):
    """Исправляет функцию clean_text для правильной работы с модулем re"""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Находим функцию clean_text
    clean_text_pattern = re.compile(r'def clean_text\(text\):(.*?)(?=def\s|$)', re.DOTALL)
    clean_text_match = clean_text_pattern.search(content)
    
    if not clean_text_match:
        print("Не удалось найти функцию clean_text")
        return False
    
    # Новая улучшенная функция clean_text с защитой от ошибок
    new_clean_text = """def clean_text(text):
    \"\"\"Очищает текстовые данные от проблемных символов\"\"\"
    import re  # Добавляем импорт re прямо в функцию для надежности
    
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
    new_content = content.replace(clean_text_match.group(0), new_clean_text)
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(new_content)
    
    print("Функция clean_text исправлена и теперь включает импорт re")
    return True

def main():
    file_path = "api_to_sql.py"
    
    # Создаем резервную копию
    backup_path = backup_original_file(file_path)
    print(f"Создана резервная копия: {backup_path}")
    
    # Добавляем импорт re в начало файла
    add_re_import(file_path)
    
    # Исправляем функцию clean_text
    fix_clean_text_function(file_path)
    
    print("Исправления успешно применены к файлу api_to_sql.py")
    print("Теперь скрипт должен корректно выполнять очистку текстовых данных с использованием модуля re.")

if __name__ == "__main__":
    main() 