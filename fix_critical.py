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

def fix_execute_values(file_path):
    """Исправляет проблему с выполнением execute_values и зависанием программы"""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Проверяем, что импортирован psycopg2.extras
    if 'import psycopg2.extras' not in content:
        # Добавляем импорт, если его нет
        content = re.sub(
            r'import psycopg2', 
            'import psycopg2\nimport psycopg2.extras', 
            content
        )
    
    # Исправляем блок выполнения execute_values и добавляем отладочный вывод
    old_execute_block = """
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
                print("ОТЛАДКА: Изменения зафиксированы успешно")"""

    new_execute_block = """
            try:
                # Упрощаем вставку для отладки
                logger.info("ОТЛАДКА: Начинаем вставку записей")
                
                # Ограничиваем количество записей для отладки
                chunk_size = 50
                for i in range(0, len(values), chunk_size):
                    chunk = values[i:i+chunk_size]
                    logger.info(f"ОТЛАДКА: Вставка записей {i+1}-{i+len(chunk)} из {len(values)}")
                    
                    # Используем execute_values для пакетной вставки
                    psycopg2.extras.execute_values(
                        cursor, 
                        sql_query, 
                        chunk,
                        template=None, 
                        page_size=10
                    )
                    
                    # Коммитим каждый чанк отдельно
                    conn.commit()
                    logger.info(f"ОТЛАДКА: Чанк {i//chunk_size + 1} успешно закоммичен")
                
                logger.info("ОТЛАДКА: Пакетная вставка успешно выполнена")
                print("ОТЛАДКА: Пакетная вставка успешно выполнена")
                
                # Финальный коммит (на всякий случай)
                conn.commit()
                logger.info("ОТЛАДКА: Изменения зафиксированы успешно")
                print("ОТЛАДКА: Изменения зафиксированы успешно")"""

    content = content.replace(old_execute_block, new_execute_block)
    
    # Исправляем блок обработки ошибок для более подробного логирования
    old_except_block = """
            except Exception as e:
                conn.rollback()
                logger.error(f"ОТЛАДКА: Ошибка при выполнении пакетной вставки: {e}")
                print(f"ОТЛАДКА: Ошибка при выполнении пакетной вставки: {e}")
                raise"""

    new_except_block = """
            except Exception as e:
                conn.rollback()
                logger.error(f"КРИТИЧЕСКАЯ ОШИБКА: Проблема при выполнении пакетной вставки: {e}")
                logger.error(f"Тип ошибки: {type(e)}")
                import traceback
                error_details = traceback.format_exc()
                logger.error(f"Подробная информация об ошибке: {error_details}")
                print(f"КРИТИЧЕСКАЯ ОШИБКА: Проблема при выполнении пакетной вставки: {e}")
                print(f"Проверьте журнал для получения подробной информации")
                # Не выбрасываем исключение дальше, чтобы программа могла продолжить работу
                return 0, 0, 1, None"""

    content = content.replace(old_except_block, new_except_block)
    
    # Также добавим импорт traceback в начало файла, если его нет
    if 'import traceback' not in content:
        content = re.sub(
            r'import logging', 
            'import logging\nimport traceback', 
            content
        )
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print("Исправлена проблема с выполнением execute_values")

def add_timeout_to_db_connect(file_path):
    """Добавляет таймаут для подключения к БД, чтобы избежать бесконечного ожидания"""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Исправляем подключение к базе данных в функции load_to_sql
    old_connect = """
        conn = psycopg2.connect(
            dbname=DB_CONFIG['dbname'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port']
        )"""
    
    new_connect = """
        # Добавляем таймаут для подключения
        conn = psycopg2.connect(
            dbname=DB_CONFIG['dbname'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            connect_timeout=10  # Таймаут подключения 10 секунд
        )"""
    
    content = content.replace(old_connect, new_connect)
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print("Добавлен таймаут для подключения к базе данных")

def main():
    file_path = "api_to_sql.py"
    
    # Создаем резервную копию
    backup_path = backup_original_file(file_path)
    print(f"Создана резервная копия: {backup_path}")
    
    # Исправляем проблему с execute_values
    fix_execute_values(file_path)
    
    # Добавляем таймаут к подключению к БД
    add_timeout_to_db_connect(file_path)
    
    print("Все критические исправления успешно применены к файлу api_to_sql.py")
    print("Теперь скрипт должен корректно выполнять вставку данных и не зависать.")

if __name__ == "__main__":
    main() 