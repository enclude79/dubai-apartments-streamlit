import os
import shutil
import logging
from datetime import datetime
import sys

# Настройка логирования
os.makedirs("logs", exist_ok=True)
log_filename = f'logs/migration_to_new_system_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Пути к файлам старой и новой системы
OLD_FILES = [
    'data_loader.py',
    'data_publisher.py',
    'find_cheapest_apartments_langchain.py',
    'api_to_csv.py',
    'csv_to_sql.py',
    'telegram_simple_publisher.py'
]

NEW_FILES = [
    'data_loader_new.py',
    'data_publisher_new.py',
    'find_cheapest_apartments_langchain_new.py',
    'api_to_csv_new.py',
    'csv_to_sql_new.py',
    'telegram_simple_publisher_new.py'
]

def create_backup():
    """Создает резервную копию старых файлов"""
    backup_dir = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    os.makedirs(backup_dir, exist_ok=True)
    
    for file_path in OLD_FILES:
        if os.path.exists(file_path):
            try:
                shutil.copy2(file_path, os.path.join(backup_dir, file_path))
                logger.info(f"Создана резервная копия файла {file_path}")
            except Exception as e:
                logger.error(f"Ошибка при создании резервной копии файла {file_path}: {e}")
                return None
    
    logger.info(f"Резервная копия создана в каталоге {backup_dir}")
    return backup_dir

def migrate_to_new_system():
    """Миграция на новую систему"""
    # Создаем резервную копию
    backup_dir = create_backup()
    if not backup_dir:
        logger.error("Не удалось создать резервную копию")
        return False
    
    # Выполняем миграцию базы данных
    logger.info("Выполнение миграции базы данных...")
    try:
        # Импортируем модуль динамически
        import migrate_table_structure
        migrate_table_structure.main()
        logger.info("Миграция базы данных выполнена успешно")
    except Exception as e:
        logger.error(f"Ошибка при миграции базы данных: {e}")
        return False
    
    # Переименовываем новые файлы в рабочие
    logger.info("Переименование новых файлов...")
    for old_file, new_file in zip(OLD_FILES, NEW_FILES):
        if os.path.exists(new_file):
            try:
                # Если старый файл существует, удаляем его
                if os.path.exists(old_file):
                    os.remove(old_file)
                    logger.info(f"Удален старый файл {old_file}")
                
                # Переименовываем новый файл
                os.rename(new_file, old_file)
                logger.info(f"Файл {new_file} переименован в {old_file}")
            except Exception as e:
                logger.error(f"Ошибка при переименовании файла {new_file}: {e}")
                return False
    
    logger.info("Миграция на новую систему завершена успешно")
    return True

def main():
    """Основная функция скрипта"""
    logger.info("Запуск миграции на новую систему...")
    print("Запуск миграции на новую систему...")
    
    if migrate_to_new_system():
        logger.info("Миграция на новую систему выполнена успешно")
        print("Миграция на новую систему выполнена успешно")
        return 0
    else:
        logger.error("Произошла ошибка при миграции на новую систему")
        print("Произошла ошибка при миграции на новую систему")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 