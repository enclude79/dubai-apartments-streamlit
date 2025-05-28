import psycopg2
import logging
import os

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Параметры базы данных
DB_CONFIG = {
    'user': 'admin',
    'password': 'Enclude79',
    'host': 'localhost',
    'port': '5432',
    'database': 'postgres'
}

def execute_sql_script(script_path):
    """Выполняет SQL-скрипт из файла"""
    try:
        # Проверяем существование файла
        if not os.path.exists(script_path):
            logger.error(f"Файл {script_path} не существует")
            return False
        
        # Читаем содержимое файла
        with open(script_path, 'r') as f:
            script_content = f.read()
        
        # Разделяем скрипт на отдельные команды
        commands = []
        current_command = []
        for line in script_content.split('\n'):
            # Игнорируем комментарии и пустые строки
            if line.strip().startswith('--') or not line.strip():
                continue
            
            current_command.append(line)
            
            # Если строка заканчивается на точку с запятой, считаем команду завершенной
            if line.strip().endswith(';'):
                commands.append('\n'.join(current_command))
                current_command = []
        
        # Если остались незавершенные команды, добавляем их
        if current_command:
            commands.append('\n'.join(current_command))
        
        # Подключаемся к базе данных
        conn = psycopg2.connect(
            dbname=DB_CONFIG['database'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port']
        )
        conn.autocommit = False  # Отключаем автокоммит для транзакции
        
        with conn.cursor() as cursor:
            # Выполняем команды по очереди
            for i, command in enumerate(commands, 1):
                if not command.strip():
                    continue
                
                logger.info(f"Выполнение команды {i}/{len(commands)}: {command[:50]}...")
                try:
                    cursor.execute(command)
                    logger.info(f"Команда {i} успешно выполнена")
                except Exception as e:
                    logger.error(f"Ошибка при выполнении команды {i}: {e}")
                    conn.rollback()
                    return False
            
            # Фиксируем изменения, если все команды выполнены успешно
            conn.commit()
            logger.info("Все команды успешно выполнены и зафиксированы")
            return True
            
    except Exception as e:
        logger.error(f"Ошибка при выполнении скрипта: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

def main():
    """Основная функция скрипта"""
    script_path = 'alter_table_script.sql'
    
    logger.info(f"Начало выполнения скрипта {script_path}")
    success = execute_sql_script(script_path)
    
    if success:
        logger.info(f"Скрипт {script_path} успешно выполнен")
    else:
        logger.error(f"Ошибка при выполнении скрипта {script_path}")

if __name__ == "__main__":
    main() 