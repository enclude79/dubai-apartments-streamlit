import os
import re
import shutil
from datetime import datetime
import psycopg2
import time
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Параметры подключения
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASSWORD', 'Enclude79'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'options': '-c statement_timeout=10000'  # Таймаут 10 секунд
}

def backup_original_file(file_path):
    """Создает резервную копию оригинального файла"""
    backup_dir = "backups"
    os.makedirs(backup_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = os.path.join(backup_dir, f"api_to_sql_{timestamp}.py.bak")
    
    shutil.copy2(file_path, backup_path)
    print(f"Создана резервная копия: {backup_path}")
    
    return backup_path

def clean_main_function(file_path):
    """Полностью переписывает функцию main с правильным логированием в начале и конце"""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Находим полное определение функции main
    main_pattern = re.compile(r'(def\s+main\s*\([^)]*\):.*?)(?=\ndef\s+create_last_run_info_table|\Z)', re.DOTALL)
    main_match = main_pattern.search(content)
    
    if not main_match:
        print("Ошибка: функция main не найдена")
        return False
    
    # Новое определение функции main с правильным логированием
    new_main_function = """def main():
    parser = argparse.ArgumentParser(description="Загрузка новых данных из API Bayut в SQL")
    parser.add_argument('--limit', type=int, default=1000, 
                      help='Максимальное количество новых записей для загрузки (по умолчанию 1000)')
    parser.add_argument('--no-csv', action='store_true',
                      help='Не сохранять данные в CSV файл (только в SQL)')
    parser.add_argument('--send-email', action='store_true',
                      help='Отправлять email-отчёт после загрузки (по умолчанию отключено)')
    parser.add_argument('--small', action='store_true',
                      help='Загрузить только 10 записей для тестирования')
    args = parser.parse_args()

    # Если указан флаг --small, устанавливаем лимит в 10 записей
    limit = 10 if args.small else args.limit

    logger.info(f"Запуск скрипта api_to_sql.py с параметрами: лимит={limit}, без CSV={args.no_csv}, отправка email={args.send_email}")
    
    # Создаем таблицу last_run_info, если она не существует
    create_last_run_info_table()
    
    # Логируем начальное количество записей в базе данных
    try:
        logger.info("Проверка начального количества записей в базе данных...")
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM bayut_properties")
                count = cursor.fetchone()[0]
                logger.info(f"Начальное количество записей в таблице bayut_properties: {count}")
                print(f"Начальное количество записей в таблице bayut_properties: {count}")
                
                cursor.execute("SELECT updated_at FROM bayut_properties ORDER BY updated_at DESC LIMIT 1")
                last_update = cursor.fetchone()
                if last_update:
                    logger.info(f"Последняя дата обновления в таблице bayut_properties перед загрузкой: {last_update[0]}")
    except Exception as e:
        logger.error(f"Ошибка при проверке начального количества записей: {e}")
    
    # Загружаем данные из API
    result = api_to_sql(max_records=limit, skip_csv=args.no_csv, send_email=args.send_email)
    
    # Логируем количество записей в базе данных после загрузки
    try:
        logger.info("Проверка количества записей в базе данных после загрузки...")
        with psycopg2.connect(**DB_CONFIG) as conn:
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

    return result"""
    
    # Заменяем всю функцию main на новую версию
    modified_content = content.replace(main_match.group(0), new_main_function)
    
    # Записываем изменения в файл
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(modified_content)
    
    print("Функция main успешно переписана с корректным логированием")
    return True

def insert_test_data():
    """Вставляет тестовые данные в базу"""
    start_time = time.time()
    
    # Создаем соединение с автокоммитом и ограниченным временем выполнения запроса
    connection = psycopg2.connect(**DB_CONFIG)
    connection.autocommit = True  # Важно! Включаем автокоммит
    
    cursor = connection.cursor()
    
    # Простой тестовый запрос
    test_id = 123456
    test_query = "INSERT INTO bayut_properties (id, title, price) VALUES (%s, %s, %s) ON CONFLICT (id) DO NOTHING"
    test_data = (test_id, f'Test Property {test_id}', 1000000)
    
    try:
        print(f"Выполняю простую вставку данных для ID {test_id}...")
        cursor.execute(test_query, test_data)
        print(f"Запрос выполнен за {time.time() - start_time:.2f} секунд")
        
        # Проверяем успешность вставки
        cursor.execute("SELECT COUNT(*) FROM bayut_properties")
        count = cursor.fetchone()[0]
        print(f"Всего записей в таблице: {count}")
        
        return True
    except Exception as e:
        print(f"Ошибка: {e}")
        return False
    finally:
        cursor.close()
        connection.close()
        print("Соединение закрыто")

if __name__ == "__main__":
    file_path = "api_to_sql.py"
    
    if not os.path.exists(file_path):
        print(f"Ошибка: файл {file_path} не найден")
        exit(1)
    
    # Создаем резервную копию
    backup_path = backup_original_file(file_path)
    
    # Очищаем функцию main
    success = clean_main_function(file_path)
    
    if success:
        print("Файл успешно обновлен с корректным логированием")
        print(f"Резервная копия сохранена в {backup_path}")
    else:
        print("Произошла ошибка при обновлении файла")
        print(f"Оригинальный файл доступен в {backup_path}")

    print("Запуск теста вставки данных...")
    insert_test_data()
    print("Тест завершен") 