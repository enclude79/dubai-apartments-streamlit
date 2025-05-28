import os
import psycopg2
import logging
from datetime import datetime
from dotenv import load_dotenv

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Загружаем переменные окружения
load_dotenv()

# Параметры базы данных
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASSWORD', 'Enclude79'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432')
}

def test_direct_insert():
    """Тестирует прямую вставку данных в базу данных"""
    # Формируем тестовую запись
    test_record = {
        'id': 999999999,  # Используем большой ID, чтобы не конфликтовать с существующими
        'title': 'TEST PROPERTY',
        'price': 1000000,
        'rooms': 3,
        'baths': 2,
        'area': 100,
        'location': 'TEST LOCATION',
        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    try:
        # Подключаемся к базе данных
        logger.info("Подключение к базе данных...")
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False  # Отключаем автоматический коммит для управления транзакцией
        
        with conn.cursor() as cursor:
            # Получаем текущее количество записей
            cursor.execute("SELECT COUNT(*) FROM bayut_properties")
            initial_count = cursor.fetchone()[0]
            logger.info(f"Начальное количество записей: {initial_count}")
            
            # Формируем SQL запрос
            columns = ', '.join(test_record.keys())
            placeholders = ', '.join(['%s'] * len(test_record))
            values = tuple(test_record.values())
            
            sql = f"""
                INSERT INTO bayut_properties ({columns})
                VALUES ({placeholders})
                ON CONFLICT (id) DO UPDATE SET
                title = EXCLUDED.title,
                price = EXCLUDED.price,
                updated_at = EXCLUDED.updated_at
            """
            
            # Выполняем запрос
            logger.info(f"Выполнение SQL-запроса: {sql}")
            cursor.execute(sql, values)
            
            # Проверяем, сколько строк было затронуто
            logger.info("Запрос выполнен, проверяем результаты...")
            
            # Получаем новое количество записей перед коммитом
            cursor.execute("SELECT COUNT(*) FROM bayut_properties WHERE id = %s", (test_record['id'],))
            before_commit_count = cursor.fetchone()[0]
            logger.info(f"Количество тестовых записей перед коммитом: {before_commit_count}")
            
            # Фиксируем изменения
            logger.info("Фиксируем изменения...")
            conn.commit()
            logger.info("Изменения зафиксированы!")
            
            # Получаем новое количество записей после коммита
            cursor.execute("SELECT COUNT(*) FROM bayut_properties")
            final_count = cursor.fetchone()[0]
            logger.info(f"Конечное количество записей: {final_count}")
            
            # Проверяем, была ли вставлена запись
            if final_count > initial_count:
                logger.info(f"УСПЕХ! Добавлена {final_count - initial_count} запись")
            else:
                logger.info("ВНИМАНИЕ! Количество записей не изменилось")
                
            # Проверяем наличие тестовой записи
            cursor.execute("SELECT * FROM bayut_properties WHERE id = %s", (test_record['id'],))
            result = cursor.fetchone()
            if result:
                logger.info(f"Тестовая запись найдена: {result}")
            else:
                logger.info("Тестовая запись НЕ найдена!")
        
        logger.info("Тест завершен")
        
    except Exception as e:
        logger.error(f"Ошибка при выполнении теста: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    test_direct_insert() 