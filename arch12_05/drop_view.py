import psycopg2
import logging
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Параметры базы данных
DB_CONFIG = {
    'dbname': 'postgres',
    'user': 'admin',
    'password': 'Enclude79',
    'host': 'localhost',
    'port': '5432'
}

def drop_view():
    """Удаляет представление bayut_api_view из базы данных"""
    conn = None
    try:
        # Подключаемся к базе данных
        conn = psycopg2.connect(**DB_CONFIG)
        conn.set_client_encoding('UTF8')
        cursor = conn.cursor()
        
        # Удаляем представление
        logger.info("Удаление представления bayut_api_view...")
        cursor.execute("DROP VIEW IF EXISTS bayut_api_view CASCADE")
        conn.commit()
        
        logger.info("Представление успешно удалено")
        print("Представление успешно удалено")
        
        return True
    except Exception as e:
        logger.error(f"Ошибка при удалении представления: {e}")
        print(f"Ошибка при удалении представления: {e}")
        return False
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    drop_view() 