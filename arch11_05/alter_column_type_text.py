import psycopg2
import logging

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

def alter_column_type():
    """Изменяет тип колонки Unnamed: 25 с numeric на text"""
    try:
        # Подключаемся к базе данных
        conn = psycopg2.connect(
            dbname=DB_CONFIG['database'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port']
        )
        conn.autocommit = True  # Автоматическая фиксация изменений
        
        with conn.cursor() as cursor:
            # SQL-запрос для изменения типа колонки
            sql = """
            ALTER TABLE bayut_properties 
            ALTER COLUMN "Unnamed: 25" TYPE text USING "Unnamed: 25"::text;
            """
            
            # Выполняем запрос
            cursor.execute(sql)
            
            logger.info("Тип колонки Unnamed: 25 успешно изменен на text")
            
    except Exception as e:
        logger.error(f"Ошибка при изменении типа колонки: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    alter_column_type() 