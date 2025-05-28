import psycopg2
import logging
import os
from datetime import datetime

# Настройка логирования
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = f'{log_dir}/create_api_view_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
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

def create_api_view():
    """Создаёт представление bayut_api_view поверх таблицы bayut_properties"""
    conn = None
    try:
        # Подключаемся к базе данных
        conn = psycopg2.connect(**DB_CONFIG)
        conn.set_client_encoding('UTF8')
        cursor = conn.cursor()
        
        # Проверяем, существует ли уже представление
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.views 
                WHERE table_name = 'bayut_api_view'
            )
        """)
        
        view_exists = cursor.fetchone()[0]
        
        if view_exists:
            logger.info("Представление bayut_api_view уже существует")
            print("Представление bayut_api_view уже существует")
            return True
        
        # Проверяем, существует ли таблица bayut_properties
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'bayut_properties'
            )
        """)
        
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            logger.error("Таблица bayut_properties не существует, невозможно создать представление")
            print("Таблица bayut_properties не существует, невозможно создать представление")
            return False
        
        # Создаем представление bayut_api_view
        logger.info("Создание представления bayut_api_view...")
        print("Создание представления bayut_api_view...")
        
        create_view_sql = """
        CREATE OR REPLACE VIEW bayut_api_view AS
        SELECT 
            id,
            title,
            price,
            rooms,
            baths,
            area,
            rent_frequency,
            location,
            cover_photo_url,
            property_url,
            category,
            property_type,
            created_at,
            updated_at,
            furnishing_status,
            completion_status,
            amenities,
            agency_name,
            contact_info,
            geography,
            agency_logo_url,
            proxy_mobile,
            keywords,
            is_verified,
            purpose,
            floor_number,
            city_level_score,
            score,
            agency_licenses,
            agency_rating
        FROM bayut_properties;
        """
        
        cursor.execute(create_view_sql)
        conn.commit()
        
        logger.info("Представление bayut_api_view успешно создано")
        print("Представление bayut_api_view успешно создано")
        
        # Вывод структуры представления
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'bayut_api_view'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        
        logger.info("Структура представления bayut_api_view:")
        print("\nСтруктура представления bayut_api_view:")
        
        for i, column in enumerate(columns):
            column_name = column[0]
            logger.info(f"{i+1}. {column_name}")
            print(f"{i+1}. {column_name}")
        
        logger.info(f"Всего колонок в представлении: {len(columns)}")
        print(f"\nВсего колонок в представлении: {len(columns)}")
        
        return True
    
    except Exception as e:
        logger.error(f"Ошибка при создании представления: {e}")
        print(f"Ошибка при создании представления: {e}")
        return False
    
    finally:
        if conn:
            conn.close()
            logger.info("Соединение с базой данных закрыто")

if __name__ == "__main__":
    create_api_view() 