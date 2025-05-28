import psycopg2
import logging
from dotenv import load_dotenv

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv()

def fix_database_encoding():
    """
    Скрипт для исправления проблем с кодировкой в базе данных.
    Конвертирует текстовые данные из WIN1251 в UTF-8.
    """
    try:
        # Параметры подключения к базе данных
        db_params = {
            'dbname': 'postgres',
            'user': 'Admin',
            'password': 'Enclude79',
            'host': 'localhost',
            'port': '5432',
            'client_encoding': 'utf8'
        }
        
        # Подключение к базе данных
        logger.info("Подключение к базе данных...")
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        
        # Проверяем текущую кодировку базы данных
        cur.execute("SHOW server_encoding;")
        server_encoding = cur.fetchone()[0]
        logger.info(f"Текущая кодировка сервера: {server_encoding}")
        
        cur.execute("SHOW client_encoding;")
        client_encoding = cur.fetchone()[0]
        logger.info(f"Текущая кодировка клиента: {client_encoding}")
        
        # Получаем список текстовых полей из таблицы temp_properties
        text_columns = []
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'temp_properties'
            AND data_type IN ('text', 'character varying')
        """)
        
        for row in cur.fetchall():
            text_columns.append(row[0])
        
        logger.info(f"Найдено {len(text_columns)} текстовых колонок для обработки")
        
        # Для каждой текстовой колонки конвертируем данные
        for column in text_columns:
            logger.info(f"Обработка колонки: {column}")
            
            try:
                # Создаем временную колонку для хранения конвертированных данных
                cur.execute(f"ALTER TABLE temp_properties ADD COLUMN temp_{column} {cur.execute('SELECT data_type FROM information_schema.columns WHERE table_name = %s AND column_name = %s', ('temp_properties', column)).fetchone()[0]};")
                
                # Конвертируем данные с обработкой ошибок
                cur.execute(f"""
                    UPDATE temp_properties 
                    SET temp_{column} = 
                        CASE 
                            WHEN {column} IS NULL THEN NULL
                            ELSE convert_to(convert_from({column}::bytea, 'WIN1251'), 'UTF8')
                        END;
                """)
                
                # Заменяем оригинальную колонку на временную
                cur.execute(f"ALTER TABLE temp_properties DROP COLUMN {column};")
                cur.execute(f"ALTER TABLE temp_properties RENAME COLUMN temp_{column} TO {column};")
                
                logger.info(f"Колонка {column} успешно конвертирована")
                
            except Exception as e:
                logger.error(f"Ошибка при конвертации колонки {column}: {e}")
                conn.rollback()  # Откатываем транзакцию в случае ошибки
        
        # Применяем изменения
        conn.commit()
        logger.info("Все изменения сохранены успешно")
        
        # Закрываем соединение
        cur.close()
        conn.close()
        logger.info("Соединение с базой данных закрыто")
        
        return True
    
    except Exception as e:
        logger.error(f"Ошибка при исправлении кодировки: {e}")
        return False

if __name__ == "__main__":
    logger.info("Запуск скрипта исправления кодировки...")
    success = fix_database_encoding()
    if success:
        logger.info("Скрипт успешно завершен!")
    else:
        logger.error("Скрипт завершился с ошибками.") 