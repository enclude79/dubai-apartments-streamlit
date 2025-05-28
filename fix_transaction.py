import os
import psycopg2
import psycopg2.extras
import pandas as pd
import numpy as np
import logging
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Настройка логирования
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = f'{log_dir}/fix_transaction_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
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

def clean_text(text):
    """Очищает текст от символов, не поддерживаемых WIN1251 (cp1251)"""
    if not isinstance(text, str):
        return text
    try:
        # Перекодируем в cp1251, заменяя неподдерживаемые символы на '?'
        return text.encode('cp1251', errors='replace').decode('cp1251')
    except Exception:
        return text.encode('ascii', 'ignore').decode('ascii')

def get_table_schema(conn, table_name):
    """Получает схему таблицы"""
    logger.info(f"Получение схемы таблицы {table_name}")
    
    with conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position;
        """)
        
        columns = cursor.fetchall()
        logger.info(f"Получена схема таблицы {table_name}: {columns}")
        return columns

def test_transaction():
    """Тестирует проблему с транзакциями на минимальном примере"""
    logger.info("Тестирование транзакций...")
    
    try:
        # Соединение 1: autocommit=True (для создания тестовой таблицы)
        logger.info("Создание соединения с autocommit=True")
        conn1 = psycopg2.connect(
            dbname=DB_CONFIG['dbname'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port']
        )
        conn1.autocommit = True
        
        with conn1.cursor() as cur:
            # Создаем тестовую таблицу
            logger.info("Создание тестовой таблицы transaction_test")
            cur.execute("""
                DROP TABLE IF EXISTS transaction_test;
                CREATE TABLE transaction_test (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100),
                    value FLOAT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            logger.info("Тестовая таблица создана успешно")
        
        # Соединение 2: autocommit=False (для тестирования транзакций)
        logger.info("Создание соединения с autocommit=False")
        conn2 = psycopg2.connect(
            dbname=DB_CONFIG['dbname'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port']
        )
        conn2.autocommit = False
        
        with conn2.cursor() as cur:
            # Вставляем тестовые данные
            logger.info("Вставка тестовых данных")
            test_data = [
                (1, "Test 1", 10.5),
                (2, "Test 2", 20.3),
                (3, "Test 3", 30.7)
            ]
            
            # Формируем SQL запрос
            sql_query = """
                INSERT INTO transaction_test (id, name, value)
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                value = EXCLUDED.value
            """
            
            # Используем execute_values для пакетной вставки
            try:
                logger.info("Выполнение пакетной вставки")
                psycopg2.extras.execute_values(
                    cur,
                    sql_query,
                    test_data,
                    template=None,
                    page_size=100
                )
                logger.info("Пакетная вставка выполнена успешно")
                
                # Проверяем данные до коммита (должны быть видны только в этом соединении)
                cur.execute("SELECT COUNT(*) FROM transaction_test")
                count_before_commit = cur.fetchone()[0]
                logger.info(f"Количество записей до коммита (в текущем соединении): {count_before_commit}")
                
                # Фиксируем изменения
                logger.info("Фиксация изменений (commit)")
                conn2.commit()
                logger.info("Изменения зафиксированы успешно")
            except Exception as e:
                conn2.rollback()
                logger.error(f"Ошибка при выполнении пакетной вставки: {e}")
                raise
        
        # Проверяем результаты в другом соединении
        with conn1.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM transaction_test")
            count_after_commit = cur.fetchone()[0]
            logger.info(f"Количество записей после коммита (в другом соединении): {count_after_commit}")
            
            if count_after_commit == count_before_commit:
                logger.info("Тест транзакций УСПЕШЕН: данные видны после коммита")
            else:
                logger.error(f"Тест транзакций НЕУДАЧЕН: данные не видны после коммита: {count_after_commit} != {count_before_commit}")
        
        # Закрываем соединения
        conn1.close()
        conn2.close()
        logger.info("Тестирование транзакций завершено")
        
    except Exception as e:
        logger.error(f"Ошибка при тестировании транзакций: {e}")

def test_load_to_sql():
    """Тестирует функцию load_to_sql с явным логированием всех шагов"""
    logger.info("Тестирование функции load_to_sql...")
    
    try:
        # Соединение с базой данных
        conn = psycopg2.connect(
            dbname=DB_CONFIG['dbname'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port']
        )
        conn.autocommit = True
        
        with conn.cursor() as cur:
            # Получаем схему таблицы bayut_properties
            schema = get_table_schema(conn, 'bayut_properties')
            
            # Проверяем существование таблицы
            cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'bayut_properties')")
            table_exists = cur.fetchone()[0]
            logger.info(f"Таблица bayut_properties существует: {table_exists}")
            
            if not table_exists:
                logger.error("Таблица bayut_properties не существует!")
                return
            
            # Проверяем, можем ли мы выполнить простую вставку
            logger.info("Тестирование простой вставки данных...")
            
            # Создаем тестовые данные
            test_data = {
                'id': 999999,  # Уникальный ID для теста
                'title': 'Test Property',
                'price': 1000000.0,
                'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # Создаем новое соединение с autocommit=False специально для транзакции
            conn_transaction = psycopg2.connect(
                dbname=DB_CONFIG['dbname'],
                user=DB_CONFIG['user'],
                password=DB_CONFIG['password'],
                host=DB_CONFIG['host'],
                port=DB_CONFIG['port']
            )
            conn_transaction.autocommit = False
            
            try:
                cursor = conn_transaction.cursor()
                
                # Формируем полный список полей и значений для вставки
                keys = test_data.keys()
                columns = ', '.join(keys)
                placeholders = ', '.join(['%s'] * len(keys))
                
                values = [test_data[key] for key in keys]
                
                # SQL запрос для вставки или обновления данных
                sql_query = f"""
                    INSERT INTO bayut_properties 
                    ({columns})
                    VALUES ({placeholders})
                    ON CONFLICT (id) DO UPDATE SET
                    title = EXCLUDED.title,
                    price = EXCLUDED.price,
                    updated_at = EXCLUDED.updated_at
                """
                
                logger.info(f"SQL запрос: {sql_query}")
                logger.info(f"Значения: {values}")
                
                # Выполняем запрос
                cursor.execute(sql_query, values)
                logger.info("Запрос выполнен успешно")
                
                # Фиксируем изменения
                conn_transaction.commit()
                logger.info("Изменения зафиксированы успешно")
                
                # Проверяем, что данные были вставлены
                with conn.cursor() as check_cur:
                    check_cur.execute(f"SELECT id, title, price FROM bayut_properties WHERE id = {test_data['id']}")
                    result = check_cur.fetchone()
                    
                    if result:
                        logger.info(f"Тестовая запись найдена: {result}")
                    else:
                        logger.error("Тестовая запись НЕ найдена в базе данных!")
                
            except Exception as e:
                conn_transaction.rollback()
                logger.error(f"Ошибка при тестировании вставки: {e}")
            finally:
                cursor.close()
                conn_transaction.close()
                logger.info("Соединение для транзакции закрыто")
        
        conn.close()
        logger.info("Тестирование функции load_to_sql завершено")
        
    except Exception as e:
        logger.error(f"Ошибка при тестировании load_to_sql: {e}")

def fix_api_to_sql():
    """Предлагает исправления для функции load_to_sql в api_to_sql.py"""
    logger.info("Анализ и предложение исправлений для api_to_sql.py...")
    
    # Рекомендации по исправлению
    recommendations = [
        "1. Убедитесь, что SQL запрос для вставки соответствует структуре таблицы bayut_properties",
        "2. Проверьте, что в values передаются все необходимые поля в правильном порядке",
        "3. Добавьте дополнительное логирование до и после conn.commit()",
        "4. Проверьте, что не используется rollback в блоке finally",
        "5. Убедитесь, что данные из API действительно содержат updated_at с более новыми датами"
    ]
    
    for rec in recommendations:
        logger.info(rec)
        print(rec)
    
    # Предлагаемое исправление
    fix_description = """
    Предлагаемое исправление для функции load_to_sql:
    
    1. Изменить SQL запрос, чтобы включать все необходимые поля таблицы bayut_properties
    2. Добавить больше логирования для отслеживания выполнения транзакции
    3. Заменить используемый код пакетной вставки на более надежный с дополнительными проверками
    """
    
    logger.info(fix_description)
    print(fix_description)

if __name__ == "__main__":
    print("Запуск диагностики проблемы с транзакциями")
    logger.info("Запуск диагностики проблемы с транзакциями")
    
    # Запускаем тесты
    test_transaction()
    test_load_to_sql()
    
    # Предлагаем исправления
    fix_api_to_sql()
    
    print("Диагностика завершена. Проверьте лог для получения подробной информации.")
    logger.info("Диагностика завершена.") 