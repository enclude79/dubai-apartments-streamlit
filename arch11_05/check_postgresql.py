import psycopg2
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Параметры подключения к базе данных
DB_PARAMS = {
    'dbname': 'postgres',
    'user': 'Admin',
    'password': 'Enclude79',
    'host': 'localhost',
    'port': '5432',
    'client_encoding': 'utf8'
}

def check_postgresql_connection():
    """Проверка подключения к PostgreSQL"""
    try:
        print("Попытка подключения к PostgreSQL...")
        conn = psycopg2.connect(**DB_PARAMS)
        
        print("Успешное подключение к PostgreSQL!")
        
        # Получение информации о сервере
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print(f"Версия PostgreSQL: {version[0]}")
        
        # Проверка настроек кодировки
        cursor.execute("SHOW client_encoding;")
        client_encoding = cursor.fetchone()[0]
        cursor.execute("SHOW server_encoding;")
        server_encoding = cursor.fetchone()[0]
        print(f"\nНастройки кодировки:")
        print(f"Клиентская кодировка: {client_encoding}")
        print(f"Серверная кодировка: {server_encoding}")
        
        # Проверка корректности обработки кириллицы
        try:
            test_text = "Тестовый текст с кириллицей: щёäüß"
            cursor.execute("SELECT %s::text;", (test_text,))
            result = cursor.fetchone()[0]
            print(f"\nТест кириллицы:")
            print(f"Отправлено: {test_text}")
            print(f"Получено:   {result}")
            print(f"Тест пройден успешно: {test_text == result}")
        except Exception as e:
            print(f"Ошибка при тестировании кириллицы: {e}")
        
        # Получение списка таблиц
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        
        tables = cursor.fetchall()
        print("\nДоступные таблицы:")
        if tables:
            for i, table in enumerate(tables, 1):
                print(f"{i}. {table[0]}")
                
                # Получаем информацию о кодировке для таблицы
                try:
                    cursor.execute(f"""
                        SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod)
                        FROM pg_catalog.pg_attribute a
                        JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
                        WHERE c.relname = '{table[0]}'
                        AND a.attnum > 0 AND NOT a.attisdropped
                        AND pg_catalog.format_type(a.atttypid, a.atttypmod) LIKE '%char%';
                    """)
                    columns = cursor.fetchall()
                    if columns:
                        print("   Текстовые колонки:")
                        for col in columns:
                            print(f"   - {col[0]}: {col[1]}")
                except Exception as e:
                    print(f"   Ошибка при получении информации о колонках: {e}")
        else:
            print("Таблицы не найдены")
        
        # Закрытие соединения
        cursor.close()
        conn.close()
        
        return True
    
    except Exception as e:
        logger.error(f"Ошибка при подключении к PostgreSQL: {e}")
        print(f"Ошибка при подключении к PostgreSQL: {e}")
        return False

if __name__ == "__main__":
    check_postgresql_connection() 