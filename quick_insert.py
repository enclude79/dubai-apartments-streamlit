import os
import psycopg2
import psycopg2.extras
import pandas as pd
import time
import logging
import argparse
from datetime import datetime
from dotenv import load_dotenv

# Настройка логирования
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = f'{log_dir}/quick_insert_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

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

def fetch_api_records(api_file_path):
    """Получает последние 10 записей из API-файла"""
    if not os.path.exists(api_file_path):
        print(f"Файл не найден: {api_file_path}")
        return []
    
    # Получаем данные из файла
    try:
        df = pd.read_csv(api_file_path)
        logger.info(f"Загружено {len(df)} записей из файла {api_file_path}")
        print(f"Загружено {len(df)} записей из файла {api_file_path}")
        return df.to_dict('records')
    except Exception as e:
        logger.error(f"Ошибка при загрузке файла: {e}")
        return []

def quick_insert_to_db(records, batch_size=1):
    """Быстрая вставка данных в базу данных"""
    if not records:
        logger.warning("Нет данных для вставки")
        return 0
    
    # Подготавливаем подключение
    conn = None
    cursor = None
    inserted_count = 0
    
    try:
        start_time = time.time()
        logger.info(f"Подключение к базе данных...")
        conn = psycopg2.connect(**DB_CONFIG, connect_timeout=10)
        
        # Важно: для транзакций отключаем автокоммит
        conn.autocommit = False
        cursor = conn.cursor()
        
        # Проверка наличия таблицы
        cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'bayut_properties');")
        table_exists = cursor.fetchone()[0]
        logger.info(f"Таблица bayut_properties существует: {table_exists}")
        
        if not table_exists:
            logger.error("Таблица bayut_properties не существует!")
            return 0
        
        # Получаем данные из первой записи для формирования SQL
        first_record = records[0]
        columns = list(first_record.keys())
        
        # Создаем более простой SQL-запрос без RETURNING
        # Используем простую вставку с параметрами
        placeholders = ', '.join(['%s'] * len(columns))
        column_str = ', '.join(columns)
        
        base_query = f"""
            INSERT INTO bayut_properties ({column_str}) 
            VALUES ({placeholders})
            ON CONFLICT (id) DO UPDATE SET
        """
        
        # Добавляем SET часть для UPDATE, исключая id
        update_parts = []
        for col in columns:
            if col != 'id':
                update_parts.append(f"{col} = EXCLUDED.{col}")
        
        sql_query = base_query + ", ".join(update_parts)
        logger.info(f"SQL запрос подготовлен")
        
        # Разбиваем данные на пакеты
        total_records = len(records)
        for i in range(0, total_records, batch_size):
            batch = records[i:i+batch_size]
            logger.info(f"Обработка записей {i+1}-{min(i+batch_size, total_records)} из {total_records}")
            print(f"Обработка записей {i+1}-{min(i+batch_size, total_records)} из {total_records}")
            
            # Выполняем вставку каждой записи отдельно для отладки
            for record in batch:
                try:
                    # Подготавливаем значения в порядке, соответствующем колонкам
                    values = [record.get(col) for col in columns]
                    
                    # Выполняем запрос
                    cursor.execute(sql_query, values)
                    
                    # Коммитим каждую запись отдельно для отладки
                    conn.commit()
                    inserted_count += 1
                    logger.info(f"Запись {inserted_count} успешно вставлена")
                    
                except Exception as e:
                    conn.rollback()
                    logger.error(f"Ошибка при вставке записи: {e}")
                    # Продолжаем со следующей записью
            
        end_time = time.time()
        logger.info(f"Всего вставлено {inserted_count} записей за {end_time - start_time:.2f} секунд")
        print(f"Всего вставлено {inserted_count} записей за {end_time - start_time:.2f} секунд")
        
        return inserted_count
    
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        if conn:
            conn.rollback()
        return 0
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        logger.info("Соединение с базой данных закрыто")

def main():
    parser = argparse.ArgumentParser(description="Быстрая вставка данных из CSV в PostgreSQL")
    parser.add_argument('--file', type=str, help='Путь к CSV файлу с данными API')
    parser.add_argument('--limit', type=int, default=1, help='Количество записей для вставки за один запрос')
    args = parser.parse_args()
    
    # Получаем последний CSV файл, если не указан явно
    api_file = args.file
    if not api_file:
        # Ищем последний CSV в директории Api_Bayat
        csv_dir = "Api_Bayat"
        if os.path.exists(csv_dir):
            csv_files = [f for f in os.listdir(csv_dir) if f.endswith('.csv')]
            if csv_files:
                csv_files.sort(reverse=True)  # Сортируем по имени в обратном порядке
                api_file = os.path.join(csv_dir, csv_files[0])
                logger.info(f"Найден последний CSV файл: {api_file}")
    
    if not api_file or not os.path.exists(api_file):
        logger.error("CSV файл не найден!")
        print("CSV файл не найден!")
        return
    
    # Получаем данные из файла
    records = fetch_api_records(api_file)
    
    if not records:
        logger.error("Не удалось получить данные из файла")
        return
    
    # Вставляем данные в базу
    inserted = quick_insert_to_db(records, args.limit)
    logger.info(f"Итого: успешно вставлено {inserted} записей из {len(records)}")
    print(f"Итого: успешно вставлено {inserted} записей из {len(records)}")

if __name__ == "__main__":
    main() 