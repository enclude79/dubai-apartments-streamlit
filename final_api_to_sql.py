import os
import requests
import psycopg2
import json
import time
import argparse
import logging
import re
from datetime import datetime
from dotenv import load_dotenv

# Настройка логирования
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = f'{log_dir}/final_api_to_sql_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

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

# Проверяем наличие API ключа
RAPIDAPI_KEY = os.getenv('RAPIDAPI_KEY')
if not RAPIDAPI_KEY:
    logger.error("RAPIDAPI_KEY не найден в переменных окружения или .env файле")
    exit(1)

# Параметры API
API_CONFIG = {
    "url": "https://bayut.p.rapidapi.com/properties/list",
    "headers": {
        "X-RapidAPI-Key": RAPIDAPI_KEY,
        "X-RapidAPI-Host": "bayut.p.rapidapi.com"
    },
    "params": {
        "locationExternalIDs": "5002,6020",
        "purpose": "for-sale",
        "hitsPerPage": "25",
        "page": "1",
        "sort": "date-desc",
        "categoryExternalID": "4"
    }
}

# Параметры БД
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASSWORD', 'Enclude79'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432')
}

def clean_text(text):
    """Очищает текст от специальных символов"""
    if text is None:
        return None
    # Используем регулярное выражение для удаления спецсимволов
    return re.sub(r'[^\w\s.,;:!?()-]', '', str(text))

def kill_hanging_queries():
    """Убивает все зависшие запросы в базе данных"""
    logger.info("Поиск и завершение зависших запросов...")
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cur = conn.cursor()
        
        # Находим все запросы INSERT, которые висят более 1 минуты
        cur.execute("""
            SELECT pid, query_start, query 
            FROM pg_stat_activity 
            WHERE state = 'active' 
            AND query_start < NOW() - INTERVAL '1 minute'
            AND query ILIKE '%INSERT INTO bayut_properties%'
            AND pid <> pg_backend_pid()
        """)
        
        hanging_queries = cur.fetchall()
        
        if hanging_queries:
            logger.info(f"Найдено {len(hanging_queries)} зависших запросов")
            
            # Завершаем каждый зависший запрос
            for pid, start_time, query in hanging_queries:
                logger.info(f"Завершение запроса (PID: {pid}, запущен: {start_time})")
                
                # Пытаемся сначала отменить запрос
                cur.execute(f"SELECT pg_cancel_backend({pid})")
                time.sleep(0.5)  # Даем время на отмену
                
                # Проверяем, все еще активен ли запрос
                cur.execute(f"SELECT 1 FROM pg_stat_activity WHERE pid = {pid} AND state = 'active'")
                if cur.fetchone():
                    # Если запрос все еще активен, применяем жесткое завершение
                    logger.info(f"Принудительное завершение запроса (PID: {pid})")
                    cur.execute(f"SELECT pg_terminate_backend({pid})")
        else:
            logger.info("Зависших запросов не обнаружено")
            
        # Также находим и завершаем запросы в состоянии 'idle in transaction'
        cur.execute("""
            SELECT pid, query_start, query 
            FROM pg_stat_activity 
            WHERE state = 'idle in transaction' 
            AND query_start < NOW() - INTERVAL '1 minute'
            AND query ILIKE '%bayut_properties%'
            AND pid <> pg_backend_pid()
        """)
        
        idle_transactions = cur.fetchall()
        
        if idle_transactions:
            logger.info(f"Найдено {len(idle_transactions)} зависших транзакций")
            
            for pid, start_time, query in idle_transactions:
                logger.info(f"Завершение транзакции (PID: {pid}, запущена: {start_time})")
                cur.execute(f"SELECT pg_terminate_backend({pid})")
        else:
            logger.info("Зависших транзакций не обнаружено")
            
    except Exception as e:
        logger.error(f"Ошибка при завершении зависших запросов: {e}")
    finally:
        if 'cur' in locals() and cur:
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()

def get_api_data(limit=None):
    """Загружает данные из API Bayut"""
    logger.info(f"Загрузка данных из API Bayut (лимит: {limit if limit else 'все'})")
    
    properties = []
    unique_ids = set()
    page = 1
    total_pages = 1  # Начальное значение, будет обновлено после первого запроса
    
    while (limit is None or len(properties) < limit) and page <= total_pages:
        try:
            # Обновляем номер страницы в параметрах
            params = API_CONFIG["params"].copy()
            params["page"] = str(page)
            
            logger.info(f"Загрузка страницы {page} из API")
            
            # Выполняем запрос к API
            response = requests.get(
                API_CONFIG["url"],
                headers=API_CONFIG["headers"],
                params=params,
                timeout=30  # Устанавливаем таймаут 30 секунд
            )
            
            # Проверяем успешность запроса
            response.raise_for_status()
            
            # Парсим JSON ответ
            data = response.json()
            
            # Получаем общее количество страниц
            if page == 1:
                count = data.get("count", 0)
                hits_per_page = int(params["hitsPerPage"])
                total_pages = (count + hits_per_page - 1) // hits_per_page
                logger.info(f"Всего доступно {count} объектов, {total_pages} страниц")
            
            # Получаем результаты
            hits = data.get("hits", [])
            
            if not hits:
                logger.warning(f"Страница {page} не содержит данных")
                break
                
            # Обрабатываем каждый объект
            for item in hits:
                property_id = item.get("id")
                
                # Пропускаем дубликаты
                if property_id in unique_ids:
                    continue
                    
                unique_ids.add(property_id)
                
                # Извлекаем необходимые данные
                property_data = {
                    "id": property_id,
                    "title": clean_text(item.get("title")),
                    "price": item.get("price"),
                    "rooms": item.get("rooms"),
                    "baths": item.get("baths"),
                    "area": item.get("area"),
                    "location": clean_text(item.get("location", [{}])[0].get("name")),
                    "property_type": clean_text(item.get("category", [{}])[0].get("name")),
                    "purpose": clean_text(item.get("purpose")),
                    "furnishing_status": clean_text(item.get("furnishingStatus"))
                }
                
                properties.append(property_data)
                
                # Проверяем, достигли ли мы лимита
                if limit and len(properties) >= limit:
                    logger.info(f"Достигнут лимит в {limit} объектов")
                    break
            
            logger.info(f"Загружено {len(properties)} объектов (уникальных)")
            
            # Переходим к следующей странице
            page += 1
            
            # Задержка между запросами для избежания блокировки API
            time.sleep(1)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка при запросе к API: {e}")
            break
        except json.decoder.JSONDecodeError as e:
            logger.error(f"Ошибка при разборе JSON: {e}")
            break
        except Exception as e:
            logger.error(f"Неожиданная ошибка: {e}")
            break
    
    logger.info(f"Всего загружено {len(properties)} уникальных объектов")
    return properties

def insert_properties(properties):
    """Вставляет данные о недвижимости в базу данных"""
    if not properties:
        logger.warning("Нет данных для вставки")
        return 0
        
    logger.info(f"Вставка {len(properties)} объектов в базу данных")
    
    conn = None
    cur = None
    
    try:
        # Завершаем все зависшие запросы перед началом вставки
        kill_hanging_queries()
        
        # Подключаемся к базе данных
        logger.info("Подключение к базе данных")
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True  # Автоматический коммит - важно!
        cur = conn.cursor()
        
        # Получаем текущее количество записей
        cur.execute("SELECT COUNT(*) FROM bayut_properties")
        count_before = cur.fetchone()[0]
        logger.info(f"Текущее количество записей в таблице: {count_before}")
        
        # Вставляем каждый объект в базу данных
        inserted = 0
        skipped = 0
        start_time = time.time()
        
        for i, prop in enumerate(properties, 1):
            try:
                # Формируем SQL запрос
                query = """
                INSERT INTO bayut_properties 
                (id, title, price, rooms, baths, area, location, property_type, purpose, furnishing_status) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
                """
                
                # Подготавливаем параметры
                params = (
                    prop.get("id"),
                    prop.get("title"),
                    prop.get("price"),
                    prop.get("rooms"),
                    prop.get("baths"),
                    prop.get("area"),
                    prop.get("location"),
                    prop.get("property_type"),
                    prop.get("purpose"),
                    prop.get("furnishing_status")
                )
                
                # Выполняем запрос
                logger.debug(f"Вставка объекта {i}/{len(properties)} (ID: {prop.get('id')})")
                cur.execute(query, params)
                
                # Проверяем, была ли вставлена запись
                if cur.rowcount > 0:
                    inserted += 1
                    if inserted % 10 == 0:
                        logger.info(f"Вставлено {inserted} объектов")
                else:
                    skipped += 1
                    
            except Exception as e:
                logger.error(f"Ошибка при вставке объекта {prop.get('id')}: {e}")
                
        # Получаем итоговое количество записей
        cur.execute("SELECT COUNT(*) FROM bayut_properties")
        count_after = cur.fetchone()[0]
        
        # Подсчитываем статистику
        total_time = time.time() - start_time
        
        logger.info(f"Вставка завершена за {total_time:.2f} секунд")
        logger.info(f"Записей до: {count_before}, после: {count_after}")
        logger.info(f"Вставлено новых: {inserted}, пропущено (дубликаты): {skipped}")
        
        if inserted > 0:
            logger.info(f"Средняя скорость вставки: {inserted/total_time:.2f} записей/секунду")
            
        return inserted
        
    except Exception as e:
        logger.error(f"Критическая ошибка при вставке данных: {e}")
        return 0
        
    finally:
        # Закрываем соединение с базой данных
        if cur:
            cur.close()
        if conn:
            conn.close()
        logger.info("Соединение с базой данных закрыто")

def analyze_table():
    """Выполняет ANALYZE для обновления статистики таблицы"""
    logger.info("Обновление статистики таблицы")
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cur = conn.cursor()
        
        start_time = time.time()
        cur.execute("ANALYZE bayut_properties")
        logger.info(f"ANALYZE выполнен за {time.time() - start_time:.2f} секунд")
        
        return True
    except Exception as e:
        logger.error(f"Ошибка при выполнении ANALYZE: {e}")
        return False
    finally:
        if 'cur' in locals() and cur:
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()

def main():
    parser = argparse.ArgumentParser(description="Загрузка данных из API Bayut в PostgreSQL")
    parser.add_argument("--limit", type=int, help="Ограничение количества объектов для загрузки")
    parser.add_argument("--kill-hanging", action="store_true", help="Только завершить зависшие запросы")
    args = parser.parse_args()
    
    logger.info("Запуск скрипта загрузки данных из API Bayut в PostgreSQL")
    
    if args.kill_hanging:
        logger.info("Режим завершения зависших запросов")
        kill_hanging_queries()
        return
    
    # Загружаем данные из API
    start_time = time.time()
    properties = get_api_data(args.limit)
    
    if not properties:
        logger.error("Не удалось получить данные из API")
        return
    
    logger.info(f"Загрузка данных из API заняла {time.time() - start_time:.2f} секунд")
    
    # Вставляем данные в базу
    inserted = insert_properties(properties)
    
    # Обновляем статистику таблицы
    if inserted > 0:
        analyze_table()
    
    total_time = time.time() - start_time
    logger.info(f"Всего загружено и обработано {len(properties)} объектов")
    logger.info(f"Общее время выполнения: {total_time:.2f} секунд")

if __name__ == "__main__":
    main() 