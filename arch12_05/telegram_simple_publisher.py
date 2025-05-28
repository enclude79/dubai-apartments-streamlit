import os
import psycopg2
import logging
import asyncio
import aiohttp
from datetime import datetime
import json
import textwrap

# Настройка логирования
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = f'{log_dir}/telegram_publisher_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

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

# Параметры Telegram бота
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '6910359575:AAEZAZIFa2YmYG5_9XZtvimxX0ZSl3uBCqo')
CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '-1002129200860')

# Получение дешевых квартир из базы данных
def get_cheapest_apartments(limit=10, max_price=3000000):
    """Получает список самых дешевых квартир из базы данных"""
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.set_client_encoding('UTF8')
        cursor = conn.cursor()
        
        # Используем API-представление если оно существует, иначе обычную таблицу
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.views 
                WHERE table_name = 'bayut_api_view'
            )
        """)
        
        view_exists = cursor.fetchone()[0]
        table_name = 'bayut_api_view' if view_exists else 'bayut_properties'
        
        # Выполняем запрос для получения дешевых квартир
        query = f"""
            SELECT 
                id, 
                title, 
                price, 
                rooms, 
                baths, 
                area, 
                rent_frequency,
                location, 
                property_url
            FROM {table_name}
            WHERE price > 0 AND price <= %s
            ORDER BY price
            LIMIT %s
        """
        
        cursor.execute(query, (max_price, limit))
        apartments = cursor.fetchall()
        
        # Преобразуем результаты в список словарей
        columns = ['id', 'title', 'price', 'rooms', 'baths', 'area', 
                  'rent_frequency', 'location', 'property_url']
        result = []
        
        for apartment in apartments:
            result.append(dict(zip(columns, apartment)))
        
        cursor.close()
        return result
    
    except Exception as e:
        logger.error(f"Ошибка при получении дешевых квартир: {e}")
        return []
    finally:
        if conn:
            conn.close()

# Отправка сообщения в Telegram
async def send_telegram_message(text, disable_web_page_preview=False):
    """Отправляет сообщение в Telegram"""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        'chat_id': CHAT_ID,
        'text': text,
        'parse_mode': 'HTML',
        'disable_web_page_preview': disable_web_page_preview
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload) as response:
                if response.status == 200:
                    logger.info("Сообщение успешно отправлено в Telegram")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"Ошибка при отправке сообщения в Telegram: {response.status}, {error_text}")
                    return False
    except Exception as e:
        logger.error(f"Ошибка при отправке сообщения в Telegram: {e}")
        return False

# Форматирование сообщения о квартире
def format_apartment_message(apartment):
    """Форматирует сообщение о квартире для Telegram"""
    title = apartment.get('title', 'Нет названия')
    price = apartment.get('price', 0)
    rooms = apartment.get('rooms', 0)
    baths = apartment.get('baths', 0)
    area = apartment.get('area', 0)
    location = apartment.get('location', 'Неизвестное местоположение')
    url = apartment.get('property_url', '#')
    
    # Формируем текст сообщения
    message = f"""
<b>🏢 {title}</b>

💰 <b>Цена:</b> {price:,} AED
🛏️ <b>Комнат:</b> {rooms}
🚿 <b>Ванных:</b> {baths}
📏 <b>Площадь:</b> {area} кв.м.
📍 <b>Расположение:</b> {location}

<a href="{url}">Подробнее о квартире</a>
"""
    return message.strip()

# Отправка нескольких сообщений с задержкой
async def send_apartments_batch(apartments, delay=2):
    """Отправляет информацию о квартирах порциями с задержкой"""
    if not apartments:
        logger.warning("Нет квартир для отправки")
        return False
    
    # Формируем заголовок
    header = f"🏠 <b>Топ-{len(apartments)} самых дешевых квартир в Дубае</b> 🌴\n\n"
    await send_telegram_message(header)
    await asyncio.sleep(1)
    
    # Отправляем информацию о каждой квартире
    for apartment in apartments:
        message = format_apartment_message(apartment)
        success = await send_telegram_message(message)
        
        if not success:
            logger.error(f"Не удалось отправить сообщение о квартире {apartment.get('id')}")
        
        # Добавляем задержку между сообщениями
        await asyncio.sleep(delay)
    
    # Формируем футер
    footer = "\n<i>Данные предоставлены WealthCompass</i>"
    await send_telegram_message(footer)
    
    return True

# Основная функция публикации
async def publish_to_telegram():
    """Публикует информацию о дешевых квартирах в Telegram"""
    logger.info("Запуск процесса публикации дешевых квартир в Telegram")
    print("Запуск процесса публикации дешевых квартир в Telegram")
    
    # Получаем дешевые квартиры из базы данных
    apartments = get_cheapest_apartments(limit=10, max_price=5000000)
    
    if not apartments:
        message = "К сожалению, не удалось найти подходящие квартиры. Пожалуйста, попробуйте позже."
        await send_telegram_message(message)
        logger.warning("Не найдено подходящих квартир для публикации")
        print("Не найдено подходящих квартир для публикации")
        return False
    
    # Публикуем информацию о квартирах в Telegram
    logger.info(f"Найдено {len(apartments)} дешевых квартир для публикации")
    print(f"Найдено {len(apartments)} дешевых квартир для публикации")
    
    success = await send_apartments_batch(apartments)
    
    if success:
        logger.info("Процесс публикации успешно завершен")
        print("Процесс публикации успешно завершен")
        return True
    else:
        logger.error("Процесс публикации завершен с ошибками")
        print("Процесс публикации завершен с ошибками")
        return False

# Основная функция скрипта
async def main():
    """Главная функция скрипта"""
    try:
        await publish_to_telegram()
    except Exception as e:
        logger.error(f"Критическая ошибка при публикации: {e}")
        print(f"Критическая ошибка при публикации: {e}")

if __name__ == "__main__":
    asyncio.run(main()) 