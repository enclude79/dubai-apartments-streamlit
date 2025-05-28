import os
import sys
import logging
import asyncio
from datetime import datetime
from load_env import load_environment_variables

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/publisher_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

async def find_cheap_apartments():
    """Находит самые дешевые квартиры"""
    from find_cheapest_apartments_langchain import main as find_apartments_main
    
    logger.info("Поиск самых дешевых квартир...")
    print("Поиск самых дешевых квартир...")
    try:
        result = find_apartments_main()
        logger.info("Поиск самых дешевых квартир завершен")
        print("Поиск самых дешевых квартир завершен")
        return True
    except Exception as e:
        logger.error(f"Ошибка при поиске самых дешевых квартир: {e}")
        print(f"Ошибка при поиске самых дешевых квартир: {e}")
        return False

async def publish_to_telegram():
    """Публикует данные в Telegram"""
    from telegram_simple_publisher import main as publish_main
    
    logger.info("Публикация данных в Telegram...")
    print("Публикация данных в Telegram...")
    try:
        publish_main()
        logger.info("Данные успешно опубликованы в Telegram")
        print("Данные успешно опубликованы в Telegram")
        return True
    except Exception as e:
        logger.error(f"Ошибка при публикации в Telegram: {e}")
        print(f"Ошибка при публикации в Telegram: {e}")
        return False

async def main():
    """Основная функция программы - автоматический запуск всего процесса публикации"""
    # Загружаем переменные окружения
    load_environment_variables()
    
    # Создаем директорию для логов, если её нет
    os.makedirs("logs", exist_ok=True)
    
    logger.info("Запуск полного процесса публикации данных...")
    print("Запуск полного процесса публикации данных...")
    
    # Выборка данных
    selection_success = await find_cheap_apartments()
    if not selection_success:
        logger.error("Завершение процесса из-за ошибки при выборке данных")
        print("Завершение процесса из-за ошибки при выборке данных")
        return 1
    
    # Публикация в Telegram
    telegram_success = await publish_to_telegram()
    if not telegram_success:
        logger.error("Завершение процесса из-за ошибки при публикации в Telegram")
        print("Завершение процесса из-за ошибки при публикации в Telegram")
        return 1
    
    logger.info("Полный процесс публикации успешно завершен")
    print("Полный процесс публикации успешно завершен")
    return 0

if __name__ == "__main__":
    sys.exit(asyncio.run(main())) 