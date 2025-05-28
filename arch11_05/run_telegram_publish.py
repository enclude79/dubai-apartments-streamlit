import asyncio
import os
import logging
from datetime import datetime
from load_env import load_environment_variables, set_telegram_env_vars
from publish_to_telegram import main as publish_main

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def run_telegram_publish():
    """Запускает публикацию в Telegram с указанием параметров"""
    # Загружаем переменные окружения из файла, если он существует
    load_environment_variables()
    
    # Проверяем, установлены ли необходимые переменные окружения
    if not os.environ.get('TELEGRAM_BOT_TOKEN') or not os.environ.get('TELEGRAM_CHANNEL_ID'):
        # Для демонстрации используем тестовые значения
        # В реальном проекте здесь должны быть настоящие значения
        bot_token = "YOUR_BOT_TOKEN"  # Замените на ваш токен бота
        channel_id = "@YOUR_CHANNEL"  # Замените на ID вашего канала или @название_канала
        
        # Устанавливаем переменные окружения
        set_telegram_env_vars(bot_token, channel_id)
        
        logger.info("Установлены демонстрационные значения для Telegram")
    
    # Запускаем публикацию
    logger.info("Запуск публикации результатов в Telegram...")
    await publish_main()

if __name__ == "__main__":
    # Запускаем публикацию
    asyncio.run(run_telegram_publish()) 