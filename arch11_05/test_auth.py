import requests
import logging
from dotenv import load_dotenv
import os
from openai import OpenAI
import telebot
import json

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_api_connection():
    """Тестирование подключения к API"""
    try:
        load_dotenv()  # Загружаем переменные окружения
        api_key = os.getenv('OPENROUTER_API_KEY')
        if not api_key:
            print("API: ❌ Ошибка: OPENROUTER_API_KEY не найден в переменных окружения")
            return False

        client = OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=api_key
        )

        response = client.chat.completions.create(
            extra_headers={
                "HTTP-Referer": "https://wealthcompas.com",
                "X-Title": "WealthCompas News Bot",
            },
            model="deepseek/deepseek-chat-v3-0324:free",
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": "Say hello!"}
            ]
        )

        print("API: ✅ Подключение успешно")
        print(f"Ответ API: {response.choices[0].message.content}")
        return True
    except Exception as e:
        print(f"API: ❌ Ошибка: {str(e)}")
        return False

def test_telegram_connection():
    """Тестирование подключения к Telegram"""
    try:
        load_dotenv()  # Загружаем переменные окружения
        bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        if not bot_token:
            print("Telegram: ❌ Ошибка: TELEGRAM_BOT_TOKEN не найден в переменных окружения")
            return False
        
        bot = telebot.TeleBot(bot_token)
        bot_info = bot.get_me()
        print(f"Telegram: ✅ Подключение успешно")
        print(f"ID бота: {bot_info.id}")
        print(f"Имя бота: {bot_info.username}")
        print(f"Может присоединяться к группам: {bot_info.can_join_groups}")
        print(f"Может читать все сообщения группы: {bot_info.can_read_all_group_messages}")
        return True
    except Exception as e:
        print(f"Telegram: ❌ Ошибка: {str(e)}")
        return False

def main():
    """Основная функция тестирования"""
    logger.info("🚀 Начало тестирования...")
    
    # Тестирование API
    logger.info("\n📡 Тестирование API...")
    api_success = test_api_connection()
    
    # Тестирование Telegram
    logger.info("\n📱 Тестирование Telegram...")
    telegram_success = test_telegram_connection()
    
    # Вывод общего результата
    logger.info("\n📊 Итоги тестирования:")
    logger.info(f"API: {'✅ Успешно' if api_success else '❌ Ошибка'}")
    logger.info(f"Telegram: {'✅ Успешно' if telegram_success else '❌ Ошибка'}")

if __name__ == "__main__":
    main() 