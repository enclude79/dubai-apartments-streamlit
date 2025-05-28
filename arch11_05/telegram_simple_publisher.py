import os
import logging
import requests
from datetime import datetime
from dotenv import load_dotenv
from find_cheapest_apartments_langchain import find_cheapest_apartments

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("telegram_publish.log"),
        logging.StreamHandler()  # Добавляем вывод в консоль
    ]
)
logger = logging.getLogger(__name__)

class SimplePublisher:
    """Простой класс для отправки сообщений в Telegram через HTTP API"""
    
    def __init__(self, token, chat_id):
        self.token = token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{token}/sendMessage"
    
    def send_message(self, text):
        """Отправляет сообщение в Telegram"""
        if not text:
            logger.error("Нет текста для отправки")
            return False
        
        # Разбиваем на части (максимальный размер сообщения 4096 символов)
        max_length = 3000  # Берем с запасом
        parts = [text[i:i+max_length] for i in range(0, len(text), max_length)]
        
        success = True
        for i, part in enumerate(parts):
            if i > 0:
                part = f"... продолжение {i+1}/{len(parts)} ...\n\n{part}"
                
            logger.info(f"Отправка части {i+1}/{len(parts)}")
            try:
                response = requests.post(
                    self.base_url,
                    data={
                        'chat_id': self.chat_id,
                        'text': part,
                        'disable_web_page_preview': True
                    },
                    timeout=30  # 30 секунд таймаут
                )
                
                if response.status_code != 200:
                    logger.error(f"Ошибка при отправке части {i+1}: {response.text}")
                    success = False
                    
            except Exception as e:
                logger.error(f"Ошибка при отправке части {i+1}: {e}")
                success = False
        
        return success

def main():
    """Основная функция скрипта"""
    print("Запуск публикации в Telegram...")
    logger.info("Начало процесса публикации")
    
    # Загружаем данные из env файла
    load_dotenv()
    print("Переменные окружения загружены")
    
    # Получаем данные из переменных окружения
    token = os.getenv('TELEGRAM_BOT_TOKEN')
    channel_id = os.getenv('TELEGRAM_CHANNEL_ID')
    
    print(f"Token: {'Получен' if token else 'Не найден'}")
    print(f"Channel ID: {'Получен' if channel_id else 'Не найден'}")
    
    if not token or not channel_id:
        logger.error("Не указаны токен бота или ID канала. Проверьте файл .env")
        return
    
    # Получаем данные о квартирах
    print("Получение данных о дешевых квартирах...")
    logger.info("Получение данных о дешевых квартирах...")
    apartments_data = find_cheapest_apartments()
    
    # Проверяем, получены ли данные
    if not apartments_data:
        logger.error("Не удалось получить данные о квартирах")
        return
        
    print(f"Получено данных: {len(apartments_data)} символов")
    
    # Публикуем в Telegram
    print("Публикация данных в Telegram...")
    publisher = SimplePublisher(token, channel_id)
    success = publisher.send_message(apartments_data)
    
    if success:
        print("Данные успешно опубликованы в Telegram")
        logger.info("Данные успешно опубликованы в Telegram")
    else:
        print("Были ошибки при публикации в Telegram")
        logger.error("Были ошибки при публикации в Telegram")

if __name__ == "__main__":
    main() 