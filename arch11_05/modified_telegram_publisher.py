import os
import asyncio
import logging
from datetime import datetime
from dotenv import load_dotenv
from telegram import Bot, ParseMode
from find_cheapest_apartments_langchain import find_cheapest_apartments

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/telegram_publish_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log", encoding='utf-8'),
        logging.StreamHandler()  # Добавляем вывод в консоль
    ]
)
logger = logging.getLogger(__name__)

class TelegramPublisher:
    """Класс для публикации контента в Telegram"""
    
    def __init__(self, token, channel_id):
        self.token = token
        self.channel_id = channel_id
        self.bot = Bot(token=token)
        
    async def publish(self, content, use_markdown=False):
        """Отправляет контент в Telegram канал"""
        if not content:
            logger.error("Нет контента для публикации")
            return False
            
        try:
            # Разбиваем сообщение на части (максимальный размер сообщения 4000 символов)
            max_length = 3000  # Уменьшаем максимальную длину для безопасности
            parts = [content[i:i+max_length] for i in range(0, len(content), max_length)]
            
            logger.info(f"Сообщение разбито на {len(parts)} частей")
            
            # Отправляем каждую часть
            for i, part in enumerate(parts):
                if i > 0:
                    part = f"... продолжение {i+1}/{len(parts)} ...\n\n{part}"
                    
                logger.info(f"Отправка части {i+1}/{len(parts)}")
                parse_mode = ParseMode.MARKDOWN if use_markdown else None
                
                try:
                    await self.bot.send_message(
                        chat_id=self.channel_id,
                        text=part,
                        parse_mode=parse_mode,
                        disable_web_page_preview=True
                    )
                    logger.info(f"Часть {i+1} успешно отправлена")
                except Exception as e:
                    logger.error(f"Ошибка при отправке части {i+1}: {e}")
                    # Повторная попытка без форматирования
                    try:
                        await self.bot.send_message(
                            chat_id=self.channel_id,
                            text=part,
                            parse_mode=None,
                            disable_web_page_preview=True
                        )
                        logger.info(f"Часть {i+1} успешно отправлена без форматирования")
                    except Exception as e2:
                        logger.error(f"Ошибка при повторной отправке части {i+1}: {e2}")
                        return False
                
                # Пауза между сообщениями для надёжности
                if i < len(parts) - 1:
                    await asyncio.sleep(2)
                    
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при публикации: {e}")
            return False

def prepare_content(content):
    """Подготавливает контент для отправки в Telegram без Markdown-форматирования"""
    # Мы не используем Markdown форматирование, 
    # но нужно убедиться что ссылки правильно отображаются
    
    return content

async def main():
    """Основная функция скрипта"""
    print("Запуск публикации в Telegram...")
    logger.info("Начало процесса публикации")
    
    # Создаем директорию для логов, если её нет
    os.makedirs("logs", exist_ok=True)
    
    # Загружаем данные из env файла
    load_dotenv()
    print("Переменные окружения загружены")
    logger.info("Переменные окружения загружены")
    
    # Получаем данные из переменных окружения
    token = os.getenv('TELEGRAM_BOT_TOKEN')
    channel_id = os.getenv('TELEGRAM_CHANNEL_ID')
    
    if not token or not channel_id:
        error_msg = "Не указаны токен бота или ID канала. Проверьте файл .env"
        logger.error(error_msg)
        print(error_msg)
        return
    
    logger.info("Токен бота и ID канала получены")
    print("Токен бота и ID канала получены")
    
    # Получаем данные о квартирах
    print("Получение данных о дешевых квартирах...")
    logger.info("Получение данных о дешевых квартирах...")
    apartments_data = find_cheapest_apartments()
    
    # Проверяем, получены ли данные
    if not apartments_data or isinstance(apartments_data, str) and apartments_data.startswith("Ошибка"):
        error_msg = f"Не удалось получить данные о квартирах: {apartments_data}"
        logger.error(error_msg)
        print(error_msg)
        return
        
    logger.info(f"Получено данных: {len(apartments_data)} символов")
    print(f"Получено данных: {len(apartments_data)} символов")
    
    # Подготавливаем текст для Telegram
    print("Подготовка данных для Telegram...")
    logger.info("Подготовка данных для Telegram...")
    formatted_content = prepare_content(apartments_data)
    
    # Публикуем в Telegram
    print("Публикация данных в Telegram...")
    logger.info("Публикация данных в Telegram...")
    publisher = TelegramPublisher(token, channel_id)
    success = await publisher.publish(formatted_content, use_markdown=False)
    
    if success:
        success_msg = "Данные успешно опубликованы в Telegram"
        print(success_msg)
        logger.info(success_msg)
    else:
        error_msg = "Ошибка при публикации в Telegram"
        print(error_msg)
        logger.error(error_msg)

if __name__ == "__main__":
    asyncio.run(main()) 