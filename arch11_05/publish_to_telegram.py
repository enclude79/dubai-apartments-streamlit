import os
import asyncio
import logging
from datetime import datetime
from telegram import Bot
from find_cheapest_apartments_with_urls import find_cheapest_apartments

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Параметры Telegram (необходимо настроить в переменных окружения)
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHANNEL_ID = os.getenv('TELEGRAM_CHANNEL_ID')

async def publish_to_telegram(content: str):
    """Публикация контента в Telegram канал"""
    if not TELEGRAM_BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN не найден в переменных окружения")
        return False
        
    if not TELEGRAM_CHANNEL_ID:
        logger.error("TELEGRAM_CHANNEL_ID не найден в переменных окружения")
        return False
        
    try:
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        
        # Разбиваем длинное сообщение на части по 4000 символов
        # (максимальный размер сообщения в Telegram)
        max_length = 4000
        parts = [content[i:i+max_length] for i in range(0, len(content), max_length)]
        
        # Добавляем заголовок с текущей датой для первой части
        current_date = datetime.now().strftime('%d.%m.%Y')
        parts[0] = f"🏠 *Три самых дешевых квартиры в каждом районе (до 40 кв.м.) - {current_date}*\n\n{parts[0]}"
        
        # Отправляем сообщения по частям
        for i, part in enumerate(parts):
            if i > 0:
                part = f"... продолжение списка квартир {i+1}/{len(parts)} ...\n\n{part}"
                
            logger.info(f"Отправка части {i+1}/{len(parts)} в Telegram канал {TELEGRAM_CHANNEL_ID}")
            await bot.send_message(
                chat_id=TELEGRAM_CHANNEL_ID,
                text=part,
                parse_mode='Markdown',
                disable_web_page_preview=True  # Отключаем предпросмотр для ссылок
            )
            
            # Делаем паузу между сообщениями, чтобы избежать ошибок флуда
            if i < len(parts) - 1:
                await asyncio.sleep(2)
                
        logger.info("Содержимое успешно опубликовано в Telegram")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при публикации в Telegram: {e}")
        return False

def format_for_telegram(content: str) -> str:
    """Форматирует содержимое для Telegram с поддержкой Markdown"""
    # Заменяем специальные символы Markdown
    content = content.replace("_", "\\_").replace("*", "\\*").replace("[", "\\[").replace("`", "\\`")
    
    # Заменяем "Район:" на выделенный текст
    content = content.replace("Район:", "*Район:*")
    
    # Заменяем обычные ссылки на Markdown-ссылки
    lines = content.split('\n')
    formatted_lines = []
    
    for line in lines:
        if "Ссылка: http" in line:
            parts = line.split("Ссылка: ")
            url = parts[1].strip()
            formatted_lines.append(f"   [🔗 Просмотреть объявление]({url})")
        else:
            formatted_lines.append(line)
    
    return '\n'.join(formatted_lines)

async def main():
    """Основная функция: найти дешевые квартиры и опубликовать их в Telegram"""
    logger.info("Запуск поиска самых дешевых квартир для публикации в Telegram...")
    
    # Получаем результаты поиска дешевых квартир
    apartments_content = find_cheapest_apartments()
    
    # Форматируем содержимое для Telegram
    telegram_content = format_for_telegram(apartments_content)
    
    # Публикуем в Telegram
    success = await publish_to_telegram(telegram_content)
    
    if success:
        logger.info("Данные о дешевых квартирах успешно опубликованы в Telegram")
    else:
        logger.error("Ошибка при публикации данных о дешевых квартирах в Telegram")

if __name__ == "__main__":
    asyncio.run(main()) 