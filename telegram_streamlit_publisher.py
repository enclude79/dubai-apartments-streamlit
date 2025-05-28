import os
import asyncio
import telegram
import subprocess
import time
import logging
from dotenv import load_dotenv
from datetime import datetime

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("streamlit_publisher.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Параметры Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHANNEL_ID")

# Параметры Streamlit
STREAMLIT_APP_FILE = "streamlit_apartments_app.py"
STREAMLIT_PORT = "8501"
STREAMLIT_PUBLIC_URL = os.getenv("STREAMLIT_PUBLIC_URL", "http://localhost:8501")  # URL вашего Streamlit приложения, когда оно опубликовано

async def send_telegram_message(bot_token, chat_id, message, disable_web_page_preview=False):
    """Отправляет сообщение в Telegram."""
    try:
        bot = telegram.Bot(token=bot_token)
        await bot.send_message(
            chat_id=chat_id,
            text=message,
            parse_mode='HTML',
            disable_web_page_preview=disable_web_page_preview
        )
        logger.info(f"Сообщение успешно отправлено в Telegram чат {chat_id}")
        return True
    except Exception as e:
        logger.error(f"Ошибка при отправке сообщения в Telegram: {e}")
        return False

def start_streamlit_app():
    """Запускает Streamlit приложение в отдельном процессе."""
    try:
        # Команда для запуска Streamlit
        command = f"streamlit run {STREAMLIT_APP_FILE} --server.port={STREAMLIT_PORT}"
        
        # Запускаем процесс
        process = subprocess.Popen(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        logger.info(f"Запущен Streamlit с командой: {command}")
        
        # Ждем некоторое время, чтобы убедиться, что сервер запустился
        time.sleep(10)
        
        # Проверяем, что процесс все еще работает
        if process.poll() is None:
            logger.info("Streamlit приложение успешно запущено")
            return process
        else:
            stdout, stderr = process.communicate()
            logger.error(f"Не удалось запустить Streamlit: {stderr.decode()}")
            return None
    except Exception as e:
        logger.error(f"Ошибка при запуске Streamlit: {e}")
        return None

async def main():
    """Основная функция - запускает Streamlit и отправляет ссылку в Telegram."""
    logger.info("Запуск процесса публикации Streamlit приложения")
    
    # Запускаем Streamlit приложение
    streamlit_process = start_streamlit_app()
    
    if streamlit_process:
        try:
            # Формируем сообщение для отправки в Telegram
            current_date = datetime.now().strftime("%d.%m.%Y")
            message = f"""
<b>🏢 Анализ рынка недвижимости в Дубае: самые дешевые квартиры</b>

Обновлен интерактивный отчет со списком самых дешевых квартир по всем регионам Дубая (площадь до 40 кв.м).

<b>Что вы найдете в отчете:</b>
• Интерактивная карта с маркерами квартир
• Топ-10 регионов с самыми низкими ценами
• Полная таблица всех квартир с возможностью сортировки

<b>Дата обновления:</b> {current_date}

<b>Ссылка на отчет:</b> 
{STREAMLIT_PUBLIC_URL}

<i>Отчет автоматически обновляется каждый день.</i>
"""
            
            # Отправляем сообщение в Telegram
            await send_telegram_message(
                TELEGRAM_BOT_TOKEN,
                TELEGRAM_CHAT_ID,
                message
            )
            
            # В реальном приложении, вы можете держать процесс запущенным
            # Для простоты примера, мы завершаем его через минуту
            logger.info("Ожидание 60 секунд перед завершением процесса...")
            await asyncio.sleep(60)
            
        except Exception as e:
            logger.error(f"Ошибка в основном процессе: {e}")
        finally:
            # Завершаем процесс Streamlit
            if streamlit_process and streamlit_process.poll() is None:
                streamlit_process.terminate()
                logger.info("Streamlit процесс завершен")
    
    logger.info("Процесс публикации Streamlit приложения завершен")

if __name__ == "__main__":
    asyncio.run(main()) 