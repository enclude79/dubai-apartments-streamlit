import os
import subprocess
import time
import requests
import asyncio
import telegram
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Параметры Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID")

# Параметры Streamlit
STREAMLIT_APP_FILE = "streamlit_app.py"
STREAMLIT_PORT = 8501

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
        print(f"Сообщение успешно отправлено в Telegram чат {chat_id}")
        return True
    except Exception as e:
        print(f"Ошибка при отправке сообщения в Telegram: {e}")
        return False

def get_localtunnel_url(port):
    """Создает туннель с помощью localtunnel и возвращает публичный URL."""
    try:
        # Запускаем localtunnel
        process = subprocess.Popen(
            f"npx localtunnel --port {port}",
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Ждем, пока localtunnel запустится и выдаст URL
        for line in process.stdout:
            if "your url is:" in line.lower():
                url = line.strip().split("your url is: ")[1]
                return url, process
                
        # Если URL не найден, возвращаем ошибку
        process.terminate()
        return None, None
    
    except Exception as e:
        print(f"Ошибка при запуске localtunnel: {e}")
        return None, None

def start_streamlit_with_localtunnel():
    # Запускаем Streamlit в отдельном процессе
    streamlit_process = subprocess.Popen(
        f"streamlit run {STREAMLIT_APP_FILE} --server.port={STREAMLIT_PORT}",
        shell=True
    )
    
    localtunnel_process = None
    
    try:
        # Ждем, чтобы Streamlit успел запуститься
        print("Запуск Streamlit...")
        time.sleep(5)
        
        # Запускаем localtunnel для порта Streamlit
        print("Создание туннеля с localtunnel...")
        public_url, localtunnel_process = get_localtunnel_url(STREAMLIT_PORT)
        
        if not public_url:
            raise Exception("Не удалось получить URL от localtunnel")
            
        print(f"Localtunnel создан: {public_url}")
        
        # Формируем сообщение для отправки в Telegram
        message = f"""
<b>🏢 Анализ рынка недвижимости в Дубае: самые дешевые квартиры</b>

Обновлен интерактивный отчет со списком самых дешевых квартир по всем регионам Дубая (площадь до 40 кв.м).

<b>Что вы найдете в отчете:</b>
• Интерактивная карта с маркерами квартир
• Топ-10 регионов с самыми низкими ценами
• Полная таблица всех квартир с возможностью сортировки

<b>Дата обновления:</b> {time.strftime('%d.%m.%Y')}

<b>Ссылка на отчет:</b> 
{public_url}

<i>Отчет будет доступен, пока запущен сервер.</i>
"""
        
        # Отправляем сообщение в Telegram
        if TELEGRAM_BOT_TOKEN and TELEGRAM_CHANNEL_ID:
            asyncio.run(send_telegram_message(
                TELEGRAM_BOT_TOKEN,
                TELEGRAM_CHANNEL_ID,
                message
            ))
        else:
            print("ВНИМАНИЕ: Не указаны TELEGRAM_BOT_TOKEN или TELEGRAM_CHANNEL_ID")
            print("Создайте файл .env и добавьте туда переменные:")
            print("TELEGRAM_BOT_TOKEN=ваш_токен")
            print("TELEGRAM_CHANNEL_ID=ваш_идентификатор_канала")
        
        print("\nСервер запущен! Для остановки нажмите Ctrl+C")
        
        # Поддерживаем сервер запущенным
        streamlit_process.wait()
        
    except KeyboardInterrupt:
        print("Завершение работы...")
    except Exception as e:
        print(f"Произошла ошибка: {e}")
    finally:
        # Завершаем процессы
        if localtunnel_process and localtunnel_process.poll() is None:
            localtunnel_process.terminate()
            print("Localtunnel процесс завершен")
            
        if streamlit_process and streamlit_process.poll() is None:
            streamlit_process.terminate()
            print("Streamlit процесс завершен")

if __name__ == "__main__":
    # Проверяем, установлен ли Node.js (необходим для npx и localtunnel)
    try:
        subprocess.run(["node", "--version"], check=True, stdout=subprocess.PIPE)
        print("Node.js установлен, запускаем сервер...")
        # Запускаем сервер
        start_streamlit_with_localtunnel()
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("Ошибка: Node.js не установлен!")
        print("Необходимо установить Node.js:")
        print("1. Скачайте Node.js с https://nodejs.org/")
        print("2. Установите Node.js и перезапустите терминал")
        print("3. После установки Node.js запустите этот скрипт снова")
        exit(1) 