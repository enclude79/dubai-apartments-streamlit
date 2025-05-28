import os
import subprocess
import time
from pyngrok import ngrok, conf
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

def start_streamlit_with_ngrok():
    # Запускаем Streamlit в отдельном процессе
    streamlit_process = subprocess.Popen(
        f"streamlit run {STREAMLIT_APP_FILE} --server.port={STREAMLIT_PORT}",
        shell=True
    )
    
    try:
        # Ждем, чтобы Streamlit успел запуститься
        print("Запуск Streamlit...")
        time.sleep(5)
        
        # Запускаем ngrok туннель к порту Streamlit
        public_url = ngrok.connect(STREAMLIT_PORT).public_url
        print(f"Ngrok туннель создан: {public_url}")
        
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
        # Закрываем ngrok туннель
        ngrok.kill()
        
        # Завершаем процесс Streamlit
        if streamlit_process and streamlit_process.poll() is None:
            streamlit_process.terminate()
            print("Streamlit процесс завершен")

if __name__ == "__main__":
    # Проверяем настройки ngrok
    try:
        # Проверка, настроен ли ngrok
        current_config = conf.get_default()
        print("Ngrok настроен, запускаем сервер...")
        # Запускаем сервер
        start_streamlit_with_ngrok()
    except Exception as e:
        print(f"Ошибка при проверке конфигурации ngrok: {e}")
        print("Необходимо установить ngrok!")
        print("1. Скачайте ngrok с https://ngrok.com/download")
        print("2. Распакуйте и поместите ngrok.exe в PATH или текущую директорию")
        print("3. Зарегистрируйтесь на ngrok.com и получите токен")
        print("4. Выполните команду: ngrok authtoken ВАШ_ТОКЕН")
        exit(1) 