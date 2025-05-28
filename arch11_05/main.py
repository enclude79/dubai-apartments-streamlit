import os
import logging
import asyncio
from datetime import datetime
from dotenv import load_dotenv
from telegram import Bot, Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from langchain_sql_integration import PropertyDatabaseAnalyzer
from news_collector import NewsCollector
from find_cheapest_apartments_with_urls import find_cheapest_apartments

# Настройка логирования
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = f'{log_dir}/telegram_bot_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv()

# Получение токена бота из переменных окружения
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
if not TELEGRAM_BOT_TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN не найден в переменных окружения")
    exit(1)

# Инициализация анализатора базы данных
property_analyzer = PropertyDatabaseAnalyzer()

# Инициализация сборщика новостей
news_collector = NewsCollector()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    user = update.effective_user
    logger.info(f"Пользователь {user.id} ({user.username}) запустил бота")
    
    welcome_message = (
        f"👋 Здравствуйте, {user.first_name}!\n\n"
        "Я бот WealthCompas для анализа рынка недвижимости в Дубае.\n\n"
        "🏢 Вы можете задать мне вопросы о рынке недвижимости, например:\n"
        "- Какова средняя цена квартир с 2 спальнями в Downtown Dubai?\n"
        "- Покажи самые дешевые квартиры до 40 кв.м. в Dubai Marina\n"
        "- В каком районе Дубая больше всего предложений?\n\n"
        "📊 Я использую актуальные данные из базы недвижимости и технологии искусственного интеллекта для анализа.\n\n"
        "Чтобы получить список всех команд, отправьте /help"
    )
    
    await update.message.reply_text(welcome_message)

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /help"""
    help_text = (
        "🤖 *Команды бота WealthCompas*\n\n"
        "/start - Начать работу с ботом\n"
        "/help - Показать список команд\n"
        "/cheapest - Показать самые дешевые квартиры до 40 кв.м. по районам\n"
        "/news - Последние новости о рынке недвижимости\n\n"
        "Вы также можете задать мне вопрос о рынке недвижимости в свободной форме, например:\n"
        "_Какова средняя цена квартир с 2 спальнями в Downtown Dubai?_"
    )
    
    await update.message.reply_text(help_text, parse_mode='Markdown')

async def cheapest_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /cheapest - показывает самые дешевые квартиры"""
    await update.message.reply_text("🔍 Ищу самые дешевые квартиры до 40 кв.м. по районам Дубая...")
    
    try:
        # Получаем данные о дешевых квартирах
        apartments_data = find_cheapest_apartments()
        
        # Отправляем результат пользователю (разбиваем на части, если нужно)
        max_length = 4000  # Максимальная длина сообщения в Telegram
        parts = [apartments_data[i:i+max_length] for i in range(0, len(apartments_data), max_length)]
        
        for i, part in enumerate(parts):
            if i > 0:
                part = f"... продолжение {i+1}/{len(parts)} ...\n\n{part}"
                
            await update.message.reply_text(part)
            
    except Exception as e:
        logger.error(f"Ошибка при получении данных о дешевых квартирах: {e}")
        await update.message.reply_text(f"😔 Произошла ошибка при поиске квартир: {str(e)}")

async def news_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /news - показывает последние новости о рынке недвижимости"""
    await update.message.reply_text("📰 Собираю последние новости о рынке недвижимости...")
    
    try:
        # Получаем новости
        news = await news_collector.collect_news("Dubai")
        
        if not news:
            await update.message.reply_text("К сожалению, не удалось найти актуальные новости.")
            return
            
        # Форматируем новости для отображения
        news_text = "📰 *Последние новости о рынке недвижимости*\n\n"
        
        for item in news[:5]:  # Показываем только 5 последних новостей
            news_text += f"📌 *{item.get('title', '')}*\n"
            news_text += f"📅 {item.get('date', '')}\n"
            news_text += f"🔗 [Подробнее]({item.get('url', '')})\n\n"
        
        await update.message.reply_text(news_text, parse_mode='Markdown', disable_web_page_preview=True)
        
    except Exception as e:
        logger.error(f"Ошибка при получении новостей: {e}")
        await update.message.reply_text(f"😔 Произошла ошибка при получении новостей: {str(e)}")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик текстовых сообщений от пользователя"""
    user_message = update.message.text
    user = update.effective_user
    
    logger.info(f"Получено сообщение от {user.id} ({user.username}): {user_message}")
    
    # Отправляем сообщение о том, что запрос обрабатывается
    processing_message = await update.message.reply_text(
        "🔍 Анализирую ваш запрос... Это может занять некоторое время."
    )
    
    try:
        # Генерируем ответ с использованием LangChain и SQL
        answer = await property_analyzer.generate_property_analysis(user_message)
        
        # Отправляем ответ пользователю
        await update.message.reply_text(answer)
        
    except Exception as e:
        logger.error(f"Ошибка при обработке запроса: {e}")
        await update.message.reply_text(
            "😔 Извините, произошла ошибка при обработке вашего запроса. "
            "Пожалуйста, попробуйте переформулировать вопрос или обратитесь позже."
        )
    finally:
        # Удаляем сообщение о обработке
        await processing_message.delete()

async def main():
    """Основная функция запуска бота"""
    logger.info("Запуск бота WealthCompas...")
    
    # Создаем экземпляр приложения
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    
    # Добавляем обработчики команд
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("cheapest", cheapest_command))
    application.add_handler(CommandHandler("news", news_command))
    
    # Добавляем обработчик текстовых сообщений
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    # Запускаем бота
    logger.info("Бот запущен и готов к работе!")
    await application.run_polling()

if __name__ == "__main__":
    asyncio.run(main())