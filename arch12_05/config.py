import os
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

# Telegram настройки
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHANNEL_ID = os.getenv('TELEGRAM_CHANNEL_ID')

# OpenRouter настройки
OPENROUTER_API_KEY = os.getenv('OPENROUTER_API_KEY')

# n8n настройки
N8N_API_KEY = os.getenv('N8N_API_KEY')
N8N_WEBHOOK_URL = os.getenv('N8N_WEBHOOK_URL')

# Время публикации
PUBLISH_TIME = "10:00"

# Промпты для каждого дня недели
PROMPTS = {
    'monday': os.getenv('MONDAY_PROMPT'),
    'tuesday': os.getenv('TUESDAY_PROMPT'),
    'wednesday': os.getenv('WEDNESDAY_PROMPT'),
    'thursday': os.getenv('THURSDAY_PROMPT'),
    'friday': os.getenv('FRIDAY_PROMPT'),
    'saturday': os.getenv('SATURDAY_PROMPT'),
    'sunday': os.getenv('SUNDAY_PROMPT')
} 