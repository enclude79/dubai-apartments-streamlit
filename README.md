# WealthCompas - Анализ недвижимости в Дубае

Приложение для анализа рынка недвижимости в Дубае на основе данных из API Bayut.

## Компоненты проекта

1. **api_to_sql.py** - скрипт для загрузки данных из API Bayut в PostgreSQL
2. **streamlit_postgres_app.py** - Streamlit-приложение для работы с PostgreSQL базой данных (локально)
3. **streamlit_sqlite_app.py** - Streamlit-приложение для работы с SQLite базой данных (для деплоя в Streamlit Cloud)
4. **postgres_to_sqlite.py** - скрипт для экспорта данных из PostgreSQL в SQLite

## Структура данных

Проект использует две базы данных:
- **PostgreSQL** - основная база данных для загрузки и хранения данных (содержит полный набор данных)
- **SQLite** (dubai_properties.db) - экспортированная версия для деплоя в Streamlit Cloud

## Порядок использования

### 1. Загрузка данных из API в PostgreSQL

```bash
python api_to_sql.py
```

Параметры запуска:
- `--limit N` - максимальное количество записей для загрузки (по умолчанию 1000)
- `--no-csv` - не сохранять данные в CSV файл (только в SQL)
- `--send-email` - отправлять email-отчёт после загрузки

### 2. Локальный запуск приложения с PostgreSQL

```bash
streamlit run streamlit_postgres_app.py
```

### 3. Экспорт данных из PostgreSQL в SQLite для деплоя в Streamlit Cloud

```bash
python postgres_to_sqlite.py
```

Этот скрипт создаст файл `dubai_properties.db`, который вы можете загрузить в Git.

### 4. Запуск приложения с SQLite (для деплоя в Streamlit Cloud)

```bash
streamlit run streamlit_sqlite_app.py
```

## Деплой в Streamlit Cloud

1. Загрузите ваш репозиторий, включая файл `dubai_properties.db`, в GitHub
2. Зарегистрируйтесь на [Streamlit Cloud](https://share.streamlit.io/)
3. Создайте новое приложение, подключив ваш репозиторий
4. Укажите путь к файлу `streamlit_sqlite_app.py`
5. Нажмите "Deploy"

## Рабочий процесс для обновления данных

1. Загрузите новые данные в PostgreSQL с помощью `api_to_sql.py`
2. Экспортируйте данные в SQLite с помощью `postgres_to_sqlite.py`
3. Загрузите обновленный файл `dubai_properties.db` в Git репозиторий
4. Streamlit Cloud автоматически обновит ваше приложение

## Настройка окружения

Создайте файл `.env` в корне проекта со следующими параметрами:

```
RAPIDAPI_KEY=ваш_ключ_api
DB_NAME=имя_базы_данных
DB_USER=пользователь
DB_PASSWORD=пароль
DB_HOST=хост
DB_PORT=порт
EMAIL_ADMIN=адрес_для_отчетов (опционально)
EMAIL_ADMIN_PASSWORD=пароль_для_почты (опционально)
```

## Требования

```
requests
pandas
psycopg2
python-dotenv
streamlit
streamlit-folium
folium
plotly
```

Установите зависимости:

```bash
pip install -r requirements.txt
```

## Особенности работы с базами данных

- **PostgreSQL** - предназначена для работы с полным объемом данных, подходит для локальной разработки и анализа
- **SQLite** - оптимизирована для загрузки в Git и деплоя в Streamlit Cloud

## Архитектура решения

Решение состоит из двух основных компонентов:

1. **API-сервер** - предоставляет доступ к данным из локальной PostgreSQL базы данных через REST API
2. **Streamlit-приложение** - клиентское веб-приложение, которое взаимодействует с API и визуализирует данные

Такая архитектура позволяет разместить Streamlit в облаке (Streamlit Community Cloud), в то время как API-сервер работает локально на вашем компьютере и предоставляет данные из вашей локальной базы PostgreSQL.

## Настройка проекта

### 1. Установка зависимостей

```bash
pip install -r requirements.txt
```

### 2. Настройка переменных окружения

Создайте файл `.env` в корне проекта:

```
# Настройки базы данных PostgreSQL
DB_HOST=localhost
DB_PORT=5432
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=your_password

# Настройки Telegram
TELEGRAM_BOT_TOKEN=your_telegram_bot_token
TELEGRAM_CHANNEL_ID=@your_channel_id

# URL API сервера для Streamlit
API_URL=http://localhost:8000
```

### 3. Запуск API-сервера локально

```bash
uvicorn api_server:app --host 0.0.0.0 --port 8000 --reload
```

После запуска, API будет доступен по адресу http://localhost:8000, а документация Swagger - по адресу http://localhost:8000/docs

### 4. Настройка доступа к API из интернета с помощью ngrok

Установите ngrok и запустите:

```bash
ngrok http 8000
```

Вы получите URL вида `https://xxxxx.ngrok.io`, который можно использовать для доступа к вашему API из интернета.

### 5. Запуск Streamlit локально

```bash
streamlit run streamlit_app.py
```

### 6. Развертывание Streamlit в облаке

1. Создайте репозиторий на GitHub и загрузите туда файлы:
   - streamlit_app.py
   - requirements.txt
   - .streamlit/secrets.toml (добавьте в .gitignore)

2. В Streamlit Community Cloud (https://share.streamlit.io/deploy) подключите ваш репозиторий

3. В настройках приложения в Streamlit Cloud обновите секреты:
   ```toml
   [connections.api]
   url = "https://ваш-ngrok-url.ngrok.io"
   ```

## Доступные API эндпоинты

- `GET /api/properties` - получение списка объектов недвижимости с пагинацией
- `GET /api/properties/{id}` - получение детальной информации об объекте по ID
- `GET /api/stats/avg_price_by_area` - средние цены по районам
- `GET /api/stats/count_by_property_type` - количество объектов по типу недвижимости
- `GET /api/map_data` - данные для отображения на карте

## Структура Streamlit-приложения

- **Карта** - интерактивная карта с маркерами объектов недвижимости
- **Статистика** - графики и диаграммы с аналитикой
- **Список объектов** - таблица с данными об объектах недвижимости 