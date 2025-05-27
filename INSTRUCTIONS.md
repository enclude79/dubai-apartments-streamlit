# Инструкция по использованию Dubai Property Analyzer (SQLite-версия)

## Обзор решения

Данное решение позволяет анализировать данные о недвижимости в Дубае с использованием SQLite-базы данных. Основное преимущество этого подхода — возможность размещения приложения в Streamlit Cloud без необходимости настройки внешнего API-сервера и туннелирования через ngrok.

## Компоненты решения

1. **База данных SQLite** (`dubai_properties.db`) — локальный файл базы данных, содержащий информацию о недвижимости
2. **Скрипт экспорта данных** (`postgres_to_sqlite.py`) — переносит данные из PostgreSQL в SQLite
3. **Планировщик обновлений** (`schedule_export.py`) — автоматизирует экспорт данных и их публикацию в GitHub
4. **Streamlit-приложение** (`streamlit_sqlite_app.py`) — визуализирует данные через веб-интерфейс
5. **Генератор тестовых данных** (`create_sample_sqlite_db.py`) — создаёт тестовую базу для отладки

## Как начать работу

### 1. Настройка окружения

```bash
# Клонируйте репозиторий (если вы этого ещё не сделали)
git clone https://github.com/your-username/dubai-property-analyzer.git
cd dubai-property-analyzer

# Установите зависимости
pip install -r requirements-sqlite.txt
```

### 2. Создание базы данных

У вас есть два варианта:

#### Вариант 1: Экспорт данных из существующей PostgreSQL-базы

```bash
# Создайте файл .env с параметрами подключения к PostgreSQL
echo "DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_database
DB_USER=your_username
DB_PASSWORD=your_password" > .env

# Запустите скрипт экспорта
python postgres_to_sqlite.py
```

#### Вариант 2: Создание тестовой базы с примерными данными

```bash
python create_sample_sqlite_db.py
```

### 3. Запуск приложения локально

```bash
streamlit run streamlit_sqlite_app.py
```

После запуска приложение будет доступно по адресу http://localhost:8501

### 4. Публикация в Streamlit Cloud

1. Создайте репозиторий на GitHub и загрузите в него следующие файлы:
   - `streamlit_sqlite_app.py`
   - `dubai_properties.db`
   - `requirements-sqlite.txt` (переименуйте в `requirements.txt`)

2. В Streamlit Community Cloud (https://share.streamlit.io/) подключите ваш репозиторий:
   - Войдите в аккаунт
   - Нажмите "New app"
   - Выберите ваш репозиторий
   - Укажите `streamlit_sqlite_app.py` как основной файл
   - Нажмите "Deploy"

## Настройка автоматического обновления данных

### Для локального компьютера

```bash
# Создайте файл .env с параметрами для GitHub
echo "GIT_USERNAME=your_github_username
GIT_EMAIL=your_email@example.com
GIT_TOKEN=your_github_token" > .env

# Запустите планировщик (обновление каждые 12 часов)
python schedule_export.py

# Или запустите с другим интервалом (например, 24 часа)
python schedule_export.py --interval 24
```

### Для GitHub Actions

Создайте файл `.github/workflows/update-data.yml` в вашем репозитории со следующим содержимым:

```yaml
name: Update SQLite Database

on:
  schedule:
    - cron: '0 */12 * * *'  # Каждые 12 часов
  workflow_dispatch:        # Возможность запуска вручную

jobs:
  update-db:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.9'
          
      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements-sqlite.txt
          
      - name: Run export script
        env:
          DB_HOST: ${{ secrets.DB_HOST }}
          DB_PORT: ${{ secrets.DB_PORT }}
          DB_NAME: ${{ secrets.DB_NAME }}
          DB_USER: ${{ secrets.DB_USER }}
          DB_PASSWORD: ${{ secrets.DB_PASSWORD }}
        run: python postgres_to_sqlite.py
        
      - name: Commit and push changes
        run: |
          git config --global user.name 'GitHub Actions'
          git config --global user.email 'actions@github.com'
          git add dubai_properties.db
          git commit -m "Обновление базы данных $(date +'%Y-%m-%d %H:%M:%S')" || echo "Нет изменений для коммита"
          git push
```

Не забудьте добавить соответствующие секреты в настройках репозитория GitHub.

## Структура базы данных

База данных содержит таблицу `properties` со следующими полями:

| Поле | Тип | Описание |
|------|-----|----------|
| id | INTEGER | Уникальный идентификатор объекта |
| title | TEXT | Название объекта недвижимости |
| description | TEXT | Описание объекта |
| price | REAL | Цена в AED |
| area | TEXT | Район Дубая |
| property_type | TEXT | Тип недвижимости |
| bedrooms | INTEGER | Количество спален |
| bathrooms | INTEGER | Количество ванных комнат |
| size | REAL | Площадь в кв.м. |
| latitude | REAL | Широта |
| longitude | REAL | Долгота |
| status | TEXT | Статус (For Sale, For Rent, Sold) |
| created_at | TEXT | Дата создания записи |

## Устранение неполадок

1. **База данных не найдена**
   - Убедитесь, что файл `dubai_properties.db` находится в той же директории, что и приложение
   - Запустите `create_sample_sqlite_db.py` для создания тестовой базы

2. **Ошибка при экспорте данных из PostgreSQL**
   - Проверьте параметры подключения в файле `.env`
   - Убедитесь, что PostgreSQL сервер запущен и доступен

3. **Ошибка при публикации в GitHub**
   - Проверьте правильность токена доступа GitHub
   - Убедитесь, что вы имеете права на запись в репозиторий

4. **Приложение в Streamlit Cloud не отображает данные**
   - Проверьте, что файл `dubai_properties.db` успешно добавлен в репозиторий
   - Убедитесь, что зависимости указаны в файле `requirements.txt`

## Дополнительная информация

Полная документация доступна в файле `README_SQLITE.md`.

При возникновении вопросов или проблем, создайте issue в репозитории или свяжитесь с разработчиком. 