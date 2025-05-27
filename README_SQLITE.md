# Dubai Property Analyzer - SQLite версия

Это версия приложения для анализа недвижимости в Дубае, использующая локальную базу данных SQLite вместо API-сервера. Данный подход позволяет развернуть приложение в Streamlit Cloud без необходимости настройки внешнего API.

## Преимущества SQLite-версии

1. **Не требуется API-сервер** - данные хранятся непосредственно в файле базы данных SQLite
2. **Простой деплой** - не нужно настраивать ngrok или другие туннели
3. **Полностью автономное решение** - приложение работает независимо от локальной базы данных
4. **Возможность автоматического обновления данных** - через GitHub Actions или локальный планировщик

## Файлы в проекте

- `postgres_to_sqlite.py` - скрипт для экспорта данных из PostgreSQL в SQLite
- `schedule_export.py` - планировщик для периодического экспорта данных и их отправки в GitHub
- `streamlit_sqlite_app.py` - Streamlit-приложение, работающее с SQLite базой данных
- `dubai_properties.db` - файл базы данных SQLite (создаётся при первом запуске скрипта экспорта)

## Настройка и использование

### 1. Экспорт данных из PostgreSQL в SQLite

Для экспорта данных из вашей локальной PostgreSQL базы в SQLite выполните следующую команду:

```bash
python postgres_to_sqlite.py
```

Это создаст файл `dubai_properties.db` с данными из вашей PostgreSQL базы.

### 2. Запуск Streamlit-приложения локально

```bash
streamlit run streamlit_sqlite_app.py
```

### 3. Настройка автоматического обновления данных

#### Вариант 1: Использование планировщика на локальном компьютере

Для настройки автоматического обновления данных и отправки их в GitHub:

1. Добавьте в файл `.env` настройки для GitHub:
   ```
   GIT_USERNAME=ваше_имя_пользователя_git
   GIT_EMAIL=ваш_email@example.com
   GIT_TOKEN=ваш_персональный_токен_доступа_github
   ```

2. Запустите планировщик:
   ```bash
   python schedule_export.py
   ```

   Планировщик будет экспортировать данные и отправлять их в GitHub каждые 12 часов.

   Опции запуска:
   - `--run-once` - выполнить экспорт и отправку один раз
   - `--interval 24` - задать интервал выполнения в часах (в данном примере 24 часа)

#### Вариант 2: Использование GitHub Actions

Вы также можете настроить GitHub Actions для автоматического обновления данных. Для этого создайте файл `.github/workflows/update-data.yml` со следующим содержимым:

```yaml
name: Update SQLite Database

on:
  schedule:
    - cron: '0 */12 * * *'  # Запускать каждые 12 часов
  workflow_dispatch:        # Позволяет запускать вручную

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
          pip install psycopg2-binary pandas python-dotenv
          
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
          git commit -m "Update database $(date +'%Y-%m-%d %H:%M:%S')" || echo "No changes to commit"
          git push
```

В этом случае вам нужно будет добавить секреты для доступа к вашей базе данных в настройках репозитория GitHub.

### 4. Деплой в Streamlit Cloud

1. Создайте репозиторий на GitHub и загрузите в него следующие файлы:
   - `streamlit_sqlite_app.py`
   - `dubai_properties.db`
   - `requirements.txt`

2. В Streamlit Community Cloud подключите ваш репозиторий.

3. В качестве основного файла укажите `streamlit_sqlite_app.py`.

## Обновление данных

При использовании планировщика или GitHub Actions, данные в базе SQLite будут автоматически обновляться. После обновления данных и их отправки в GitHub, Streamlit Cloud автоматически перезапустит приложение с новыми данными.

## Требования

- Python 3.7+
- Streamlit 1.28+
- Pandas
- SQLite3 (встроен в Python)
- Folium и streamlit-folium для карт
- Plotly для графиков

## Примечание

Эта версия приложения предназначена для ситуаций, когда данные обновляются относительно редко или когда небольшая задержка в обновлении данных (от момента изменения в PostgreSQL до обновления в SQLite) приемлема. Для приложений, требующих доступа к данным в реальном времени, рекомендуется использовать версию с API-сервером. 