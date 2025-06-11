import os
import pandas as pd
import numpy as np
import folium
from folium.plugins import MarkerCluster
import psycopg2
import asyncio
import telegram
from dotenv import load_dotenv
from datetime import datetime
import ftplib
import jinja2

# Загружаем переменные окружения
load_dotenv()

# Параметры Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID")

# Параметры FTP
FTP_HOST = os.getenv("FTP_HOST", "")
FTP_USER = os.getenv("FTP_USER", "")
FTP_PASSWORD = os.getenv("FTP_PASSWORD", "")
FTP_DIRECTORY = os.getenv("FTP_DIRECTORY", "/public_html/dubai-reports/")
BASE_URL = os.getenv("BASE_URL", "https://ваш-домен.com/dubai-reports/")

# Функция для подключения к БД
def connect_to_db():
    """Устанавливает соединение с базой данных PostgreSQL."""
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )
        return conn
    except Exception as e:
        print(f"Ошибка подключения к БД: {e}")
        return None

# Функция для парсинга географических данных
def parse_geography(geo_str):
    """
    Парсит строку геоданных 'Широта: ..., Долгота: ...' и возвращает широту и долготу.
    """
    if not isinstance(geo_str, str):
        return None, None
    try:
        parts = geo_str.replace('Широта: ', '').replace(' Долгота: ', '').split(',')
        if len(parts) == 2:
            lat = float(parts[0].strip())
            lng = float(parts[1].strip())
            return lat, lng
    except (ValueError, IndexError):
        pass
    return None, None

# Функция для получения данных о самых дешевых квартирах
def fetch_cheapest_apartments_by_region(conn):
    """
    Извлекает топ-3 самых дешевых квартир по каждому региону с площадью до 40 кв.м.
    """
    query = """
    WITH ranked_apartments AS (
        SELECT 
            id, 
            location, 
            area, 
            price, 
            geography,
            ROW_NUMBER() OVER (PARTITION BY location ORDER BY price ASC) as rank
        FROM bayut_properties
        WHERE area <= 40 AND price > 0
    )
    SELECT 
        id, 
        location, 
        area, 
        price, 
        geography,
        rank
    FROM ranked_apartments
    WHERE rank <= 3
    ORDER BY location, rank
    """
    
    try:
        # Выполняем запрос
        df = pd.read_sql_query(query, conn)
        
        if df.empty: 
            print("Данные не найдены в БД.")
            return None
        
        # Обрабатываем географические данные
        df[['latitude', 'longitude']] = df['geography'].apply(lambda x: pd.Series(parse_geography(x)))
        
        # Удаляем строки без координат
        df = df.dropna(subset=['latitude', 'longitude'])
        
        # Создаем URL-ссылки на объявления
        df['url'] = df['id'].apply(lambda x: f"https://www.bayut.com/property/{x}/")
        
        return df
    
    except Exception as e:
        print(f"Ошибка при извлечении данных из БД: {e}")
        return None

# Функция для создания интерактивной карты с Folium
def create_interactive_map(df):
    """
    Создает интерактивную карту с маркерами для квартир.
    """
    # Создаем карту, центрированную по среднему значению координат
    center_lat = df['latitude'].mean()
    center_lon = df['longitude'].mean()
    
    m = folium.Map(location=[center_lat, center_lon], 
                   zoom_start=11, 
                   tiles='CartoDB positron')
    
    # Добавляем кластеры маркеров для лучшей производительности
    marker_cluster = MarkerCluster().add_to(m)
    
    # Добавляем маркеры для каждой квартиры
    for idx, row in df.iterrows():
        # Создаем всплывающее окно (popup) с информацией о квартире
        popup_html = f"""
        <div style="width: 200px">
            <h4>{row['location']}</h4>
            <b>Цена:</b> {int(row['price']):,} AED<br>
            <b>Площадь:</b> {row['area']:.1f} кв.м<br>
            <b>Рейтинг:</b> #{row['rank']} в регионе<br>
            <a href="{row['url']}" target="_blank">Открыть объявление</a>
        </div>
        """
        
        # Определяем цвет маркера в зависимости от ранга
        color = 'green' if row['rank'] == 1 else 'blue' if row['rank'] == 2 else 'red'
        
        # Добавляем маркер на карту
        folium.Marker(
            location=[row['latitude'], row['longitude']],
            popup=folium.Popup(popup_html, max_width=300),
            tooltip=f"{row['location']}: {int(row['price']):,} AED",
            icon=folium.Icon(color=color, icon='home', prefix='fa')
        ).add_to(marker_cluster)
    
    return m

# Функция для создания статистики по регионам
def create_region_stats(df):
    """
    Создает статистику по регионам для отображения.
    """
    # Группируем данные по регионам и вычисляем статистику
    region_stats = df.groupby('location').agg(
        count=('id', 'count'),
        min_price=('price', 'min'),
        max_price=('price', 'max'),
        avg_price=('price', 'mean')
    ).reset_index()
    
    # Сортируем по минимальной цене
    region_stats = region_stats.sort_values('min_price')
    
    return region_stats

# Функция для генерации HTML-страницы с отчетом
def generate_html_report(df):
    """
    Генерирует HTML-страницу с отчетом о самых дешевых квартирах.
    """
    # Создаем папку для отчетов, если ее нет
    os.makedirs('reports', exist_ok=True)
    
    # Генерируем уникальное имя файла
    current_date = datetime.now().strftime('%Y%m%d_%H%M%S')
    file_name = f"cheapest_apartments_{current_date}.html"
    file_path = os.path.join('reports', file_name)
    
    # Получаем карту и статистику
    map_obj = create_interactive_map(df)
    region_stats = create_region_stats(df)
    
    # Сохраняем карту во временный файл
    map_html = map_obj._repr_html_()
    
    # Подготавливаем данные для таблицы
    table_data = df.copy()
    table_data['price_formatted'] = table_data['price'].apply(lambda x: f"{int(x):,} AED")
    table_data['area_formatted'] = table_data['area'].apply(lambda x: f"{x:.1f} кв.м")
    table_data['link'] = table_data.apply(lambda row: f'<a href="{row["url"]}" target="_blank">Открыть</a>', axis=1)
    
    # Создаем HTML-шаблон
    template = """
    <!DOCTYPE html>
    <html lang="ru">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Самые дешевые квартиры в Дубае</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha1/dist/css/bootstrap.min.css" rel="stylesheet">
        <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha1/dist/js/bootstrap.bundle.min.js"></script>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <style>
            body { font-family: Arial, sans-serif; margin: 0; padding: 0; }
            .header { background-color: #003366; color: white; padding: 20px; }
            .container { max-width: 1200px; margin: 0 auto; padding: 20px; }
            .map-container { height: 500px; margin-bottom: 30px; }
            .stats-container { margin-bottom: 30px; }
            .table-container { margin-bottom: 30px; }
            table { width: 100%; border-collapse: collapse; }
            th, td { padding: 10px; text-align: left; border: 1px solid #ddd; }
            th { background-color: #f2f2f2; }
            tr:nth-child(even) { background-color: #f9f9f9; }
            .footer { background-color: #f2f2f2; padding: 20px; text-align: center; }
            .card { margin-bottom: 20px; }
        </style>
    </head>
    <body>
        <div class="header">
            <div class="container">
                <h1>Самые дешевые квартиры в Дубае</h1>
                <p>Дата обновления: {{ current_date }}</p>
            </div>
        </div>

        <div class="container">
            <div class="row">
                <div class="col-md-12">
                    <div class="card">
                        <div class="card-header">
                            <h2>Интерактивная карта</h2>
                        </div>
                        <div class="card-body">
                            <div class="map-container">
                                {{ map_html|safe }}
                            </div>
                            <div class="legend">
                                <p><span style="color: green;">●</span> - самая дешевая квартира в регионе</p>
                                <p><span style="color: blue;">●</span> - вторая по цене квартира в регионе</p>
                                <p><span style="color: red;">●</span> - третья по цене квартира в регионе</p>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            <div class="row">
                <div class="col-md-12">
                    <div class="card">
                        <div class="card-header">
                            <h2>Топ-10 регионов с самыми низкими ценами</h2>
                        </div>
                        <div class="card-body">
                            <div class="stats-container">
                                <canvas id="regionsChart"></canvas>
                            </div>
                            <script>
                                var ctx = document.getElementById('regionsChart').getContext('2d');
                                var chart = new Chart(ctx, {
                                    type: 'bar',
                                    data: {
                                        labels: {{ region_labels|safe }},
                                        datasets: [{
                                            label: 'Минимальная цена (AED)',
                                            data: {{ region_prices|safe }},
                                            backgroundColor: 'rgba(0, 123, 255, 0.7)',
                                            borderColor: 'rgba(0, 123, 255, 1)',
                                            borderWidth: 1
                                        }]
                                    },
                                    options: {
                                        scales: {
                                            y: {
                                                beginAtZero: true,
                                                title: {
                                                    display: true,
                                                    text: 'Цена (AED)'
                                                }
                                            },
                                            x: {
                                                title: {
                                                    display: true,
                                                    text: 'Регион'
                                                }
                                            }
                                        },
                                        plugins: {
                                            legend: {
                                                display: true,
                                                position: 'top'
                                            }
                                        }
                                    }
                                });
                            </script>
                        </div>
                    </div>
                </div>
            </div>

            <div class="row">
                <div class="col-md-12">
                    <div class="card">
                        <div class="card-header">
                            <h2>Полный список квартир</h2>
                        </div>
                        <div class="card-body">
                            <div class="table-container">
                                <table class="table table-striped">
                                    <thead>
                                        <tr>
                                            <th>Регион</th>
                                            <th>Цена</th>
                                            <th>Площадь</th>
                                            <th>Ранг в регионе</th>
                                            <th>Ссылка</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {% for _, row in table_data.iterrows() %}
                                        <tr>
                                            <td>{{ row.location }}</td>
                                            <td>{{ row.price_formatted }}</td>
                                            <td>{{ row.area_formatted }}</td>
                                            <td>{{ row.rank }}</td>
                                            <td>{{ row.link|safe }}</td>
                                        </tr>
                                        {% endfor %}
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <div class="footer">
            <p>© {{ current_year }} Wealth Compass. Все права защищены.</p>
        </div>
    </body>
    </html>
    """
    
    # Получаем топ-10 регионов для графика
    top_regions = region_stats.head(10)
    region_labels = top_regions['location'].tolist()
    region_prices = top_regions['min_price'].tolist()
    
    # Подготавливаем данные для шаблона
    template_data = {
        'current_date': datetime.now().strftime('%d.%m.%Y'),
        'current_year': datetime.now().year,
        'map_html': map_html,
        'region_labels': region_labels,
        'region_prices': region_prices,
        'table_data': table_data[['location', 'price_formatted', 'area_formatted', 'rank', 'link']]
    }
    
    # Генерируем HTML с использованием Jinja2
    env = jinja2.Environment()
    template = env.from_string(template)
    html_content = template.render(**template_data)
    
    # Записываем HTML в файл
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    return file_path, file_name

# Функция для загрузки файла на FTP-сервер
def upload_to_ftp(local_file_path, remote_file_name):
    """
    Загружает файл на FTP-сервер.
    """
    if not all([FTP_HOST, FTP_USER, FTP_PASSWORD]):
        print("ВНИМАНИЕ: Не указаны параметры FTP в .env файле")
        return None
    
    try:
        # Подключаемся к FTP-серверу
        ftp = ftplib.FTP(FTP_HOST)
        ftp.login(FTP_USER, FTP_PASSWORD)
        
        # Переходим в нужную директорию
        try:
            ftp.cwd(FTP_DIRECTORY)
        except ftplib.error_perm:
            # Если директории нет, пытаемся создать ее
            dirs = FTP_DIRECTORY.split('/')
            current_dir = ""
            for d in dirs:
                if not d:
                    continue
                current_dir += f"/{d}"
                try:
                    ftp.cwd(current_dir)
                except:
                    ftp.mkd(current_dir)
                    ftp.cwd(current_dir)
        
        # Загружаем файл
        with open(local_file_path, 'rb') as f:
            ftp.storbinary(f'STOR {remote_file_name}', f)
        
        # Закрываем соединение
        ftp.quit()
        
        # Возвращаем URL файла
        return f"{BASE_URL}{remote_file_name}"
    
    except Exception as e:
        print(f"Ошибка при загрузке файла на FTP: {e}")
        return None

# Функция для отправки сообщения в Telegram
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

# Основная функция
def main():
    # Подключаемся к БД
    conn = connect_to_db()
    if not conn:
        print("Не удалось подключиться к базе данных.")
        return
    
    try:
        # Получаем данные
        print("Получение данных из БД...")
        apartments_data = fetch_cheapest_apartments_by_region(conn)
        
        if apartments_data is None or apartments_data.empty:
            print("Нет данных для отображения.")
            return
        
        # Генерируем HTML-отчет
        print("Генерация HTML-отчета...")
        html_file_path, html_file_name = generate_html_report(apartments_data)
        
        # Загружаем отчет на FTP
        print("Загрузка отчета на FTP...")
        report_url = upload_to_ftp(html_file_path, html_file_name)
        
        if not report_url:
            print("Не удалось загрузить отчет на FTP.")
            print(f"Отчет сохранен локально: {html_file_path}")
            return
        
        # Формируем сообщение для отправки в Telegram
        message = f"""
<b>🏢 Анализ рынка недвижимости в Дубае: самые дешевые квартиры</b>

Обновлен интерактивный отчет со списком самых дешевых квартир по всем регионам Дубая (площадь до 40 кв.м).

<b>Что вы найдете в отчете:</b>
• Интерактивная карта с маркерами квартир
• Топ-10 регионов с самыми низкими ценами
• Полная таблица всех квартир с возможностью сортировки

<b>Дата обновления:</b> {datetime.now().strftime('%d.%m.%Y')}

<b>Ссылка на отчет:</b> 
{report_url}
"""
        
        # Отправляем сообщение в Telegram
        if TELEGRAM_BOT_TOKEN and TELEGRAM_CHANNEL_ID:
            print("Отправка сообщения в Telegram...")
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
        
        print(f"Готово! Отчет доступен по адресу: {report_url}")
    
    finally:
        # Закрываем соединение с БД
        conn.close()

if __name__ == "__main__":
    main() 