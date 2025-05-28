import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from io import BytesIO
import telegram
from telegram import InlineKeyboardMarkup, InlineKeyboardButton
import os
import asyncio
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
import geopandas
import contextily as cx
import numpy as np
from matplotlib.patches import Patch
import matplotlib.colors
import random
import traceback

# Загрузка переменных окружения для токена Telegram и ID чата
load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHANNEL_ID")
BAYUT_BASE_URL = "https://www.bayut.com/property/"  # Базовый URL для объявлений

# Параметры подключения к базе данных
DB_PARAMS = {
    'dbname': os.getenv("DB_NAME", "postgres"),
    'user': os.getenv("DB_USER", "admin"),
    'password': os.getenv("DB_PASSWORD", "Enclude79"),
    'host': os.getenv("DB_HOST", "localhost"),
    'port': os.getenv("DB_PORT", "5432")
}

def connect_to_db():
    """Устанавливает соединение с базой данных PostgreSQL."""
    try:
        print("Подключение к базе данных...")
        conn = psycopg2.connect(**DB_PARAMS)
        print("Подключение успешно установлено.")
        return conn
    except Exception as e:
        print(f"Ошибка подключения к БД: {e}")
        return None

def parse_geography(geo_str):
    """
    Парсит строку геоданных 'Широта: ..., Долгота: ...' и возвращает широту и долготу.
    Возвращает (None, None) если парсинг не удался.
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
        return None, None
    return None, None

def fetch_cheapest_apartments_by_region(conn):
    """
    Извлекает топ-3 самых дешевых квартир по каждому региону с площадью до 40 кв.м.
    """
    print("[ЗАПУСК] Извлечение топ-3 самых дешевых квартир по каждому региону (площадь <= 40 кв.m).")
    
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
        print("\n[ЗАПРОС] Выполняем SQL-запрос для получения данных...")
        df = pd.read_sql_query(query, conn)
        
        if df.empty: 
            print("Данные не найдены в БД."); 
            return None
        
        print(f"Запрос выполнен успешно. Получено {len(df)} строк.")
        print(f"Столбцы в DataFrame: {df.columns.tolist()}")
        
        # Базовая информация о данных
        print(f"Пример данных (первые 5 строк):")
        print(df.head())
        
        # Обрабатываем географические данные
        print("\nОбработка географических данных...")
        print(f"Пример значения geography: {df['geography'].iloc[0] if len(df) > 0 else 'нет данных'}")
        
        df[['latitude', 'longitude']] = df['geography'].apply(lambda x: pd.Series(parse_geography(x)))
        print("География обработана.")
        
        # Проверяем наличие координат
        print(f"Строк с непустыми координатами: {df.dropna(subset=['latitude', 'longitude']).shape[0]}")
        
        # Удаляем строки без координат
        df.dropna(subset=['latitude', 'longitude'], inplace=True)
        
        # Создаем URL-ссылки на объявления на основе ID
        print("Создаем URL-ссылки на объявления...")
        df['url'] = df['id'].apply(lambda x: f"{BAYUT_BASE_URL}{x}/")
        
        # Базовая информация о результатах
        regions_count = df['location'].nunique()
        total_apartments = len(df)
        
        print(f"Извлечено {total_apartments} квартир из {regions_count} регионов.")
        print(f"Уникальные регионы: {df['location'].unique().tolist()}")
        
        # Анализ данных по регионам
        region_stats = df.groupby('location').agg(
            count=('id', 'count'),
            min_price=('price', 'min'),
            max_price=('price', 'max'),
            avg_price=('price', 'mean')
        ).reset_index()
        
        print("\nСтатистика по регионам:")
        for _, row in region_stats.iterrows():
            print(f"Регион: {row['location']}")
            print(f"  Количество квартир: {row['count']}")
            print(f"  Минимальная цена: {row['min_price']:.2f}")
            print(f"  Максимальная цена: {row['max_price']:.2f}")
            print(f"  Средняя цена: {row['avg_price']:.2f}")
            
        return df
    
    except Exception as e:
        print(f"Ошибка при извлечении данных из БД: {e}")
        print(traceback.format_exc())
        return None

def generate_color_mapping(unique_regions):
    """
    Создает уникальное цветовое отображение для каждого региона.
    """
    print(f"Создание цветового отображения для {len(unique_regions)} регионов...")
    
    # Получаем список цветов из цветовой карты tab20
    cmap = plt.get_cmap('tab20')
    colors = [cmap(i) for i in range(20)]
    
    # Если регионов больше 20, создаем дополнительные цвета
    if len(unique_regions) > 20:
        cmap_extra = plt.get_cmap('tab20b')
        colors.extend([cmap_extra(i) for i in range(20)])
    
    # Если все еще не хватает цветов, добавляем случайные
    while len(colors) < len(unique_regions):
        colors.append((
            random.random(),
            random.random(),
            random.random(),
            1.0  # Альфа-канал (непрозрачность)
        ))
    
    # Создаем отображение регион -> цвет
    color_mapping = {}
    for i, region in enumerate(unique_regions):
        color_mapping[region] = colors[i]
    
    print("Цветовое отображение создано успешно.")
    return color_mapping

def create_apartment_buttons(apartments_df):
    """
    Создает инлайн-клавиатуру с кнопками для объявлений квартир.
    Группирует кнопки по регионам и ограничивает количество регионов для удобства пользователей.
    """
    print("Создание инлайн-клавиатуры для объявлений...")
    
    if apartments_df is None or apartments_df.empty:
        return None
    
    # Проверяем наличие необходимых столбцов
    required_cols = ['id', 'location', 'price', 'url']
    if not all(col in apartments_df.columns for col in required_cols):
        missing_cols = [col for col in required_cols if col not in apartments_df.columns]
        print(f"Отсутствуют необходимые столбцы для создания кнопок: {missing_cols}")
        return None
    
    # Находим топ-10 регионов с самыми дешевыми квартирами
    cheapest_by_region = apartments_df.groupby('location')['price'].min().sort_values().head(12)
    top_regions = cheapest_by_region.index.tolist()
    
    print(f"Выбраны топ-12 регионов с самыми дешевыми квартирами: {top_regions}")
    
    # Создаем кнопки только для топ регионов
    keyboard = []
    
    for region in top_regions:
        region_data = apartments_df[apartments_df['location'] == region].sort_values('price').head(1)
        
        if not region_data.empty:
            row = region_data.iloc[0]
            price = int(row['price'])
            url = row['url']
            
            # Создаем кнопку с текстом "Регион: Цена AED"
            button_text = f"{region}: {price:,} AED"
            button = InlineKeyboardButton(text=button_text, url=url)
            
            # Добавляем кнопку в новом ряду
            keyboard.append([button])
    
    print(f"Создано {len(keyboard)} кнопок для регионов.")
    return InlineKeyboardMarkup(keyboard)

def send_telegram_report(image_buffer, apartments_df, bot_token, chat_id, caption=""):
    """
    Отправляет изображение в Telegram чат с кнопками для перехода к объявлениям.
    """
    if not bot_token or not chat_id:
        print("Токен бота или ID чата не указаны. Отправка в Telegram невозможна.")
        return False
    
    try:
        print(f"Отправка отчета в Telegram (chat_id: {chat_id})...")
        bot = telegram.Bot(token=bot_token)
        image_buffer.seek(0)
        
        # Создаем клавиатуру с кнопками-ссылками на объявления
        reply_markup = create_apartment_buttons(apartments_df)
        
        if reply_markup:
            bot.send_photo(
                chat_id=chat_id, 
                photo=image_buffer, 
                caption=caption, 
                parse_mode='HTML',
                reply_markup=reply_markup
            )
        else:
            # Если не удалось создать клавиатуру, отправляем только изображение
            bot.send_photo(
                chat_id=chat_id, 
                photo=image_buffer, 
                caption=caption, 
                parse_mode='HTML'
            )
        
        print(f"Отчет успешно отправлен в Telegram чат/канал {chat_id}.")
        return True
    
    except telegram.error.BadRequest as e:
        print(f"Ошибка BadRequest при отправке в Telegram: {e}")
        if "Chat not found" in str(e):
            print(f"Убедитесь, что ID чата {chat_id} корректен и бот добавлен в чат/канал.")
        elif "Wrong file identifier/HTTP URL specified" in str(e):
            print("Проблема с отправляемым файлом изображения.")
        return False
    except Exception as e:
        print(f"Не удалось отправить отчет в Telegram: {e}")
        print(traceback.format_exc())
        return False

def generate_map_report(apartments_df):
    """
    Создает карту с топ-3 самыми дешевыми квартирами по каждому региону.
    """
    print("\nСоздание карты с самыми дешевыми квартирами...")
    
    if apartments_df is None or apartments_df.empty:
        print("Нет данных для создания карты.")
        return None, []
    
    # Проверяем наличие всех необходимых столбцов
    required_cols = ['location', 'latitude', 'longitude', 'price', 'area', 'rank']
    if not all(col in apartments_df.columns for col in required_cols):
        missing_cols = [col for col in required_cols if col not in apartments_df.columns]
        print(f"В DataFrame отсутствуют необходимые столбцы: {missing_cols}")
        return None, []
    
    # Очищаем DataFrame от строк без координат
    df_cleaned = apartments_df.dropna(subset=['latitude', 'longitude'])
    if df_cleaned.empty:
        print("Нет данных с координатами для отображения на карте.")
        return None, []
    
    print(f"Данные для карты подготовлены: {len(df_cleaned)} квартир.")
    
    # Получаем уникальные регионы и создаем цветовое отображение
    unique_regions = df_cleaned['location'].unique()
    print(f"Найдено {len(unique_regions)} уникальных регионов для отображения.")
    
    color_mapping = generate_color_mapping(unique_regions)
    
    # Добавляем столбец с цветом для каждой квартиры
    df_cleaned['color'] = df_cleaned['location'].map(color_mapping)
    
    # Создаем GeoDataFrame для отображения на карте
    print("Создание GeoDataFrame...")
    try:
        gdf = geopandas.GeoDataFrame(
            df_cleaned, 
            geometry=geopandas.points_from_xy(df_cleaned.longitude, df_cleaned.latitude),
            crs="EPSG:4326"
        )
        gdf_web_mercator = gdf.to_crs(epsg=3857)
        print("GeoDataFrame создан успешно.")
    except Exception as e:
        print(f"Ошибка при создании GeoDataFrame: {e}")
        print(traceback.format_exc())
        return None, []
    
    # Создаем карту
    print("Создание визуализации карты...")
    fig, ax = plt.subplots(1, 1, figsize=(12, 12))
    
    # Отображаем точки на карте
    for region in unique_regions:
        region_data = gdf_web_mercator[gdf_web_mercator['location'] == region]
        region_data.plot(
            ax=ax, 
            color=color_mapping[region],
            markersize=80, 
            alpha=0.7,
            edgecolor='black',
            linewidth=0.5,
            label=region
        )
    
    # Добавляем базовую карту
    print("Добавление базовой карты...")
    try:
        cx.add_basemap(ax, crs=gdf_web_mercator.crs.to_string(), source=cx.providers.OpenStreetMap.Mapnik)
        print("Базовая карта добавлена успешно.")
    except Exception as e:
        print(f"Не удалось добавить базовую карту: {e}")
        print(traceback.format_exc())
    
    # Подготавливаем данные для подписи в Telegram
    print("Подготовка данных для подписи...")
    legend_details = []
    
    for region in unique_regions:
        # Получаем статистику по региону для легенды
        region_data = df_cleaned[df_cleaned['location'] == region]
        min_price = region_data['price'].min()
        avg_price = region_data['price'].mean()
        
        legend_details.append(
            f"<b>{region}</b>: мин. {int(min_price):,} AED, средн. {int(avg_price):,} AED"
        )
    
    # Настраиваем заголовок и отключаем оси
    ax.set_title(
        f'Топ-3 самых дешевых квартир по регионам Дубая (≤40 кв.м)\n{datetime.now().strftime("%Y-%m-%d")}',
        fontsize=14
    )
    ax.set_axis_off()
    plt.tight_layout(pad=1.0)
    
    # Генерируем имя файла с текущей датой и временем
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f'cheapest_apartments_by_region_{timestamp}.png'
    
    # Сохраняем отчет в папку cheapest_apartments
    print("Сохранение отчета...")
    cheapest_apartments_dir = Path('reports') / 'cheapest_apartments'
    cheapest_apartments_dir.mkdir(parents=True, exist_ok=True)
    
    cheapest_apartments_filepath = cheapest_apartments_dir / filename
    
    try:
        plt.savefig(cheapest_apartments_filepath, format='png', bbox_inches='tight', dpi=200)
        print(f"Отчет сохранен: {cheapest_apartments_filepath}")
    except Exception as e:
        print(f"Ошибка сохранения файла в папку cheapest_apartments: {e}")
        print(traceback.format_exc())
        plt.close(fig)
        return None, []
    
    # Дополнительно сохраняем отчет в папку price_volatility
    price_volatility_dir = Path('reports') / 'price_volatility'
    price_volatility_dir.mkdir(parents=True, exist_ok=True)
    
    price_volatility_filepath = price_volatility_dir / filename
    
    try:
        plt.savefig(price_volatility_filepath, format='png', bbox_inches='tight', dpi=200)
        print(f"Копия отчета сохранена: {price_volatility_filepath}")
    except Exception as e:
        print(f"Ошибка сохранения файла в папку price_volatility: {e}")
        print(traceback.format_exc())
    
    # Сохраняем в буфер для отправки в Telegram
    print("Сохранение изображения в буфер для Telegram...")
    img_buffer = BytesIO()
    plt.savefig(img_buffer, format='png', bbox_inches='tight', dpi=200)
    img_buffer.seek(0)
    
    plt.close(fig)
    print("Создание карты завершено успешно.")
    return img_buffer, legend_details

async def main():
    print("=" * 80)
    print("Запуск генератора отчета о самых дешевых квартирах...")
    print("=" * 80)
    
    conn = connect_to_db()
    if not conn:
        print("Не удалось подключиться к БД.")
        return
    
    try:
        # Получаем данные о самых дешевых квартирах по регионам
        apartments_data = fetch_cheapest_apartments_by_region(conn)
        
        if apartments_data is None or apartments_data.empty:
            print("Нет данных для создания отчета.")
            return
        
        # Создаем карту и получаем данные для подписи
        report_image_buffer, legend_details = generate_map_report(apartments_data)
        
        if report_image_buffer:
            # Формируем подпись для Telegram
            current_date = datetime.now().strftime("%d.%m.%Y")
            
            caption_title = f"<b>Топ-3 самых дешевых квартир по регионам Дубая (≤40 кв.м)</b>\nДата: {current_date}\n\n"
            caption_info = "<i>Нажмите на кнопки ниже, чтобы просмотреть объявления.</i>"
            
            # Сокращаем подпись, так как у нас теперь есть кнопки с информацией
            full_caption = caption_title + caption_info
            
            # Инициализируем бота
            bot = telegram.Bot(token=TELEGRAM_BOT_TOKEN)
            
            # Создаем клавиатуру с кнопками-ссылками на объявления
            reply_markup = create_apartment_buttons(apartments_data)
            
            if reply_markup:
                try:
                    # Отправляем фото с кнопками
                    report_image_buffer.seek(0)
                    await bot.send_photo(
                        chat_id=TELEGRAM_CHAT_ID, 
                        photo=report_image_buffer, 
                        caption=full_caption, 
                        parse_mode='HTML',
                        reply_markup=reply_markup
                    )
                    print(f"Отчет успешно отправлен в Telegram чат/канал {TELEGRAM_CHAT_ID}.")
                except Exception as e:
                    print(f"Ошибка при отправке отчета в Telegram: {e}")
                    print(traceback.format_exc())
            else:
                print("Не удалось создать интерактивные кнопки для Telegram.")
    
    except Exception as e:
        print(f"Произошла ошибка в основном процессе: {e}")
        print(traceback.format_exc())
    
    finally:
        if conn:
            conn.close()
            print("Соединение с БД закрыто.")
            print("=" * 80)
            print("Работа скрипта завершена.")
            print("=" * 80)

if __name__ == '__main__':
    asyncio.run(main()) 