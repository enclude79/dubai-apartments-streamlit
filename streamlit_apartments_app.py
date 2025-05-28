import streamlit as st
import pandas as pd
import numpy as np
import folium
from folium.plugins import MarkerCluster
from streamlit_folium import st_folium
import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime

# Загрузка переменных окружения
load_dotenv()

# Заголовок приложения
st.set_page_config(
    page_title="Самые дешевые квартиры в Дубае",
    page_icon="🏢",
    layout="wide"
)

# Функция для подключения к БД
def connect_to_db():
    """Устанавливает соединение с базой данных PostgreSQL."""
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME", "postgres"),
            user=os.getenv("DB_USER", "admin"),
            password=os.getenv("DB_PASSWORD", "Enclude79"),
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432")
        )
        return conn
    except Exception as e:
        st.error(f"Ошибка подключения к БД: {e}")
        return None

# Функция для парсинга географических данных
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
            st.error("Данные не найдены в БД.")
            return None
        
        # Обрабатываем географические данные
        df[['latitude', 'longitude']] = df['geography'].apply(lambda x: pd.Series(parse_geography(x)))
        
        # Удаляем строки без координат
        df = df.dropna(subset=['latitude', 'longitude'])
        
        # Создаем URL-ссылки на объявления
        df['url'] = df['id'].apply(lambda x: f"https://www.bayut.com/property/{x}/")
        
        return df
    
    except Exception as e:
        st.error(f"Ошибка при извлечении данных из БД: {e}")
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
    Создает статистику по регионам для отображения на боковой панели.
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

# Основная функция приложения
def main():
    st.title("🏢 Топ-3 самых дешевых квартир по регионам Дубая")
    st.caption(f"Дата обновления: {datetime.now().strftime('%d.%m.%Y')}")
    
    # Подключаемся к БД
    conn = connect_to_db()
    if not conn:
        st.error("Не удалось подключиться к базе данных.")
        return
    
    # Получаем данные
    with st.spinner("Загрузка данных..."):
        apartments_data = fetch_cheapest_apartments_by_region(conn)
    
    if apartments_data is None or apartments_data.empty:
        st.error("Нет данных для отображения.")
        return
    
    # Закрываем соединение с БД
    conn.close()
    
    # Создаем двухколоночный макет
    col1, col2 = st.columns([3, 1])
    
    with col1:
        # Создаем и отображаем карту
        st.subheader("Интерактивная карта")
        st.caption("Маркеры: 🟢 - самая дешевая, 🔵 - вторая по цене, 🔴 - третья по цене")
        
        folium_map = create_interactive_map(apartments_data)
        st_folium(folium_map, width=800, height=500)
        
        # Добавляем информацию о том, как пользоваться картой
        with st.expander("Как пользоваться картой"):
            st.markdown("""
            - **Увеличить/уменьшить масштаб**: используйте колесо мыши или кнопки + и - в левом верхнем углу
            - **Перемещение по карте**: удерживайте левую кнопку мыши и перетаскивайте карту
            - **Информация о квартире**: нажмите на маркер, чтобы увидеть подробную информацию
            - **Открыть объявление**: нажмите на ссылку "Открыть объявление" во всплывающем окне
            """)
    
    with col2:
        # Отображаем статистику по регионам
        st.subheader("Топ-10 регионов с самыми низкими ценами")
        
        region_stats = create_region_stats(apartments_data)
        
        # Отображаем топ-10 регионов с самыми низкими ценами
        for i, row in region_stats.head(10).iterrows():
            with st.container():
                st.markdown(f"**{i+1}. {row['location']}**")
                st.markdown(f"Мин. цена: **{int(row['min_price']):,} AED**")
                st.markdown(f"Средняя цена: {int(row['avg_price']):,} AED")
                st.markdown("---")
    
    # Добавляем таблицу со всеми данными
    st.subheader("Все квартиры")
    
    # Подготовка данных для таблицы
    table_data = apartments_data[['location', 'price', 'area', 'rank', 'url']].copy()
    table_data['price'] = table_data['price'].apply(lambda x: f"{int(x):,} AED")
    table_data['area'] = table_data['area'].apply(lambda x: f"{x:.1f} кв.м")
    table_data['rank'] = table_data['rank'].apply(lambda x: f"#{x}")
    
    # Переименовываем столбцы для отображения
    table_data.columns = ['Регион', 'Цена', 'Площадь', 'Рейтинг', 'Ссылка']
    
    # Создаем ссылки для последнего столбца
    table_data['Ссылка'] = table_data['Ссылка'].apply(lambda x: f'<a href="{x}" target="_blank">Перейти</a>')
    
    # Отображаем таблицу с возможностью сортировки и фильтрации
    st.dataframe(
        table_data,
        column_config={
            "Ссылка": st.column_config.LinkColumn()
        },
        hide_index=True,
        use_container_width=True
    )
    
    # Добавляем информацию о проекте
    st.markdown("---")
    st.caption("© 2025 Wealth Compass | Анализ рынка недвижимости в Дубае")

if __name__ == "__main__":
    main() 