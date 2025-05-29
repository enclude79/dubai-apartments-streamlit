import streamlit as st
import pandas as pd
import sqlite3
import folium
from folium.plugins import MarkerCluster
from streamlit_folium import folium_static
import plotly.express as px
import plotly.graph_objects as go
import os
from datetime import datetime
import colorsys
import re
import traceback

# Путь к SQLite базе данных
SQLITE_DB_PATH = "dubai_properties.db"

st.set_page_config(
    page_title="Dubai Property Analysis",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Функция для получения соединения с SQLite
def get_db_connection():
    """Создает соединение с базой данных SQLite"""
    try:
        conn = sqlite3.connect(SQLITE_DB_PATH)
        return conn
    except Exception as e:
        st.error(f"Ошибка подключения к базе данных SQLite: {e}")
        return None

# Функция для выполнения запросов к базе данных
@st.cache_data(ttl=300)  # Кэшируем на 5 минут
def execute_query(query, params=None):
    """Выполняет SQL-запрос и возвращает результаты в виде DataFrame"""
    try:
        conn = get_db_connection()
        if conn is None:
            return None
        
        if params:
            df = pd.read_sql_query(query, conn, params=params)
        else:
            df = pd.read_sql_query(query, conn)
        
        conn.close()
        return df
    except Exception as e:
        st.error(f"Ошибка выполнения запроса: {e}")
        return None

def extract_coordinates(geo_str):
    """Извлекает координаты из строки формата 'Широта: X, Долгота: Y'"""
    if not isinstance(geo_str, str):
        return None, None
    
    lat_pattern = r'Широта:\s*([\d\.]+)'
    lng_pattern = r'Долгота:\s*([\d\.]+)'
    
    lat_match = re.search(lat_pattern, geo_str)
    lng_match = re.search(lng_pattern, geo_str)
    
    # Явное преобразование в float
    try:
        lat = float(lat_match.group(1)) if lat_match else None
        lng = float(lng_match.group(1)) if lng_match else None
    except (ValueError, TypeError) as e:
        print(f"Ошибка преобразования координат: {e}, исходная строка: {geo_str}")
        return None, None
    
    return lat, lng

def get_properties(limit=100, offset=0, max_size=None):
    """Получает список объектов недвижимости с пагинацией и фильтрацией по площади"""
    base_query = "SELECT * FROM properties"
    
    conditions = []
    params = []
    
    if max_size is not None:
        conditions.append("area <= ?")
        params.append(max_size)
    
    if conditions:
        base_query += " WHERE " + " AND ".join(conditions)
    
    query = base_query + " ORDER BY id LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    
    df = execute_query(query, params=tuple(params))
    
    # Получаем общее количество записей с учетом фильтра
    count_query = "SELECT COUNT(*) as total FROM properties"
    if conditions:
        count_query += " WHERE " + " AND ".join(conditions)
    
    count_df = execute_query(count_query, params=tuple(params[:-2]) if params[:-2] else None)
    total_count = count_df.iloc[0]['total'] if count_df is not None else 0
    
    return {
        "data": df,
        "total": total_count,
        "limit": limit,
        "offset": offset
    }

def get_cheapest_properties_by_area(top_n=3, min_size=None, max_size=None):
    """Получает самые недорогие объекты недвижимости по районам с фильтрацией по площади"""
    query = """
    WITH RankedProperties AS (
        SELECT 
            id, title, price, location, property_type, area, rooms, baths, geography,  -- Явно перечисляем нужные колонки
            ROW_NUMBER() OVER (PARTITION BY location ORDER BY price) as price_rank
        FROM properties
        WHERE 1=1 
    """
    
    params = []
    if min_size is not None:
        query += " AND area >= ?" # area здесь - это площадь из БД
        params.append(min_size)
    
    if max_size is not None:
        query += " AND area <= ?" # area здесь - это площадь из БД
        params.append(max_size)
    
    query += f"""
    )
    SELECT id, title, price, location, property_type, area, rooms, baths, geography, price_rank FROM RankedProperties  -- Явно перечисляем нужные колонки
    WHERE price_rank <= ?
    ORDER BY location, price_rank
    """
    params.append(top_n)
    
    df = execute_query(query, params=tuple(params))
    
    if df is not None and not df.empty:
        if 'rooms' in df.columns:
            df['bedrooms'] = df['rooms']
        if 'baths' in df.columns:
            df['bathrooms'] = df['baths']
        
        # 'area' в df - это числовая площадь из БД.
        # 'location' в df - это текстовый район.
        # Для карты нам нужен район для цветов и легенды, и площадь для информации.
        # Не будем переименовывать 'area' (площадь).
        # Для единообразия с тем, как карта ожидает район, будем использовать 'location' напрямую 
        # или создадим 'district' если это предпочтительнее для остального кода.
        # Пока оставим 'location' как есть, и 'area' как есть (числовая площадь).
        # Карта должна будет использовать 'location' для группировки по цветам/легенде.
    
    return df

def get_property(property_id):
    """Получает детальную информацию об объекте недвижимости по ID"""
    query = "SELECT id, title, price, location, property_type, area, rooms, baths, geography, property_url FROM properties WHERE id = ?"
    df = execute_query(query, params=(property_id,))
    
    if df is not None and not df.empty:
        property_dict = df.iloc[0].to_dict()
        
        if 'geography' in property_dict and property_dict['geography']:
            lat, lng = extract_coordinates(property_dict['geography'])
            property_dict['latitude'] = lat
            property_dict['longitude'] = lng
        
        if 'rooms' in property_dict:
            property_dict['bedrooms'] = property_dict['rooms']
        if 'baths' in property_dict:
            property_dict['bathrooms'] = property_dict['baths']
        
        # 'area' в property_dict - это числовая площадь из БД.
        # 'location' в property_dict - это текстовый район.
        # Не перезаписываем 'area'.
        
        return property_dict
    
    return None

def get_avg_price_by_area():
    """Получает среднюю цену по районам"""
    query = """
    SELECT location as area, AVG(price) as avg_price, COUNT(*) as count
    FROM properties
    GROUP BY location
    ORDER BY avg_price DESC
    """
    return execute_query(query)

def get_count_by_property_type():
    """Получает количество объектов по типу недвижимости"""
    query = """
    SELECT property_type, COUNT(*) as count
    FROM properties
    GROUP BY property_type
    ORDER BY count DESC
    """
    return execute_query(query)

def get_map_data(max_size=None):
    """Получает данные для отображения на карте с фильтрацией по площади"""
    query = """
    SELECT id, title, price, location as area, property_type, area as size, rooms as bedrooms, baths as bathrooms, 
           geography
    FROM properties
    WHERE geography IS NOT NULL
    """
    
    params = []
    if max_size is not None:
        query += " AND area <= ?"
        params.append(max_size)
    
    df = execute_query(query, params=tuple(params) if params else None)
    
    # Преобразуем строки с координатами в нужный формат
    if df is not None and not df.empty:
        # Применяем функцию к столбцу geography
        df['latitude'] = None
        df['longitude'] = None
        
        for idx, row in df.iterrows():
            if isinstance(row['geography'], str):
                lat, lng = extract_coordinates(row['geography'])
                df.at[idx, 'latitude'] = lat
                df.at[idx, 'longitude'] = lng
        
        # Добавляем стандартные имена колонок для совместимости
        df['lat'] = df['latitude']
        df['lon'] = df['longitude']
        
        # Удаляем строки без координат
        df = df.dropna(subset=['latitude', 'longitude'])
        
        # Добавляем отладочную информацию
        if not df.empty:
            print(f"Пример данных после извлечения координат: {df.head(3)[['geography', 'latitude', 'longitude']].to_dict('records')}")
    
    return df

def generate_area_colors(areas):
    """Создает цвета для каждого района из предопределенного списка Folium."""
    folium_supported_colors = [
        'red', 'blue', 'green', 'purple', 'orange', 'darkred', 'lightred', 'beige', 'darkblue', 'darkgreen',
        'cadetblue', 'darkpurple', 'pink', 'lightblue', 'lightgreen', 'gray', 'black', 'lightgray'
        # 'white' плохо видно на карте, поэтому исключен для маркеров
    ]
    unique_areas = sorted(list(set(areas))) # Сортируем для стабильного назначения цветов
    num_areas = len(unique_areas)
    num_colors = len(folium_supported_colors)
    
    colors = {}
    for i, area in enumerate(unique_areas):
        # Циклически используем доступные цвета, если районов больше, чем цветов
        colors[area] = folium_supported_colors[i % num_colors]
    
    return colors

def get_folium_color(hex_color_or_name):
    """Возвращает HEX-цвет или имя цвета для Folium Icon.
       Теперь эта функция не нужна, так как generate_area_colors возвращает имена.
       Оставляем на случай, если где-то еще используется, но лучше удалить.
    """
    # Просто возвращаем как есть, предполагая, что это уже имя цвета
    return hex_color_or_name

def main():
    st.title("Анализ недвижимости в Дубае")

    # Проверка наличия и доступности базы данных
    if not os.path.isfile(SQLITE_DB_PATH):
        st.error(f"Файл базы данных не найден: {SQLITE_DB_PATH}")
        st.info("Проверьте, что файл базы данных загружен в репозиторий и находится в корневой директории проекта.")
        return

    # --- Компактные фильтры сверху ---
    with st.container():
        col1, col2 = st.columns([2, 1])
        with col1:
            min_area_val, max_area_val = st.slider(
                "Площадь (кв.м.)",
                min_value=10,
                max_value=400,
                value=(40, 150),
                step=5,
                key="area_slider"
            )
        with col2:
            top_n = st.selectbox(
                "Топ самых недорогих квартир по региону",
                [3, 5, 10, 15, 20],
                index=1,
                key="top_n_select"
            )

    st.header(f"Топ-{top_n} недорогих объектов по районам (площадь от {min_area_val} до {max_area_val} кв.м.)")

    # Получаем данные "топ N по районам с фильтром площади"
    map_data_df = get_cheapest_properties_by_area(top_n=top_n, min_size=min_area_val, max_size=max_area_val)

    if map_data_df is not None and not map_data_df.empty:
        if 'latitude' not in map_data_df.columns or 'longitude' not in map_data_df.columns:
            coords = map_data_df['geography'].apply(lambda x: pd.Series(extract_coordinates(x), index=['latitude', 'longitude']))
            map_data_df = pd.concat([map_data_df, coords], axis=1)

        valid_coords_df = map_data_df[map_data_df['latitude'].notna() & map_data_df['longitude'].notna()].copy()
        valid_coords_df['lat'] = pd.to_numeric(valid_coords_df['latitude'], errors='coerce')
        valid_coords_df['lon'] = pd.to_numeric(valid_coords_df['longitude'], errors='coerce')
        valid_coords_df.dropna(subset=['lat', 'lon'], inplace=True)

        if not valid_coords_df.empty:
            map_center_lat = valid_coords_df['lat'].mean()
            map_center_lon = valid_coords_df['lon'].mean()
            dubai_map = folium.Map(location=[map_center_lat, map_center_lon], zoom_start=10)
            marker_cluster = folium.plugins.MarkerCluster().add_to(dubai_map)

            if 'location' in valid_coords_df.columns:
                area_colors = generate_area_colors(valid_coords_df['location'])
            else:
                area_colors = {}

            for _, row in valid_coords_df.iterrows():
                try:
                    price_str = f"{row.get('price', 'Н/Д'):,} AED" if pd.notna(row.get('price')) else "Цена Н/Д"
                    size_val = row.get('area')
                    size_str = f"{size_val} кв.м." if pd.notna(size_val) else "Площадь Н/Д"
                    tooltip_text = f"{price_str} - {size_str}"
                    area_name = row.get('location', 'Неизвестно')
                    marker_color_name = area_colors.get(area_name, 'blue')
                    property_url = row.get('property_url') if 'property_url' in row else None
                    # Формируем ссылку только если она есть
                    if property_url and isinstance(property_url, str) and property_url.strip():
                        more_link = f'<a href="{property_url}" target="_blank" rel="noopener noreferrer">Подробнее</a>'
                    else:
                        more_link = ''
                    popup_html = (
                        f"<b>{row.get('title', 'Без названия')}</b><br>"
                        f"Цена: {price_str}<br>"
                        f"Район: {area_name}<br>"
                        f"Площадь: {size_str}<br>"
                        f"Тип: {row.get('property_type', 'Н/Д')}<br>"
                        f"Спальни: {row.get('bedrooms', 'Н/Д')}<br>"
                        f"Ванные: {row.get('bathrooms', 'Н/Д')}<br>"
                        f"{more_link}"
                    )
                    folium.Marker(
                        location=[row['lat'], row['lon']],
                        popup=folium.Popup(popup_html, max_width=300),
                        tooltip=tooltip_text,
                        icon=folium.Icon(color=marker_color_name, icon='home', prefix='fa')
                    ).add_to(marker_cluster)
                except Exception:
                    pass
            folium_static(dubai_map, width=None, height=600)
        else:
            st.warning("Нет объектов с координатами для отображения на карте.")
    else:
        st.warning("Нет данных для отображения.")

    # Проверяем, есть ли запрос на просмотр детальной информации (оставляем, если полезно)
    query_params = st.query_params
    property_id_query = query_params.get("property_id", None) # Изменил имя переменной во избежание конфликта
    
    if property_id_query:
        property_details = get_property(property_id_query) 
        if property_details:
            st.subheader(f"Детали объекта: {property_details.get('title', 'Без названия')}")
            formatted_data = {
                "ID Объекта": property_details.get('id', 'Н/Д'), # Добавляем ID
                "Название": property_details.get('title', 'Без названия'),
                "Цена": f"{property_details.get('price', 'Н/Д')} AED",
                "Район": property_details.get('location', 'Н/Д'), 
                "Тип": property_details.get('property_type', 'Н/Д'),
                "Площадь": f"{property_details.get('area')} кв.м." if pd.notna(property_details.get('area')) else "Площадь Н/Д", 
                "Спальни": property_details.get('bedrooms', 'Н/Д'),
                "Ванные": property_details.get('bathrooms', 'Н/Д'),
                "Ссылка на источник": property_details.get('property_url', 'Н/Д') # Добавляем ссылку
                # Поля "Дата обновления" нужно будет добавить сюда,
                # если они есть в property_details (т.е. в get_property и в БД)
            }
            col1, col2 = st.columns(2)
            with col1:
                for key, value in formatted_data.items():
                    # Для id и ссылки можно сделать специальное отображение, если нужно
                    if key == "Ссылка на источник" and value and value != 'Н/Д':
                        st.markdown(f"**{key}:** <a href='{value}' target='_blank'>{value}</a>", unsafe_allow_html=True)
                    elif key == "ID Объекта":
                        st.write(f"**{key}:** {value}") # Просто выводим ID
                    elif key != 'id': # Старое условие, которое теперь не совсем актуально, но не мешает
                        st.write(f"**{key}:** {value}")
            with col2:
                 # ... (можно добавить карту для одного объекта или график сравнения цены)
                 pass # пока пусто

if __name__ == "__main__":
    main() 