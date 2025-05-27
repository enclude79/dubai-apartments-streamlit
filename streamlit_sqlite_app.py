import streamlit as st
import pandas as pd
import sqlite3
import folium
from streamlit_folium import folium_static
import plotly.express as px
import plotly.graph_objects as go
import os
from datetime import datetime
import random
import numpy as np
import colorsys

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

def get_properties(limit=100, offset=0, max_size=None):
    """Получает список объектов недвижимости с пагинацией и фильтрацией по площади"""
    base_query = "SELECT * FROM properties"
    
    conditions = []
    params = []
    
    if max_size is not None:
        conditions.append("size <= ?")
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

def get_cheapest_properties_by_area(top_n=3, max_size=None):
    """Получает самые недорогие объекты недвижимости по районам с фильтрацией по площади"""
    query = """
    WITH RankedProperties AS (
        SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY area ORDER BY price) as price_rank
        FROM properties
        WHERE 1=1
    """
    
    params = []
    if max_size is not None:
        query += " AND size <= ?"
        params.append(max_size)
    
    query += f"""
    )
    SELECT * FROM RankedProperties
    WHERE price_rank <= ?
    ORDER BY area, price_rank
    """
    params.append(top_n)
    
    return execute_query(query, params=tuple(params))

def get_property(property_id):
    """Получает детальную информацию об объекте недвижимости по ID"""
    query = "SELECT * FROM properties WHERE id = ?"
    df = execute_query(query, params=(property_id,))
    
    if df is not None and not df.empty:
        return df.iloc[0].to_dict()
    
    return None

def get_avg_price_by_area():
    """Получает среднюю цену по районам"""
    query = """
    SELECT area, AVG(price) as avg_price, COUNT(*) as count
    FROM properties
    GROUP BY area
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
    SELECT id, title, price, area, property_type, size, bedrooms, bathrooms, latitude, longitude 
    FROM properties
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL
    """
    
    params = []
    if max_size is not None:
        query += " AND size <= ?"
        params.append(max_size)
    
    return execute_query(query, params=tuple(params) if params else None)

def generate_area_colors(areas):
    """Создает уникальные цвета для каждого района"""
    unique_areas = list(set(areas))
    num_areas = len(unique_areas)
    
    # Генерируем равномерно распределенные цвета по HSV-кругу
    colors = {}
    for i, area in enumerate(unique_areas):
        hue = i / num_areas
        # Преобразуем HSV в RGB, затем в hex
        r, g, b = colorsys.hsv_to_rgb(hue, 0.8, 0.9)
        color = f'#{int(r*255):02x}{int(g*255):02x}{int(b*255):02x}'
        colors[area] = color
    
    return colors

def main():
    st.title("Анализ недвижимости в Дубае")
    
    # Отображаем информацию о базе данных
    db_exists = os.path.isfile(SQLITE_DB_PATH)
    if db_exists:
        db_size = os.path.getsize(SQLITE_DB_PATH) / (1024 * 1024)  # Размер в МБ
        db_modified = datetime.fromtimestamp(os.path.getmtime(SQLITE_DB_PATH))
        st.sidebar.info(
            f"База данных: {SQLITE_DB_PATH}\n"
            f"Размер: {db_size:.2f} МБ\n"
            f"Последнее обновление: {db_modified.strftime('%d.%m.%Y %H:%M')}"
        )
    else:
        st.sidebar.error(f"База данных {SQLITE_DB_PATH} не найдена!")
        st.error(f"База данных {SQLITE_DB_PATH} не найдена! Убедитесь, что файл базы данных находится в той же директории, что и приложение.")
        return
    
    # Боковая панель с фильтрами
    st.sidebar.header("Параметры")
    
    # Фильтр по площади объектов
    max_size = st.sidebar.slider("Максимальная площадь (кв.м.)", 20, 500, 40, 5)
    
    # Выбор количества недорогих квартир для отображения
    top_n = st.sidebar.selectbox("Топ самых недорогих квартир по региону", [3, 5, 10], index=0)
    
    # Главные вкладки
    tab1, tab2, tab3, tab4 = st.tabs(["Карта", "Недорогие по районам", "Статистика", "Список объектов"])
    
    with tab1:
        st.header("Карта объектов недвижимости")
        st.caption(f"Объекты площадью до {max_size} кв.м.")
        
        # Получаем данные для карты
        map_data_df = get_map_data(max_size=max_size)
        
        if map_data_df is not None and not map_data_df.empty:
            # Генерируем цвета для районов
            area_colors = generate_area_colors(map_data_df['area'])
            
            # Создаем карту
            dubai_map = folium.Map(location=[25.2048, 55.2708], zoom_start=11)
            
            # Добавляем кластеризацию
            marker_cluster = folium.plugins.MarkerCluster().add_to(dubai_map)
            
            # Добавляем маркеры
            for _, row in map_data_df.iterrows():
                if pd.notna(row['latitude']) and pd.notna(row['longitude']):
                    # Получаем цвет для района
                    area = row.get('area', 'Неизвестно')
                    color = area_colors.get(area, '#3186cc')  # Если район не найден, используем цвет по умолчанию
                    
                    # Создаем иконку с нужным цветом
                    icon = folium.Icon(icon="home", prefix="fa", color=color.lstrip('#'))
                    
                    # Форматируем всплывающее окно
                    popup_text = f"""
                    <b>{row.get('title', 'Без названия')}</b><br>
                    Цена: {row.get('price', 'Н/Д')} AED<br>
                    Район: {area}<br>
                    Тип: {row.get('property_type', 'Н/Д')}<br>
                    Площадь: {row.get('size', 'Н/Д')} кв.м.<br>
                    Спальни: {row.get('bedrooms', 'Н/Д')}<br>
                    Ванные: {row.get('bathrooms', 'Н/Д')}<br>
                    <a href="?property_id={row.get('id')}" target="_self">Подробнее</a>
                    """
                    
                    folium.Marker(
                        location=[row['latitude'], row['longitude']],
                        popup=folium.Popup(popup_text, max_width=300),
                        tooltip=f"{row.get('title', 'Объект')} - {row.get('price', 'Н/Д')} AED",
                        icon=icon
                    ).add_to(marker_cluster)
            
            # Добавляем легенду для цветов районов
            legend_html = """
            <div style="position: fixed; 
                        bottom: 50px; left: 50px; width: 250px; height: auto;
                        border:2px solid grey; z-index:9999; font-size:12px;
                        background-color:white; padding: 10px;
                        overflow-y: auto; max-height: 300px;">
                <div style="font-weight: bold; margin-bottom: 5px;">Районы:</div>
            """
            
            # Добавляем цвета для каждого района
            for area, color in area_colors.items():
                legend_html += f"""
                <div style="display: flex; align-items: center; margin-bottom: 3px;">
                    <div style="background-color: {color}; width: 15px; height: 15px; margin-right: 5px;"></div>
                    <div>{area}</div>
                </div>
                """
            
            legend_html += "</div>"
            dubai_map.get_root().html.add_child(folium.Element(legend_html))
            
            # Отображаем карту
            folium_static(dubai_map, width=1200, height=600)
            
            st.info(f"Отображено {len(map_data_df)} объектов недвижимости. Точки раскрашены по районам.")
        else:
            st.warning("Не удалось загрузить данные для карты")
    
    with tab2:
        st.header("Самые недорогие квартиры по районам")
        st.caption(f"Топ-{top_n} недорогих квартир площадью до {max_size} кв.м. в каждом районе")
        
        # Получаем самые недорогие объекты по районам
        cheapest_df = get_cheapest_properties_by_area(top_n=top_n, max_size=max_size)
        
        if cheapest_df is not None and not cheapest_df.empty:
            # Сортируем по району и цене
            cheapest_df = cheapest_df.sort_values(['area', 'price'])
            
            # Создаем график
            fig = px.bar(
                cheapest_df,
                x='area',
                y='price',
                color='area',
                hover_data=['title', 'size', 'bedrooms', 'bathrooms'],
                labels={
                    'area': 'Район',
                    'price': 'Цена (AED)',
                    'title': 'Название',
                    'size': 'Площадь (кв.м.)',
                    'bedrooms': 'Спальни',
                    'bathrooms': 'Ванные'
                },
                title=f'Топ-{top_n} недорогих квартир в каждом районе'
            )
            
            # Настраиваем макет
            fig.update_layout(xaxis_tickangle=-45)
            
            # Отображаем график
            st.plotly_chart(fig, use_container_width=True)
            
            # Отображаем таблицу с деталями
            st.subheader("Детали недорогих квартир")
            display_columns = ['title', 'area', 'price', 'size', 'bedrooms', 'bathrooms', 'property_type']
            st.dataframe(
                cheapest_df[display_columns],
                column_config={
                    "title": "Название",
                    "area": "Район",
                    "price": st.column_config.NumberColumn("Цена (AED)", format="%.0f"),
                    "size": st.column_config.NumberColumn("Площадь (кв.м.)", format="%.1f"),
                    "bedrooms": "Спальни",
                    "bathrooms": "Ванные",
                    "property_type": "Тип недвижимости"
                },
                use_container_width=True
            )
        else:
            st.warning("Не удалось загрузить данные о недорогих квартирах")
    
    with tab3:
        st.header("Статистика по недвижимости")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Средняя цена по районам
            avg_price_by_area_df = get_avg_price_by_area()
            
            if avg_price_by_area_df is not None and not avg_price_by_area_df.empty:
                df_area = avg_price_by_area_df.sort_values(by='avg_price', ascending=False).head(10)
                
                fig = px.bar(
                    df_area, 
                    x='area', 
                    y='avg_price',
                    color='avg_price',
                    labels={'area': 'Район', 'avg_price': 'Средняя цена (AED)'},
                    title='Топ-10 районов по средней цене'
                )
                
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("Не удалось загрузить статистику по районам")
        
        with col2:
            # Количество объектов по типу
            count_by_type_df = get_count_by_property_type()
            
            if count_by_type_df is not None and not count_by_type_df.empty:
                fig = px.pie(
                    count_by_type_df, 
                    values='count', 
                    names='property_type', 
                    title='Распределение по типам недвижимости'
                )
                
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("Не удалось загрузить статистику по типам недвижимости")
    
    with tab4:
        st.header("Список объектов недвижимости")
        st.caption(f"Объекты площадью до {max_size} кв.м.")
        
        # Параметры пагинации
        page = st.sidebar.number_input("Страница", min_value=1, value=1)
        limit = st.sidebar.selectbox("Объектов на странице", [10, 25, 50, 100], index=1)
        offset = (page - 1) * limit
        
        # Получаем список объектов с учетом фильтра по площади
        properties_result = get_properties(limit=limit, offset=offset, max_size=max_size)
        
        if properties_result and 'data' in properties_result and properties_result['data'] is not None:
            # Отображаем таблицу
            df_properties = properties_result['data']
            
            if not df_properties.empty:
                # Выбираем только нужные колонки для отображения
                display_columns = ['id', 'title', 'price', 'area', 'size', 'property_type', 'bedrooms', 'bathrooms']
                df_display = df_properties[display_columns] if all(col in df_properties.columns for col in display_columns) else df_properties
                
                st.dataframe(
                    df_display,
                    column_config={
                        "title": "Название",
                        "price": st.column_config.NumberColumn("Цена (AED)", format="%.0f"),
                        "area": "Район",
                        "size": st.column_config.NumberColumn("Площадь (кв.м.)", format="%.1f"),
                        "property_type": "Тип недвижимости",
                        "bedrooms": "Спальни",
                        "bathrooms": "Ванные"
                    },
                    use_container_width=True
                )
                
                # Информация о пагинации
                total_pages = (properties_result['total'] // limit) + (1 if properties_result['total'] % limit > 0 else 0)
                st.info(f"Показано {len(df_properties)} из {properties_result['total']} объектов. Страница {page} из {total_pages}")
            else:
                st.warning("Нет данных для отображения")
        else:
            st.warning("Не удалось загрузить список объектов")

    # Проверяем, есть ли запрос на просмотр детальной информации
    query_params = st.query_params
    property_id = query_params.get("property_id", None)
    
    if property_id:
        property_details = get_property(property_id)
        if property_details:
            st.subheader(f"Детали объекта: {property_details.get('title', 'Без названия')}")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.write(f"**Цена:** {property_details.get('price', 'Н/Д')} AED")
                st.write(f"**Район:** {property_details.get('area', 'Н/Д')}")
                st.write(f"**Тип недвижимости:** {property_details.get('property_type', 'Н/Д')}")
                st.write(f"**Спальни:** {property_details.get('bedrooms', 'Н/Д')}")
                st.write(f"**Ванные:** {property_details.get('bathrooms', 'Н/Д')}")
            
            with col2:
                st.write(f"**Площадь:** {property_details.get('size', 'Н/Д')} кв.м.")
                st.write(f"**Статус:** {property_details.get('status', 'Н/Д')}")
                
                # Если есть координаты, показываем маленькую карту
                if pd.notna(property_details.get('latitude')) and pd.notna(property_details.get('longitude')):
                    property_map = folium.Map(
                        location=[property_details['latitude'], property_details['longitude']], 
                        zoom_start=15
                    )
                    
                    folium.Marker(
                        location=[property_details['latitude'], property_details['longitude']],
                        popup=property_details.get('title', 'Объект'),
                        icon=folium.Icon(icon="home", prefix="fa")
                    ).add_to(property_map)
                    
                    folium_static(property_map, width=400, height=300)

if __name__ == "__main__":
    main() 