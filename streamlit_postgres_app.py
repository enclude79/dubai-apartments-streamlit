import streamlit as st
import pandas as pd
import psycopg2
import psycopg2.extras
import folium
from streamlit_folium import folium_static
import plotly.express as px
import plotly.graph_objects as go
import os
from datetime import datetime
import random
import numpy as np
import colorsys
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Параметры подключения к PostgreSQL
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASSWORD', 'Enclude79'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432')
}

st.set_page_config(
    page_title="Dubai Property Analysis",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Функция для получения соединения с PostgreSQL
def get_db_connection():
    """Создает соединение с базой данных PostgreSQL"""
    try:
        # Выводим информацию о подключении для диагностики (без пароля)
        connection_info = {
            'dbname': DB_CONFIG['dbname'],
            'user': DB_CONFIG['user'],
            'host': DB_CONFIG['host'],
            'port': DB_CONFIG['port']
        }
        st.write(f"Попытка подключения к БД: {connection_info}")
        
        conn = psycopg2.connect(
            dbname=DB_CONFIG['dbname'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            connect_timeout=10  # Добавляем таймаут
        )
        st.success("Подключение к базе данных успешно установлено!")
        return conn
    except Exception as e:
        st.error(f"Ошибка подключения к базе данных PostgreSQL: {e}")
        st.write("Проверьте следующие моменты:")
        st.write("1. База данных запущена и доступна по указанному адресу")
        st.write("2. Пользователь и пароль указаны верно")
        st.write("3. База данных и таблица bayut_properties существуют")
        st.write(f"4. Подключение возможно с текущего компьютера (для удаленной БД)")
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
    base_query = "SELECT * FROM bayut_properties"
    
    conditions = []
    params = []
    
    if max_size is not None:
        conditions.append("size <= %s")
        params.append(max_size)
    
    if conditions:
        base_query += " WHERE " + " AND ".join(conditions)
    
    query = base_query + " ORDER BY id LIMIT %s OFFSET %s"
    params.extend([limit, offset])
    
    df = execute_query(query, params=tuple(params))
    
    # Получаем общее количество записей с учетом фильтра
    count_query = "SELECT COUNT(*) as total FROM bayut_properties"
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
        FROM bayut_properties
        WHERE 1=1
    """
    
    params = []
    if max_size is not None:
        query += " AND size <= %s"
        params.append(max_size)
    
    query += f"""
    )
    SELECT * FROM RankedProperties
    WHERE price_rank <= %s
    ORDER BY area, price_rank
    """
    params.append(top_n)
    
    return execute_query(query, params=tuple(params))

def get_property(property_id):
    """Получает детальную информацию об объекте недвижимости по ID"""
    query = "SELECT * FROM bayut_properties WHERE id = %s"
    df = execute_query(query, params=(property_id,))
    
    if df is not None and not df.empty:
        return df.iloc[0].to_dict()
    
    return None

def get_avg_price_by_area():
    """Получает среднюю цену по районам"""
    query = """
    SELECT area, AVG(price) as avg_price, COUNT(*) as count
    FROM bayut_properties
    GROUP BY area
    ORDER BY avg_price DESC
    """
    return execute_query(query)

def get_count_by_property_type():
    """Получает количество объектов по типу недвижимости"""
    query = """
    SELECT property_type, COUNT(*) as count
    FROM bayut_properties
    GROUP BY property_type
    ORDER BY count DESC
    """
    return execute_query(query)

def get_map_data(max_size=None):
    """Получает данные для отображения на карте с фильтрацией по площади"""
    query = """
    SELECT id, title, price, area, property_type, size, bedrooms, bathrooms, latitude, longitude 
    FROM bayut_properties
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL
    """
    
    params = []
    if max_size is not None:
        query += " AND size <= %s"
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
    
    # Проверка подключения к базе данных
    try:
        conn = get_db_connection()
        if conn is None:
            st.error("Не удалось подключиться к базе данных PostgreSQL.")
            st.info("Проверьте настройки подключения в файле .env или значения по умолчанию в коде.")
            return
            
        # Проверяем структуру базы данных
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM bayut_properties")
            count = cursor.fetchone()[0]
            st.sidebar.info(f"База данных: PostgreSQL\nКоличество объектов: {count:,}")
            cursor.close()
            conn.close()
        except Exception as e:
            st.error(f"Ошибка при проверке таблицы 'bayut_properties': {e}")
            st.info("Проверьте, что таблица bayut_properties существует в базе данных.")
            return
    except Exception as e:
        st.error(f"Ошибка при подключении к базе данных: {e}")
        return
    
    # Добавляем фильтр по размеру
    max_size = st.sidebar.slider(
        "Максимальная площадь (кв.м)",
        min_value=0,
        max_value=1000,
        value=500,
        step=50
    )
    
    # Создаем вкладки
    tab1, tab2, tab3, tab4 = st.tabs(["Обзор", "Карта", "Детали", "Сравнение"])
    
    with tab1:
        st.header("Обзор рынка недвижимости")
        
        # Получаем данные по типам недвижимости
        property_types_df = get_count_by_property_type()
        if property_types_df is not None and not property_types_df.empty:
            # Горизонтальный бар-чарт для типов недвижимости
            fig = px.bar(
                property_types_df,
                y='property_type',
                x='count',
                title='Распределение по типам недвижимости',
                labels={'count': 'Количество объектов', 'property_type': 'Тип недвижимости'},
                orientation='h',
                color='count',
                color_continuous_scale='Viridis'
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        # Получаем среднюю цену по районам
        price_by_area_df = get_avg_price_by_area()
        if price_by_area_df is not None and not price_by_area_df.empty:
            # Фильтруем и берем топ 15 районов по количеству объектов
            top_areas = price_by_area_df.sort_values('count', ascending=False).head(15)
            
            # Бар-чарт для средней цены по районам
            fig = px.bar(
                top_areas,
                x='area',
                y='avg_price',
                title='Средняя цена по районам (топ 15 по количеству объектов)',
                labels={'avg_price': 'Средняя цена (AED)', 'area': 'Район', 'count': 'Количество объектов'},
                color='count',
                color_continuous_scale='Viridis',
                text_auto='.2s'
            )
            fig.update_traces(texttemplate='%{y:,.0f}', textposition='outside')
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
        
        # Получаем самые дешевые объекты по районам
        cheapest_by_area_df = get_cheapest_properties_by_area(top_n=1, max_size=max_size)
        if cheapest_by_area_df is not None and not cheapest_by_area_df.empty:
            # Фильтруем и берем топ 10 районов
            top_cheapest = cheapest_by_area_df.sort_values('price').head(10)
            
            st.subheader("Самые дешевые предложения по районам")
            cols = st.columns(2)
            
            with cols[0]:
                # Таблица с самыми дешевыми предложениями
                st.dataframe(
                    top_cheapest[['area', 'title', 'price', 'size', 'bedrooms']],
                    column_config={
                        'area': 'Район',
                        'title': 'Название',
                        'price': st.column_config.NumberColumn('Цена (AED)', format="%.0f"),
                        'size': st.column_config.NumberColumn('Площадь (кв.м)', format="%.1f"),
                        'bedrooms': 'Спальни'
                    },
                    use_container_width=True
                )
            
            with cols[1]:
                # Бар-чарт для самых дешевых предложений
                fig = px.bar(
                    top_cheapest,
                    x='area',
                    y='price',
                    title='Самые дешевые предложения по районам',
                    labels={'price': 'Цена (AED)', 'area': 'Район'},
                    color='bedrooms',
                    text_auto='.2s'
                )
                fig.update_traces(texttemplate='%{y:,.0f}', textposition='outside')
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        st.header("Карта объектов недвижимости")
        
        # Получаем данные для карты
        map_data = get_map_data(max_size=max_size)
        if map_data is not None and not map_data.empty:
            # Фильтруем строки с действительными координатами
            map_data = map_data.dropna(subset=['latitude', 'longitude'])
            
            if not map_data.empty:
                # Генерируем цвета для районов
                area_colors = generate_area_colors(map_data['area'])
                
                # Создаем карту
                m = folium.Map(location=[25.2048, 55.2708], zoom_start=11)
                
                # Добавляем маркеры
                for idx, row in map_data.iterrows():
                    # Цвет маркера по району
                    color = area_colors.get(row['area'], '#3388ff')
                    
                    # Формируем всплывающую подсказку
                    popup_html = f"""
                    <div style="width: 200px">
                        <h4>{row['title']}</h4>
                        <p><b>Цена:</b> {row['price']:,.0f} AED</p>
                        <p><b>Район:</b> {row['area']}</p>
                        <p><b>Тип:</b> {row['property_type']}</p>
                        <p><b>Площадь:</b> {row['size']:,.1f} кв.м</p>
                        <p><b>Спальни:</b> {row['bedrooms']}</p>
                        <p><b>Санузлы:</b> {row['bathrooms']}</p>
                    </div>
                    """
                    
                    folium.Marker(
                        [row['latitude'], row['longitude']],
                        popup=folium.Popup(popup_html, max_width=300),
                        tooltip=f"{row['title']} ({row['price']:,.0f} AED)",
                        icon=folium.Icon(color='blue', icon='home', prefix='fa')
                    ).add_to(m)
                
                # Добавляем легенду
                legend_html = """
                <div style="position: fixed; bottom: 50px; left: 50px; z-index: 1000; background-color: white; padding: 10px; border: 2px solid grey; border-radius: 5px;">
                <h4>Районы</h4>
                """
                
                # Добавляем 10 наиболее распространенных районов
                top_areas = map_data['area'].value_counts().head(10).index.tolist()
                for area in top_areas:
                    color = area_colors.get(area, '#3388ff')
                    legend_html += f'<div><span style="background-color:{color}; width:15px; height:15px; display:inline-block; margin-right:5px;"></span>{area}</div>'
                
                legend_html += "</div>"
                
                m.get_root().html.add_child(folium.Element(legend_html))
                
                # Отображаем карту
                folium_static(m, width=1000, height=600)
            else:
                st.warning("Нет объектов с действительными координатами для отображения на карте.")
        else:
            st.error("Не удалось получить данные для карты.")
    
    with tab3:
        st.header("Детальная информация")
        
        # Пагинация для списка объектов
        properties_per_page = 10
        page = st.number_input("Страница", min_value=1, value=1, step=1)
        offset = (page - 1) * properties_per_page
        
        # Получаем список объектов
        properties_result = get_properties(limit=properties_per_page, offset=offset, max_size=max_size)
        
        if properties_result is not None and properties_result["data"] is not None:
            properties_df = properties_result["data"]
            total_properties = properties_result["total"]
            
            # Отображаем информацию о пагинации
            total_pages = (total_properties + properties_per_page - 1) // properties_per_page
            st.write(f"Показаны записи {offset+1}-{min(offset+properties_per_page, total_properties)} из {total_properties} (Страница {page} из {total_pages})")
            
            # Отображаем список объектов
            if not properties_df.empty:
                # Выбираем нужные колонки для списка
                list_columns = ['id', 'title', 'price', 'area', 'size', 'bedrooms', 'bathrooms']
                
                # Создаем интерактивную таблицу
                st.dataframe(
                    properties_df[list_columns],
                    column_config={
                        'id': 'ID',
                        'title': 'Название',
                        'price': st.column_config.NumberColumn('Цена (AED)', format="%.0f"),
                        'area': 'Район',
                        'size': st.column_config.NumberColumn('Площадь (кв.м)', format="%.1f"),
                        'bedrooms': 'Спальни',
                        'bathrooms': 'Санузлы'
                    },
                    use_container_width=True
                )
                
                # Выбор объекта для детальной информации
                selected_id = st.selectbox(
                    "Выберите объект для просмотра деталей:",
                    options=properties_df['id'].tolist(),
                    format_func=lambda x: f"ID: {x} - {properties_df[properties_df['id'] == x]['title'].values[0]}"
                )
                
                if selected_id:
                    # Получаем детальную информацию об объекте
                    property_details = get_property(selected_id)
                    
                    if property_details:
                        st.subheader(property_details['title'])
                        
                        # Разделяем информацию на колонки
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.write(f"**Цена:** {property_details['price']:,.0f} AED")
                            st.write(f"**Район:** {property_details.get('area', 'Н/Д')}")
                            st.write(f"**Тип недвижимости:** {property_details.get('property_type', 'Н/Д')}")
                            st.write(f"**Площадь:** {property_details.get('size', 'Н/Д'):,.1f} кв.м")
                            st.write(f"**Спальни:** {property_details.get('bedrooms', 'Н/Д')}")
                            st.write(f"**Санузлы:** {property_details.get('bathrooms', 'Н/Д')}")
                            
                            # Если есть координаты, добавляем мини-карту
                            if property_details.get('latitude') and property_details.get('longitude'):
                                mini_map = folium.Map(
                                    location=[property_details['latitude'], property_details['longitude']], 
                                    zoom_start=15
                                )
                                folium.Marker(
                                    [property_details['latitude'], property_details['longitude']],
                                    tooltip=property_details['title'],
                                    icon=folium.Icon(color='red', icon='home', prefix='fa')
                                ).add_to(mini_map)
                                st.write("**Расположение:**")
                                folium_static(mini_map, width=400, height=300)
                        
                        with col2:
                            # Дополнительная информация
                            st.write(f"**Статус:** {property_details.get('status', 'Н/Д')}")
                            st.write(f"**Год постройки:** {property_details.get('year_built', 'Н/Д')}")
                            st.write(f"**Девелопер:** {property_details.get('developer', 'Н/Д')}")
                            st.write(f"**Дата обновления:** {property_details.get('updated_at', 'Н/Д')}")
                            
                            # Описание объекта
                            if property_details.get('description'):
                                st.write("**Описание:**")
                                st.write(property_details['description'])
                    else:
                        st.warning(f"Не удалось получить детальную информацию для объекта с ID {selected_id}")
            else:
                st.warning("Нет объектов, соответствующих выбранным критериям.")
        else:
            st.error("Не удалось получить список объектов.")
    
    with tab4:
        st.header("Сравнительный анализ")
        
        # Получаем данные для сравнения
        properties_all = get_properties(limit=1000, max_size=max_size)
        
        if properties_all is not None and properties_all["data"] is not None:
            df = properties_all["data"]
            
            if not df.empty:
                # Анализ по району и типу недвижимости
                st.subheader("Сравнение цен по районам и типам недвижимости")
                
                # Получаем уникальные районы и типы недвижимости
                areas = df['area'].dropna().unique()
                property_types = df['property_type'].dropna().unique()
                
                # Выбор районов для сравнения
                selected_areas = st.multiselect(
                    "Выберите районы для сравнения:",
                    options=areas,
                    default=list(areas)[:5] if len(areas) > 5 else list(areas)
                )
                
                # Выбор типов недвижимости для сравнения
                selected_types = st.multiselect(
                    "Выберите типы недвижимости для сравнения:",
                    options=property_types,
                    default=list(property_types)[:3] if len(property_types) > 3 else list(property_types)
                )
                
                if selected_areas and selected_types:
                    # Фильтруем данные
                    filtered_df = df[(df['area'].isin(selected_areas)) & (df['property_type'].isin(selected_types))]
                    
                    if not filtered_df.empty:
                        # Создаем сводную таблицу
                        pivot_df = filtered_df.pivot_table(
                            values='price',
                            index='area',
                            columns='property_type',
                            aggfunc='mean'
                        )
                        
                        # Отображаем сводную таблицу
                        st.write("Средняя цена по районам и типам недвижимости (AED):")
                        st.dataframe(
                            pivot_df.style.format("{:,.0f}"),
                            use_container_width=True
                        )
                        
                        # Строим графики
                        fig = px.bar(
                            filtered_df,
                            x='area',
                            y='price',
                            color='property_type',
                            barmode='group',
                            title='Сравнение средних цен по районам и типам недвижимости',
                            labels={'price': 'Средняя цена (AED)', 'area': 'Район', 'property_type': 'Тип недвижимости'},
                            height=500
                        )
                        fig.update_layout(xaxis={'categoryorder': 'total descending'})
                        st.plotly_chart(fig, use_container_width=True)
                        
                        # Сравнение цены за квадратный метр
                        filtered_df['price_per_sqm'] = filtered_df['price'] / filtered_df['size']
                        
                        fig2 = px.box(
                            filtered_df,
                            x='area',
                            y='price_per_sqm',
                            color='property_type',
                            title='Распределение цен за квадратный метр по районам и типам недвижимости',
                            labels={'price_per_sqm': 'Цена за кв.м (AED)', 'area': 'Район', 'property_type': 'Тип недвижимости'},
                            height=500
                        )
                        fig2.update_layout(xaxis={'categoryorder': 'total descending'})
                        st.plotly_chart(fig2, use_container_width=True)
                    else:
                        st.warning("Нет данных для выбранных районов и типов недвижимости.")
                else:
                    st.info("Пожалуйста, выберите районы и типы недвижимости для сравнения.")
            else:
                st.warning("Нет данных для анализа.")
        else:
            st.error("Не удалось получить данные для сравнительного анализа.")

if __name__ == "__main__":
    main() 