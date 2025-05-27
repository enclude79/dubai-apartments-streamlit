import streamlit as st
import pandas as pd
import sqlite3
import folium
from streamlit_folium import folium_static
import plotly.express as px
import plotly.graph_objects as go
import os
from datetime import datetime

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

def get_properties(limit=100, offset=0):
    """Получает список объектов недвижимости с пагинацией"""
    query = "SELECT * FROM properties ORDER BY id LIMIT ? OFFSET ?"
    df = execute_query(query, params=(limit, offset))
    
    # Получаем общее количество записей
    count_df = execute_query("SELECT COUNT(*) as total FROM properties")
    total_count = count_df.iloc[0]['total'] if count_df is not None else 0
    
    return {
        "data": df,
        "total": total_count,
        "limit": limit,
        "offset": offset
    }

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

def get_map_data():
    """Получает данные для отображения на карте"""
    query = """
    SELECT id, title, price, area, property_type, latitude, longitude 
    FROM properties
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL
    """
    return execute_query(query)

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
    
    # Боковая панель
    st.sidebar.header("Параметры")
    
    # Главные вкладки
    tab1, tab2, tab3 = st.tabs(["Карта", "Статистика", "Список объектов"])
    
    with tab1:
        st.header("Карта объектов недвижимости")
        
        # Получаем данные для карты
        map_data_df = get_map_data()
        
        if map_data_df is not None and not map_data_df.empty:
            # Создаем карту с кластеризацией
            dubai_map = folium.Map(location=[25.2048, 55.2708], zoom_start=11)
            
            # Добавляем маркеры
            for _, row in map_data_df.iterrows():
                if pd.notna(row['latitude']) and pd.notna(row['longitude']):
                    # Форматируем всплывающее окно
                    popup_text = f"""
                    <b>{row.get('title', 'Без названия')}</b><br>
                    Цена: {row.get('price', 'Н/Д')} AED<br>
                    Район: {row.get('area', 'Н/Д')}<br>
                    Тип: {row.get('property_type', 'Н/Д')}<br>
                    <a href="?property_id={row.get('id')}" target="_blank">Подробнее</a>
                    """
                    
                    folium.Marker(
                        location=[row['latitude'], row['longitude']],
                        popup=folium.Popup(popup_text, max_width=300),
                        tooltip=f"{row.get('title', 'Объект')} - {row.get('price', 'Н/Д')} AED",
                        icon=folium.Icon(icon="home", prefix="fa")
                    ).add_to(dubai_map)
            
            # Отображаем карту
            folium_static(dubai_map, width=1200, height=600)
        else:
            st.warning("Не удалось загрузить данные для карты")
    
    with tab2:
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
    
    with tab3:
        st.header("Список объектов недвижимости")
        
        # Параметры пагинации
        page = st.sidebar.number_input("Страница", min_value=1, value=1)
        limit = st.sidebar.selectbox("Объектов на странице", [10, 25, 50, 100], index=1)
        offset = (page - 1) * limit
        
        # Получаем список объектов
        properties_result = get_properties(limit=limit, offset=offset)
        
        if properties_result and 'data' in properties_result and properties_result['data'] is not None:
            # Отображаем таблицу
            df_properties = properties_result['data']
            
            if not df_properties.empty:
                # Выбираем только нужные колонки для отображения
                display_columns = ['id', 'title', 'price', 'area', 'property_type', 'bedrooms', 'bathrooms']
                df_display = df_properties[display_columns] if all(col in df_properties.columns for col in display_columns) else df_properties
                
                st.dataframe(df_display, use_container_width=True)
                
                # Информация о пагинации
                total_pages = (properties_result['total'] // limit) + (1 if properties_result['total'] % limit > 0 else 0)
                st.info(f"Показано {len(df_properties)} из {properties_result['total']} объектов. Страница {page} из {total_pages}")
            else:
                st.warning("Нет данных для отображения")
        else:
            st.warning("Не удалось загрузить список объектов")

    # Проверяем, есть ли запрос на просмотр детальной информации
    query_params = st.experimental_get_query_params()
    property_id = query_params.get("property_id", [None])[0]
    
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