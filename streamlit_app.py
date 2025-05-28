import streamlit as st
import pandas as pd
import requests
import folium
from streamlit_folium import folium_static
import plotly.express as px
import plotly.graph_objects as go
import os
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

# Параметры API
API_URL = os.getenv("API_URL", "http://localhost:8000")

st.set_page_config(
    page_title="Dubai Property Analysis",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Функция для получения данных из API
@st.cache_data(ttl=300) # Кэшируем на 5 минут
def fetch_data(endpoint, params=None):
    try:
        response = requests.get(f"{API_URL}{endpoint}", params=params)
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"Ошибка получения данных: {response.status_code}")
            return None
    except Exception as e:
        st.error(f"Ошибка подключения к API: {e}")
        return None

def main():
    st.title("Анализ недвижимости в Дубае")
    
    # Боковая панель
    st.sidebar.header("Параметры")
    
    # Главные вкладки
    tab1, tab2, tab3 = st.tabs(["Карта", "Статистика", "Список объектов"])
    
    with tab1:
        st.header("Карта объектов недвижимости")
        
        # Получаем данные для карты
        map_data = fetch_data("/api/map_data")
        
        if map_data:
            # Создаем карту с кластеризацией
            dubai_map = folium.Map(location=[25.2048, 55.2708], zoom_start=11)
            
            # Добавляем маркеры
            for item in map_data:
                if item.get('latitude') and item.get('longitude'):
                    # Форматируем всплывающее окно
                    popup_text = f"""
                    <b>{item.get('title', 'Без названия')}</b><br>
                    Цена: {item.get('price', 'Н/Д')} AED<br>
                    Район: {item.get('area', 'Н/Д')}<br>
                    Тип: {item.get('property_type', 'Н/Д')}<br>
                    <a href="?property_id={item.get('id')}" target="_blank">Подробнее</a>
                    """
                    
                    folium.Marker(
                        location=[item['latitude'], item['longitude']],
                        popup=folium.Popup(popup_text, max_width=300),
                        tooltip=f"{item.get('title', 'Объект')} - {item.get('price', 'Н/Д')} AED",
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
            avg_price_by_area = fetch_data("/api/stats/avg_price_by_area")
            
            if avg_price_by_area:
                df_area = pd.DataFrame(avg_price_by_area)
                df_area = df_area.sort_values(by='avg_price', ascending=False).head(10)
                
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
            count_by_type = fetch_data("/api/stats/count_by_property_type")
            
            if count_by_type:
                df_type = pd.DataFrame(count_by_type)
                
                fig = px.pie(
                    df_type, 
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
        properties = fetch_data("/api/properties", params={"limit": limit, "offset": offset})
        
        if properties and 'data' in properties:
            # Отображаем таблицу
            df_properties = pd.DataFrame(properties['data'])
            
            if not df_properties.empty:
                # Выбираем только нужные колонки для отображения
                display_columns = ['id', 'title', 'price', 'area', 'property_type', 'bedrooms', 'bathrooms']
                df_display = df_properties[display_columns] if all(col in df_properties.columns for col in display_columns) else df_properties
                
                st.dataframe(df_display, use_container_width=True)
                
                # Информация о пагинации
                st.info(f"Показано {len(properties['data'])} из {properties['total']} объектов. Страница {page} из {(properties['total'] // limit) + 1}")
            else:
                st.warning("Нет данных для отображения")
        else:
            st.warning("Не удалось загрузить список объектов")

    # Проверяем, есть ли запрос на просмотр детальной информации
    property_id = st.query_params.get("property_id")
    if property_id:
        property_details = fetch_data(f"/api/properties/{property_id}")
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
                if property_details.get('latitude') and property_details.get('longitude'):
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