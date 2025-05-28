import psycopg2
import pandas as pd
import json
from datetime import datetime

# Параметры подключения к базе данных
DB_PARAMS = {
    'dbname': 'postgres',
    'user': 'admin',
    'password': 'Enclude79',
    'host': 'localhost',
    'port': '5432'
}

def analyze_properties():
    """Анализ данных о недвижимости"""
    try:
        print("Подключение к базе данных...")
        conn = psycopg2.connect(**DB_PARAMS)
        print("Подключение успешно!")
        
        # Создаем DataFrame из данных
        query = """
        SELECT 
            "Unnamed: 1" as title,
            "Unnamed: 2" as price,
            "Unnamed: 3" as bedrooms,
            "Unnamed: 4" as bathrooms,
            "Unnamed: 5" as area,
            "Unnamed: 7" as location,
            "Unnamed: 15" as status,
            "Unnamed: 16" as amenities,
            "Unnamed: 18" as agency,
            "Unnamed: 20" as coordinates,
            "Unnamed: 24" as tags
        FROM bayut_properties
        """
        
        df = pd.read_sql_query(query, conn)
        
        # Анализ цен
        print("\nАнализ цен:")
        print(f"Средняя цена: {df['price'].mean():,.2f} AED")
        print(f"Минимальная цена: {df['price'].min():,.2f} AED")
        print(f"Максимальная цена: {df['price'].max():,.2f} AED")
        print(f"Медианная цена: {df['price'].median():,.2f} AED")
        
        # Анализ по количеству спален
        print("\nРаспределение по количеству спален:")
        bedroom_stats = df.groupby('bedrooms').agg({
            'price': ['count', 'mean', 'min', 'max'],
            'area': 'mean'
        }).round(2)
        print(bedroom_stats)
        
        # Анализ по статусу
        print("\nРаспределение по статусу:")
        status_stats = df.groupby('status').agg({
            'price': ['count', 'mean'],
            'area': 'mean'
        }).round(2)
        print(status_stats)
        
        # Анализ агентств
        print("\nТоп-10 агентств по количеству объектов:")
        agency_stats = df['agency'].value_counts().head(10)
        print(agency_stats)
        
        # Анализ местоположения
        print("\nТоп-10 районов по количеству объектов:")
        # Извлекаем район из JSON-строки местоположения
        def extract_neighborhood(loc_str):
            try:
                loc_data = json.loads(loc_str)
                for item in loc_data:
                    if item.get('type') == 'neighbourhood':
                        return item.get('name')
                return None
            except:
                return None
        
        df['neighborhood'] = df['location'].apply(extract_neighborhood)
        neighborhood_stats = df['neighborhood'].value_counts().head(10)
        print(neighborhood_stats)
        
        # Анализ удобств
        print("\nТоп-10 самых популярных удобств:")
        def extract_amenities(amenities_str):
            if pd.isna(amenities_str):
                return []
            return [a.strip() for a in amenities_str.split(',')]
        
        all_amenities = []
        for amenities in df['amenities'].dropna():
            all_amenities.extend(extract_amenities(amenities))
        
        amenities_stats = pd.Series(all_amenities).value_counts().head(10)
        print(amenities_stats)
        
        # Анализ тегов
        print("\nТоп-10 самых популярных тегов:")
        def extract_tags(tags_str):
            if pd.isna(tags_str):
                return []
            try:
                return json.loads(tags_str)
            except:
                return []
        
        all_tags = []
        for tags in df['tags'].dropna():
            all_tags.extend(extract_tags(tags))
        
        tags_stats = pd.Series(all_tags).value_counts().head(10)
        print(tags_stats)
        
        # Сохраняем результаты анализа в файл
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f'property_analysis_{timestamp}.txt'
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("Анализ данных о недвижимости\n")
            f.write("=" * 50 + "\n\n")
            
            f.write("Анализ цен:\n")
            f.write(f"Средняя цена: {df['price'].mean():,.2f} AED\n")
            f.write(f"Минимальная цена: {df['price'].min():,.2f} AED\n")
            f.write(f"Максимальная цена: {df['price'].max():,.2f} AED\n")
            f.write(f"Медианная цена: {df['price'].median():,.2f} AED\n\n")
            
            f.write("Распределение по количеству спален:\n")
            f.write(str(bedroom_stats) + "\n\n")
            
            f.write("Распределение по статусу:\n")
            f.write(str(status_stats) + "\n\n")
            
            f.write("Топ-10 агентств по количеству объектов:\n")
            f.write(str(agency_stats) + "\n\n")
            
            f.write("Топ-10 районов по количеству объектов:\n")
            f.write(str(neighborhood_stats) + "\n\n")
            
            f.write("Топ-10 самых популярных удобств:\n")
            f.write(str(amenities_stats) + "\n\n")
            
            f.write("Топ-10 самых популярных тегов:\n")
            f.write(str(tags_stats) + "\n")
        
        print(f"\nРезультаты анализа сохранены в файл: {output_file}")
        
        # Закрытие соединения
        conn.close()
        print("\nАнализ завершен!")
        
    except Exception as e:
        print(f"Ошибка при анализе данных: {e}")

if __name__ == "__main__":
    analyze_properties() 