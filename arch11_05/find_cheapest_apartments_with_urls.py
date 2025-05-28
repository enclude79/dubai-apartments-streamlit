import os
import pandas as pd
import psycopg2
import json
import logging
from datetime import datetime
import re

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Параметры подключения к базе данных
DB_PARAMS = {
    'dbname': 'postgres',
    'user': 'Admin',
    'password': 'Enclude79',
    'host': 'localhost',
    'port': '5432'
}

def get_basic_query():
    """Возвращает SQL-запрос, который извлекает название района из JSON и выбирает топ-3 квартиры по району"""
    return '''
    WITH LocationData AS (
        SELECT 
            id, 
            title, 
            price, 
            rooms AS bedrooms, 
            area, 
            property_url AS url,
            -- Извлекаем название района из JSON
            (
                SELECT value->>'name' FROM jsonb_array_elements(location::jsonb) value
                WHERE (value->>'type' = 'neighbourhood' OR value->>'level' = '2')
                LIMIT 1
            ) AS neighborhood
        FROM bayut_properties
        WHERE area <= 40
    ),
    RankedProperties AS (
        SELECT 
            id, title, price, bedrooms, area, url, neighborhood,
            ROW_NUMBER() OVER (PARTITION BY neighborhood ORDER BY price ASC) as rank
        FROM LocationData
        WHERE neighborhood IS NOT NULL
    )
    SELECT id, title, price, bedrooms, area, url, neighborhood
    FROM RankedProperties
    WHERE rank <= 3
    ORDER BY neighborhood, price ASC
    '''

def find_cheapest_apartments():
    """Находит три самых дешевых квартиры в каждом районе"""
    try:
        # Подключение к базе данных
        conn = psycopg2.connect(**DB_PARAMS)
        print("Подключение к базе данных успешно")
        
        # Получение данных
        query = get_basic_query()
        print("Выполнение запроса для получения топ-3 квартир по району...")
        df = pd.read_sql_query(query, conn)
        
        # Закрытие соединения
        conn.close()
        
        if df.empty:
            return "Не найдено квартир площадью до 40 кв.м."
        
        print(f"Получено {len(df)} квартир (топ-3 по каждому району)")
        
        # Форматирование результатов
        output = format_apartments_result(df)
        
        # Сохранение результатов в файл
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f'cheapest_apartments_with_urls_{timestamp}.txt'
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(output)
        
        print(f"Результаты сохранены в файл: {output_file}")
        
        return output
        
    except Exception as e:
        logger.error(f"Ошибка при поиске дешевых квартир: {e}")
        return f"Произошла ошибка при поиске квартир: {str(e)}"

def format_apartments_result(df):
    """Форматирует результаты в удобный для чтения вид"""
    if df.empty:
        return "Не найдено квартир, соответствующих заданным критериям."
    
    # Группировка по районам
    output = "Три самых дешевых квартиры (площадь до 40 кв.м.) в каждом районе:\n\n"
    
    for neighborhood, group in df.groupby('neighborhood'):
        output += f"Район: {neighborhood}\n"
        output += "-" * 30 + "\n"
        
        # Сортировка по цене
        for i, (_, row) in enumerate(group.sort_values('price').iterrows(), 1):
            output += f"{i}. {row['title']}\n"
            output += f"   ID: {row['id']}\n"
            output += f"   Цена: {float(row['price']):,.2f} AED\n"
            output += f"   Площадь: {float(row['area']):,.2f} кв.м.\n"
            output += f"   Спальни: {row['bedrooms']}\n"
            output += f"   Ссылка: {row['url']}\n"
            output += "\n"
        
        output += "\n"
    
    return output

def main():
    """Основная функция программы"""
    print("Запуск поиска трех самых дешевых квартир в каждом районе...")
    result = find_cheapest_apartments()
    print(result)

if __name__ == "__main__":
    main() 