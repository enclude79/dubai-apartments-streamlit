import os
import pandas as pd
import logging
from datetime import datetime
import json
import re
import psycopg2
from langchain_community.utilities import SQLDatabase
from load_env import load_environment_variables

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Загружаем переменные окружения
load_environment_variables()

# Параметры подключения к базе данных
DB_PARAMS = {
    'dbname': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASSWORD', 'Enclude79'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432')
}

# Создаем строку подключения
CONNECTION_STRING = f"postgresql://{DB_PARAMS['user']}:{DB_PARAMS['password']}@{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['dbname']}"

def extract_neighborhood(location_json):
    """Извлекает название района из JSON строки местоположения"""
    try:
        # Сначала пытаемся исправить JSON с одинарными кавычками
        if location_json and "'" in location_json:
            location_json = location_json.replace("'", '"')
        
        # Парсим JSON
        data = json.loads(location_json)
        
        # Ищем элемент с type = neighbourhood
        for item in data:
            if item.get('type') == 'neighbourhood':
                return item.get('name')
        
        # Ищем элемент с level = 2 (обычно это район)
        for item in data:
            if item.get('level') == 2:
                return item.get('name')
        
        # Если не нашли ни по типу, ни по уровню, берем город (level = 1)
        for item in data:
            if item.get('level') == 1:
                return item.get('name')
        
        # Если ничего не нашли, возвращаем первый элемент name
        if data and len(data) > 0:
            return data[0].get('name')
        
        return None
    except (json.JSONDecodeError, TypeError) as e:
        # Если JSON невалидный, пытаемся извлечь название с помощью регулярных выражений
        try:
            # Ищем тип neighbourhood
            neighbourhood_match = re.search(r'"type": "neighbourhood".*?"name": "([^"]+)"', location_json)
            if neighbourhood_match:
                return neighbourhood_match.group(1)
            
            # Ищем уровень 2
            level2_match = re.search(r'"level": 2.*?"name": "([^"]+)"', location_json)
            if level2_match:
                return level2_match.group(1)
            
            # Ищем уровень 1
            level1_match = re.search(r'"level": 1.*?"name": "([^"]+)"', location_json)
            if level1_match:
                return level1_match.group(1)
            
            # Ищем любое название
            name_match = re.search(r'"name": "([^"]+)"', location_json)
            if name_match:
                return name_match.group(1)
        except:
            pass
        
        logger.error(f"Ошибка при извлечении района: {e}")
        return None

def find_cheapest_apartments():
    """Находит три самых дешевых квартиры в каждой локации с использованием LangChain"""
    try:
        # Используем стандартный psycopg2 для выполнения SQL запроса вместо SQLDatabase.engine
        print("Выполнение запроса для получения всех маленьких квартир...")
        logger.info("Подключение к базе данных...")
        
        try:
            conn = psycopg2.connect(**DB_PARAMS)
        except Exception as e:
            logger.error(f"Ошибка подключения к базе данных: {e}")
            return f"Ошибка подключения к базе данных: {str(e)}"
        
        # SQL запрос для получения дешевых квартир
        sql_query = """
        SELECT 
            id, 
            "Unnamed: 1" AS title, 
            "Unnamed: 2" AS price, 
            "Unnamed: 3" AS bedrooms,
            "Unnamed: 5" AS area,
            "Unnamed: 7" AS location_json,
            "Unnamed: 9" AS url
        FROM bayut_properties
        WHERE "Unnamed: 5" <= 40
        ORDER BY "Unnamed: 2" ASC
        """
        
        try:
            # Выполнение запроса с помощью pandas
            df = pd.read_sql_query(sql_query, conn)
        except Exception as e:
            logger.error(f"Ошибка выполнения SQL запроса: {e}")
            return f"Ошибка выполнения SQL запроса: {str(e)}"
        finally:
            conn.close()
        
        if df.empty:
            logger.warning("Не найдено квартир площадью до 40 кв.м.")
            return "Не найдено квартир площадью до 40 кв.м."
        
        logger.info(f"Получено {len(df)} квартир площадью до 40 кв.м.")
        print(f"Получено {len(df)} квартир площадью до 40 кв.м.")
        
        # Обработка JSON данных и извлечение районов
        print("Извлечение районов из JSON данных...")
        logger.info("Извлечение районов из JSON данных...")
        df['neighborhood'] = df['location_json'].apply(extract_neighborhood)
        
        # Удаление строк с отсутствующими районами
        df = df.dropna(subset=['neighborhood'])
        
        if df.empty:
            logger.warning("Не удалось извлечь информацию о районах из данных.")
            return "Не удалось извлечь информацию о районах из данных."
        
        # Группировка по районам и выбор трех самых дешевых квартир в каждом районе
        print("Группировка данных по районам...")
        logger.info("Группировка данных по районам...")
        result = []
        
        for neighborhood, group in df.groupby('neighborhood'):
            # Сортировка по цене и выбор топ-3
            top3 = group.sort_values('price').head(3)
            
            # Добавление в результат
            for _, row in top3.iterrows():
                result.append({
                    'neighborhood': neighborhood,
                    'id': row['id'],
                    'title': row['title'],
                    'price': row['price'],
                    'bedrooms': row['bedrooms'],
                    'area': row['area'],
                    'url': row['url']
                })
        
        # Создание DataFrame из результатов
        result_df = pd.DataFrame(result)
        
        # Форматирование результатов
        output = format_apartments_result(result_df)
        
        # Сохранение результатов в файл
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f'cheapest_apartments_with_urls_{timestamp}.txt'
        
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(output)
            logger.info(f"Результаты сохранены в файл: {output_file}")
            print(f"Результаты сохранены в файл: {output_file}")
        except Exception as e:
            logger.error(f"Ошибка при сохранении результатов в файл: {e}")
            print(f"Ошибка при сохранении результатов в файл: {e}")
        
        return output
        
    except Exception as e:
        error_msg = f"Ошибка при поиске дешевых квартир: {e}"
        logger.error(error_msg)
        return error_msg

def format_apartments_result(df):
    """Форматирует результаты в удобный для чтения вид"""
    if df.empty:
        return "Не найдено квартир, соответствующих заданным критериям."
    
    # Группировка по районам
    output = "Три самых дешевых квартиры (площадь до 40 кв.м.) в каждом районе:\n\n"
    
    for neighborhood, group in df.groupby('neighborhood'):
        output += f"Район: {neighborhood}\n"
        output += "=" * 50 + "\n"
        
        # Сортировка по цене
        for i, (_, row) in enumerate(group.sort_values('price').iterrows(), 1):
            # Удаляем строку с датой публикации
            
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
    print("Запуск поиска трех самых дешевых квартир в каждой локации...")
    result = find_cheapest_apartments()
    print(result)

if __name__ == "__main__":
    main() 