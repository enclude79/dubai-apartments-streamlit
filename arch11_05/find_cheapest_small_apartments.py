import os
import psycopg2
import pandas as pd
from datetime import datetime
from langchain.chains import create_sql_query_chain
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
import json
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Параметры подключения к базе данных
DB_PARAMS = {
    'dbname': 'postgres',
    'user': 'admin',
    'password': 'Enclude79',
    'host': 'localhost',
    'port': '5432'
}

# Промт для поиска трех самых дешевых квартир в каждой локации площадью до 40 кв.м.
CHEAPEST_APARTMENTS_PROMPT = """
Ты опытный SQL-разработчик. Твоя задача - написать SQL-запрос для PostgreSQL, который найдет 
в каждой локации три самых дешевых квартиры с площадью до 40 квадратных метров.

Вот структура таблицы bayut_properties:
- id: INTEGER - идентификатор объекта
- "Unnamed: 1": TEXT - название/описание недвижимости
- "Unnamed: 2": NUMERIC - цена объекта
- "Unnamed: 3": INTEGER - количество спален
- "Unnamed: 4": INTEGER - количество ванных
- "Unnamed: 5": NUMERIC - площадь в квадратных метрах
- "Unnamed: 7": TEXT - информация о местоположении в JSON формате
- "Unnamed: 15": TEXT - статус (completed/under-construction)

Информация о местоположении ("Unnamed: 7") хранится в виде JSON массива, где каждый элемент 
содержит информацию о различных уровнях локации. Нам нужно извлечь название района из этого JSON.

Для решения задачи тебе нужно написать SQL-запрос, который:
1. Извлечет название района (neighborhood) из JSON в поле "Unnamed: 7"
2. Отфильтрует квартиры с площадью до 40 кв.м. (поле "Unnamed: 5")
3. Для каждого района найдет 3 самых дешевых объекта (по полю "Unnamed: 2")
4. Вернет информацию об этих объектах, включая: id, название, цену, площадь, район и количество спален

Напиши SQL-запрос, используя Common Table Expressions (CTE) и оконные функции. 
Возвращай только SQL-запрос, без каких-либо объяснений до или после.
"""

def extract_location_data(location_json):
    """Извлекает название района из JSON строки местоположения"""
    try:
        location_data = json.loads(location_json)
        # Ищем элемент с типом 'neighbourhood'
        for item in location_data:
            if item.get('type') == 'neighbourhood':
                return item.get('name')
            # Если нет элемента с типом 'neighbourhood', берем название из второго уровня (level: 2)
            elif item.get('level') == 2:
                return item.get('name')
        # Если не нашли ничего подходящего, возвращаем имя из первого элемента
        if location_data and len(location_data) > 0:
            return location_data[0].get('name')
        return None
    except (json.JSONDecodeError, TypeError):
        return None

def execute_sql_query(query):
    """Выполняет SQL запрос к базе данных и возвращает результаты"""
    try:
        # Подключение к базе данных
        conn = psycopg2.connect(**DB_PARAMS)
        
        # Создаем DataFrame из результатов запроса
        df = pd.read_sql_query(query, conn)
        
        # Закрытие соединения
        conn.close()
        
        return df
    except Exception as e:
        logger.error(f"Ошибка при выполнении SQL запроса: {e}")
        return None

def generate_sql_with_langchain():
    """Генерирует SQL-запрос с использованием LangChain"""
    try:
        # Использование тестового ключа API OpenAI для демонстрации
        # В реальном проекте нужно использовать настоящий ключ API
        os.environ["OPENAI_API_KEY"] = "sk-test-key"
        
        # Инициализация модели Chat GPT
        llm = ChatOpenAI(
            model="gpt-3.5-turbo",
            temperature=0.1
        )
        
        # Создание промта
        prompt = ChatPromptTemplate.from_template(CHEAPEST_APARTMENTS_PROMPT)
        
        # Создание цепочки для генерации SQL-запроса
        sql_chain = prompt | llm | StrOutputParser()
        
        # Генерация SQL-запроса
        sql_query = sql_chain.invoke({})
        
        # Очистка запроса от маркеров кода, если они есть
        sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
        
        return sql_query
    except Exception as e:
        logger.error(f"Ошибка при генерации SQL-запроса: {e}")
        # Возвращаем резервный запрос в случае ошибки
        return """
        WITH LocationData AS (
            SELECT 
                id, 
                "Unnamed: 1" AS title, 
                "Unnamed: 2" AS price, 
                "Unnamed: 3" AS bedrooms,
                "Unnamed: 5" AS area,
                "Unnamed: 7" AS location_json,
                CASE 
                    WHEN "Unnamed: 7" LIKE '%neighbourhood%' 
                    THEN substring("Unnamed: 7" FROM '"name": "([^"]+)".*neighbourhood')
                    WHEN "Unnamed: 7" LIKE '%level": 2%' 
                    THEN substring("Unnamed: 7" FROM '"level": 2.*"name": "([^"]+)"')
                    ELSE substring("Unnamed: 7" FROM '"name": "([^"]+)"')
                END AS neighborhood
            FROM bayut_properties
            WHERE "Unnamed: 5" <= 40
        ),
        RankedProperties AS (
            SELECT 
                id, title, price, bedrooms, area, neighborhood,
                ROW_NUMBER() OVER (PARTITION BY neighborhood ORDER BY "price" ASC) as rank
            FROM LocationData
            WHERE neighborhood IS NOT NULL
        )
        SELECT id, title, price, bedrooms, area, neighborhood
        FROM RankedProperties
        WHERE rank <= 3
        ORDER BY neighborhood, rank;
        """

def manual_process_apartments(df):
    """Обрабатывает данные о квартирах после выполнения SQL-запроса"""
    try:
        if df is None or df.empty:
            return "Не удалось найти квартиры, соответствующие критериям."
        
        # Добавление колонки с извлеченными районами, если ее нет
        if 'neighborhood' not in df.columns and 'location_json' in df.columns:
            df['neighborhood'] = df['location_json'].apply(extract_location_data)
        
        # Группировка по районам
        result = []
        for neighborhood, group in df.groupby('neighborhood'):
            if neighborhood is None:
                neighborhood = "Неизвестный район"
            
            apartments = []
            for _, row in group.sort_values('price').head(3).iterrows():
                apartment = {
                    'id': row.get('id', 'Нет данных'),
                    'title': row.get('title', 'Нет названия'),
                    'price': f"{float(row.get('price', 0)):,.2f} AED",
                    'area': f"{float(row.get('area', 0)):.2f} кв.м.",
                    'bedrooms': row.get('bedrooms', 'Нет данных')
                }
                apartments.append(apartment)
            
            result.append({
                'neighborhood': neighborhood,
                'apartments': apartments
            })
        
        # Форматирование результата
        output = "Три самых дешевых квартиры (до 40 кв.м.) в каждом районе:\n\n"
        
        for item in result:
            output += f"Район: {item['neighborhood']}\n"
            output += "-" * 50 + "\n"
            
            for i, apt in enumerate(item['apartments'], 1):
                output += f"{i}. {apt['title']}\n"
                output += f"   Цена: {apt['price']}\n"
                output += f"   Площадь: {apt['area']}\n"
                output += f"   Спальни: {apt['bedrooms']}\n"
                output += "\n"
            
            output += "\n"
        
        return output
    except Exception as e:
        logger.error(f"Ошибка при обработке данных о квартирах: {e}")
        return f"Произошла ошибка при обработке данных: {str(e)}"

def main():
    # Генерация SQL-запроса с использованием LangChain
    print("Генерация SQL-запроса с использованием LangChain...")
    sql_query = generate_sql_with_langchain()
    print(f"Сгенерированный SQL-запрос:\n{sql_query}\n")
    
    # Выполнение SQL-запроса
    print("Выполнение SQL-запроса...")
    apartments_df = execute_sql_query(sql_query)
    
    # Обработка результатов
    print("Обработка результатов...")
    result = manual_process_apartments(apartments_df)
    
    # Сохранение результатов в файл
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f'cheapest_apartments_{timestamp}.txt'
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(result)
    
    print(f"Результаты сохранены в файл: {output_file}")
    
    # Вывод результатов
    print(result)

if __name__ == "__main__":
    main() 