import os
import pandas as pd
import psycopg2
import json
import logging
from datetime import datetime
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser

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

# Промт для нахождения 3 самых дешевых квартир, оптимизированный под работу с данными JSON
CHEAPEST_APARTMENTS_JSON_OPTIMIZED_PROMPT = """
Ты эксперт по PostgreSQL и работе с JSON в SQL. Напиши SQL-запрос для нахождения 
трех самых дешевых квартир площадью до 40 кв.м. в каждой локации (районе).

## Структура таблицы bayut_properties:
- id: INTEGER - ID объекта
- "Unnamed: 1": TEXT - название объекта
- "Unnamed: 2": NUMERIC - цена
- "Unnamed: 3": INTEGER - спальни
- "Unnamed: 5": NUMERIC - площадь (кв.м.)
- "Unnamed: 7": TEXT - JSON с данными о местоположении

## Пример содержимого поля "Unnamed: 7" (JSON с местоположением):
```
[
  {"id": 1, "level": 0, "externalID": "5001", "name": "UAE"},
  {"id": 2, "level": 1, "externalID": "5002", "name": "Dubai"},
  {"id": 54, "level": 2, "externalID": "5093", "name": "Business Bay", "type": "neighbourhood"},
  {"id": 3517, "level": 3, "externalID": "11881", "name": "Urban Oasis by Missoni"}
]
```

## Требования к запросу:
1. Извлеки название района ('neighbourhood') из JSON в "Unnamed: 7" используя функции JSON в PostgreSQL
2. Отфильтруй квартиры с площадью <= 40 кв.м.
3. Получи 3 самых дешевых квартиры для каждого района
4. Используй оконные функции (ROW_NUMBER) для ранжирования
5. Верни поля: id, title, price, bedrooms, area, neighborhood
6. Отсортируй результат по району и цене

## Техническая реализация:
- Используй jsonb_array_elements для обработки JSON массива
- Используй CTE для пошаговой обработки данных
- Используй ROW_NUMBER() для ранжирования квартир в каждом районе

Верни только SQL-запрос, без пояснений.
"""

class PropertySearcher:
    """Класс для поиска недвижимости с использованием LangChain и SQL"""
    
    def __init__(self, db_params=None, table_name="bayut_properties"):
        """Инициализация с параметрами подключения к БД"""
        self.db_params = db_params or DB_PARAMS
        self.table_name = table_name
        
        # Установка тестового ключа API для демонстрации
        # В реальном проекте необходимо заменить на настоящий ключ
        os.environ["OPENAI_API_KEY"] = "sk-test-key"
        
        # Инициализация модели LLM
        self.llm = ChatOpenAI(
            model="gpt-3.5-turbo",
            temperature=0.1  # Низкая температура для более детерминированных результатов
        )

    def generate_sql_query(self, prompt=CHEAPEST_APARTMENTS_JSON_OPTIMIZED_PROMPT):
        """Генерация SQL запроса с использованием LangChain"""
        try:
            # Создание цепочки LangChain
            prompt_template = ChatPromptTemplate.from_template(prompt)
            chain = prompt_template | self.llm | StrOutputParser()
            
            # Генерация SQL запроса
            sql_query = chain.invoke({})
            
            # Очистка от маркеров кода и лишних символов
            sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
            
            # Вывод запроса для отладки
            logger.info(f"Сгенерированный SQL запрос:\n{sql_query}")
            
            return sql_query
            
        except Exception as e:
            logger.error(f"Ошибка при генерации SQL запроса: {e}")
            
            # Возвращаем запрос по умолчанию в случае ошибки
            return self.get_default_sql_query()
    
    def get_default_sql_query(self):
        """Возвращает SQL запрос по умолчанию"""
        # Поскольку у нас нет настоящего ключа API, мы будем использовать этот запрос напрямую
        return """
        WITH LocationData AS (
            SELECT 
                id, 
                "Unnamed: 1" AS title, 
                "Unnamed: 2" AS price, 
                "Unnamed: 3" AS bedrooms,
                "Unnamed: 5" AS area,
                "Unnamed: 7" AS location_json,
                -- Извлекаем название района из JSON
                CASE 
                    WHEN "Unnamed: 7" LIKE '%neighbourhood%' 
                    THEN substring("Unnamed: 7" FROM '"type": "neighbourhood".*?"name": "([^"]+)"')
                    WHEN "Unnamed: 7" NOT LIKE '%neighbourhood%' AND "Unnamed: 7" LIKE '%"level": 2%' 
                    THEN substring("Unnamed: 7" FROM '"level": 2.*?"name": "([^"]+)"')
                    ELSE substring("Unnamed: 7" FROM '"level": 1.*?"name": "([^"]+)"')
                END AS neighborhood
            FROM bayut_properties
            WHERE "Unnamed: 5" <= 40
        ),
        RankedProperties AS (
            SELECT 
                id, title, price, bedrooms, area, neighborhood,
                ROW_NUMBER() OVER (PARTITION BY neighborhood ORDER BY price ASC) as rank
            FROM LocationData
            WHERE neighborhood IS NOT NULL
        )
        SELECT id, title, price, bedrooms, area, neighborhood
        FROM RankedProperties
        WHERE rank <= 3
        ORDER BY neighborhood, rank;
        """

    def execute_query(self, query):
        """Выполнение SQL запроса и получение результатов"""
        try:
            # Подключение к базе данных
            conn = psycopg2.connect(**self.db_params)
            
            # Создание DataFrame из результатов запроса
            df = pd.read_sql_query(query, conn)
            
            # Закрытие соединения
            conn.close()
            
            return df
            
        except Exception as e:
            logger.error(f"Ошибка при выполнении запроса: {e}")
            return None

    def format_results(self, df):
        """Форматирование результатов запроса в удобный для чтения вид"""
        if df is None or df.empty:
            return "Не найдено квартир, соответствующих заданным критериям."
        
        # Группировка результатов по району
        result_text = "Три самых дешевых квартиры (площадь до 40 кв.м.) в каждом районе:\n\n"
        
        for neighborhood, group in df.groupby('neighborhood'):
            result_text += f"Район: {neighborhood}\n"
            result_text += "=" * 50 + "\n"
            
            # Сортировка по цене и вывод топ-3
            for i, (_, row) in enumerate(group.sort_values('price').iterrows(), 1):
                if i > 3:  # Ограничиваем вывод тремя квартирами
                    break
                    
                result_text += f"{i}. {row['title']}\n"
                result_text += f"   ID: {row['id']}\n"
                result_text += f"   Цена: {float(row['price']):,.2f} AED\n"
                result_text += f"   Площадь: {float(row['area']):,.2f} кв.м.\n"
                result_text += f"   Спальни: {row['bedrooms']}\n"
                result_text += "\n"
            
            result_text += "\n"
        
        return result_text

    def search_cheapest_apartments(self):
        """Основной метод для поиска самых дешевых квартир"""
        # В реальном проекте мы бы использовали LangChain для генерации запроса
        # Но поскольку у нас нет ключа API, используем запрос по умолчанию
        sql_query = self.get_default_sql_query()
        print("Использован SQL запрос по умолчанию")
        
        # Выполнение запроса
        print("Выполнение SQL запроса...")
        results_df = self.execute_query(sql_query)
        
        # Форматирование и вывод результатов
        print("Форматирование результатов...")
        formatted_results = self.format_results(results_df)
        
        # Сохранение результатов в файл
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f'cheapest_apartments_{timestamp}.txt'
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(formatted_results)
        
        print(f"Результаты сохранены в файл: {output_file}")
        
        return formatted_results

def main():
    """Основная функция программы"""
    print("Запуск поиска самых дешевых квартир...")
    
    # Создание объекта для поиска
    property_searcher = PropertySearcher()
    
    # Поиск квартир
    results = property_searcher.search_cheapest_apartments()
    
    # Вывод результатов
    print(results)

if __name__ == "__main__":
    main() 