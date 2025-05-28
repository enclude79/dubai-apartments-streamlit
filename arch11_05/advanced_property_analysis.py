import asyncio
import logging
from langchain_sql_integration import PropertyDatabaseAnalyzer
import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv()

# Параметры подключения к базе данных
DB_PARAMS = {
    'dbname': 'postgres',
    'user': 'Admin',
    'password': 'Enclude79',
    'host': 'localhost',
    'port': '5432'
}

def get_db_stats():
    """Получение базовой статистики из базы данных"""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()
        
        # Количество записей
        cursor.execute("SELECT COUNT(*) FROM temp_properties")
        total_count = cursor.fetchone()[0]
        print(f"Всего записей в базе данных: {total_count}")
        
        # Доступные регионы
        cursor.execute("SELECT DISTINCT region FROM temp_properties WHERE region IS NOT NULL AND region != ''")
        regions = [r[0] for r in cursor.fetchall()]
        print(f"Доступные регионы: {len(regions)}")
        print(regions[:10] if len(regions) > 10 else regions)
        
        # Ценовой диапазон
        cursor.execute("SELECT MIN(price), MAX(price), AVG(price) FROM temp_properties WHERE price > 0")
        min_price, max_price, avg_price = cursor.fetchone()
        print(f"Ценовой диапазон: от {min_price:.2f} до {max_price:.2f}, средняя: {avg_price:.2f}")
        
        # Распределение по типам недвижимости
        cursor.execute("SELECT property_type, COUNT(*) FROM temp_properties WHERE property_type IS NOT NULL GROUP BY property_type ORDER BY COUNT(*) DESC")
        property_types = cursor.fetchall()
        if property_types:
            print("Топ-5 типов недвижимости:")
            for i, (type_name, count) in enumerate(property_types[:5], 1):
                print(f"{i}. {type_name}: {count} объектов")
        
        cursor.close()
        conn.close()
        
        return {
            "total_count": total_count,
            "regions": regions,
            "price_range": (min_price, max_price, avg_price),
            "property_types": property_types
        }
    
    except Exception as e:
        logger.error(f"Ошибка при получении статистики из БД: {e}")
        return None

async def run_advanced_analysis(query):
    """Запуск продвинутого анализа с использованием LLM + SQL"""
    try:
        analyzer = PropertyDatabaseAnalyzer()
        
        # Получаем анализ на основе запроса
        print(f"Запрос для анализа: '{query}'")
        print("Генерация ответа...")
        result = await analyzer.generate_property_analysis(query)
        
        print("\nРЕЗУЛЬТАТ АНАЛИЗА:")
        print("=" * 80)
        print(result)
        print("=" * 80)
        
        return result
    
    except Exception as e:
        logger.error(f"Ошибка при анализе данных: {e}")
        return f"Произошла ошибка при анализе: {e}"

async def market_dashboard():
    """Создание панели анализа рынка недвижимости Дубая"""
    try:
        # Получаем общую статистику
        stats = get_db_stats()
        if not stats:
            print("Не удалось получить статистику из базы данных")
            return
            
        # Аналитические запросы
        queries = [
            "Сравни средние цены за квадратный метр в топ-5 самых дорогих районах Дубая.",
            "Проанализируй, как зависит цена от площади для 1-комнатных квартир в Downtown Dubai.",
            "Найди топ-3 самых недорогих квартир до 40 кв.м. по каждому району Дубая и дай рекомендации для инвесторов.",
            "Какие районы Дубая предлагают наилучший баланс цены и качества для инвестиций в 2025 году?",
            "На основе статистики по ценам за последние 6 месяцев, в каких районах ожидается наибольший рост стоимости недвижимости?"
        ]
        
        # Выполняем один запрос в качестве примера
        selected_query = queries[2]  # Запрос для топ-3 квартир
        result = await run_advanced_analysis(selected_query)
        
        print("\nАналитический отчет сгенерирован успешно!")
        return result
    
    except Exception as e:
        logger.error(f"Ошибка при создании аналитической панели: {e}")
        return None

if __name__ == "__main__":
    print("=" * 80)
    print("ПРОДВИНУТЫЙ АНАЛИЗ РЫНКА НЕДВИЖИМОСТИ ДУБАЯ")
    print("=" * 80)
    
    # Асинхронный запуск анализа
    result = asyncio.run(market_dashboard())
    
    if result:
        # Сохраняем результат в файл для дальнейшего использования
        with open("market_analysis_result.txt", "w", encoding="utf-8") as f:
            f.write(result)
        print(f"Результат анализа сохранен в файл market_analysis_result.txt")
    
    print("Анализ завершен!") 