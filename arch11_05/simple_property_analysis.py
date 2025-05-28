import asyncio
import logging
from langchain_sql_integration import PropertyDatabaseAnalyzer

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def test_simple_query(query, table_name="properties_optimized"):
    """Тестирование простого запроса к базе данных через LangChain"""
    try:
        # Создаем анализатор
        analyzer = PropertyDatabaseAnalyzer(table_name=table_name)
        
        # Запрашиваем анализ
        print(f"Запрос: {query}")
        result = await analyzer.generate_property_analysis(query, query_type='advanced')
        
        # Выводим результат
        print("\nРЕЗУЛЬТАТ:")
        print("=" * 80)
        print(result)
        print("=" * 80)
        
        return result
    except Exception as e:
        logger.error(f"Ошибка при выполнении запроса: {e}")
        return None

async def main():
    """Основная функция для тестирования различных запросов"""
    print("=" * 80)
    print("ТЕСТИРОВАНИЕ ИНТЕГРАЦИИ LANGCHAIN + SQL НА ОПТИМИЗИРОВАННОЙ ТАБЛИЦЕ")
    print("=" * 80)
    
    # Запросы для тестирования работы с оптимизированной таблицей
    test_queries = [
        "Какие категории цен представлены в базе и сколько объектов в каждой категории?",
        "Какая средняя стоимость за квадратный метр в разных категориях цен?",
        "Покажи 5 наиболее выгодных предложений по соотношению цена/площадь",
        "Сколько в среднем стоит квартира с 2 комнатами?",
        "Сравни средние цены в разных типах недвижимости"
    ]
    
    # Запускаем первый запрос для тестирования
    result = await test_simple_query(test_queries[0])
    
    # Сохраняем результат в файл
    if result:
        with open("optimized_analysis_result.txt", "w", encoding="utf-8") as f:
            f.write(result)
        print(f"Результат сохранен в файл optimized_analysis_result.txt")
    
    print("Тестирование завершено!")

if __name__ == "__main__":
    asyncio.run(main()) 