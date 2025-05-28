import asyncio
import json
from langchain_sql_integration import PropertyDatabaseAnalyzer

async def main():
    # Создаем анализатор
    analyzer = PropertyDatabaseAnalyzer()
    
    # Выводим схему таблицы
    print("СХЕМА ТАБЛИЦЫ:")
    print(analyzer.table_schema)
    print("-" * 80)
    
    # Тестируем простой SQL запрос
    query = "SELECT construction_status, COUNT(*) as count FROM temp_properties GROUP BY construction_status"
    
    print("ВЫПОЛНЕНИЕ ЗАПРОСА:")
    print(query)
    print("-" * 80)
    
    results = analyzer.execute_sql_query(query)
    print("РЕЗУЛЬТАТЫ:")
    print(json.dumps(results, indent=2, ensure_ascii=False))
    print("-" * 80)
    
    # Тестируем генерацию ответа на вопрос
    question = "Сколько объектов недвижимости находится в стадии строительства?"
    
    print(f"ВОПРОС: {question}")
    print("-" * 80)
    
    # Генерируем ответ
    answer = await analyzer.generate_property_analysis(question)
    
    print("ОТВЕТ:")
    print(answer)
    print("-" * 80)

if __name__ == "__main__":
    asyncio.run(main()) 