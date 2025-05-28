import asyncio
import json
from langchain_sql_integration import PropertyDatabaseAnalyzer

async def main():
    try:
        print("Starting test...")
        
        # Создаем анализатор
        analyzer = PropertyDatabaseAnalyzer()
        
        # Выводим схему таблицы
        print("TABLE SCHEMA:")
        print(analyzer.table_schema)
        print("-" * 80)
        
        # Тестируем простой SQL запрос
        query = "SELECT construction_status, COUNT(*) as count FROM temp_properties GROUP BY construction_status"
        
        print("EXECUTING QUERY:")
        print(query)
        print("-" * 80)
        
        results = analyzer.execute_sql_query(query)
        print("RESULTS:")
        for result in results:
            print(f"- {result['construction_status']}: {result['count']} properties")
        print("-" * 80)
        
        # Тестируем генерацию ответа на вопрос
        question = "Сколько объектов недвижимости находится в стадии строительства?"
        
        print(f"QUESTION: {question}")
        print("-" * 80)
        
        # Генерируем ответ
        print("Generating answer...")
        answer = await analyzer.generate_property_analysis(question)
        
        print("ANSWER:")
        print(answer)
        print("-" * 80)
        
        print("Test completed successfully!")
    except Exception as e:
        print(f"Error during test: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main()) 