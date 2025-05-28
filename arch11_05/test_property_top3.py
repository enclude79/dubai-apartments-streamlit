import asyncio
import json
from langchain_sql_integration import PropertyDatabaseAnalyzer

async def main():
    try:
        print("=== ТЕСТИРОВАНИЕ ЗАПРОСА ТОП-3 КВАРТИР ПО РАЙОНАМ ===")
        
        # Создаем анализатор
        analyzer = PropertyDatabaseAnalyzer()
        print("Анализатор успешно инициализирован")
        
        # ТЕСТ 1: Прямое тестирование метода получения топ-3 квартир по району
        print("\n=== ТЕСТ 1: Прямой вызов метода get_latest_property_by_region ===")
        results = analyzer.get_latest_property_by_region(max_area=40, top_n=3)
        
        # Проверяем, что результаты не являются строкой (сообщение об ошибке)
        if isinstance(results, str):
            print(f"Ошибка при получении результатов: {results}")
        else:
            print(f"Количество полученных записей: {len(results)}")
            
            # Выводим результаты по районам
            regions = {}
            for prop in results:
                region = prop.get('region', 'Неизвестный район')
                if region not in regions:
                    regions[region] = []
                regions[region].append(prop)
            
            print(f"Найдено {len(regions)} районов с квартирами до 40 кв.м.")
            for region, properties in regions.items():
                print(f"\nРайон: {region}")
                print(f"Количество квартир: {len(properties)}")
                for idx, prop in enumerate(properties, 1):
                    print(f"  {idx}. {prop.get('title', 'Без названия')} - {prop.get('price', 'Цена не указана')} - {prop.get('area', 'Площадь не указана')} кв.м.")
        
        # ТЕСТ 2: Тестирование через вопрос пользователя
        test_question = "Найди топ-3 самых недорогих маленьких квартир до 40 кв.м. по каждому району Дубая"
        
        print("\n=== ТЕСТ 2: Вызов через вопрос пользователя ===")
        print(f"Вопрос: {test_question}")
        print("Генерация ответа...")
        
        # Генерируем ответ
        answer = await analyzer.generate_property_analysis(test_question)
        
        print("\nОТВЕТ:")
        print("=" * 80)
        print(answer)
        print("=" * 80)
        
        # ТЕСТ 3: Другая формулировка вопроса
        test_question2 = "Покажи для каждой локации Дубая до 3-х самых новых объявлений о квартирах площадью до 40 кв.м."
        
        print("\n=== ТЕСТ 3: Другая формулировка вопроса ===")
        print(f"Вопрос: {test_question2}")
        print("Генерация ответа...")
        
        # Генерируем ответ
        answer2 = await analyzer.generate_property_analysis(test_question2)
        
        print("\nОТВЕТ:")
        print("=" * 80)
        print(answer2)
        print("=" * 80)
        
        print("\nТестирование завершено успешно!")
        
    except Exception as e:
        print(f"Ошибка при тестировании: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main()) 