import asyncio
import logging
import traceback
from langchain_sql_integration import PropertyDatabaseAnalyzer

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

async def main():
    # Тестовые вопросы для анализа
    test_questions = [
        "Какова средняя цена объектов недвижимости с 2 спальнями?",
        "Сколько объектов недвижимости находится в стадии строительства?",
        "Кто из застройщиков имеет наибольшее количество объектов в базе данных?",
        "Какова средняя стоимость квадратного метра недвижимости?",
        "В каком районе Дубая больше всего объектов недвижимости?"
    ]
    
    try:
        print("====== ТЕСТИРОВАНИЕ LANGCHAIN SQL ИНТЕГРАЦИИ ======")
        print("\nИнициализация анализатора базы данных...")
        
        # Инициализация анализатора базы данных
        analyzer = PropertyDatabaseAnalyzer()
        print("Анализатор успешно инициализирован")
        
        # Выбираем один вопрос для тестирования
        test_question = test_questions[1]  # Вопрос о объектах в стадии строительства
        
        print(f"\nВопрос: {test_question}")
        print("Генерация ответа...")
        
        # Подробный вывод для отладки
        print("\nПолучение схемы базы данных...")
        db_schema = analyzer.db.get_table_info()
        print(f"Схема базы: {db_schema}")
        
        # Получаем ответ от LangChain
        answer = await analyzer.generate_property_analysis(test_question)
        
        print("\nОтвет:")
        print("=" * 80)
        print(answer)
        print("=" * 80)
        
        print("\nТестирование завершено успешно!")
        
    except Exception as e:
        logger.error(f"Ошибка при тестировании: {e}")
        print(f"\nПроизошла ошибка: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main()) 