import os
import sys
import logging
import asyncio
from datetime import datetime
import argparse

# Настройка логирования
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = f'{log_dir}/main_process_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

async def run_full_process():
    """Запускает полный процесс: загрузка данных, анализ и публикация"""
    logger.info("Запуск полного процесса обработки данных о недвижимости")
    print("\n===== Запуск полного процесса обработки данных о недвижимости =====")
    
    # Шаг 1: Загрузка данных из API в CSV
    print("\n----- Шаг 1: Загрузка данных из API в CSV -----")
    logger.info("Шаг 1: Загрузка данных из API в CSV")
    
    try:
        from api_to_csv import main as api_to_csv_main
        csv_path = api_to_csv_main()
        
        if not csv_path:
            logger.error("Ошибка при загрузке данных из API в CSV")
            print("Ошибка при загрузке данных из API в CSV")
            return False
        
        logger.info(f"Данные успешно загружены в CSV: {csv_path}")
        print(f"Данные успешно загружены в CSV: {csv_path}")
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных из API в CSV: {e}")
        print(f"Ошибка при загрузке данных из API в CSV: {e}")
        return False
    
    # Шаг 2: Загрузка данных из CSV в базу данных
    print("\n----- Шаг 2: Загрузка данных из CSV в базу данных -----")
    logger.info("Шаг 2: Загрузка данных из CSV в базу данных")
    
    try:
        from fix_encoding_improved import main as fix_encoding_main
        success = fix_encoding_main() == 0
        
        if not success:
            logger.error("Ошибка при загрузке данных из CSV в базу данных")
            print("Ошибка при загрузке данных из CSV в базу данных")
            return False
        
        logger.info("Данные успешно загружены в базу данных")
        print("Данные успешно загружены в базу данных")
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных из CSV в базу данных: {e}")
        print(f"Ошибка при загрузке данных из CSV в базу данных: {e}")
        return False
    
    # Шаг 3: Поиск и анализ дешевых квартир
    print("\n----- Шаг 3: Поиск и анализ дешевых квартир -----")
    logger.info("Шаг 3: Поиск и анализ дешевых квартир")
    
    try:
        from find_cheapest_apartments_langchain import find_cheapest_apartments
        analysis_text = find_cheapest_apartments()
        
        if not analysis_text or analysis_text.startswith("Ошибка") or analysis_text.startswith("Не удалось"):
            logger.error("Ошибка при анализе дешевых квартир")
            print("Ошибка при анализе дешевых квартир")
            return False
        
        logger.info("Анализ дешевых квартир успешно выполнен")
        print("Анализ дешевых квартир успешно выполнен")
    except Exception as e:
        logger.error(f"Ошибка при анализе дешевых квартир: {e}")
        print(f"Ошибка при анализе дешевых квартир: {e}")
        return False
    
    # Шаг 4: Публикация результатов в Telegram
    print("\n----- Шаг 4: Публикация результатов в Telegram -----")
    logger.info("Шаг 4: Публикация результатов в Telegram")
    
    try:
        from telegram_simple_publisher import publish_to_telegram
        success = await publish_to_telegram()
        
        if not success:
            logger.error("Ошибка при публикации результатов в Telegram")
            print("Ошибка при публикации результатов в Telegram")
            return False
        
        logger.info("Результаты успешно опубликованы в Telegram")
        print("Результаты успешно опубликованы в Telegram")
    except Exception as e:
        logger.error(f"Ошибка при публикации результатов в Telegram: {e}")
        print(f"Ошибка при публикации результатов в Telegram: {e}")
        return False
    
    # Все шаги успешно выполнены
    logger.info("Полный процесс успешно завершен")
    print("\n===== Полный процесс успешно завершен =====")
    return True

async def run_single_step(step):
    """Запускает отдельный шаг процесса"""
    if step == "api":
        # Загрузка данных из API в CSV
        try:
            from api_to_csv import main as api_to_csv_main
            csv_path = api_to_csv_main()
            if csv_path:
                print(f"Данные успешно загружены в CSV: {csv_path}")
                return True
            else:
                print("Ошибка при загрузке данных из API в CSV")
                return False
        except Exception as e:
            print(f"Ошибка при загрузке данных из API в CSV: {e}")
            return False
    
    elif step == "db":
        # Загрузка данных из CSV в базу данных
        try:
            from fix_encoding_improved import main as fix_encoding_main
            success = fix_encoding_main() == 0
            if success:
                print("Данные успешно загружены в базу данных")
                return True
            else:
                print("Ошибка при загрузке данных из CSV в базу данных")
                return False
        except Exception as e:
            print(f"Ошибка при загрузке данных из CSV в базу данных: {e}")
            return False
    
    elif step == "analyze":
        # Поиск и анализ дешевых квартир
        try:
            from find_cheapest_apartments_langchain import find_cheapest_apartments
            analysis_text = find_cheapest_apartments()
            if analysis_text and not analysis_text.startswith("Ошибка") and not analysis_text.startswith("Не удалось"):
                print("Анализ дешевых квартир успешно выполнен")
                return True
            else:
                print("Ошибка при анализе дешевых квартир")
                return False
        except Exception as e:
            print(f"Ошибка при анализе дешевых квартир: {e}")
            return False
    
    elif step == "telegram":
        # Публикация результатов в Telegram
        try:
            from telegram_simple_publisher import publish_to_telegram
            success = await publish_to_telegram()
            if success:
                print("Результаты успешно опубликованы в Telegram")
                return True
            else:
                print("Ошибка при публикации результатов в Telegram")
                return False
        except Exception as e:
            print(f"Ошибка при публикации результатов в Telegram: {e}")
            return False
    
    else:
        print(f"Неизвестный шаг: {step}")
        return False

def show_menu():
    """Показывает интерактивное меню для выбора действия"""
    print("\n===== WealthCompass - Система анализа недвижимости =====")
    print("1. Запустить полный процесс")
    print("2. Загрузить данные из API в CSV")
    print("3. Загрузить данные из CSV в базу данных")
    print("4. Выполнить анализ дешевых квартир")
    print("5. Опубликовать результаты в Telegram")
    print("0. Выход")
    
    choice = input("Выберите действие (0-5): ")
    return choice

async def main():
    """Основная функция скрипта"""
    parser = argparse.ArgumentParser(description="WealthCompass - Система анализа недвижимости")
    parser.add_argument("--step", choices=["full", "api", "db", "analyze", "telegram"], 
                      help="Выполнить конкретный шаг процесса")
    parser.add_argument("--interactive", action="store_true", 
                      help="Запустить в интерактивном режиме")
    
    args = parser.parse_args()
    
    if args.step:
        # Запуск конкретного шага из командной строки
        if args.step == "full":
            await run_full_process()
        else:
            await run_single_step(args.step)
    elif args.interactive:
        # Интерактивный режим
        while True:
            choice = show_menu()
            
            if choice == "0":
                print("Выход из программы")
                break
            elif choice == "1":
                await run_full_process()
            elif choice == "2":
                await run_single_step("api")
            elif choice == "3":
                await run_single_step("db")
            elif choice == "4":
                await run_single_step("analyze")
            elif choice == "5":
                await run_single_step("telegram")
            else:
                print("Неверный выбор. Пожалуйста, выберите действие из списка.")
            
            input("\nНажмите Enter для продолжения...")
    else:
        # По умолчанию запускаем интерактивный режим
        while True:
            choice = show_menu()
            
            if choice == "0":
                print("Выход из программы")
                break
            elif choice == "1":
                await run_full_process()
            elif choice == "2":
                await run_single_step("api")
            elif choice == "3":
                await run_single_step("db")
            elif choice == "4":
                await run_single_step("analyze")
            elif choice == "5":
                await run_single_step("telegram")
            else:
                print("Неверный выбор. Пожалуйста, выберите действие из списка.")
            
            input("\nНажмите Enter для продолжения...")

if __name__ == "__main__":
    asyncio.run(main()) 