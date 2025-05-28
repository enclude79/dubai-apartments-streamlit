import os
import sys
import logging
from datetime import datetime
from load_env import load_environment_variables
import re

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/data_loader_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_data_from_api():
    """Получает данные из API и сохраняет в CSV"""
    from api_to_csv import main as api_to_csv_main
    import io
    from contextlib import redirect_stdout
    
    logger.info("Получение данных из API...")
    print("Получение данных из API...")
    try:
        # Перехватываем вывод функции в переменную
        output = io.StringIO()
        with redirect_stdout(output):
            api_to_csv_main()
        
        # Получаем вывод и ищем строку с путем к CSV файлу
        output_text = output.getvalue()
        print(output_text)  # Выводим перехваченный вывод
        
        # Ищем путь к CSV файлу в выводе
        csv_path_match = re.search(r'CSV_PATH:(.*?)$', output_text, re.MULTILINE)
        if csv_path_match:
            csv_path = csv_path_match.group(1).strip()
            if os.path.exists(csv_path):
                logger.info(f"Данные успешно сохранены в CSV файл: {csv_path}")
                print(f"Данные успешно сохранены в CSV файл: {csv_path}")
                return csv_path
        
        # Если не нашли в выводе, ищем в логах
        for file in os.listdir('logs'):
            if file.startswith('api_to_csv_') and file.endswith('.log'):
                with open(os.path.join('logs', file), 'r', encoding='utf-8') as f:
                    content = f.read()
                    file_match = re.search(r'Данные сохранены в файл: (.*?)$', content, re.MULTILINE)
                    if file_match:
                        csv_path = file_match.group(1).strip()
                        if os.path.exists(csv_path):
                            logger.info(f"Данные успешно сохранены в CSV файл (из логов): {csv_path}")
                            print(f"Данные успешно сохранены в CSV файл (из логов): {csv_path}")
                            return csv_path
        
        # Поиск в текущем каталоге
        for root, dirs, files in os.walk('Api_Bayat'):
            for file in files:
                if file.endswith('.csv') and 'bayut_properties' in file and file.startswith('bayut_properties_sale_' + datetime.now().strftime('%Y%m%d')):
                    csv_path = os.path.join(root, file)
                    logger.info(f"Данные успешно сохранены в CSV файл (найден в каталоге): {csv_path}")
                    print(f"Данные успешно сохранены в CSV файл (найден в каталоге): {csv_path}")
                    return csv_path
                    
        logger.error("Ошибка при получении данных из API: CSV файл не найден в выводе")
        print("Ошибка при получении данных из API: CSV файл не найден в выводе")
        return None
    except Exception as e:
        logger.error(f"Ошибка при получении данных из API: {e}")
        print(f"Ошибка при получении данных из API: {e}")
        return None

def load_data_from_csv(csv_path):
    """Загружает данные из CSV файла в базу данных"""
    from csv_to_sql import CsvToSqlLoader, DB_CONFIG
    
    if not os.path.exists(csv_path):
        logger.error(f"Файл не найден: {csv_path}")
        print(f"Файл не найден: {csv_path}")
        return False
    
    logger.info(f"Загрузка данных из {csv_path} в базу данных...")
    print(f"Загрузка данных из {csv_path} в базу данных...")
    loader = CsvToSqlLoader(DB_CONFIG)
    success = loader.run(csv_path)
    
    if success:
        logger.info("Данные успешно загружены в базу данных")
        print("Данные успешно загружены в базу данных")
        return True
    else:
        logger.error("Ошибка при загрузке данных в базу данных")
        print("Ошибка при загрузке данных в базу данных")
        return False

def main():
    """Основная функция программы - автоматический запуск всего процесса загрузки"""
    # Загружаем переменные окружения
    load_environment_variables()
    
    # Создаем директорию для логов, если её нет
    os.makedirs("logs", exist_ok=True)
    
    logger.info("Запуск полного процесса загрузки данных...")
    print("Запуск полного процесса загрузки данных...")
    
    # Получение данных из API и сохранение в CSV
    csv_path = get_data_from_api()
    if not csv_path:
        logger.error("Завершение процесса из-за ошибки при получении данных из API")
        print("Завершение процесса из-за ошибки при получении данных из API")
        return 1
    
    # Загрузка данных из CSV в базу данных
    sql_success = load_data_from_csv(csv_path)
    if not sql_success:
        logger.error("Завершение процесса из-за ошибки при загрузке данных в базу данных")
        print("Завершение процесса из-за ошибки при загрузке данных в базу данных")
        return 1
    
    logger.info("Полный процесс загрузки данных успешно завершен")
    print("Полный процесс загрузки данных успешно завершен")
    return 0

if __name__ == "__main__":
    sys.exit(main()) 