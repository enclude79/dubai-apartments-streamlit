import requests
import json
from datetime import datetime

# Параметры API
API_CONFIG = {
    "url": "https://bayut.p.rapidapi.com/properties/list",
    "headers": {
        "X-RapidAPI-Key": "86b3cfbc80msh3cd99bad2e7126dp18c722jsnc5b7ca0b0d3d",
        "X-RapidAPI-Host": "bayut.p.rapidapi.com"
    },
    "params": {
        "locationExternalIDs": "5002,6020",  # Дубай
        "purpose": "for-sale",
        "hitsPerPage": "25",
        "sort": "date-desc",  # Сортировка по дате (сначала новые)
        "categoryExternalID": "4",  # Квартиры
        "isDeveloper": "true",  # Только от застройщиков
        "completionStatus": ["off-plan", "under-construction"]  # Строящиеся объекты
    }
}

def test_api():
    """Тестирует API и выводит информацию о полученных данных"""
    print("Тестирование API Bayut...")

    try:
        # Делаем запрос к API
        response = requests.get(
            API_CONFIG["url"],
            headers=API_CONFIG["headers"],
            params=API_CONFIG["params"]
        )
        response.raise_for_status()  # Проверяем на ошибки
        
        # Разбираем ответ
        data = response.json()
        
        # Выводим общую информацию
        print(f"Статус ответа: {response.status_code}")
        print(f"Всего объектов: {len(data.get('hits', []))}")
        
        # Анализируем даты
        if 'hits' in data and data['hits']:
            print("\nАнализ дат в данных:")
            
            # Собираем все даты
            dates = []
            for item in data['hits']:
                if 'createdAt' in item:
                    created_at = datetime.fromtimestamp(item.get('createdAt', 0))
                    dates.append(created_at)
            
            if dates:
                # Сортируем даты
                dates.sort()
                
                print(f"Самая ранняя дата: {dates[0]}")
                print(f"Самая поздняя дата: {dates[-1]}")
                print(f"Количество дат: {len(dates)}")
                
                # Анализируем временные промежутки
                if len(dates) > 1:
                    time_diffs = [(dates[i+1] - dates[i]).total_seconds() / 3600 for i in range(len(dates)-1)]
                    avg_diff = sum(time_diffs) / len(time_diffs)
                    print(f"Средний интервал между датами: {avg_diff:.2f} часов")
        
        # Выводим примеры ID
        print("\nПримеры ID объектов:")
        for i, item in enumerate(data.get('hits', [])[:5]):
            print(f"{i+1}. ID: {item.get('id')}, Дата: {datetime.fromtimestamp(item.get('createdAt', 0))}")
            
        # Сохраняем результат в файл для дальнейшего анализа
        with open('api_test_response.json', 'w') as f:
            json.dump(data, f, indent=2)
        print("\nРезультат сохранен в файл api_test_response.json")
            
    except Exception as e:
        print(f"Ошибка при выполнении запроса: {e}")

if __name__ == "__main__":
    test_api() 