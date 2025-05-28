import sqlite3
import json

# Подключаемся к базе данных
conn = sqlite3.connect('dubai_properties.db')
cursor = conn.cursor()

# Проверяем колонку geography
print("=== Анализ колонки geography ===")
cursor.execute('SELECT geography FROM properties WHERE geography IS NOT NULL LIMIT 1;')
geo_data = cursor.fetchone()
print(f"Raw geography data: {geo_data}")

if geo_data and geo_data[0]:
    try:
        # Пробуем распарсить как JSON
        parsed = json.loads(geo_data[0]) if isinstance(geo_data[0], str) else geo_data[0]
        print(f"Parsed JSON: {parsed}")
        # Проверяем структуру
        if isinstance(parsed, dict):
            print(f"Contains 'lat': {'lat' in parsed}")
            print(f"Contains 'lng': {'lng' in parsed}")
            for key in parsed.keys():
                print(f"Key: {key}, Value: {parsed[key]}")
    except (json.JSONDecodeError, TypeError) as e:
        print(f"Geography is not valid JSON: {e}")
        print(f"Type: {type(geo_data[0])}")
        # Если это не JSON, проверим, есть ли в нём текст, который можно использовать
        if isinstance(geo_data[0], str) and ('lat' in geo_data[0] or 'lng' in geo_data[0]):
            print("Содержит текстовые упоминания координат")
            print(geo_data[0])
else:
    print("Нет данных geography")

# Закрываем соединение
conn.close() 