import sqlite3
import json

# Подключаемся к базе данных
conn = sqlite3.connect('dubai_properties.db')
cursor = conn.cursor()

# Получаем структуру таблицы
print("=== Структура таблицы properties ===")
cursor.execute('PRAGMA table_info(properties);')
for col in cursor.fetchall():
    print(col)

# Проверяем наличие колонки geography и ее формат
print("\n=== Пример данных geography ===")
cursor.execute('SELECT geography FROM properties WHERE geography IS NOT NULL LIMIT 1;')
geo_data = cursor.fetchone()
print(f"Raw geography data: {geo_data}")
if geo_data and geo_data[0]:
    try:
        # Пробуем распарсить как JSON
        parsed = json.loads(geo_data[0])
        print(f"Parsed JSON: {parsed}")
        # Проверяем наличие ключей lat и lng
        if isinstance(parsed, dict):
            print(f"Contains 'lat': {'lat' in parsed}")
            print(f"Contains 'lng': {'lng' in parsed}")
    except json.JSONDecodeError:
        print("Geography is not valid JSON")

# Проверяем наличие колонки bedrooms
print("\n=== Колонка bedrooms ===")
cursor.execute("SELECT name FROM pragma_table_info('properties') WHERE name='bedrooms';")
if cursor.fetchone():
    cursor.execute("SELECT bedrooms FROM properties WHERE bedrooms IS NOT NULL LIMIT 5;")
    print(f"Примеры значений bedrooms: {[row[0] for row in cursor.fetchall()]}")
else:
    print("Колонка 'bedrooms' отсутствует в таблице")

# Проверяем наличие колонки, которая может содержать данные о спальнях
print("\n=== Поиск альтернативных колонок для спален ===")
potential_columns = ['beds', 'bedroom', 'bedroom_count', 'room_count', 'rooms']
for col in potential_columns:
    cursor.execute(f"SELECT name FROM pragma_table_info('properties') WHERE name='{col}';")
    if cursor.fetchone():
        cursor.execute(f"SELECT {col} FROM properties WHERE {col} IS NOT NULL LIMIT 5;")
        print(f"Колонка '{col}' существует. Примеры значений: {[row[0] for row in cursor.fetchall()]}")

# Получаем все колонки таблицы
print("\n=== Все колонки таблицы properties ===")
cursor.execute("SELECT name FROM pragma_table_info('properties');")
all_columns = [row[0] for row in cursor.fetchall()]
print(", ".join(all_columns))

# Закрываем соединение
conn.close() 