import sqlite3
import pandas as pd

# ID квартир, которые нам нужно проверить
property_ids = [
    8353108,  # Wasl Gate - 35.30 кв.м.
    8338129,  # Wasl Gate - 35.30 кв.м.
    8231173,  # Wasl Gate - 33.45 кв.м.
    8351004,  # Yas Island - 31.96 кв.м.
    8351825,  # Yas Island - 33.89 кв.м.
    8417826   # Yas Island - 33.82 кв.м.
]

# Подключаемся к базе данных
conn = sqlite3.connect('dubai_properties.db')
cursor = conn.cursor()

print("=== Проверка наличия квартир в базе данных ===")

# Проверяем структуру таблицы properties
cursor.execute("PRAGMA table_info(properties)")
columns = cursor.fetchall()
print("Структура таблицы properties:")
for col in columns:
    print(f"  {col[1]} ({col[2]})")

# Получаем количество записей в таблице
cursor.execute("SELECT COUNT(*) FROM properties")
total_count = cursor.fetchone()[0]
print(f"\nВсего объектов в базе: {total_count}")

# Проверяем каждый ID
print("\n=== Результаты поиска по ID ===")
found_count = 0
for property_id in property_ids:
    cursor.execute("SELECT id, title, area, property_type, size, price, bedrooms FROM properties WHERE id = ?", (property_id,))
    result = cursor.fetchone()
    
    if result:
        found_count += 1
        print(f"ID {property_id} НАЙДЕН:")
        print(f"  Название: {result[1]}")
        print(f"  Район: {result[2]}")
        print(f"  Тип: {result[3]}")
        print(f"  Площадь: {result[4]} кв.м.")
        print(f"  Цена: {result[5]} AED")
        print(f"  Спальни: {result[6]}")
        print("---")
    else:
        print(f"ID {property_id} НЕ НАЙДЕН в базе данных")

print(f"\nНайдено {found_count} из {len(property_ids)} искомых квартир")

# Проверяем квартиры с маленькой площадью
print("\n=== Поиск квартир с площадью до 40 кв.м. ===")
cursor.execute("SELECT id, title, area, property_type, size, price FROM properties WHERE size <= 40 ORDER BY size")
small_properties = cursor.fetchall()

if small_properties:
    print(f"Найдено {len(small_properties)} квартир с площадью до 40 кв.m.:")
    for prop in small_properties:
        print(f"ID: {prop[0]}, Название: {prop[1]}, Район: {prop[2]}, Площадь: {prop[4]} кв.м., Цена: {prop[5]} AED")
else:
    print("Квартир с площадью до 40 кв.м. в базе данных не найдено")

# Закрываем соединение
conn.close() 