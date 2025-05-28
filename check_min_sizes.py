import sqlite3
import pandas as pd

# Подключаемся к базе данных
conn = sqlite3.connect('dubai_properties.db')
cursor = conn.cursor()

print("=== Анализ квартир с минимальной площадью ===")

# Получаем общую статистику по площади
cursor.execute("SELECT MIN(size), MAX(size), AVG(size) FROM properties")
min_size, max_size, avg_size = cursor.fetchone()
print(f"Минимальная площадь в базе: {min_size:.2f} кв.м.")
print(f"Максимальная площадь в базе: {max_size:.2f} кв.м.")
print(f"Средняя площадь в базе: {avg_size:.2f} кв.м.")

# Получаем 10 квартир с минимальной площадью
print("\n=== 10 квартир с минимальной площадью ===")
cursor.execute("""
SELECT id, title, area, property_type, size, price, bedrooms 
FROM properties 
ORDER BY size ASC 
LIMIT 10
""")
smallest_properties = cursor.fetchall()

for prop in smallest_properties:
    prop_id, title, area, prop_type, size, price, bedrooms = prop
    print(f"ID: {prop_id}")
    print(f"  Название: {title}")
    print(f"  Район: {area}")
    print(f"  Тип: {prop_type}")
    print(f"  Площадь: {size:.2f} кв.м.")
    print(f"  Цена: {price:.2f} AED")
    print(f"  Спальни: {bedrooms}")
    print("---")

# Проверяем распределение по диапазонам площади
print("\n=== Распределение по диапазонам площади ===")
ranges = [
    (0, 50), (51, 100), (101, 150), (151, 200), 
    (201, 300), (301, 400), (401, 500), (501, float('inf'))
]

for min_range, max_range in ranges:
    cursor.execute(f"""
    SELECT COUNT(*) 
    FROM properties 
    WHERE size BETWEEN {min_range} AND {max_range}
    """)
    count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM properties")
    total = cursor.fetchone()[0]
    percentage = (count / total) * 100 if total > 0 else 0
    print(f"{min_range}-{max_range} кв.м.: {count} ({percentage:.2f}%)")

# Проверяем есть ли объекты площадью до 40 кв.м.
cursor.execute("SELECT COUNT(*) FROM properties WHERE size <= 40")
count_below_40 = cursor.fetchone()[0]
cursor.execute("SELECT COUNT(*) FROM properties WHERE size BETWEEN 41 AND 60")
count_41_to_60 = cursor.fetchone()[0]

print(f"\nКвартир площадью до 40 кв.м.: {count_below_40}")
print(f"Квартир площадью от 41 до 60 кв.м.: {count_41_to_60}")

# Закрываем соединение
conn.close() 