import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import os

# Подключаемся к базе данных
conn = sqlite3.connect('dubai_properties.db')

# Общая статистика
print("=== Анализ данных по площади квартир ===")
total_count = pd.read_sql_query("SELECT COUNT(*) as count FROM properties", conn).iloc[0]['count']
print(f"Всего объектов в базе: {total_count}")

# Проверяем минимальную и максимальную площадь
size_stats = pd.read_sql_query("SELECT MIN(size) as min_size, MAX(size) as max_size, AVG(size) as avg_size FROM properties", conn)
min_size = size_stats.iloc[0]['min_size']
max_size = size_stats.iloc[0]['max_size']
avg_size = size_stats.iloc[0]['avg_size']
print(f"Минимальная площадь: {min_size:.2f} кв.м.")
print(f"Максимальная площадь: {max_size:.2f} кв.м.")
print(f"Средняя площадь: {avg_size:.2f} кв.м.")

# Проверка распределения площади
print("\n=== Распределение площади квартир ===")
bins = [0, 50, 100, 150, 200, 300, 400, 500, 1000, float('inf')]
bin_labels = ['0-50', '51-100', '101-150', '151-200', '201-300', '301-400', '401-500', '501-1000', '>1000']

# Получаем все значения площади
sizes_df = pd.read_sql_query("SELECT size FROM properties", conn)
sizes = sizes_df['size'].values

# Распределяем по бинам
bin_counts = pd.cut(sizes, bins=bins, labels=bin_labels).value_counts().sort_index()
for bin_label, count in bin_counts.items():
    print(f"{bin_label} кв.м.: {count} ({count/total_count*100:.2f}%)")

# Найдем самые маленькие квартиры
print("\n=== 10 самых маленьких квартир ===")
small_properties = pd.read_sql_query("SELECT id, title, area, property_type, size, price FROM properties ORDER BY size ASC LIMIT 10", conn)
print(small_properties)

# Анализируем распределение по типам недвижимости
print("\n=== Распределение типов недвижимости ===")
property_types = pd.read_sql_query("SELECT property_type, COUNT(*) as count FROM properties GROUP BY property_type ORDER BY count DESC", conn)
print(property_types)

# Проверяем наличие пустых значений
print("\n=== Проверка данных ===")
null_count = pd.read_sql_query("SELECT COUNT(*) as count FROM properties WHERE size IS NULL", conn).iloc[0]['count']
print(f"Объектов без указания площади: {null_count}")

# Проверяем наличие аномально маленьких значений
tiny_count = pd.read_sql_query("SELECT COUNT(*) as count FROM properties WHERE size < 20", conn).iloc[0]['count']
print(f"Объектов с площадью менее 20 кв.м.: {tiny_count}")

# Проверяем возможный порог отсечения
print("\n=== Анализ возможного порога отсечения по площади ===")
for threshold in [40, 50, 60, 70, 80, 90, 100]:
    count = pd.read_sql_query(f"SELECT COUNT(*) as count FROM properties WHERE size <= {threshold}", conn).iloc[0]['count']
    print(f"Объектов с площадью до {threshold} кв.м.: {count} ({count/total_count*100:.2f}%)")

# Запрашиваем данные об объектах с площадью около 40 кв.м.
print("\n=== Объекты с площадью около 40 кв.м. ===")
around_40 = pd.read_sql_query("SELECT id, title, area, property_type, size, price FROM properties WHERE size BETWEEN 40 AND 45 ORDER BY size LIMIT 5", conn)
print(around_40)

conn.close()

print("\nАнализ завершен!") 