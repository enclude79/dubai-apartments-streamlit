import psycopg2
import traceback
import json

# Параметры подключения
conn_params = {
    'dbname': 'postgres',
    'user': 'Admin',
    'password': 'Enclude79',
    'host': 'localhost',
    'port': '5432'
}

try:
    # Подключение к базе данных
    print("Подключение к базе данных...")
    conn = psycopg2.connect(**conn_params)
    print("Подключение успешно!")
    
    # Создание курсора
    cur = conn.cursor()
    
    # Проверка количества записей
    cur.execute("SELECT COUNT(*) FROM temp_properties")
    count = cur.fetchone()[0]
    print(f"Количество записей в таблице: {count}")
    
    # 1. Анализ цен на недвижимость
    print("\n1. Анализ цен на недвижимость:")
    cur.execute("""
        SELECT 
            MIN(price) as min_price,
            MAX(price) as max_price,
            AVG(price) as avg_price,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) as median_price
        FROM temp_properties
        WHERE price > 0
    """)
    price_stats = cur.fetchone()
    print(f"Минимальная цена: {price_stats[0]}")
    print(f"Максимальная цена: {price_stats[1]}")
    print(f"Средняя цена: {price_stats[2]:.2f}")
    print(f"Медианная цена: {price_stats[3]:.2f}")
    
    # 2. Распределение по количеству комнат
    print("\n2. Распределение по количеству комнат:")
    cur.execute("""
        SELECT rooms, COUNT(*) as count
        FROM temp_properties
        WHERE rooms > 0
        GROUP BY rooms
        ORDER BY rooms
    """)
    rooms_distribution = cur.fetchall()
    for row in rooms_distribution:
        print(f"{row[0]} комнат(ы): {row[1]} объектов")
    
    # 3. Распределение по типам недвижимости
    print("\n3. Распределение по типам недвижимости:")
    cur.execute("""
        SELECT property_type, COUNT(*) as count
        FROM temp_properties
        WHERE property_type != ''
        GROUP BY property_type
        ORDER BY count DESC
        LIMIT 10
    """)
    property_type_distribution = cur.fetchall()
    for row in property_type_distribution:
        print(f"{row[0]}: {row[1]} объектов")
    
    # 4. Распределение по застройщикам (топ-5)
    print("\n4. Распределение по застройщикам (топ-5):")
    cur.execute("""
        SELECT developer, COUNT(*) as count
        FROM temp_properties
        WHERE developer != ''
        GROUP BY developer
        ORDER BY count DESC
        LIMIT 5
    """)
    developer_distribution = cur.fetchall()
    for row in developer_distribution:
        print(f"{row[0]}: {row[1]} объектов")
    
    # 5. Соотношение цены к площади
    print("\n5. Средняя цена за кв. метр:")
    cur.execute("""
        SELECT 
            AVG(price / NULLIF(area, 0)) as avg_price_per_sqm
        FROM temp_properties
        WHERE price > 0 AND area > 0
    """)
    avg_price_per_sqm = cur.fetchone()[0]
    print(f"Средняя цена за кв. метр: {avg_price_per_sqm:.2f}")
    
    # 6. Распределение по статусу строительства
    print("\n6. Распределение по статусу строительства:")
    cur.execute("""
        SELECT construction_status, COUNT(*) as count
        FROM temp_properties
        WHERE construction_status != ''
        GROUP BY construction_status
        ORDER BY count DESC
    """)
    status_distribution = cur.fetchall()
    for row in status_distribution:
        print(f"{row[0]}: {row[1]} объектов")
    
except Exception as e:
    print(f"Ошибка при анализе данных: {e}")
    print("Детали ошибки:")
    traceback.print_exc()
finally:
    if 'cur' in locals() and cur:
        cur.close()
    if 'conn' in locals() and conn:
        conn.close()
    print("\nСоединение с базой данных закрыто.") 