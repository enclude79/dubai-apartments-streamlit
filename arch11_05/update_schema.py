import psycopg2
import traceback

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
    
    # Удаление существующей таблицы temp_properties
    print("Удаление существующей таблицы temp_properties...")
    cur.execute("DROP TABLE IF EXISTS temp_properties;")
    conn.commit()
    
    # Создание новой таблицы с полной структурой
    print("Создание новой таблицы temp_properties с полной структурой...")
    create_table_query = """
    CREATE TABLE temp_properties (
        id VARCHAR(50),
        title VARCHAR(255),
        price NUMERIC,
        rooms INTEGER,
        bathrooms INTEGER,
        area NUMERIC,
        region TEXT,
        location TEXT,
        photos TEXT,
        listing_link TEXT,
        category VARCHAR(100),
        property_type VARCHAR(100),
        publication_date TIMESTAMP,
        last_update TIMESTAMP,
        parking_spaces VARCHAR(100),
        construction_status VARCHAR(100),
        features TEXT,
        description TEXT,
        developer VARCHAR(255),
        contacts TEXT,
        coordinates TEXT,
        developer_link TEXT,
        developer_logo TEXT,
        verification_status TEXT,
        keywords TEXT,
        view_count VARCHAR(50),
        photo_count VARCHAR(50),
        video_count VARCHAR(50),
        panorama_count VARCHAR(50),
        floor_count VARCHAR(50),
        developer_licenses TEXT,
        developer_rating TEXT
    );
    """
    
    cur.execute(create_table_query)
    conn.commit()
    print("Таблица успешно создана!")
    
    # Проверка структуры таблицы
    print("\nПроверка структуры таблицы:")
    cur.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'temp_properties'
        ORDER BY ordinal_position
    """)
    
    columns = cur.fetchall()
    for column in columns:
        print(f"- {column[0]}: {column[1]}")
    
except Exception as e:
    print(f"Ошибка: {e}")
    print("Детали ошибки:")
    traceback.print_exc()
finally:
    if 'cur' in locals():
        cur.close()
    if 'conn' in locals():
        conn.close()
    print("Соединение закрыто.") 