import psycopg2
import os
import traceback

# Параметры подключения
conn_params = {
    'dbname': 'postgres',
    'user': 'Admin',
    'password': 'Enclude79',
    'host': 'localhost',
    'port': '5432'
}

# Путь к CSV файлу
csv_path = r"C:\WealthCompas\Api_Bayat\bayut_properties_sale_6m_20250401.csv"

try:
    # Проверка существования файла
    print(f"Проверка существования файла: {csv_path}")
    if not os.path.exists(csv_path):
        print(f"ОШИБКА: Файл не найден: {csv_path}")
        exit(1)
    
    print(f"Файл найден: {csv_path}")
    file_size = os.path.getsize(csv_path) / (1024 * 1024)  # Размер в МБ
    print(f"Размер файла: {file_size:.2f} МБ")
    
    # Подключение к базе данных
    print("Подключение к базе данных...")
    conn = psycopg2.connect(**conn_params)
    print("Подключение успешно!")
    
    # Создание курсора
    cur = conn.cursor()
    
    # Проверка существования таблицы
    print("Проверка существования таблицы temp_properties...")
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'temp_properties'
    """)
    
    if not cur.fetchone():
        print("ОШИБКА: Таблица temp_properties не существует!")
        print("Сначала запустите update_schema.py для создания таблицы.")
        exit(1)
    
    # Очистка таблицы перед загрузкой
    print("Очистка таблицы перед загрузкой...")
    cur.execute("TRUNCATE TABLE temp_properties;")
    conn.commit()
    
    # Загрузка данных из CSV
    print("Загрузка данных из CSV...")
    # Формируем команду COPY с полным списком столбцов
    copy_query = f"""
    COPY temp_properties(
        id, title, price, rooms, bathrooms, area, region, location, 
        photos, listing_link, category, property_type, publication_date, 
        last_update, parking_spaces, construction_status, features, 
        description, developer, contacts, coordinates, developer_link, 
        developer_logo, verification_status, keywords, view_count, 
        photo_count, video_count, panorama_count, floor_count, 
        developer_licenses, developer_rating
    )
    FROM '{csv_path}'
    DELIMITER ','
    CSV HEADER
    QUOTE '"';
    """
    
    cur.execute(copy_query)
    conn.commit()
    
    # Проверка количества загруженных записей
    cur.execute("SELECT COUNT(*) FROM temp_properties")
    count = cur.fetchone()[0]
    print(f"Данные успешно загружены! Количество загруженных записей: {count}")
    
    # Вывести первые несколько записей для проверки
    if count > 0:
        print("\nПримеры загруженных данных (первые 3 записи):")
        cur.execute("""
            SELECT id, title, price, rooms, bathrooms, area, region, developer
            FROM temp_properties
            LIMIT 3
        """)
        
        rows = cur.fetchall()
        for row in rows:
            print(f"ID: {row[0]}, Название: {row[1]}")
            print(f"Цена: {row[2]}, Комнат: {row[3]}, Ванных: {row[4]}, Площадь: {row[5]}")
            print(f"Регион: {row[6]}, Застройщик: {row[7]}")
            print("---")
    
except Exception as e:
    print(f"Ошибка при загрузке данных: {e}")
    print("Детали ошибки:")
    traceback.print_exc()
finally:
    if 'cur' in locals() and cur:
        cur.close()
    if 'conn' in locals() and conn:
        conn.close()
    print("Соединение с базой данных закрыто.") 