import psycopg2
import traceback

# Параметры подключения
conn_params = {
    'dbname': 'postgres',  # Имя базы данных
    'user': 'Admin',       # Пользователь
    'password': 'Enclude79', # Пароль
    'host': 'localhost',   # Хост
    'port': '5432'         # Порт
}

try:
    # Подключение к базе данных
    print("Попытка подключения к базе данных...")
    conn = psycopg2.connect(**conn_params)
    print("Подключение к базе данных успешно!")
    
    # Создание курсора
    cursor = conn.cursor()
    
    print("Удаление таблицы, если она существует...")
    # Удаление таблицы, если она уже существует
    cursor.execute("DROP TABLE IF EXISTS temp_properties;")
    
    print("Создание таблицы temp_properties...")
    # Создание таблицы temp_properties
    # Обратите внимание, что структура таблицы должна соответствовать вашему CSV файлу
    create_table_query = """
    CREATE TABLE temp_properties (
        title VARCHAR(255),
        price NUMERIC,
        rooms INTEGER
    );
    """
    
    cursor.execute(create_table_query)
    conn.commit()
    print("Таблица temp_properties успешно создана.")
    
    # Проверка создания таблицы
    cursor.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'temp_properties'
    """)
    if cursor.fetchone():
        print("Подтверждено: таблица temp_properties существует в базе данных.")
    else:
        print("Ошибка: таблица temp_properties не была создана!")
    
except Exception as e:
    print(f"Ошибка: {e}")
    print("Подробная информация об ошибке:")
    traceback.print_exc()
finally:
    if 'conn' in locals():
        if 'cursor' in locals():
            cursor.close()
        conn.close()
        print("Соединение с базой данных закрыто.") 