import psycopg2

# Параметры подключения
conn_params = {
    'dbname': 'postgres',  # Имя базы данных
    'user': 'Admin',       # Пользователь
    'password': 'Enclude79', # Пароль
    'host': 'localhost',   # Хост
    'port': '5432'         # Порт
}

try:
    # Попытка подключения к базе данных
    conn = psycopg2.connect(**conn_params)
    print("Подключение к базе данных успешно!")
    
    # Проверка существующих таблиц
    cursor = conn.cursor()
    cursor.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public'
    """)
    tables = cursor.fetchall()
    print("Существующие таблицы:")
    for table in tables:
        print(f"- {table[0]}")
    
    cursor.close()
except Exception as e:
    print(f"Ошибка при подключении к базе данных: {e}")
finally:
    if 'conn' in locals():
        conn.close()
        print("Соединение с базой данных закрыто.") 