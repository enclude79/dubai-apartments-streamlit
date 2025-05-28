import psycopg2

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
    
    # Получение списка колонок
    print("\nКолонки в таблице bayut_properties:")
    cur.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'bayut_properties'
        ORDER BY ordinal_position;
    """)
    
    columns = cur.fetchall()
    for col in columns:
        print(f"{col[0]}: {col[1]}")
    
    # Закрытие соединения
    cur.close()
    conn.close()
    print("\nСоединение закрыто")
    
except Exception as e:
    print(f"Ошибка: {e}") 