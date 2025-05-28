import psycopg2
import traceback

# Параметры подключения
conn_params = {
    'dbname': 'postgres',
    'user': 'Admin',
    'password': 'Enclude79',
    'host': 'localhost',
    'port': '5432',
    'client_encoding': 'UTF8'
}

try:
    print("Подключение к базе данных...")
    conn = psycopg2.connect(**conn_params)
    print("Подключение успешно!")
    
    # Создание курсора
    cur = conn.cursor()
    
    # Проверка существования таблицы temp_properties
    print("\nПроверка таблицы temp_properties...")
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'temp_properties'
    """)
    
    if cur.fetchone():
        print("Таблица temp_properties существует.")
        
        # Проверка количества строк
        cur.execute("SELECT COUNT(*) FROM temp_properties")
        count = cur.fetchone()[0]
        print(f"Количество строк в таблице: {count}")
        
        # Проверка колонок
        cur.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'temp_properties' 
            ORDER BY ordinal_position
        """)
        
        columns = cur.fetchall()
        print("\nСтруктура таблицы:")
        for col in columns:
            print(f"- {col[0]}: {col[1]}")
        
        # Простой запрос к таблице
        print("\nВыполнение тестового запроса...")
        cur.execute("""
            SELECT construction_status, COUNT(*) 
            FROM temp_properties 
            WHERE construction_status IS NOT NULL 
            GROUP BY construction_status
        """)
        
        results = cur.fetchall()
        print("\nРезультаты запроса (статусы строительства):")
        for row in results:
            print(f"- {row[0]}: {row[1]} объектов")
        
    else:
        print("Таблица temp_properties не существует!")
        
except Exception as e:
    print(f"Ошибка: {e}")
    traceback.print_exc()
finally:
    if 'cur' in locals():
        cur.close()
    if 'conn' in locals():
        conn.close()
    print("\nСоединение с базой данных закрыто.") 