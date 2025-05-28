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
    
    # Проверка существования таблицы temp_properties
    print("Проверка существования таблицы temp_properties...")
    cursor.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'temp_properties'
    """)
    if cursor.fetchone():
        print("Таблица temp_properties существует.")
        
        # Проверка структуры таблицы
        print("Проверка структуры таблицы temp_properties...")
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'temp_properties'
        """)
        columns = cursor.fetchall()
        print("Столбцы в таблице temp_properties:")
        for column in columns:
            print(f"- {column[0]}: {column[1]}")
        
        # Подсчет количества записей
        cursor.execute("SELECT COUNT(*) FROM temp_properties")
        count = cursor.fetchone()[0]
        print(f"Количество записей в таблице temp_properties: {count}")
        
        # Вывод первых 5 записей для проверки данных
        if count > 0:
            print("\nПервые 5 записей в таблице temp_properties:")
            cursor.execute("SELECT * FROM temp_properties LIMIT 5")
            rows = cursor.fetchall()
            for row in rows:
                print(row)
        else:
            print("Таблица temp_properties пуста. Данные не были загружены.")
    else:
        print("Таблица temp_properties не существует!")
        
        # Проверка всех существующих таблиц
        print("\nСписок всех таблиц в базе данных:")
        cursor.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
        """)
        tables = cursor.fetchall()
        for table in tables:
            print(f"- {table[0]}")
            
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