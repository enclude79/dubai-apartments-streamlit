import psycopg2
import os

# Параметры подключения к базе данных
DB_PARAMS = {
    'dbname': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASSWORD', 'Enclude79'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432')
}

print("Попытка подключения к базе данных с параметрами:")
for key, value in DB_PARAMS.items():
    print(f"  {key}: {value}")

try:
    # Подключение к базе данных
    conn = psycopg2.connect(**DB_PARAMS)
    print("\nСоединение с базой данных успешно установлено!")
    
    # Создание курсора для выполнения запросов
    cursor = conn.cursor()
    
    # Проверка наличия таблицы bayut_properties
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'bayut_properties'
        )
    """)
    
    table_exists = cursor.fetchone()[0]
    
    if table_exists:
        print("Таблица bayut_properties существует")
        
        # Получение количества записей
        cursor.execute("SELECT COUNT(*) FROM bayut_properties")
        row_count = cursor.fetchone()[0]
        print(f"Количество записей в таблице: {row_count}")
    else:
        print("Таблица bayut_properties не существует")
    
    # Закрытие соединения
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"\nОшибка при подключении к базе данных: {e}") 