import psycopg2

# Параметры подключения к базе данных
DB_PARAMS = {
    'dbname': 'postgres',
    'user': 'Admin',
    'password': 'Enclude79',
    'host': 'localhost',
    'port': '5432'
}

def check_database():
    """Проверка наличия данных в базе данных"""
    try:
        print("Подключение к базе данных...")
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        print("Подключение успешно!")
        
        # Проверка существования таблицы
        print("\nПроверка таблицы temp_properties:")
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'temp_properties'
            )
        """)
        table_exists = cur.fetchone()[0]
        print(f"Таблица temp_properties существует: {table_exists}")
        
        if not table_exists:
            print("Таблица не существует. Анализ невозможен.")
            return
        
        # Проверка количества записей
        print("\nПроверка количества записей:")
        cur.execute("SELECT COUNT(*) FROM temp_properties")
        count = cur.fetchone()[0]
        print(f"Количество записей в таблице: {count}")
        
        # Проверка пустых значений для строковых полей
        print("\nПроверка пустых значений в строковых полях:")
        for field in ['id', 'title', 'region']:
            cur.execute(f"SELECT COUNT(*) FROM temp_properties WHERE {field} IS NULL OR {field} = ''")
            null_count = cur.fetchone()[0]
            print(f"Поле {field}: {null_count} пустых значений из {count} ({null_count/count*100:.2f}%)")
        
        # Проверка пустых значений для числовых полей
        print("\nПроверка пустых значений в числовых полях:")
        for field in ['price', 'area']:
            cur.execute(f"SELECT COUNT(*) FROM temp_properties WHERE {field} IS NULL")
            null_count = cur.fetchone()[0]
            print(f"Поле {field}: {null_count} пустых значений из {count} ({null_count/count*100:.2f}%)")
        
        # Получение нескольких записей
        print("\nПервые 5 записей (с любыми значениями):")
        cur.execute("""
            SELECT id, title, price, area, region 
            FROM temp_properties 
            LIMIT 5
        """)
        records = cur.fetchall()
        
        if records:
            for i, record in enumerate(records, 1):
                print(f"Запись {i}:")
                print(f"  ID: {record[0]}")
                print(f"  Название: {record[1] or 'Пусто'}")
                print(f"  Цена: {record[2] or 'Пусто'}")
                print(f"  Площадь: {record[3] or 'Пусто'}")
                print(f"  Регион: {record[4] or 'Пусто'}")
        
        # Проверка структуры таблицы
        print("\nСтруктура таблицы:")
        cur.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'temp_properties' 
            ORDER BY ordinal_position
        """)
        columns = cur.fetchall()
        for col in columns:
            print(f"  {col[0]}: {col[1]}")
        
        # Закрытие соединения
        cur.close()
        conn.close()
        print("\nПроверка завершена!")
        
    except Exception as e:
        print(f"Ошибка при проверке базы данных: {e}")

if __name__ == "__main__":
    check_database() 