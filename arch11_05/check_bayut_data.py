import psycopg2

# Параметры подключения к базе данных
DB_PARAMS = {
    'dbname': 'postgres',
    'user': 'admin',
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
        print("\nПроверка таблицы bayut_properties:")
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'bayut_properties'
            )
        """)
        table_exists = cur.fetchone()[0]
        print(f"Таблица bayut_properties существует: {table_exists}")
        
        if not table_exists:
            print("Таблица не существует. Анализ невозможен.")
            return
        
        # Проверка количества записей
        print("\nПроверка количества записей:")
        cur.execute("SELECT COUNT(*) FROM bayut_properties")
        count = cur.fetchone()[0]
        print(f"Количество записей в таблице: {count}")
        
        # Получение нескольких записей
        print("\nПервые 5 записей:")
        cur.execute("""
            SELECT * FROM bayut_properties 
            LIMIT 5
        """)
        records = cur.fetchall()
        
        if records:
            # Получаем названия колонок
            cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'bayut_properties' 
                ORDER BY ordinal_position
            """)
            columns = [col[0] for col in cur.fetchall()]
            
            for i, record in enumerate(records, 1):
                print(f"\nЗапись {i}:")
                for col, val in zip(columns, record):
                    print(f"  {col}: {val or 'Пусто'}")
        
        # Проверка структуры таблицы
        print("\nСтруктура таблицы:")
        cur.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'bayut_properties' 
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