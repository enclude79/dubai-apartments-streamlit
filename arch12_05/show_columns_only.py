import psycopg2

# Параметры базы данных
DB_CONFIG = {
    'dbname': 'postgres',
    'user': 'admin',
    'password': 'Enclude79',
    'host': 'localhost',
    'port': '5432'
}

def show_table_columns():
    """Выводит только наименования колонок таблицы bayut_properties"""
    conn = None
    try:
        # Подключаемся к базе данных
        conn = psycopg2.connect(**DB_CONFIG)
        conn.set_client_encoding('UTF8')
        cursor = conn.cursor()
        
        # Получаем список колонок
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'bayut_properties'
            ORDER BY ordinal_position
        """)
        
        columns = [row[0] for row in cursor.fetchall()]
        
        # Выводим наименования колонок
        print("\n===== Наименования колонок таблицы bayut_properties =====")
        for i, col_name in enumerate(columns):
            print(f"{i+1}. {col_name}")
        
        print(f"\nВсего колонок: {len(columns)}")
        
        cursor.close()
        
        # Получаем информацию о API представлении, если оно существует
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.views 
                    WHERE table_name = 'bayut_api_view'
                )
            """)
            
            view_exists = cursor.fetchone()[0]
            
            if view_exists:
                print("\n===== Наименования колонок в представлении bayut_api_view =====")
                cursor.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = 'bayut_api_view'
                    ORDER BY ordinal_position
                """)
                
                view_columns = [row[0] for row in cursor.fetchall()]
                for i, col_name in enumerate(view_columns):
                    print(f"{i+1}. {col_name}")
                
                print(f"\nВсего колонок в представлении: {len(view_columns)}")
            
            cursor.close()
        except Exception as e:
            print(f"Не удалось получить информацию о представлении: {e}")
        
    except Exception as e:
        print(f"Ошибка при получении колонок таблицы: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    show_table_columns() 