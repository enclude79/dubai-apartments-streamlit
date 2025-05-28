import psycopg2

# Параметры подключения к базе данных
DB_PARAMS = {
    'dbname': 'postgres',
    'user': 'Admin',
    'password': 'Enclude79',
    'host': 'localhost',
    'port': '5432'
}

def check_test_data():
    """Проверка тестовых данных в базе данных"""
    try:
        print("Подключение к базе данных...")
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        print("Подключение успешно!")
        
        # Проверка количества тестовых записей
        cur.execute("SELECT COUNT(*) FROM temp_properties WHERE id LIKE 'TEST%'")
        count = cur.fetchone()[0]
        print(f"Количество тестовых записей в таблице: {count}")
        
        if count > 0:
            # Проверка полноты данных в тестовых записях
            print("\nПроверка полноты данных в тестовых записях:")
            
            # Проверка строковых полей
            for field in ['title', 'region', 'property_type']:
                cur.execute(f"SELECT COUNT(*) FROM temp_properties WHERE id LIKE 'TEST%' AND ({field} IS NULL OR {field} = '')")
                null_count = cur.fetchone()[0]
                print(f"Поле {field}: {null_count} пустых значений из {count} ({null_count/count*100:.2f}%)")
            
            # Проверка числовых полей
            for field in ['price', 'area']:
                cur.execute(f"SELECT COUNT(*) FROM temp_properties WHERE id LIKE 'TEST%' AND {field} IS NULL")
                null_count = cur.fetchone()[0]
                print(f"Поле {field}: {null_count} пустых значений из {count} ({null_count/count*100:.2f}%)")
            
            # Получение нескольких тестовых записей
            print("\nПервые 5 тестовых записей:")
            cur.execute("""
                SELECT id, title, price, area, region, property_type 
                FROM temp_properties 
                WHERE id LIKE 'TEST%'
                LIMIT 5
            """)
            records = cur.fetchall()
            
            for i, record in enumerate(records, 1):
                print(f"Запись {i}:")
                print(f"  ID: {record[0]}")
                print(f"  Название: {record[1] or 'Пусто'}")
                print(f"  Цена: {record[2] or 'Пусто'}")
                print(f"  Площадь: {record[3] or 'Пусто'}")
                print(f"  Регион: {record[4] or 'Пусто'}")
                print(f"  Тип: {record[5] or 'Пусто'}")
        
        # Закрытие соединения
        cur.close()
        conn.close()
        print("\nПроверка завершена!")
        
    except Exception as e:
        print(f"Ошибка при проверке тестовых данных: {e}")

if __name__ == "__main__":
    check_test_data() 