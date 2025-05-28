import psycopg2
import os
import traceback

try:
    # Подключение к базе данных
    print("Подключение к базе данных...")
    conn = psycopg2.connect(
        dbname="postgres",
        user="Admin",
        password="Enclude79",
        host="localhost",
        port="5432"
    )
    print("Подключение успешно!")
    
    # Создание курсора
    cur = conn.cursor()
    
    # Путь к CSV файлу
    csv_path = r"C:\WealthCompas\Api_Bayat\bayut_properties_sale_6m_20250401.csv"
    
    if not os.path.exists(csv_path):
        print(f"ОШИБКА: Файл не найден: {csv_path}")
    else:
        print(f"Файл найден: {csv_path}")
        file_size = os.path.getsize(csv_path) / (1024 * 1024)  # Размер в МБ
        print(f"Размер файла: {file_size:.2f} МБ")
        
        # Проверка таблицы
        print("Проверка таблицы temp_properties...")
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = 'temp_properties'
        """)
        
        if not cur.fetchone():
            print("Создание таблицы temp_properties...")
            cur.execute("""
                CREATE TABLE temp_properties (
                    title VARCHAR(255),
                    price NUMERIC,
                    rooms INTEGER
                )
            """)
            conn.commit()
            print("Таблица создана!")
        else:
            print("Таблица уже существует.")
            print("Очистка старых данных...")
            cur.execute("TRUNCATE TABLE temp_properties")
            conn.commit()
        
        # Чтение первых нескольких строк CSV для проверки
        print("Чтение первых строк CSV для анализа...")
        with open(csv_path, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                if i == 0:
                    print(f"Заголовок CSV: {line.strip()}")
                elif i <= 3:
                    print(f"Строка {i}: {line.strip()}")
                else:
                    break
        
        # Загрузка данных
        print("Загрузка данных из CSV...")
        try:
            copy_query = f"""
            COPY temp_properties(title, price, rooms)
            FROM '{csv_path}'
            DELIMITER ','
            CSV HEADER;
            """
            
            cur.execute(copy_query)
            conn.commit()
            
            # Проверка загрузки
            cur.execute("SELECT COUNT(*) FROM temp_properties")
            count = cur.fetchone()[0]
            print(f"Загружено записей: {count}")
        except Exception as e:
            print(f"Ошибка при копировании данных: {e}")
            print("Детали ошибки:")
            traceback.print_exc()
        
except Exception as e:
    print(f"Ошибка: {e}")
    print("Детали ошибки:")
    traceback.print_exc()
finally:
    if 'cur' in locals():
        cur.close()
    if 'conn' in locals():
        conn.close()
    print("Соединение закрыто.") 