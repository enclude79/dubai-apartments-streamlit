import pandas as pd
import psycopg2
import os
import traceback

# Параметры подключения
conn_params = {
    'dbname': 'postgres',  # Имя базы данных
    'user': 'Admin',       # Имя пользователя
    'password': 'Enclude79',  # Пароль
    'host': 'localhost',   # Хост
    'port': '5432'         # Порт
}

# Путь к вашему CSV файлу
csv_file_path = r'C:\WealthCompas\Api_Bayat\bayut_properties_sale_6m_20250401.csv'

try:
    # Проверка существования файла
    print(f"Проверка существования файла: {csv_file_path}")
    if not os.path.exists(csv_file_path):
        print(f"ОШИБКА: Файл не найден: {csv_file_path}")
        exit(1)
    else:
        print(f"Файл найден: {csv_file_path}")
        file_size = os.path.getsize(csv_file_path) / (1024 * 1024)  # Размер в МБ
        print(f"Размер файла: {file_size:.2f} МБ")

    # Подключение к базе данных
    print("Попытка подключения к базе данных...")
    conn = psycopg2.connect(**conn_params)
    print("Подключение к базе данных успешно!")

    # Создание курсора
    cur = conn.cursor()
    
    # Проверка существования таблицы
    print("Проверка существования таблицы temp_properties...")
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'temp_properties'
    """)
    if not cur.fetchone():
        print("ОШИБКА: Таблица temp_properties не существует!")
        print("Создание таблицы temp_properties...")
        
        # Создаем таблицу, если она не существует
        create_table_query = """
        CREATE TABLE temp_properties (
            title VARCHAR(255),
            price NUMERIC,
            rooms INTEGER
        );
        """
        cur.execute(create_table_query)
        conn.commit()
        print("Таблица temp_properties создана!")
    else:
        print("Таблица temp_properties уже существует.")
        
        # Очищаем таблицу перед загрузкой новых данных
        print("Очистка таблицы перед загрузкой новых данных...")
        cur.execute("TRUNCATE TABLE temp_properties;")
        conn.commit()
        print("Таблица очищена.")

    # SQL запрос для загрузки данных из CSV в временную таблицу
    print("Попытка загрузки данных из CSV в таблицу temp_properties...")
    copy_query = f"""
    COPY temp_properties(title, price, rooms)
    FROM '{csv_file_path}'
    DELIMITER ','
    CSV HEADER;
    """

    # Выполнение команды COPY
    cur.execute(copy_query)
    conn.commit()  # Сохранение изменений
    
    # Проверка количества загруженных записей
    cur.execute("SELECT COUNT(*) FROM temp_properties")
    count = cur.fetchone()[0]
    print(f"Данные успешно загружены! Количество загруженных записей: {count}")
    
    # Вывод первых 5 записей для проверки
    if count > 0:
        print("\nПервые 5 записей в таблице temp_properties:")
        cur.execute("SELECT * FROM temp_properties LIMIT 5")
        rows = cur.fetchall()
        for row in rows:
            print(row)
    
except Exception as e:
    print(f"Ошибка при загрузке данных: {e}")
    print("Подробная информация об ошибке:")
    traceback.print_exc()
finally:
    # Закрытие курсора и соединения
    if 'cur' in locals() and cur:
        cur.close()
    if 'conn' in locals() and conn:
        conn.close()
    print("Соединение с базой данных закрыто.")