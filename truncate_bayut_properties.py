import psycopg2

DB_CONFIG = {
    'dbname': 'postgres',
    'user': 'admin',
    'password': 'Enclude79',
    'host': 'localhost',
    'port': '5432'
}

def truncate_table():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.set_client_encoding('UTF8')
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE bayut_properties;")
        conn.commit()
        print("Таблица bayut_properties успешно очищена.")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Ошибка при очистке таблицы: {e}")

if __name__ == "__main__":
    truncate_table() 