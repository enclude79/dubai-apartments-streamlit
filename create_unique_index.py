import psycopg2

DB_CONFIG = {
    'dbname': 'postgres',
    'user': 'admin',
    'password': 'Enclude79',
    'host': 'localhost',
    'port': '5432'
}

def create_unique_index():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.set_client_encoding('UTF8')
        cursor = conn.cursor()
        cursor.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_bayut_properties_id_updated_at_created_at ON bayut_properties(id, updated_at, created_at);
        """)
        conn.commit()
        print("Уникальный индекс по id, updated_at, created_at успешно создан или уже существует.")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Ошибка при создании индекса: {e}")

if __name__ == "__main__":
    create_unique_index() 