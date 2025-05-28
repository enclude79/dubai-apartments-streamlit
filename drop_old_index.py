import psycopg2

DB_CONFIG = {
    'dbname': 'postgres',
    'user': 'admin',
    'password': 'Enclude79',
    'host': 'localhost',
    'port': '5432'
}

def drop_old_index():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.set_client_encoding('UTF8')
        cursor = conn.cursor()
        cursor.execute("DROP INDEX IF EXISTS idx_bayut_properties_id_updated_at;")
        conn.commit()
        print("Старый уникальный индекс idx_bayut_properties_id_updated_at успешно удалён (если существовал).")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Ошибка при удалении индекса: {e}")

if __name__ == "__main__":
    drop_old_index() 