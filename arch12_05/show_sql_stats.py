import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_PARAMS = {
    'dbname': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'Admin'),
    'password': os.getenv('DB_PASSWORD', 'Enclude79'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432')
}

def show_stats():
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    cur.execute('SELECT COUNT(*), MAX(created_at) FROM bayut_properties')
    count, max_date = cur.fetchone()
    print(f'Всего записей в bayut_properties: {count}')
    print(f'Последняя дата created_at: {max_date}')
    cur.close()
    conn.close()

if __name__ == "__main__":
    show_stats() 