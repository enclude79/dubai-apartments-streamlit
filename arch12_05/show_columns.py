import psycopg2
import os
from load_env import load_environment_variables

# Загружаем переменные окружения
load_environment_variables()

# Параметры подключения к базе данных
DB_PARAMS = {
    'dbname': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASSWORD', 'Enclude79'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432')
}

# Подключаемся к базе данных
conn = psycopg2.connect(**DB_PARAMS)
cur = conn.cursor()

# Выполняем запрос для получения списка полей
cur.execute('''
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = 'bayut_properties'
    ORDER BY ordinal_position
''')

# Выводим результаты
print("Название поля | Тип данных")
print("-" * 40)
for row in cur.fetchall():
    print(f"{row[0]:15} | {row[1]}")

# Закрываем соединение
conn.close() 