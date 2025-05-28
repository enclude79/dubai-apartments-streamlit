import psycopg2
import os
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

# Параметры подключения к базе данных
DB_PARAMS = {
    'dbname': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASSWORD', 'Enclude79'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432')
}

def get_top_updated_records(table_name, limit=10):
    """Получает топ записей с наибольшим количеством различных дат обновления"""
    conn = None
    cursor = None
    
    try:
        # Подключаемся к базе данных
        print(f"Подключение к базе данных с параметрами: {DB_PARAMS}")
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()
        
        # SQL запрос для получения id и количества различных updated_at
        # сортировка по количеству в убывающем порядке и лимит 10 записей
        query = f"""
            SELECT id, COUNT(DISTINCT updated_at) as update_count
            FROM {table_name}
            GROUP BY id
            ORDER BY update_count DESC
            LIMIT {limit};
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        print(f"\nТоп-{limit} записей с наибольшим количеством различных дат обновления:")
        print("=" * 50)
        print(f"{'ID':<15} {'Количество обновлений':<20}")
        print("-" * 50)
        
        for row in results:
            print(f"{row[0]:<15} {row[1]:<20}")
            
        return results
        
    except Exception as e:
        print(f"Ошибка при выполнении запроса: {e}")
        return []
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            print("\nСоединение с базой данных закрыто.")

def check_table_structure(table_name):
    """Проверяет структуру таблицы в базе данных"""
    conn = None
    cursor = None
    
    try:
        # Подключаемся к базе данных
        print(f"Подключение к базе данных с параметрами: {DB_PARAMS}")
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()
        
        # Проверяем существование таблицы
        cursor.execute(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = '{table_name}'
            );
        """)
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            print(f"Таблица {table_name} не существует в базе данных!")
            return
        
        print(f"Таблица {table_name} найдена.")
        
        # Получаем структуру таблицы
        cursor.execute(f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position;
        """)
        
        columns = cursor.fetchall()
        
        print(f"\nСтруктура таблицы {table_name}:")
        print("=" * 60)
        print(f"{'Колонка':<30} {'Тип':<20} {'Nullable':<10}")
        print("-" * 60)
        
        for column in columns:
            print(f"{column[0]:<30} {column[1]:<20} {column[2]:<10}")
        
        # Проверяем наличие updated_at и других важных колонок
        required_columns = ['id', 'price', 'updated_at']
        missing_columns = [col for col in required_columns if col not in [c[0] for c in columns]]
        
        if missing_columns:
            print(f"\nВНИМАНИЕ! Отсутствуют важные колонки: {', '.join(missing_columns)}")
        else:
            print("\nВсе необходимые колонки присутствуют.")
        
        # Получаем количество записей в таблице
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        print(f"\nКоличество записей в таблице: {row_count}")
        
        # Проверяем распределение updated_at, если колонка существует
        if 'updated_at' in [c[0] for c in columns]:
            cursor.execute(f"""
                SELECT 
                    COUNT(*) as total,
                    COUNT(updated_at) as with_updated_at,
                    COUNT(DISTINCT updated_at) as unique_dates
                FROM {table_name}
            """)
            stats = cursor.fetchone()
            
            print(f"\nСтатистика по колонке updated_at:")
            print(f"Всего записей: {stats[0]}")
            print(f"Записей с непустым updated_at: {stats[1]} ({stats[1]/stats[0]*100 if stats[0] > 0 else 0:.2f}%)")
            print(f"Уникальных дат обновления: {stats[2]}")
            
            # Получаем минимальную и максимальную даты
            cursor.execute(f"""
                SELECT 
                    MIN(updated_at) as min_date,
                    MAX(updated_at) as max_date
                FROM {table_name}
                WHERE updated_at IS NOT NULL
            """)
            date_range = cursor.fetchone()
            if date_range[0] and date_range[1]:
                print(f"Диапазон дат: с {date_range[0]} по {date_range[1]}")
        
        # Проверяем наличие записей с несколькими обновлениями (важно для price_volatility_reporter.py)
        cursor.execute(f"""
            WITH id_counts AS (
                SELECT id, COUNT(*) as update_count
                FROM {table_name}
                GROUP BY id
                HAVING COUNT(*) > 1
            )
            SELECT COUNT(*) FROM id_counts
        """)
        multi_update_count = cursor.fetchone()[0]
        print(f"\nКоличество ID с несколькими записями (обновлениями): {multi_update_count}")
        
        if multi_update_count == 0:
            print("ВНИМАНИЕ! В базе нет объектов с историей обновлений. Это может вызывать проблемы в скриптах, анализирующих изменения цен.")
        
        # Проверим конкретные ID, которые упоминались в price_volatility_reporter.py
        target_ids = [8303238, 8417332]
        for target_id in target_ids:
            cursor.execute(f"""
                SELECT id, price, updated_at
                FROM {table_name}
                WHERE id = {target_id}
                ORDER BY updated_at
            """)
            rows = cursor.fetchall()
            
            print(f"\nЗаписи для ID {target_id}:")
            if not rows:
                print(f"ID {target_id} не найден в базе данных.")
            else:
                print(f"Найдено {len(rows)} записей:")
                for row in rows:
                    print(f"  ID: {row[0]}, Цена: {row[1]}, Дата обновления: {row[2]}")
                
                if len(rows) >= 2:
                    # Рассчитываем процентное изменение цены между последними двумя записями
                    latest = rows[-1]
                    previous = rows[-2]
                    if previous[1] and previous[1] != 0:
                        pct_change = ((latest[1] - previous[1]) / previous[1]) * 100
                        print(f"  Изменение цены: {pct_change:.2f}%")
                    else:
                        print("  Невозможно рассчитать изменение цены (предыдущая цена равна 0 или NULL)")
        
    except Exception as e:
        print(f"Ошибка при проверке структуры таблицы: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            print("\nСоединение с базой данных закрыто.")

if __name__ == "__main__":
    print("Проверка структуры базы данных...")
    check_table_structure('bayut_properties')
    # Вызов новой функции для получения топ-10 записей с наибольшим количеством обновлений
    get_top_updated_records('bayut_properties', 10) 