import os
import psycopg2
import time
import sys
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Параметры подключения к БД
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASSWORD', 'Enclude79'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'options': '-c statement_timeout=3000'  # Ограничиваем время запроса 3 секундами
}

def check_blocking_locks():
    """Проверяет блокировки в базе данных"""
    print("Проверка блокировок в базе данных...")
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cur = conn.cursor()
        
        # Находим блокировки, которые ожидают выполнения
        cur.execute("""
            SELECT 
                blocked_locks.pid AS blocked_pid,
                blocked_activity.usename AS blocked_user,
                blocking_locks.pid AS blocking_pid,
                blocking_activity.usename AS blocking_user,
                blocked_activity.query AS blocked_statement,
                blocking_activity.query AS blocking_statement
            FROM pg_catalog.pg_locks blocked_locks
            JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
            JOIN pg_catalog.pg_locks blocking_locks 
                ON blocking_locks.locktype = blocked_locks.locktype
                AND blocking_locks.DATABASE IS NOT DISTINCT FROM blocked_locks.DATABASE
                AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
                AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
                AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
                AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
                AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
                AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
                AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
                AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
                AND blocking_locks.pid != blocked_locks.pid
            JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
            WHERE NOT blocked_locks.GRANTED;
        """)
        
        blocking_locks = cur.fetchall()
        
        if not blocking_locks:
            print("Блокировок не обнаружено")
        else:
            print(f"Найдено {len(blocking_locks)} блокировок")
            
            for lock in blocking_locks:
                print(f"Блокирующий PID: {lock[2]}, заблокированный PID: {lock[0]}")
                print(f"Блокирующий запрос: {lock[5][:100]}...")
                print(f"Заблокированный запрос: {lock[4][:100]}...")
                print("-" * 80)
                
            # Убиваем блокирующие процессы
            for lock in blocking_locks:
                blocking_pid = lock[2]
                print(f"Завершение блокирующего процесса с PID {blocking_pid}")
                try:
                    cur.execute(f"SELECT pg_terminate_backend({blocking_pid})")
                except Exception as e:
                    print(f"Ошибка при завершении процесса {blocking_pid}: {e}")
        
        return blocking_locks
    
    except Exception as e:
        print(f"Ошибка при проверке блокировок: {e}")
        return []
    finally:
        if 'cur' in locals() and cur:
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()

def kill_all_processes_on_table():
    """Завершает все процессы, связанные с таблицей bayut_properties"""
    print("Поиск и завершение ВСЕХ процессов, связанных с таблицей bayut_properties...")
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cur = conn.cursor()
        
        # Находим все процессы, связанные с таблицей (даже не INSERT)
        cur.execute("""
            SELECT pid, state, query, query_start 
            FROM pg_stat_activity 
            WHERE query ILIKE '%bayut_properties%' 
            AND pid <> pg_backend_pid()
            ORDER BY query_start
        """)
        
        processes = cur.fetchall()
        
        if not processes:
            print("Процессов, связанных с таблицей bayut_properties, не найдено")
            return 0
            
        print(f"Найдено {len(processes)} процессов, связанных с таблицей bayut_properties")
        
        # Завершаем каждый процесс принудительно
        killed_count = 0
        for pid, state, query, query_start in processes:
            print(f"Процесс {pid}, состояние: {state}, запущен: {query_start}")
            print(f"Запрос: {query[:100]}...")
            
            try:
                # Пытаемся сначала отменить
                print(f"Отмена запроса с PID {pid}...")
                cur.execute(f"SELECT pg_cancel_backend({pid})")
                time.sleep(0.5)  # Даем время на отмену
                
                # Затем в любом случае убиваем процесс
                print(f"Принудительное завершение процесса с PID {pid}...")
                cur.execute(f"SELECT pg_terminate_backend({pid})")
                killed_count += 1
            except Exception as e:
                print(f"Ошибка при завершении процесса {pid}: {e}")
        
        print(f"Завершено {killed_count} процессов")
        return killed_count
        
    except Exception as e:
        print(f"Ошибка при завершении процессов: {e}")
        return 0
    finally:
        if 'cur' in locals() and cur:
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()

def check_locks_on_table():
    """Проверяет блокировки на таблице bayut_properties"""
    print("Проверка блокировок на таблице bayut_properties...")
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cur = conn.cursor()
        
        # Сначала найдем OID таблицы
        cur.execute("SELECT oid FROM pg_class WHERE relname = 'bayut_properties'")
        result = cur.fetchone()
        
        if not result:
            print("Таблица bayut_properties не найдена")
            return []
            
        table_oid = result[0]
        
        # Находим все блокировки на таблице
        cur.execute("""
            SELECT 
                l.pid, 
                a.usename, 
                a.query_start,
                l.mode, 
                l.granted,
                a.query
            FROM pg_locks l
            JOIN pg_stat_activity a ON l.pid = a.pid
            WHERE l.relation = %s
            ORDER BY l.granted DESC, l.pid
        """, (table_oid,))
        
        locks = cur.fetchall()
        
        if not locks:
            print("Блокировок на таблице bayut_properties не обнаружено")
        else:
            print(f"Найдено {len(locks)} блокировок на таблице bayut_properties")
            
            for lock in locks:
                print(f"PID: {lock[0]}, пользователь: {lock[1]}, старт: {lock[2]}")
                print(f"Тип блокировки: {lock[3]}, выполнена: {lock[4]}")
                print(f"Запрос: {lock[5][:100]}...")
                print("-" * 80)
        
        return locks
    
    except Exception as e:
        print(f"Ошибка при проверке блокировок на таблице: {e}")
        return []
    finally:
        if 'cur' in locals() and cur:
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()

def analyze_and_vacuum_table():
    """Выполняет ANALYZE и VACUUM на таблице"""
    print("Выполнение ANALYZE и VACUUM на таблице bayut_properties...")
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True  # Для VACUUM необходим autocommit
        cur = conn.cursor()
        
        print("Выполнение ANALYZE...")
        start_time = time.time()
        cur.execute("ANALYZE bayut_properties")
        print(f"ANALYZE выполнен за {time.time() - start_time:.2f} секунд")
        
        print("Выполнение VACUUM...")
        start_time = time.time()
        cur.execute("VACUUM bayut_properties")
        print(f"VACUUM выполнен за {time.time() - start_time:.2f} секунд")
        
        return True
    except Exception as e:
        print(f"Ошибка при выполнении ANALYZE/VACUUM: {e}")
        return False
    finally:
        if 'cur' in locals() and cur:
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()

def test_minimal_insert():
    """Выполняет тестовую вставку данных"""
    print("\nВыполнение тестовой вставки данных...")
    
    # Жестко заданные значения для вставки
    test_id = int(time.time())
    test_title = "SUPER_TEST"
    test_price = 1000000
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cur = conn.cursor()
        
        print(f"Выполнение INSERT: id={test_id}, title='{test_title}', price={test_price}")
        start_time = time.time()
        
        cur.execute(
            "INSERT INTO bayut_properties (id, title, price) VALUES (%s, %s, %s)",
            (test_id, test_title, test_price)
        )
        
        end_time = time.time()
        print(f"Запрос выполнен за {end_time - start_time:.2f} секунд")
        
        # Проверка, что запись существует
        cur.execute("SELECT COUNT(*) FROM bayut_properties WHERE id = %s", (test_id,))
        count = cur.fetchone()[0]
        
        if count > 0:
            print(f"Запись с ID {test_id} успешно вставлена")
            return True
        else:
            print(f"Запись с ID {test_id} не найдена")
            return False
            
    except Exception as e:
        print(f"Ошибка при тестовой вставке: {e}")
        return False
    finally:
        if 'cur' in locals() and cur:
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()

def show_table_stats():
    """Показывает статистику по таблице"""
    print("Статистика по таблице bayut_properties:")
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cur = conn.cursor()
        
        # Проверяем количество записей
        cur.execute("SELECT COUNT(*) FROM bayut_properties")
        count = cur.fetchone()[0]
        print(f"Всего записей: {count}")
        
        # Проверяем статистику автовакуума
        cur.execute("""
            SELECT 
                last_vacuum, 
                last_autovacuum, 
                last_analyze, 
                last_autoanalyze 
            FROM pg_stat_user_tables 
            WHERE relname = 'bayut_properties'
        """)
        stats = cur.fetchone()
        
        if stats:
            print(f"Последний VACUUM: {stats[0]}")
            print(f"Последний автоматический VACUUM: {stats[1]}")
            print(f"Последний ANALYZE: {stats[2]}")
            print(f"Последний автоматический ANALYZE: {stats[3]}")
        
        return True
    except Exception as e:
        print(f"Ошибка при получении статистики: {e}")
        return False
    finally:
        if 'cur' in locals() and cur:
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()

def get_table_size():
    """Получает размер таблицы"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cur = conn.cursor()
        
        # Размер таблицы
        cur.execute("SELECT pg_size_pretty(pg_total_relation_size('bayut_properties'))")
        total_size = cur.fetchone()[0]
        
        cur.execute("SELECT pg_size_pretty(pg_relation_size('bayut_properties'))")
        table_size = cur.fetchone()[0]
        
        cur.execute("SELECT pg_size_pretty(pg_indexes_size('bayut_properties'))")
        index_size = cur.fetchone()[0]
        
        print(f"Общий размер: {total_size}")
        print(f"Размер таблицы: {table_size}")
        print(f"Размер индексов: {index_size}")
        
        return True
    except Exception as e:
        print(f"Ошибка при получении размера таблицы: {e}")
        return False
    finally:
        if 'cur' in locals() and cur:
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()

def fix_everything():
    """Последовательно выполняет все шаги для устранения проблемы"""
    print("=== НАЧАЛО ОПЕРАЦИИ ПОЛНОГО ВОССТАНОВЛЕНИЯ ===")
    
    # Шаг 1: Проверяем блокирующие процессы
    print("\n=== ШАГ 1: Проверка блокировок ===")
    check_blocking_locks()
    
    # Шаг 2: Проверяем блокировки на таблице
    print("\n=== ШАГ 2: Проверка блокировок на таблице ===")
    check_locks_on_table()
    
    # Шаг 3: Завершаем все процессы на таблице
    print("\n=== ШАГ 3: Завершение всех процессов на таблице ===")
    kill_all_processes_on_table()
    
    # Шаг 4: Повторная проверка блокировок
    print("\n=== ШАГ 4: Повторная проверка блокировок ===")
    check_locks_on_table()
    
    # Шаг 5: Анализ и очистка таблицы
    print("\n=== ШАГ 5: ANALYZE и VACUUM таблицы ===")
    analyze_and_vacuum_table()
    
    # Шаг 6: Статистика по таблице
    print("\n=== ШАГ 6: Статистика по таблице ===")
    show_table_stats()
    get_table_size()
    
    # Шаг 7: Тестовая вставка
    print("\n=== ШАГ 7: Тестовая вставка ===")
    result = test_minimal_insert()
    
    print("\n=== ИТОГИ ОПЕРАЦИИ ===")
    if result:
        print("Операция УСПЕШНА! Таблица работает корректно.")
    else:
        print("Операция НЕУДАЧНА! Проблемы с таблицей сохраняются.")
    
    return result

if __name__ == "__main__":
    fix_everything() 