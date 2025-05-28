import os
import sys
import subprocess
import time
import psycopg2

def check_postgres_running():
    """Проверяет, запущен ли PostgreSQL, пытаясь установить соединение"""
    try:
        # Параметры подключения
        conn = psycopg2.connect(
            dbname='postgres',
            user='admin',
            password='Enclude79',
            host='localhost',
            port='5432',
            connect_timeout=3  # Таймаут 3 секунды
        )
        conn.close()
        return True
    except Exception:
        return False

def start_postgres():
    """Запускает PostgreSQL сервер"""
    pg_ctl_path = r"C:\Users\Administrator\anaconda3\Library\bin\pg_ctl.exe"
    data_dir = r"C:\PostgreSQLData"
    
    if not os.path.exists(pg_ctl_path):
        print(f"ОШИБКА: Не найден исполняемый файл pg_ctl по пути: {pg_ctl_path}")
        return False
    
    if not os.path.exists(data_dir):
        print(f"ОШИБКА: Не найдена директория данных PostgreSQL: {data_dir}")
        return False
    
    print("Запуск PostgreSQL...")
    
    try:
        # Запуск PostgreSQL
        subprocess.run([pg_ctl_path, "-D", data_dir, "start"], 
                       check=True, 
                       stdout=subprocess.PIPE, 
                       stderr=subprocess.PIPE,
                       text=True)
        
        # Ждем некоторое время для запуска сервера
        print("Ожидание запуска сервера...")
        for i in range(10):
            if check_postgres_running():
                print("PostgreSQL успешно запущен!")
                return True
            time.sleep(2)
        
        print("Превышено время ожидания запуска PostgreSQL.")
        return False
    
    except subprocess.CalledProcessError as e:
        print(f"Ошибка при запуске PostgreSQL: {e}")
        print(f"Вывод: {e.output}")
        print(f"Ошибка: {e.stderr}")
        return False
    
    except Exception as e:
        print(f"Неизвестная ошибка: {e}")
        return False

def main():
    """Основная функция"""
    print("Проверка статуса PostgreSQL...")
    
    if check_postgres_running():
        print("PostgreSQL уже запущен!")
    else:
        print("PostgreSQL не запущен. Попытка запуска...")
        if start_postgres():
            print("\nБаза данных готова к использованию!")
        else:
            print("\nНе удалось запустить PostgreSQL.")
            print("Проверьте пути в скрипте и права доступа.")
    
    input("\nНажмите Enter для выхода...")

if __name__ == "__main__":
    main() 