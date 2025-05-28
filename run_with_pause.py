import os
import sys
import subprocess
import time

def run_script_with_pause():
    """Запускает api_to_sql.py и делает паузу в конце"""
    
    print("Запуск скрипта api_to_sql.py с параметрами --limit 10 --no-csv")
    print("=" * 60)
    
    # Запуск скрипта
    try:
        result = subprocess.run(
            ["python", "api_to_sql.py", "--limit", "10", "--no-csv"], 
            capture_output=True, 
            text=True,
            check=True
        )
        
        # Вывод stdout
        print("ВЫВОД СКРИПТА:")
        print("-" * 60)
        print(result.stdout)
        
        # Вывод stderr, если есть
        if result.stderr:
            print("ОШИБКИ:")
            print("-" * 60)
            print(result.stderr)
        
        print("=" * 60)
        print(f"Скрипт завершился с кодом: {result.returncode}")
        
    except subprocess.CalledProcessError as e:
        print(f"Скрипт завершился с ошибкой (код {e.returncode}):")
        print("-" * 60)
        print(e.stdout)
        print("-" * 60)
        print(e.stderr)
    except Exception as e:
        print(f"Ошибка при запуске скрипта: {e}")
    
    # Ожидание ввода пользователя
    input("\nНажмите Enter для выхода...")

if __name__ == "__main__":
    run_script_with_pause() 