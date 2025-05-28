import pandas as pd
import sys

def check_columns(file_path):
    print(f"Проверка колонок в файле: {file_path}")
    try:
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        print("Колонки в файле:")
        for i, col in enumerate(df.columns):
            print(f"{i}: {col}")
            
        print(f"\nРазмер файла: {df.shape}")
        print(f"Количество записей: {len(df)}")
        
        # Пример первых строк
        print("\nПример первых 3 строк:")
        print(df.head(3))
        
        return df
    except Exception as e:
        print(f"Ошибка при чтении файла: {e}")
        return None

if __name__ == "__main__":
    if len(sys.argv) > 1:
        check_columns(sys.argv[1])
    else:
        print("Укажите путь к файлу в качестве аргумента") 