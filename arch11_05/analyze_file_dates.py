import pandas as pd
import sys
import os
from datetime import datetime

def analyze_csv_file(csv_path):
    """Анализирует CSV-файл и выводит информацию о датах и объеме данных"""
    try:
        # Проверяем существование файла
        if not os.path.exists(csv_path):
            print(f"Файл {csv_path} не существует")
            return
            
        # Загружаем CSV в DataFrame
        df = pd.read_csv(csv_path, encoding='utf-8-sig')
        
        # Выводим информацию о файле
        print(f"Файл: {csv_path}")
        print(f"Размер файла: {os.path.getsize(csv_path) / (1024*1024):.2f} МБ")
        print(f"Количество записей: {len(df)}")
        
        # Анализируем даты (если есть соответствующие колонки)
        date_columns = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower() or 'Unnamed: 12' in col or 'Unnamed: 13' in col]
        for col in date_columns:
            if col in df.columns:
                try:
                    print(f"\nКолонка: {col}")
                    print(f"  Минимальная дата: {df[col].min()}")
                    print(f"  Максимальная дата: {df[col].max()}")
                    
                    # Преобразуем строковые даты в datetime для более точного анализа
                    try:
                        df[f'{col}_dt'] = pd.to_datetime(df[col])
                        min_date = df[f'{col}_dt'].min()
                        max_date = df[f'{col}_dt'].max()
                        
                        # Вычисляем разницу между датами
                        date_diff = max_date - min_date
                        
                        print(f"  Диапазон дат: {min_date.strftime('%Y-%m-%d %H:%M:%S')} - {max_date.strftime('%Y-%m-%d %H:%M:%S')}")
                        print(f"  Период данных: {date_diff.days} дней, {date_diff.seconds//3600} часов")
                    except Exception as e:
                        print(f"  Ошибка при анализе дат: {e}")
                except Exception as e:
                    print(f"  Ошибка при обработке колонки {col}: {e}")
        
        # Проверяем, есть ли колонка с ID
        if 'id' in df.columns:
            print(f"\nУникальных ID: {df['id'].nunique()} из {len(df)}")
            
        return df
    except Exception as e:
        print(f"Ошибка при анализе файла: {e}")
        return None

def compare_files(file1, file2):
    """Сравнивает два CSV-файла"""
    try:
        # Загружаем оба файла
        df1 = pd.read_csv(file1, encoding='utf-8-sig')
        df2 = pd.read_csv(file2, encoding='utf-8-sig')
        
        # Выводим основную информацию
        print(f"\n=== Сравнение файлов ===")
        print(f"Файл 1: {file1} ({len(df1)} записей)")
        print(f"Файл 2: {file2} ({len(df2)} записей)")
        
        # Проверяем, есть ли колонка с ID
        if 'id' in df1.columns and 'id' in df2.columns:
            # Находим общие ID
            common_ids = set(df1['id']).intersection(set(df2['id']))
            print(f"Общих ID: {len(common_ids)}")
            print(f"Уникальных ID в файле 1: {len(set(df1['id']) - set(df2['id']))}")
            print(f"Уникальных ID в файле 2: {len(set(df2['id']) - set(df1['id']))}")
            
            # Вычисляем процент совпадения
            overlap_percent = (len(common_ids) / min(len(df1), len(df2))) * 100
            print(f"Процент совпадения данных: {overlap_percent:.2f}%")
            
            # Если файлы сильно совпадают, выводим предупреждение
            if overlap_percent > 90:
                print(f"ВНИМАНИЕ: Файлы имеют высокий процент совпадения данных!")
        
    except Exception as e:
        print(f"Ошибка при сравнении файлов: {e}")

def main():
    """Основная функция для анализа файлов"""
    if len(sys.argv) < 2:
        print("Использование: python analyze_file_dates.py <путь_к_файлу> [путь_к_файлу_для_сравнения]")
        return
        
    # Анализируем первый файл
    csv_path = sys.argv[1]
    analyze_csv_file(csv_path)
    
    # Если указан второй файл, сравниваем их
    if len(sys.argv) > 2:
        compare_files(csv_path, sys.argv[2])

if __name__ == "__main__":
    main() 