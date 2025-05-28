import pandas as pd
import sys

def analyze_file(file_path):
    print(f"Анализ файла: {file_path}")
    df = pd.read_csv(file_path, encoding='utf-8-sig')
    print(f"Количество записей: {len(df)}")
    
    # Анализируем даты в колонках Unnamed: 12 и Unnamed: 13
    for col in ['Unnamed: 12', 'Unnamed: 13']:
        if col in df.columns:
            print(f"\nКолонка: {col}")
            try:
                print(f"Минимальная дата: {df[col].min()}")
                print(f"Максимальная дата: {df[col].max()}")
                
                # Преобразуем в datetime
                df[f'{col}_dt'] = pd.to_datetime(df[col])
                min_date = df[f'{col}_dt'].min()
                max_date = df[f'{col}_dt'].max()
                date_diff = max_date - min_date
                
                print(f"Период данных: {date_diff.days} дней, {date_diff.seconds//3600} часов")
            except Exception as e:
                print(f"Ошибка при анализе дат: {e}")
    
    # Проверяем ID
    if 'id' in df.columns:
        unique_ids = df['id'].nunique()
        print(f"\nУникальных ID: {unique_ids} из {len(df)}")
        if unique_ids < len(df):
            print("Есть дубликаты ID!")
    
    return df

if __name__ == "__main__":
    if len(sys.argv) > 1:
        analyze_file(sys.argv[1])
    else:
        print("Укажите путь к файлу в качестве аргумента") 