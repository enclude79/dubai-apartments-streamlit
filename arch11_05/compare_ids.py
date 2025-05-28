import pandas as pd
import sys

def compare_property_ids(file1, file2):
    """Сравнивает ID объектов недвижимости между двумя CSV файлами"""
    print(f"Сравнение ID между файлами:")
    print(f"Файл 1: {file1}")
    print(f"Файл 2: {file2}")
    
    try:
        # Загружаем первый файл
        df1 = pd.read_csv(file1, encoding='utf-8-sig')
        print(f"Файл 1: {len(df1)} записей")
        
        # Определяем колонку с ID
        id_col1 = 'id' if 'id' in df1.columns else 'ID' if 'ID' in df1.columns else None
        if not id_col1:
            print("Ошибка: не найдена колонка с ID в первом файле")
            return
        
        # Загружаем второй файл
        df2 = pd.read_csv(file2, encoding='utf-8-sig')
        print(f"Файл 2: {len(df2)} записей")
        
        # Определяем колонку с ID
        id_col2 = 'id' if 'id' in df2.columns else 'ID' if 'ID' in df2.columns else None
        if not id_col2:
            print("Ошибка: не найдена колонка с ID во втором файле")
            return
        
        # Получаем множества ID из обоих файлов
        ids1 = set(df1[id_col1])
        ids2 = set(df2[id_col2])
        
        # Находим пересечение (общие ID)
        common_ids = ids1.intersection(ids2)
        common_count = len(common_ids)
        
        # Находим уникальные ID для каждого файла
        unique_ids1 = ids1 - ids2
        unique_ids2 = ids2 - ids1
        
        # Выводим статистику
        print(f"\nОбщих ID: {common_count}")
        print(f"Уникальных ID только в файле 1: {len(unique_ids1)}")
        print(f"Уникальных ID только в файле 2: {len(unique_ids2)}")
        
        # Вычисляем процент перекрытия
        overlap_percent1 = (common_count / len(ids1)) * 100 if ids1 else 0
        overlap_percent2 = (common_count / len(ids2)) * 100 if ids2 else 0
        
        print(f"\nФайл 1 содержит {overlap_percent1:.2f}% ID из файла 2")
        print(f"Файл 2 содержит {overlap_percent2:.2f}% ID из файла 1")
        
        # Если процент перекрытия высокий, выводим предупреждение
        if overlap_percent1 > 80 or overlap_percent2 > 80:
            print("\nВНИМАНИЕ: Высокий процент совпадения ID между файлами!")
            
        if common_count > 0:
            # Выводим несколько примеров общих ID
            print("\nПримеры общих ID:")
            for id_val in list(common_ids)[:5]:  # Первые 5 общих ID
                print(id_val)
            
        # Анализируем даты в общих ID, если это возможно
        date_columns1 = [col for col in df1.columns if 'date' in str(col).lower() or 'time' in str(col).lower() or 'Unnamed: 12' == col or 'Дата публикации' == col]
        date_columns2 = [col for col in df2.columns if 'date' in str(col).lower() or 'time' in str(col).lower() or 'Unnamed: 12' == col or 'Дата публикации' == col]
        
        if common_count > 0 and date_columns1 and date_columns2:
            print("\nАнализ дат для общих ID:")
            
            # Создаем новые DataFrame только с общими ID
            df1_common = df1[df1[id_col1].isin(common_ids)]
            df2_common = df2[df2[id_col2].isin(common_ids)]
            
            # Сортируем по ID для правильного сопоставления
            df1_common = df1_common.sort_values(by=id_col1)
            df2_common = df2_common.sort_values(by=id_col2)
            
            # Сравниваем даты для нескольких примеров
            for i, (date_col1, date_col2) in enumerate(zip(date_columns1[:2], date_columns2[:2])):
                print(f"\nСравнение даты в колонках: {date_col1} (файл 1) и {date_col2} (файл 2)")
                
                # Берем первые 5 примеров
                for j in range(min(5, df1_common.shape[0])):
                    id_val = df1_common.iloc[j][id_col1]
                    date1 = df1_common.iloc[j][date_col1]
                    
                    # Находим соответствующую запись во втором файле
                    date2 = df2_common[df2_common[id_col2] == id_val][date_col2].values[0]
                    
                    print(f"  ID {id_val}: {date1} - {date2}")
    
    except Exception as e:
        print(f"Ошибка при сравнении файлов: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Использование: python compare_ids.py <файл1> <файл2>")
    else:
        compare_property_ids(sys.argv[1], sys.argv[2]) 