import os
import re
import shutil
from datetime import datetime

def backup_original_file(file_path):
    """Создает резервную копию оригинального файла"""
    backup_dir = "backups"
    os.makedirs(backup_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = os.path.join(backup_dir, f"api_to_sql_{timestamp}.py.bak")
    
    shutil.copy2(file_path, backup_path)
    print(f"Создана резервная копия: {backup_path}")
    
    return backup_path

def optimize_load_to_sql(file_path):
    """Оптимизирует функцию load_to_sql для более эффективной загрузки данных"""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Находим функцию load_to_sql
    load_to_sql_pattern = re.compile(r'def load_to_sql\(properties_data\):.*?return new_records, updated_records, 0, max_updated_at', re.DOTALL)
    load_to_sql_match = load_to_sql_pattern.search(content)
    
    if not load_to_sql_match:
        print("Ошибка: функция load_to_sql не найдена")
        return False
    
    # Создаем новую оптимизированную версию функции load_to_sql
    new_load_to_sql = """def load_to_sql(properties_data):
    """Загружает данные напрямую в SQL без использования CSV"""
    if not properties_data:
        logger.warning("Нет данных для загрузки в базу данных")
        print("Нет данных для загрузки в базу данных")
        return 0, 0, 1, None
    
    # Подключаемся к базе данных для проверки структуры
    db_config_no_table = {k: v for k, v in DB_CONFIG.items() if k != "table"}
    
    try:
        # Преобразуем данные в безопасные типы Python
        safe_properties_data = []
        
        for item in properties_data:
            safe_item = {}
            for key, value in item.items():
                if isinstance(value, np.integer):
                    safe_item[key] = int(value)  # numpy.int64 -> int
                elif isinstance(value, np.floating):
                    safe_item[key] = float(value)  # numpy.float64 -> float
                elif isinstance(value, np.bool_):
                    safe_item[key] = bool(value)  # numpy.bool_ -> bool
                else:
                    safe_item[key] = value
            safe_properties_data.append(safe_item)
        
        # Используем новый список с безопасными типами
        properties_data = safe_properties_data
        
        # Преобразуем список словарей в DataFrame для дальнейшей обработки
        df = pd.DataFrame(properties_data)
        
        # Логируем информацию о данных
        logger.info(f"Загрузка {len(df)} записей в базу данных")
        print(f"Загрузка {len(df)} записей в базу данных")
        
        # Очищаем текстовые данные от проблемных символов
        for col in df.columns:
            if df[col].dtype == 'object':  # Для строковых столбцов
                df[col] = df[col].apply(clean_text)
        
        # Удаляем дубликаты по (id, updated_at)
        df = df.drop_duplicates(subset=['id', 'updated_at'])
        logger.info(f"После удаления дубликатов осталось {len(df)} записей")
        print(f"После удаления дубликатов осталось {len(df)} записей")
        
        # Создаем новое соединение с autocommit=False специально для транзакции
        conn = psycopg2.connect(**db_config_no_table)
        conn.autocommit = False  # Устанавливаем autocommit=False для транзакции
        cursor = conn.cursor()
        
        try:
            # Получаем список всех колонок в таблице
            cursor.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name = 'bayut_properties' 
                ORDER BY ordinal_position;
            """)
            db_columns = [row[0] for row in cursor.fetchall()]
            logger.info(f"Колонки в таблице: {', '.join(db_columns[:5])}... (всего {len(db_columns)})")
            
            # Находим пересечение между колонками таблицы и колонками DataFrame
            common_columns = list(set(db_columns).intersection(set(df.columns)))
            logger.info(f"Используем {len(common_columns)} общих колонок для вставки данных")
            
            # Создаем SQL запрос с корректными именами колонок
            columns_str = ", ".join(common_columns)
            placeholders = ", ".join(["%s" for _ in common_columns])
            
            # Создаем список кортежей с данными для вставки
            values = []
            for _, row in df.iterrows():
                row_values = [row[col] if col in row else None for col in common_columns]
                values.append(tuple(row_values))
            
            # SQL запрос для вставки или обновления данных
            sql_query = f"""
                INSERT INTO bayut_properties 
                ({columns_str})
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                {", ".join([f"{col} = EXCLUDED.{col}" for col in common_columns if col != 'id'])}
            """
            
            logger.info(f"Выполнение пакетной вставки {len(values)} записей")
            print(f"Выполнение пакетной вставки {len(values)} записей")
            
            # Используем execute_values для пакетной вставки
            psycopg2.extras.execute_values(
                cursor, 
                sql_query, 
                values,
                template=None, 
                page_size=100
            )
            
            # Фиксируем изменения
            conn.commit()
            logger.info("Транзакция успешно зафиксирована")
            print("Транзакция успешно зафиксирована")
            
            # Определяем количество вставленных и обновленных записей
            # Точное количество неизвестно из-за использования пакетной вставки
            new_records = len(values)
            updated_records = 0
            
            logger.info(f"Всего обработано записей: {new_records}")
            print(f"Всего обработано записей: {new_records}")
            
            # Ищем максимальную дату обновления
            max_updated_at = None
            if 'updated_at' in df.columns:
                max_updated_at = df['updated_at'].max()
                logger.info(f"Максимальная дата обновления: {max_updated_at}")
            
            return new_records, updated_records, 0, max_updated_at
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Ошибка при выполнении пакетной вставки: {e}")
            print(f"Ошибка при выполнении пакетной вставки: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
            logger.info("Соединение для транзакции закрыто")
            
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных в SQL: {e}")
        print(f"Ошибка при загрузке данных в SQL: {e}")
        return 0, 0, 1, None"""
    
    # Заменяем функцию load_to_sql на новую оптимизированную версию
    modified_content = content.replace(load_to_sql_match.group(0), new_load_to_sql)
    
    # Записываем изменения в файл
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(modified_content)
    
    print("Функция load_to_sql успешно оптимизирована для более эффективной загрузки данных")
    return True

if __name__ == "__main__":
    file_path = "api_to_sql.py"
    
    if not os.path.exists(file_path):
        print(f"Ошибка: файл {file_path} не найден")
        exit(1)
    
    # Создаем резервную копию
    backup_path = backup_original_file(file_path)
    
    # Оптимизируем функцию load_to_sql
    success = optimize_load_to_sql(file_path)
    
    if success:
        print("Файл успешно обновлен с оптимизированной функцией load_to_sql")
        print(f"Резервная копия сохранена в {backup_path}")
    else:
        print("Произошла ошибка при обновлении файла")
        print(f"Оригинальный файл доступен в {backup_path}") 