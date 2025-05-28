def load_to_sql(properties_data):
    """Загружает данные напрямую в SQL без использования CSV"""
    if not properties_data:
        logger.warning("Нет данных для загрузки в базу данных")
        print("Нет данных для загрузки в базу данных")
        return 0, 0, 1, None
    
    # Подключаемся к базе данных для проверки структуры
    db = DatabaseConnection(DB_CONFIG)
    if not db.connect():
        logger.error("ОТЛАДКА: Ошибка подключения к базе данных в load_to_sql")
        print("ОТЛАДКА: Ошибка подключения к базе данных в load_to_sql")
        return 0, 0, 1, None
    
    conn = None
    cursor = None
    
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
        logger.info(f"ОТЛАДКА: После удаления дубликатов осталось {len(df)} записей")
        
        # Создаем новое соединение с autocommit=False специально для транзакции
        conn = psycopg2.connect(
            dbname=DB_CONFIG['dbname'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port']
        )
        conn.autocommit = False  # Устанавливаем autocommit=False для новой транзакции
        cursor = conn.cursor()
        
        try:
            # Сохраняем ID обработанных записей
            processed_ids = []
            
            # Проверяем существование таблицы
            cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'bayut_properties');")
            table_exists = cursor.fetchone()[0]
            logger.info(f"ОТЛАДКА: Таблица bayut_properties существует: {table_exists}")
            print(f"ОТЛАДКА: Таблица bayut_properties существует: {table_exists}")
            
            if not table_exists:
                logger.error("ОТЛАДКА: Таблица bayut_properties не существует! Необходимо создать таблицу.")
                print("ОТЛАДКА: Таблица bayut_properties не существует! Необходимо создать таблицу.")
                return 0, 0, 1, None
            
            # Получаем список всех колонок
            cursor.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name = 'bayut_properties' 
                ORDER BY ordinal_position;
            """)
            db_columns = [row[0] for row in cursor.fetchall()]
            logger.info(f"ОТЛАДКА: Колонки в таблице: {db_columns}")
            
            # Создаем список кортежей с данными для вставки
            values = []
            for _, row in df.iterrows():
                property_id = int(row['id']) if pd.notna(row['id']) else None
                title = str(row['title']) if pd.notna(row['title']) else None
                price = float(row['price']) if pd.notna(row['price']) else None
                updated_at = row['updated_at'] if pd.notna(row['updated_at']) else None
                
                values.append((property_id, title, price, updated_at))
                processed_ids.append(property_id)
            
            # SQL запрос для вставки или обновления данных
            sql_query = """
                INSERT INTO bayut_properties 
                (id, title, price, updated_at)
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                title = EXCLUDED.title,
                price = EXCLUDED.price,
                updated_at = EXCLUDED.updated_at
            """
            
            logger.info(f"ОТЛАДКА: Выполнение пакетной вставки {len(values)} записей")
            print(f"ОТЛАДКА: Выполнение пакетной вставки {len(values)} записей")
            
            try:
                # Используем execute_values для пакетной вставки
                psycopg2.extras.execute_values(
                    cursor, 
                    sql_query, 
                    values,
                    template=None, 
                    page_size=100
                )
                
                logger.info("ОТЛАДКА: Пакетная вставка успешно выполнена")
                print("ОТЛАДКА: Пакетная вставка успешно выполнена")
                
                # Фиксируем изменения
                conn.commit()
                logger.info("ОТЛАДКА: Изменения зафиксированы успешно")
                print("ОТЛАДКА: Изменения зафиксированы успешно")
                
                # Определяем количество вставленных и обновленных записей
                # Так как мы использовали пакетную вставку, точное количество неизвестно
                new_records = len(values)
                updated_records = 0
                
                logger.info(f"Всего обработано записей: {len(values)}")
                logger.info(f"Добавлено записей: {new_records}")
                
                print(f"Всего обработано записей: {len(values)}")
                print(f"Добавлено записей: {new_records}")
                
                # Ищем максимальную дату обновления
                max_updated_at = None
                if 'updated_at' in df.columns:
                    max_updated_at = df['updated_at'].max()
                    logger.info(f"ОТЛАДКА: Максимальная дата обновления: {max_updated_at}")
                
                return new_records, updated_records, 0, max_updated_at
            
            except Exception as e:
                conn.rollback()
                logger.error(f"ОТЛАДКА: Ошибка при выполнении пакетной вставки: {e}")
                print(f"ОТЛАДКА: Ошибка при выполнении пакетной вставки: {e}")
                raise
        finally:
            cursor.close()
            conn.close()
            logger.info("ОТЛАДКА: Закрыто соединение для транзакции")
            print("ОТЛАДКА: Закрыто соединение для транзакции")
    
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных в SQL: {e}")
        print(f"Ошибка при загрузке данных в SQL: {e}")
        return 0, 0, 1, None 