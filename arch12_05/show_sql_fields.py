import psycopg2

# Параметры базы данных
DB_CONFIG = {
    'dbname': 'postgres',
    'user': 'admin',
    'password': 'Enclude79',
    'host': 'localhost',
    'port': '5432'
}

def get_table_fields():
    """Получает и выводит список полей таблицы bayut_properties в SQL"""
    conn = None
    try:
        # Подключаемся к базе данных
        conn = psycopg2.connect(**DB_CONFIG)
        conn.set_client_encoding('UTF8')
        cursor = conn.cursor()
        
        # Получаем список колонок с типами данных и другой информацией
        cursor.execute("""
            SELECT 
                column_name, 
                data_type, 
                character_maximum_length,
                is_nullable
            FROM information_schema.columns
            WHERE table_name = 'bayut_properties'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        
        # Выводим заголовок таблицы
        print("\n===== Структура таблицы bayut_properties в SQL =====")
        print("{:<3} {:<20} {:<15} {:<10} {:<10}".format("№", "Название поля", "Тип данных", "Размер", "NULL"))
        print("-" * 60)
        
        # Выводим информацию о колонках
        for i, (col_name, data_type, max_length, is_nullable) in enumerate(columns):
            size = str(max_length) if max_length is not None else "-"
            nullable = "Да" if is_nullable == "YES" else "Нет"
            print("{:<3} {:<20} {:<15} {:<10} {:<10}".format(i+1, col_name, data_type, size, nullable))
        
        # Получаем общее количество записей
        cursor.execute("SELECT COUNT(*) FROM bayut_properties")
        count = cursor.fetchone()[0]
        print(f"\nВсего записей в таблице: {count}")
        
        # Получаем информацию о индексах
        cursor.execute("""
            SELECT
                indexname,
                indexdef
            FROM
                pg_indexes
            WHERE
                tablename = 'bayut_properties'
        """)
        
        indexes = cursor.fetchall()
        
        if indexes:
            print("\n===== Индексы в таблице =====")
            for idx_name, idx_def in indexes:
                print(f"Имя: {idx_name}")
                print(f"Определение: {idx_def}")
                print("-" * 60)
        
        cursor.close()
        
    except Exception as e:
        print(f"Ошибка при получении структуры таблицы: {e}")
    finally:
        if conn:
            conn.close()

def get_csv_field_mapping():
    """Показывает соответствие полей CSV и полей API Bayut"""
    # Соответствие полей CSV-файла полям API Bayut
    field_mapping = {
        'id': 'id',
        'title': 'title',
        'price': 'price',
        'rooms': 'rooms',
        'baths': 'baths',
        'area': 'area',
        'rent_frequency': 'rentFrequency',
        'location': 'location[].name (уровень 2)',
        'cover_photo_url': 'coverPhoto.url',
        'property_url': 'externalID (как URL)',
        'category': 'category[0].name',
        'property_type': 'propertyType',
        'created_at': 'createdAt (конвертировано в дату)',
        'updated_at': 'updatedAt (конвертировано в дату)',
        'furnishing_status': 'furnishingStatus',
        'completion_status': 'completionStatus',
        'amenities': 'amenities (как строка)',
        'agency_name': 'agency.name',
        'contact_info': 'phoneNumber (мобильный и WhatsApp)',
        'geography': 'geography (lat и lng)',
        'agency_logo_url': 'agency.logo.url',
        'proxy_mobile': 'phoneNumber.proxyMobile',
        'keywords': 'keywords (как JSON)',
        'is_verified': 'isVerified',
        'purpose': 'purpose',
        'floor_number': 'floorNumber',
        'city_level_score': 'cityLevelScore',
        'score': 'score',
        'agency_licenses': 'agency.licenses (как JSON)',
        'agency_rating': 'agency.rating'
    }
    
    print("\n===== Соответствие полей CSV и полей API Bayut =====")
    print("{:<20} {:<30}".format("Поле в CSV/SQL", "Поле в API Bayut"))
    print("-" * 60)
    
    for csv_field, api_field in field_mapping.items():
        print("{:<20} {:<30}".format(csv_field, api_field))

if __name__ == "__main__":
    get_table_fields()
    get_csv_field_mapping() 