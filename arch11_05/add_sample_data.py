import psycopg2
import random
from datetime import datetime, timedelta
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Параметры подключения к базе данных
DB_PARAMS = {
    'dbname': 'postgres',
    'user': 'Admin',
    'password': 'Enclude79',
    'host': 'localhost',
    'port': '5432'
}

# Тестовые данные
REGIONS = [
    "Downtown Dubai", "Dubai Marina", "Palm Jumeirah", "Business Bay", 
    "Jumeirah Beach Residence", "Jumeirah Village Circle", "Arabian Ranches",
    "Dubai Silicon Oasis", "Dubai Hills Estate", "Dubai Sports City"
]

PROPERTY_TYPES = ["Apartment", "Villa", "Townhouse", "Penthouse", "Office", "Retail", "Land"]

TITLES = [
    "Luxury Apartment with Amazing Views", "Spacious Villa with Private Pool",
    "Modern Townhouse in Prime Location", "Elegant Penthouse with Rooftop Terrace",
    "Cozy Studio in Downtown", "Beachfront Property with Sea Access",
    "Newly Renovated Apartment", "Stunning Panoramic Views Apartment",
    "High-end Villa with Garden", "Investment Opportunity in Popular Area"
]

CONSTRUCTION_STATUS = ["Completed", "Under Construction", "Off-plan"]

FEATURES = [
    "Balcony", "Swimming Pool", "Gym", "Security", "Parking",
    "Beach Access", "Garden", "Children's Play Area", "BBQ Area", 
    "Smart Home", "Concierge Service", "Pet Friendly"
]

DEVELOPERS = [
    "Emaar Properties", "Nakheel", "DAMAC Properties", "Dubai Properties",
    "Meraas", "Azizi Developments", "Deyaar Development", "Seven Tides",
    "Omniyat", "Sobha Realty", "Danube Properties"
]

def generate_random_properties(num_properties=100):
    """Генерация случайных объектов недвижимости для тестирования"""
    properties = []
    
    # Диапазон дат публикации (последние 6 месяцев)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=180)
    
    # Диапазоны параметров
    price_ranges = {
        "Apartment": (500000, 5000000),
        "Villa": (2000000, 20000000),
        "Townhouse": (1500000, 8000000),
        "Penthouse": (3000000, 15000000),
        "Office": (1000000, 10000000),
        "Retail": (2000000, 8000000),
        "Land": (5000000, 50000000)
    }
    
    area_ranges = {
        "Apartment": (40, 250),
        "Villa": (200, 800),
        "Townhouse": (150, 350),
        "Penthouse": (200, 500),
        "Office": (80, 500),
        "Retail": (50, 300),
        "Land": (500, 5000)
    }
    
    for i in range(num_properties):
        # Уникальный ID
        prop_id = f"TEST{1000000 + i}"
        
        # Выбор типа недвижимости
        property_type = random.choice(PROPERTY_TYPES)
        
        # Генерация основных параметров
        price = random.randint(*price_ranges[property_type])
        area = random.randint(*area_ranges[property_type])
        rooms = random.randint(1, 5) if property_type in ["Apartment", "Villa", "Townhouse", "Penthouse"] else None
        bathrooms = random.randint(1, 5) if rooms else None
        
        # Регион и другие параметры
        region = random.choice(REGIONS)
        title = random.choice(TITLES)
        status = random.choice(CONSTRUCTION_STATUS)
        developer = random.choice(DEVELOPERS)
        
        # Дата публикации
        days_offset = random.randint(0, (end_date - start_date).days)
        publication_date = start_date + timedelta(days=days_offset)
        
        # Добавляем объект в список
        properties.append({
            "id": prop_id,
            "title": title,
            "price": price,
            "rooms": rooms,
            "bathrooms": bathrooms,
            "area": area,
            "region": region,
            "property_type": property_type,
            "publication_date": publication_date.strftime("%Y-%m-%d %H:%M:%S"),
            "construction_status": status,
            "developer": developer
        })
    
    return properties

def add_sample_data(num_properties=100):
    """Добавление тестовых данных в базу данных"""
    try:
        # Генерация тестовых данных
        properties = generate_random_properties(num_properties)
        
        # Подключение к базе данных
        print(f"Подключение к базе данных для добавления {num_properties} тестовых объектов...")
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        
        # Очистка существующих тестовых данных
        print("Удаление существующих тестовых записей...")
        cur.execute("DELETE FROM temp_properties WHERE id LIKE 'TEST%'")
        conn.commit()
        
        # Добавление новых данных
        print("Добавление новых тестовых данных...")
        
        for prop in properties:
            # Формирование запроса для вставки
            columns = ", ".join(prop.keys())
            placeholders = ", ".join(["%s"] * len(prop))
            
            query = f"""
            INSERT INTO temp_properties ({columns})
            VALUES ({placeholders})
            """
            
            # Выполнение запроса
            cur.execute(query, list(prop.values()))
        
        # Сохранение изменений
        conn.commit()
        
        # Проверка количества добавленных записей
        cur.execute("SELECT COUNT(*) FROM temp_properties WHERE id LIKE 'TEST%'")
        count = cur.fetchone()[0]
        
        # Закрытие соединения
        cur.close()
        conn.close()
        
        print(f"Успешно добавлено {count} тестовых объектов в базу данных!")
        return count
        
    except Exception as e:
        logger.error(f"Ошибка при добавлении тестовых данных: {e}")
        return 0

if __name__ == "__main__":
    num_properties = 200  # Количество тестовых объектов для добавления
    added = add_sample_data(num_properties)
    
    if added > 0:
        print(f"Операция завершена успешно! Добавлено {added} тестовых объектов недвижимости.")
    else:
        print("Не удалось добавить тестовые данные. Проверьте журнал ошибок.") 