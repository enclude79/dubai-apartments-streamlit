import json

# Соответствие полей API и полей в базе данных
field_mapping = {
    'id': 'id',
    'title': 'Unnamed: 1',
    'price': 'Unnamed: 2',
    'rooms': 'Unnamed: 3',
    'baths': 'Unnamed: 4',
    'area': 'Unnamed: 5',
    'rentFrequency': 'Unnamed: 6',
    'location': 'Unnamed: 7',
    'coverPhoto.url': 'Unnamed: 8',
    'externalID (ссылка)': 'Unnamed: 9',
    'category[0].name': 'Unnamed: 10',
    'type': 'Unnamed: 11',
    'createdAt': 'Unnamed: 12',
    'updatedAt': 'Unnamed: 13',
    'furnishingStatus': 'Unnamed: 14',
    'completionStatus': 'Unnamed: 15',
    'amenities': 'Unnamed: 16',
    'rentFrequency (дубликат)': 'Unnamed: 17',
    'agency.name': 'Unnamed: 18',
    'phoneNumber': 'Unnamed: 19',
    'geography (координаты)': 'Unnamed: 20',
    'agency.name (дубликат)': 'Unnamed: 21',
    'agency.logo.url': 'Unnamed: 22',
    'phoneNumber.proxyMobile': 'Unnamed: 23',
    'keywords': 'Unnamed: 24',
    'isVerified': 'Unnamed: 25',
    'purpose': 'Unnamed: 26',
    'floorNumber': 'Unnamed: 27',
    'cityLevelScore': 'Unnamed: 28',
    'score': 'Unnamed: 29',
    'agency.licenses': 'Unnamed: 30',
    'agency.rating': 'Unnamed: 31'
}

# Словарь с описаниями полей
field_descriptions = {
    'id': 'Уникальный идентификатор объявления',
    'title': 'Название объявления',
    'price': 'Цена объекта недвижимости',
    'rooms': 'Количество комнат',
    'baths': 'Количество ванных комнат',
    'area': 'Площадь объекта недвижимости в кв.м.',
    'rentFrequency': 'Частота аренды (для аренды)',
    'location': 'Данные о местоположении в формате JSON',
    'coverPhoto.url': 'URL основной фотографии объекта',
    'externalID (ссылка)': 'Ссылка на объявление на сайте Bayut',
    'category[0].name': 'Название категории объекта',
    'type': 'Тип объекта недвижимости',
    'createdAt': 'Дата создания объявления',
    'updatedAt': 'Дата обновления объявления',
    'furnishingStatus': 'Статус меблировки',
    'completionStatus': 'Статус завершения строительства',
    'amenities': 'Список удобств объекта',
    'rentFrequency (дубликат)': 'Частота аренды (дубликат)',
    'agency.name': 'Название агентства недвижимости',
    'phoneNumber': 'Контактные телефоны',
    'geography (координаты)': 'Географические координаты объекта',
    'agency.name (дубликат)': 'Название агентства (дубликат)',
    'agency.logo.url': 'URL логотипа агентства',
    'phoneNumber.proxyMobile': 'Прокси-телефон для связи',
    'keywords': 'Ключевые слова для поиска',
    'isVerified': 'Является ли объявление проверенным',
    'purpose': 'Цель объявления (продажа/аренда)',
    'floorNumber': 'Номер этажа',
    'cityLevelScore': 'Рейтинг объекта на уровне города',
    'score': 'Общий рейтинг объекта',
    'agency.licenses': 'Лицензии агентства',
    'agency.rating': 'Рейтинг агентства'
}

# Выводим результат в файл
with open('api_fields_mapping.txt', 'w', encoding='utf-8') as f:
    f.write("Соответствие полей API и полей в базе данных:\n")
    f.write("-" * 70 + "\n")
    f.write(f"{'Поле API':<30} | {'Поле в базе данных':<20} | {'Описание'}\n")
    f.write("-" * 70 + "\n")
    
    for api_field, db_field in field_mapping.items():
        description = field_descriptions.get(api_field, '')
        f.write(f"{api_field:<30} | {db_field:<20} | {description}\n")
    
    f.write("\nПоля даты публикации:\n")
    f.write("-" * 70 + "\n")
    f.write("createdAt (Unnamed: 12) - Дата создания объявления\n")
    f.write("updatedAt (Unnamed: 13) - Дата обновления объявления\n")

print("Информация о полях API сохранена в файл api_fields_mapping.txt") 