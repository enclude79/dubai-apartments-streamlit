import requests
import pandas as pd
import time
from datetime import datetime

def check_property_exists(external_id, headers):
    """Проверяет существование объявления через API details"""
    url = "https://bayut.p.rapidapi.com/properties/detail"
    querystring = {
        "externalID": external_id,
        "lang": "ru"
    }
    
    try:
        response = requests.request("GET", url, headers=headers, params=querystring)
        if response.status_code == 200:
            data = response.json()
            # Проверяем наличие важных полей в ответе
            if data.get('id') and data.get('title') and data.get('price'):
                print(f"Объявление {external_id} активно")
                return True
        print(f"Объявление {external_id} не активно или недоступно")
        return False
    except Exception as e:
        print(f"Ошибка при проверке объявления {external_id}: {e}")
        return False

def fetch_properties_sale():
    url = "https://bayut.p.rapidapi.com/properties/list"
    
    headers = {
        "X-RapidAPI-Key": "86b3cfbc80msh3cd99bad2e7126dp18c722jsnc5b7ca0b0d3d",
        "X-RapidAPI-Host": "bayut.p.rapidapi.com"
    }
    
    all_properties = []
    page = 1
    max_pages = 50  # Увеличиваем количество страниц для получения большего объема данных
    
    while page <= max_pages:
        querystring = {
            "locationExternalIDs": "5002,6020",  # Дубай
            "purpose": "for-sale",
            "hitsPerPage": "25",  # Увеличиваем количество объявлений на странице
            "page": str(page),
            "sort": "date-desc",  # Сортировка по дате (сначала новые)
            "categoryExternalID": "4",  # Квартиры
            "isDeveloper": "true",  # Только от застройщиков
            "completionStatus": ["off-plan", "under-construction"]  # Строящиеся объекты
        }
        
        try:
            print(f"Получение страницы {page}...")
            response = requests.get(url, headers=headers, params=querystring)
            response.raise_for_status()
            data = response.json()
            
            if not data.get('hits'):
                print("Больше объявлений не найдено")
                break
            
            for property in data['hits']:
                # Проверяем дату публикации (в пределах 6 месяцев)
                created_at = datetime.fromtimestamp(property.get('createdAt', 0))
                if (datetime.now() - created_at).days <= 180:  # 180 дней = ~6 месяцев
                    property_data = {
                        'ID': property.get('id'),
                        'Название': property.get('title'),
                        'Цена': property.get('price'),
                        'Комнат': property.get('rooms'),
                        'Ванных': property.get('baths'),
                        'Площадь': property.get('area'),
                        'Регион': property.get('region'),
                        'Локация': property.get('location'),
                        'Фото': property.get('coverPhoto', {}).get('url'),
                        'Ссылка на объявление': f"https://www.bayut.com/property/details-{property.get('externalID')}.html",
                        'Категория': property.get('category', [{}])[0].get('name'),
                        'Тип недвижимости': property.get('type'),
                        'Дата публикации': created_at.strftime('%Y-%m-%d %H:%M:%S'),
                        'Последнее обновление': datetime.fromtimestamp(property.get('updatedAt', 0)).strftime('%Y-%m-%d %H:%M:%S'),
                        'Количество парковочных мест': property.get('parkingSpaces'),
                        'Статус строительства': property.get('completionStatus'),
                        'Особенности': ', '.join(property.get('amenities', [])),
                        'Описание': property.get('description'),
                        'Застройщик': property.get('agency', {}).get('name'),
                        'Контакты': f"Тел: {property.get('phoneNumber', {}).get('mobile')}; WhatsApp: {property.get('phoneNumber', {}).get('whatsapp')}",
                        'Координаты': f"Широта: {property.get('geography', {}).get('lat')}, Долгота: {property.get('geography', {}).get('lng')}",
                        'Ссылка на застройщика': property.get('agency', {}).get('name'),
                        'Логотип застройщика': property.get('agency', {}).get('logo', {}).get('url'),
                        'Статус верификации': property.get('verificationStatus'),
                        'Ключевые слова': property.get('keywords'),
                        'Счетчик просмотров': property.get('viewsCount'),
                        'Счетчик фото': property.get('photoCount'),
                        'Счетчик видео': property.get('videoCount'),
                        'Счетчик панорам': property.get('panoramaCount'),
                        'Счетчик этажей': property.get('floorNumber'),
                        'Лицензии застройщика': str(property.get('agency', {}).get('licenses')),
                        'Рейтинг застройщика': property.get('agency', {}).get('rating'),
                    }
                    all_properties.append(property_data)
                    print(f"Добавлено объявление: {property.get('externalID')} от {created_at.strftime('%Y-%m-%d')}")
            
            print(f"Получено объявлений на странице: {len(data['hits'])}")
            page += 1
            time.sleep(2)  # Уменьшаем задержку между запросами
            
        except Exception as e:
            print(f"Ошибка при получении данных: {e}")
            break
    
    # Создаем DataFrame
    df = pd.DataFrame(all_properties)
    
    # Формируем имя файла с текущей датой
    current_date = datetime.now().strftime("%Y%m%d")
    output_file = f'bayut_properties_sale_6m_{current_date}.csv'
    
    # Сохраняем в CSV
    df.to_csv(output_file, index=False, encoding='utf-8-sig', quoting=1)
    print(f"\nДанные сохранены в файл: {output_file}")
    print(f"Всего получено объектов: {len(all_properties)}")

if __name__ == "__main__":
    fetch_properties_sale() 