"""
Примеры SQL-запросов и улучшенные промпты для LangChain SQL интеграции.
Этот файл содержит набор примеров, которые можно использовать для обучения LLM 
правильно генерировать SQL-запросы для конкретных задач.
"""

# Примеры SQL-запросов для разных сценариев
EXAMPLE_QUERIES = [
    {
        "question": "Найти топ-3 самых дешевых квартир площадью до 40 квадратных метров в Downtown Dubai.",
        "sql": """
        SELECT id, title, price, area, rooms, location, region, publication_date
        FROM temp_properties
        WHERE area <= 40 AND region = 'Downtown Dubai'
        ORDER BY price ASC
        LIMIT 3;
        """
    },
    {
        "question": "Какие 3 самых новых объявления о квартирах до 40 кв.м. в Dubai Marina?",
        "sql": """
        SELECT id, title, price, area, rooms, location, region, publication_date 
        FROM temp_properties
        WHERE area <= 40 AND region = 'Dubai Marina'
        ORDER BY publication_date DESC
        LIMIT 3;
        """
    },
    {
        "question": "Покажи самые дешевые небольшие квартиры (до 40 кв.м.) в Palm Jumeirah за последний месяц.",
        "sql": """
        SELECT id, title, price, area, rooms, location, region, publication_date
        FROM temp_properties
        WHERE area <= 40 
          AND region = 'Palm Jumeirah'
          AND publication_date >= CURRENT_DATE - INTERVAL '30 days'
        ORDER BY price ASC
        LIMIT 3;
        """
    },
    {
        "question": "Составь список всех районов Дубая в порядке убывания средней цены квартир.",
        "sql": """
        SELECT region, AVG(price) as avg_price, COUNT(*) as count
        FROM temp_properties
        WHERE region IS NOT NULL
        GROUP BY region
        ORDER BY avg_price DESC;
        """
    },
    {
        "question": "Для каждого района найди топ-3 самых недорогих квартир площадью до 40 кв.м.",
        "sql": """
        WITH RankedProperties AS (
            SELECT 
                id, title, price, area, rooms, location, region, publication_date,
                ROW_NUMBER() OVER (PARTITION BY region ORDER BY price ASC) as rank
            FROM temp_properties
            WHERE area <= 40 AND region IS NOT NULL
        )
        SELECT id, title, price, area, rooms, location, region, publication_date
        FROM RankedProperties
        WHERE rank <= 3
        ORDER BY region, rank;
        """
    }
]

# Улучшенный промпт для SQL-запросов
SQL_PROMPT_TEMPLATE = """
Ты - эксперт по анализу данных рынка недвижимости в Дубае и SQL.

Твоя задача - написать оптимальный SQL-запрос на основе вопроса пользователя.

База данных содержит таблицу недвижимости (temp_properties) со следующими колонками:
- id: уникальный идентификатор объекта недвижимости (character varying)
- title: название/заголовок объявления (character varying)
- price: цена объекта в местной валюте (numeric)
- rooms: количество комнат (integer)
- bathrooms: количество ванных комнат (integer)
- area: площадь в квадратных метрах (numeric)
- region: район Дубая (text)
- location: конкретное местоположение (text)
- property_type: тип недвижимости (character varying)
- publication_date: дата публикации объявления (timestamp)
- construction_status: статус строительства (character varying)
- developer: застройщик (character varying)

Вот несколько примеров запросов:

Вопрос: Найти топ-3 самых дешевых квартир площадью до 40 квадратных метров в Downtown Dubai.
SQL: SELECT id, title, price, area, rooms, location, region, publication_date FROM temp_properties WHERE area <= 40 AND region = 'Downtown Dubai' ORDER BY price ASC LIMIT 3;

Вопрос: Для каждого района найди топ-3 самых недорогих квартир площадью до 40 кв.м.
SQL: WITH RankedProperties AS (SELECT id, title, price, area, rooms, location, region, publication_date, ROW_NUMBER() OVER (PARTITION BY region ORDER BY price ASC) as rank FROM temp_properties WHERE area <= 40 AND region IS NOT NULL) SELECT id, title, price, area, rooms, location, region, publication_date FROM RankedProperties WHERE rank <= 3 ORDER BY region, rank;

Вопрос пользователя: {question}

Напиши только SQL-запрос для решения задачи, без объяснений или комментариев:
"""

# Улучшенный промпт для анализа результатов SQL-запроса
ANALYSIS_PROMPT_TEMPLATE = """
Ты - эксперт по рынку недвижимости в Дубае.

На основе SQL-запроса и его результатов, дай подробный анализ и ответь на вопрос пользователя.
Твой ответ должен содержать:

1. Краткое резюме полученных данных (количество найденных объектов, общие тренды)
2. Для каждого объекта или района в результатах:
   - Основные характеристики (цена, площадь, местоположение)
   - Почему это может быть хорошим вариантом для инвестора или покупателя
   - Особенности района, если они известны
3. Сравнение объектов или районов между собой
4. Рекомендации на основе данных

SQL-запрос использованный для анализа:
{query}

Результаты запроса:
{result}

Вопрос пользователя:
{question}

Дай подробный аналитический ответ на русском языке, структурируй информацию для удобного чтения:
""" 