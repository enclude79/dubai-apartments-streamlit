from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import psycopg2.extras
import pandas as pd
from pydantic import BaseModel
import os
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

app = FastAPI(title="Dubai Property API", 
              description="API для доступа к данным о недвижимости в Дубае")

# Настройка CORS для доступа из Streamlit Cloud
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # В продакшене лучше указать конкретные домены
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Параметры подключения к БД
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

def get_db_connection():
    """Создает подключение к базе данных PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except Exception as e:
        print(f"Ошибка подключения к БД: {e}")
        return None

@app.get("/")
def read_root():
    """Корневой эндпоинт для проверки работы API"""
    return {"message": "Dubai Property API работает"}

@app.get("/api/properties")
def get_properties(limit: int = 100, offset: int = 0):
    """Получение списка объектов недвижимости с пагинацией"""
    try:
        conn = get_db_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="Ошибка подключения к базе данных")
        
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        # Получение данных с пагинацией
        cursor.execute(
            "SELECT * FROM properties ORDER BY id LIMIT %s OFFSET %s",
            (limit, offset)
        )
        properties = cursor.fetchall()
        
        # Получение общего количества записей
        cursor.execute("SELECT COUNT(*) FROM properties")
        total_count = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        # Преобразуем результаты в список словарей
        result = []
        for row in properties:
            result.append(dict(row))
        
        return {
            "total": total_count,
            "limit": limit,
            "offset": offset,
            "data": result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка получения данных: {str(e)}")

@app.get("/api/properties/{property_id}")
def get_property(property_id: int):
    """Получение детальной информации об объекте недвижимости по ID"""
    try:
        conn = get_db_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="Ошибка подключения к базе данных")
        
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute("SELECT * FROM properties WHERE id = %s", (property_id,))
        property_data = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if not property_data:
            raise HTTPException(status_code=404, detail=f"Объект недвижимости с ID {property_id} не найден")
        
        return dict(property_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка получения данных: {str(e)}")

@app.get("/api/stats/avg_price_by_area")
def get_avg_price_by_area():
    """Получение средней цены по районам"""
    try:
        conn = get_db_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="Ошибка подключения к базе данных")
        
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute("""
            SELECT area, AVG(price) as avg_price, COUNT(*) as count
            FROM properties
            GROUP BY area
            ORDER BY avg_price DESC
        """)
        stats = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        result = []
        for row in stats:
            result.append(dict(row))
        
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка получения статистики: {str(e)}")

@app.get("/api/stats/count_by_property_type")
def get_count_by_property_type():
    """Получение количества объектов по типу недвижимости"""
    try:
        conn = get_db_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="Ошибка подключения к базе данных")
        
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute("""
            SELECT property_type, COUNT(*) as count
            FROM properties
            GROUP BY property_type
            ORDER BY count DESC
        """)
        stats = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        result = []
        for row in stats:
            result.append(dict(row))
        
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка получения статистики: {str(e)}")

@app.get("/api/map_data")
def get_map_data():
    """Получение данных для отображения на карте"""
    try:
        conn = get_db_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="Ошибка подключения к базе данных")
        
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute("""
            SELECT id, title, price, area, property_type, latitude, longitude 
            FROM properties
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        """)
        map_data = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        result = []
        for row in map_data:
            result.append(dict(row))
        
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка получения данных для карты: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api_server:app", host="0.0.0.0", port=8000, reload=True) 