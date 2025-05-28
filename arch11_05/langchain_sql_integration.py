import os
import logging
import psycopg2
import re
from dotenv import load_dotenv
from langchain.chains import create_sql_query_chain
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from pydantic import BaseModel, Field
from langchain_core.output_parsers import StrOutputParser
from sql_prompts_examples import SQL_PROMPT_TEMPLATE, ANALYSIS_PROMPT_TEMPLATE, EXAMPLE_QUERIES
from advanced_sql_prompts import (
    ADVANCED_SQL_PROMPT, ADVANCED_ANALYSIS_PROMPT, 
    JSON_FIELD_SQL_PROMPT, MULTI_STEP_ANALYSIS_PROMPT,
    UNDERVALUED_PROPERTY_PROMPT, EXPORT_FRIENDLY_ANALYSIS,
    get_customized_prompt
)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv()

class PropertyDatabaseAnalyzer:
    def __init__(self, table_name="bayut_properties"):
        # Параметры подключения к базе данных
        self.db_params = {
            'dbname': 'postgres',
            'user': 'Admin',
            'password': 'Enclude79',
            'host': 'localhost',
            'port': '5432',
            'client_encoding': 'utf8'
        }

        # Сохраняем имя таблицы для использования в запросах
        self.table_name = table_name
        
        # Инициализация LLM модели (DeepSeek через OpenRouter)
        openrouter_api_key = os.getenv('OPENROUTER_API_KEY')
        if not openrouter_api_key:
            # Используем тестовый ключ для демонстрации
            openrouter_api_key = "sk-or-v1-test"
            logger.warning("OPENROUTER_API_KEY отсутствует в переменных окружения, используется тестовый ключ")
        
        self.llm = ChatOpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=openrouter_api_key,
            model="deepseek/deepseek-chat-v3-0324:free",
            temperature=0.3, # Уменьшаем температуру для более точных SQL-запросов
            model_kwargs={
                "extra_headers": {
                    "HTTP-Referer": "https://wealthcompas.com",
                    "X-Title": "WealthCompas Properties Analyzer"
                }
            }
        )
        
        # Получение схемы таблицы
        self.table_schema = self.get_table_schema()
        
        # Подготовка компонентов для анализа данных
        self._setup_components()
    
    def get_table_schema(self):
        """Получение схемы таблицы bayut_properties"""
        try:
            # Подключение к базе данных
            conn = psycopg2.connect(**self.db_params)
            # Явно устанавливаем кодировку для соединения
            conn.set_client_encoding('UTF8')
            cur = conn.cursor()
            
            # Запрос на получение структуры таблицы
            cur.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = %s 
                ORDER BY ordinal_position
            """, (self.table_name,))
            
            columns = cur.fetchall()
            schema = f"Таблица {self.table_name}:\n"
            for col in columns:
                schema += f"- {col[0]}: {col[1]}\n"
            
            # Закрытие соединения
            cur.close()
            conn.close()
            
            return schema
        except Exception as e:
            logger.error(f"Ошибка при получении схемы таблицы: {e}")
            return f"Не удалось получить схему таблицы {self.table_name}."
    
    def execute_sql_query(self, query):
        """Выполнение SQL запроса к базе данных"""
        try:
            # Подключение к базе данных
            conn = psycopg2.connect(**self.db_params)
            
            # Явно устанавливаем кодировку для соединения
            conn.set_client_encoding('UTF8')
            cur = conn.cursor()
            
            # Выполнение запроса
            cur.execute(query)
            
            # Получение результатов
            results = cur.fetchall()
            
            # Получение имен столбцов
            column_names = [desc[0] for desc in cur.description] if cur.description else []
            
            # Форматирование результатов в читаемый вид
            formatted_results = []
            for row in results:
                formatted_row = {}
                for i, val in enumerate(row):
                    if i < len(column_names):
                        # Обработка значений перед добавлением в результат
                        if isinstance(val, str):
                            try:
                                # Пытаемся преобразовать строку в UTF-8
                                val = val.encode('utf-8', 'ignore').decode('utf-8')
                            except Exception:
                                # В случае ошибки заменяем проблемные символы
                                val = str(val).encode('utf-8', 'replace').decode('utf-8')
                        formatted_row[column_names[i]] = val
                    else:
                        formatted_row[f"column_{i}"] = val
                formatted_results.append(formatted_row)
            
            # Закрытие соединения
            cur.close()
            conn.close()
            
            return formatted_results
        except Exception as e:
            logger.error(f"Ошибка при выполнении SQL запроса: {e}")
            # В случае ошибки кодировки, возвращаем пустой список вместо сообщения об ошибке
            if "encoding" in str(e).lower():
                logger.warning("Обнаружена ошибка кодировки. Возвращаем пустой список результатов.")
                return []
            return f"Ошибка при выполнении запроса: {e}"
        
    def _setup_components(self):
        """Настройка компонентов LangChain"""
        # Базовые шаблоны промптов
        self.sql_prompt = ChatPromptTemplate.from_template(SQL_PROMPT_TEMPLATE)
        self.answer_prompt = ChatPromptTemplate.from_template(ANALYSIS_PROMPT_TEMPLATE)
        
        # Продвинутые шаблоны промптов
        self.advanced_sql_prompt = ChatPromptTemplate.from_template(ADVANCED_SQL_PROMPT)
        self.advanced_analysis_prompt = ChatPromptTemplate.from_template(ADVANCED_ANALYSIS_PROMPT)
        
        # Специализированные шаблоны
        self.json_sql_prompt = ChatPromptTemplate.from_template(JSON_FIELD_SQL_PROMPT)
        self.multi_step_prompt = ChatPromptTemplate.from_template(MULTI_STEP_ANALYSIS_PROMPT)
        self.undervalued_prompt = ChatPromptTemplate.from_template(UNDERVALUED_PROPERTY_PROMPT)
        self.export_analysis_prompt = ChatPromptTemplate.from_template(EXPORT_FRIENDLY_ANALYSIS)
    
    def get_latest_property_by_region(self, max_area=40, top_n=3):
        """Получение последних объявлений с небольшими квартирами для каждого района"""
        try:
            # SQL-запрос для выборки топ-3 квартир по каждому району
            query = f"""
            WITH RankedProperties AS (
                SELECT 
                    id, title, price, area, rooms, location, region, publication_date,
                    ROW_NUMBER() OVER (PARTITION BY region ORDER BY publication_date DESC, price ASC) as rank
                FROM {self.table_name}
                WHERE area <= %s AND region IS NOT NULL
            )
            SELECT id, title, price, area, rooms, location, region, publication_date
            FROM RankedProperties
            WHERE rank <= %s
            ORDER BY region, rank;
            """
            
            # Подключение к базе данных
            conn = psycopg2.connect(**self.db_params)
            
            # Явно устанавливаем кодировку для соединения
            conn.set_client_encoding('UTF8')
            cur = conn.cursor()
            
            # Выполнение запроса с параметрами
            cur.execute(query, (max_area, top_n))
            
            # Получение результатов
            results = cur.fetchall()
            
            # Получение имен столбцов
            column_names = [desc[0] for desc in cur.description] if cur.description else []
            
            # Форматирование результатов в читаемый вид
            formatted_results = []
            for row in results:
                formatted_row = {}
                for i, val in enumerate(row):
                    if i < len(column_names):
                        # Обработка значений перед добавлением в результат
                        if isinstance(val, str):
                            try:
                                # Пытаемся преобразовать строку в UTF-8
                                val = val.encode('utf-8', 'ignore').decode('utf-8')
                            except Exception:
                                # В случае ошибки заменяем проблемные символы
                                val = str(val).encode('utf-8', 'replace').decode('utf-8')
                        formatted_row[column_names[i]] = val
                    else:
                        formatted_row[f"column_{i}"] = val
                formatted_results.append(formatted_row)
            
            # Закрытие соединения
            cur.close()
            conn.close()
            
            return formatted_results
        except Exception as e:
            logger.error(f"Ошибка при выполнении запроса по регионам: {e}")
            # В случае ошибки кодировки, возвращаем пустой список вместо сообщения об ошибке
            if "encoding" in str(e).lower():
                logger.warning("Обнаружена ошибка кодировки. Возвращаем пустой список результатов.")
                return []
            return f"Ошибка при выполнении запроса: {e}"
        
    async def generate_property_analysis(self, question: str, query_type: str = 'basic') -> str:
        """
        Генерация анализа недвижимости на основе вопроса пользователя
        
        Args:
            question: Вопрос пользователя
            query_type: Тип запроса ('basic', 'json', 'multi_step', 'undervalued', 'export', 'auto')
        """
        try:
            logger.info(f"Получен вопрос: {question}")
            
            # Проверяем, запрашивает ли пользователь топ-3 квартиры до 40 кв.м. по районам
            if any(keyword in question.lower() for keyword in ["топ-3", "топ 3", "небольш", "40 кв", "маленьк"]) and \
               any(keyword in question.lower() for keyword in ["район", "регион", "локаци", "област"]):
                logger.info("Обнаружен запрос на топ-3 квартиры по районам")
                # Используем специализированный запрос
                result = self.get_latest_property_by_region(max_area=40, top_n=3)
                
                # Создаем запрос для объяснения в анализе
                query = f"""
                WITH RankedProperties AS (
                    SELECT 
                        id, title, price, area, rooms, location, region, publication_date,
                        ROW_NUMBER() OVER (PARTITION BY region ORDER BY publication_date DESC, price ASC) as rank
                    FROM {self.table_name}
                    WHERE area <= 40 AND region IS NOT NULL
                )
                SELECT id, title, price, area, rooms, location, region, publication_date
                FROM RankedProperties
                WHERE rank <= 3
                ORDER BY region, rank;
                """
            else:
                # Выбираем соответствующий шаблон промпта
                if query_type == 'auto':
                    # Автоматическое определение типа запроса
                    custom_prompt = get_customized_prompt('auto', question)
                    sql_chain = ChatPromptTemplate.from_template(custom_prompt) | self.llm | StrOutputParser()
                    logger.info(f"Использован автоматически определенный тип запроса")
                elif query_type == 'basic':
                    sql_chain = self.sql_prompt | self.llm | StrOutputParser()
                    logger.info(f"Использован базовый тип запроса")
                elif query_type == 'advanced':
                    sql_chain = self.advanced_sql_prompt | self.llm | StrOutputParser()
                    logger.info(f"Использован продвинутый тип запроса")
                elif query_type == 'json':
                    sql_chain = self.json_sql_prompt | self.llm | StrOutputParser()
                    logger.info(f"Использован JSON тип запроса")
                elif query_type == 'multi_step':
                    sql_chain = self.multi_step_prompt | self.llm | StrOutputParser()
                    logger.info(f"Использован многошаговый тип запроса")
                elif query_type == 'undervalued':
                    sql_chain = self.undervalued_prompt | self.llm | StrOutputParser()
                    logger.info(f"Использован тип запроса для недооцененных объектов")
                else:
                    sql_chain = self.advanced_sql_prompt | self.llm | StrOutputParser()
                    logger.info(f"По умолчанию использован продвинутый тип запроса")
                
                # Генерация SQL запроса
                query_raw = sql_chain.invoke({"question": question})
                
                # Очистка запроса от лишних символов
                query = self.clean_sql_query(query_raw)
                logger.info(f"Сгенерирован SQL запрос: {query}")
                
                # Исправление имени таблицы, если необходимо
                query = query.replace(self.table_name, self.table_name)
                
                # Выполнение SQL запроса
                try:
                    result = self.execute_sql_query(query)
                    logger.info(f"Результат выполнения: {result}")
                except Exception as e:
                    # Если запрос неудачный, попробуем использовать резервный запрос
                    logger.error(f"Ошибка при выполнении запроса: {e}")
                    fallback_query = f"""
                    SELECT id, title, price, area, rooms, location, region, publication_date
                    FROM {self.table_name}
                    WHERE area <= 40
                    ORDER BY publication_date DESC
                    LIMIT 10;
                    """
                    result = self.execute_sql_query(fallback_query)
                    query = fallback_query  # Обновляем запрос для анализа
                    logger.info(f"Результат выполнения резервного запроса: {result}")
            
            # Выбираем подходящий шаблон для анализа
            if query_type == 'export':
                analysis_prompt = self.export_analysis_prompt
                logger.info(f"Использован экспортный тип анализа")
            else:
                analysis_prompt = self.advanced_analysis_prompt
                logger.info(f"Использован продвинутый тип анализа")
            
            # Генерация ответа
            answer_chain = analysis_prompt | self.llm | StrOutputParser()
            answer = answer_chain.invoke({
                "query": query,
                "result": result,
                "question": question
            })
            
            logger.info("Успешно сгенерирован анализ данных")
            return answer
            
        except Exception as e:
            logger.error(f"Ошибка при анализе данных: {e}", exc_info=True)
            return "Произошла ошибка при анализе данных о недвижимости."
    
    def clean_sql_query(self, query: str) -> str:
        """Очистка SQL запроса от лишних символов"""
        # Удаление блоков кода и маркеров, часто генерируемых LLM
        query = re.sub(r'^```sql\s*', '', query, flags=re.IGNORECASE)
        query = re.sub(r'```$', '', query)
        query = re.sub(r'^```\s*', '', query)
        query = re.sub(r'^\s*>\+?', '', query)
        
        # Удаление других специальных символов в начале строки
        query = re.sub(r'^[^a-zA-Z0-9]*', '', query)
        
        # Приведение регистра ключевых слов (только для читаемости, можно удалить)
        sql_keywords = ['SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'HAVING', 'JOIN', 
                         'LEFT JOIN', 'RIGHT JOIN', 'WITH', 'AS', 'OVER', 'PARTITION BY', 
                         'ROW_NUMBER', 'RANK', 'DENSE_RANK', 'LIMIT']
        pattern = '|'.join(r'\b{}\b'.format(keyword) for keyword in sql_keywords)
        query = re.sub(pattern, lambda m: m.group(0).upper(), query, flags=re.IGNORECASE)
        
        # Финальная проверка и очистка
        query = query.strip()
        
        return query 