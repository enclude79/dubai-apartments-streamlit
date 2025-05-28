import httpx
import logging
from datetime import datetime
from typing import List, Dict
from bs4 import BeautifulSoup
from lxml import etree

logger = logging.getLogger(__name__)

class NewsCollector:
    def __init__(self):
        self.base_url = "https://imexre.com/ru/news/"
        self.client = httpx.Client()

    async def collect_news(self, area: str) -> List[Dict]:
        """
        Собирает новости с сайта imexre.com
        """
        try:
            # Отправляем запрос на сайт
            response = await self.client.get(self.base_url)
            
            if response.status_code == 200:
                # Парсим HTML
                soup = BeautifulSoup(response.text, 'lxml')
                news_items = soup.find_all('div', class_='news-item')
                
                news_data = []
                for item in news_items:
                    # Извлекаем данные из новости
                    title = item.find('h3', class_='news-item__title')
                    date = item.find('div', class_='news-item__date')
                    link = item.find('a', class_='news-item__link')
                    
                    if title and date and link:
                        news_data.append({
                            'title': title.text.strip(),
                            'date': date.text.strip(),
                            'url': f"https://imexre.com{link['href']}",
                            'source': 'IMEXRE'
                        })
                
                logger.info(f"Successfully collected {len(news_data)} news items")
                return news_data
            else:
                logger.error(f"Error collecting news: {response.status_code} - {response.text}")
                return []

        except Exception as e:
            logger.error(f"Error in collect_news: {e}")
            return []

    def format_news_for_prompt(self, news: List[Dict]) -> str:
        """
        Форматирует собранные новости для включения в промпт
        """
        if not news:
            return ""

        formatted_news = "Актуальные новости с сайта IMEXRE:\n\n"
        
        for item in news:
            formatted_news += f"- {item.get('title', '')}\n"
            formatted_news += f"  Дата: {item.get('date', '')}\n"
            formatted_news += f"  Ссылка: {item.get('url', '')}\n\n"

        return formatted_news 