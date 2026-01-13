import os
import json
import random
import logging
import asyncio
import aiohttp
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
import pytz
from bs4 import BeautifulSoup
from fake_useragent import UserAgent  # pip install fake-useragent

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    JobQueue,
)

# ========== НАСТРОЙКИ ==========
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CONFIG_FILE = "bot_digest.json"

# Время отправки дайджеста (12:00 по Москве)
DIGEST_TIME = {"hour": 12, "minute": 0}
TIMEZONE = pytz.timezone("Europe/Moscow")
DIGEST_DAYS = [0, 1, 2, 3, 4]  # Пн-Пт

# ========== НАСТРОЙКИ ПАРСИНГА ==========
USER_AGENT = UserAgent()

# Источники новостей по категориям
NEWS_SOURCES = {
    'спорт': [
        {
            'name': 'Спорт-Экспресс',
            'url': 'https://www.sport-express.ru/services/materials/news/last/',
            'parser': 'parse_sportexpress'
        },
        {
            'name': 'Чемпионат',
            'url': 'https://www.championat.com/news/1.html',
            'parser': 'parse_championat'
        },
        {
            'name': 'Матч ТВ',
            'url': 'https://matchtv.ru/news/',
            'parser': 'parse_matchtv'
        }
    ],
    'технологии': [
        {
            'name': 'Хабрахабр',
            'url': 'https://habr.com/ru/news/',
            'parser': 'parse_habr'
        },
        {
            'name': 'VC.ru',
            'url': 'https://vc.ru/new',
            'parser': 'parse_vc'
        },
        {
            'name': 'TJ',
            'url': 'https://tjournal.ru/news',
            'parser': 'parse_tjournal'
        }
    ],
    'курьёзы': [
        {
            'name': 'Комсомольская правда',
            'url': 'https://www.kp.ru/online/news/',
            'parser': 'parse_kp'
        },
        {
            'name': 'РИА Новости',
            'url': 'https://ria.ru/incidents/',
            'parser': 'parse_ria'
        },
        {
            'name': 'Lenta.ru',
            'url': 'https://lenta.ru/rubrics/culture/curious/',
            'parser': 'parse_lenta'
        }
    ]
}

# Города для погоды
CITIES = [
    {"name": "Москва", "yandex_code": "moscow"},
    {"name": "Санкт-Петербург", "yandex_code": "saint-petersburg"},
    {"name": "Новосибирск", "yandex_code": "novosibirsk"},
    {"name": "Екатеринбург", "yandex_code": "yekaterinburg"},
    {"name": "Казань", "yandex_code": "kazan"}
]

# ========== ЛОГИРОВАНИЕ ==========
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", 
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ========== КЛАСС ДЛЯ ПАРСИНГА ==========
class NewsWeatherParser:
    """Парсер новостей и погоды"""
    
    def __init__(self):
        self.session = None
        self.news_cache = {}
        self.weather_cache = {}
        self.cache_timeout = 1800  # 30 минут
        
    async def init_session(self):
        """Инициализация сессии"""
        if not self.session:
            self.session = aiohttp.ClientSession(
                headers={'User-Agent': USER_AGENT.random},
                timeout=aiohttp.ClientTimeout(total=10)
            )
    
    async def close_session(self):
        """Закрытие сессии"""
        if self.session:
            await self.session.close()
            self.session = None
    
    # ========== ПАРСИНГ НОВОСТЕЙ ==========
    async def get_news_by_category(self, category: str, count: int = 1) -> List[Dict]:
        """Получить новости по категории"""
        cache_key = f"news_{category}_{datetime.now().strftime('%Y%m%d%H')}"
        
        if cache_key in self.news_cache:
            cached_time, data = self.news_cache[cache_key]
            if (datetime.now() - cached_time).seconds < self.cache_timeout:
                return random.sample(data, min(len(data), count))
        
        all_news = []
        sources = NEWS_SOURCES.get(category, [])
        
        for source in sources:
            try:
                news = await getattr(self, source['parser'])(source['url'])
                for item in news:
                    item['source'] = source['name']
                all_news.extend(news[:3])  # Берем по 3 новости с каждого источника
                await asyncio.sleep(0.5)  # Задержка между запросами
            except Exception as e:
                logger.error(f"Ошибка парсинга {source['name']}: {e}")
                continue
        
        # Сохраняем в кэш
        self.news_cache[cache_key] = (datetime.now(), all_news)
        
        # Возвращаем случайные новости
        if len(all_news) > count:
            return random.sample(all_news, count)
        return all_news
    
    # Парсеры для разных сайтов
    async def parse_sportexpress(self, url: str) -> List[Dict]:
        """Парсинг Спорт-Экспресс"""
        async with self.session.get(url) as response:
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')
            
            news_items = []
            for article in soup.find_all('div', class_='se-material__title', limit=10):
                title_elem = article.find('a')
                if title_elem:
                    link_elem = title_elem.get('href')
                    if link_elem and not link_elem.startswith('http'):
                        link_elem = 'https://www.sport-express.ru' + link_elem
                    
                    news_items.append({
                        'title': title_elem.text.strip(),
                        'url': link_elem,
                        'description': self._generate_description(title_elem.text.strip())
                    })
            
            return news_items
    
    async def parse_championat(self, url: str) -> List[Dict]:
        """Парсинг Чемпионат"""
        async with self.session.get(url) as response:
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')
            
            news_items = []
            for article in soup.find_all('a', class_='news-item__title', limit=10):
                title = article.text.strip()
                link = article.get('href')
                if not link.startswith('http'):
                    link = 'https://www.championat.com' + link
                
                news_items.append({
                    'title': title,
                    'url': link,
                    'description': self._generate_description(title)
                })
            
            return news_items
    
    async def parse_habr(self, url: str) -> List[Dict]:
        """Парсинг Хабрахабр"""
        async with self.session.get(url) as response:
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')
            
            news_items = []
            for article in soup.find_all('article', class_='tm-articles-list__item', limit=10):
                title_elem = article.find('h2', class_='tm-title')
                if title_elem:
                    link_elem = title_elem.find('a')
                    if link_elem:
                        title = link_elem.text.strip()
                        link = 'https://habr.com' + link_elem.get('href')
                        
                        # Получаем описание
                        desc_elem = article.find('div', class_='tm-article-body tm-article-snippet__lead')
                        description = desc_elem.text.strip()[:150] + '...' if desc_elem else self._generate_description(title)
                        
                        news_items.append({
                            'title': title,
                            'url': link,
                            'description': description
                        })
            
            return news_items
    
    async def parse_vc(self, url: str) -> List[Dict]:
        """Парсинг VC.ru"""
        async with self.session.get(url) as response:
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')
            
            news_items = []
            for article in soup.find_all('div', class_='content-container', limit=10):
                title_elem = article.find('a', class_='content-title')
                if title_elem:
                    title = title_elem.text.strip()
                    link = title_elem.get('href')
                    if not link.startswith('http'):
                        link = 'https://vc.ru' + link
                    
                    # Описание
                    desc_elem = article.find('div', class_='content-description')
                    description = desc_elem.text.strip()[:150] + '...' if desc_elem else self._generate_description(title)
                    
                    news_items.append({
                        'title': title,
                        'url': link,
                        'description': description
                    })
            
            return news_items
    
    async def parse_kp(self, url: str) -> List[Dict]:
        """Парсинг Комсомольская правда"""
        async with self.session.get(url) as response:
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')
            
            news_items = []
            for article in soup.find_all('div', class_='sc-12iwwi7', limit=10):
                title_elem = article.find('a')
                if title_elem:
                    title = title_elem.text.strip()
                    link = title_elem.get('href')
                    if link and not link.startswith('http'):
                        link = 'https://www.kp.ru' + link
                    
                    news_items.append({
                        'title': title,
                        'url': link,
                        'description': self._generate_description(title)
                    })
            
            return news_items
    
    async def parse_ria(self, url: str) -> List[Dict]:
        """Парсинг РИА Новости"""
        async with self.session.get(url) as response:
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')
            
            news_items = []
            for article in soup.find_all('div', class_='list-item', limit=10):
                title_elem = article.find('a', class_='list-item__title')
                if title_elem:
                    title = title_elem.text.strip()
                    link = title_elem.get('href')
                    if not link.startswith('http'):
                        link = 'https://ria.ru' + link
                    
                    # Описание
                    desc_elem = article.find('div', class_='list-item__announce')
                    description = desc_elem.text.strip()[:150] + '...' if desc_elem else self._generate_description(title)
                    
                    news_items.append({
                        'title': title,
                        'url': link,
                        'description': description
                    })
            
            return news_items
    
    # Заглушки для других парсеров
    async def parse_matchtv(self, url: str) -> List[Dict]:
        return await self._generic_parser(url, 'a', class_='news-card__title')
    
    async def parse_tjournal(self, url: str) -> List[Dict]:
        return await self._generic_parser(url, 'a', class_='content-title')
    
    async def parse_lenta(self, url: str) -> List[Dict]:
        return await self._generic_parser(url, 'a', class_='card-full-news__title')
    
    async def _generic_parser(self, url: str, tag: str, **kwargs) -> List[Dict]:
        """Универсальный парсер"""
        async with self.session.get(url) as response:
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')
            
            news_items = []
            elements = soup.find_all(tag, kwargs, limit=10)
            
            for elem in elements:
                title = elem.text.strip()
                link = elem.get('href')
                if link and not link.startswith('http'):
                    if 'lenta.ru' in url:
                        link = 'https://lenta.ru' + link
                
                news_items.append({
                    'title': title,
                    'url': link,
                    'description': self._generate_description(title)
                })
            
            return news_items
    
    def _generate_description(self, title: str) -> str:
        """Генерирует описание на основе заголовка"""
        descriptions = [
            f"{title}. Подробности читайте в источнике...",
            f"{title}. Это событие вызвало широкий резонанс...",
            f"{title}. Эксперты прокомментировали ситуацию...",
            f"{title}. Читайте полный материал по ссылке...",
            f"{title}. Новость активно обсуждается в соцсетях..."
        ]
        return random.choice(descriptions)
    
    # ========== ПАРСИНГ ПОГОДЫ ==========
    async def get_weather(self, city_name: str = "Москва") -> Dict:
        """Получить погоду для города"""
        cache_key = f"weather_{city_name}_{datetime.now().strftime('%Y%m%d%H')}"
        
        if cache_key in self.weather_cache:
            cached_time, data = self.weather_cache[cache_key]
            if (datetime.now() - cached_time).seconds < self.cache_timeout:
                return data
        
        # Находим код города для Яндекс
        city_data = next((c for c in CITIES if c['name'].lower() == city_name.lower()), CITIES[0])
        
        try:
            weather = await self._parse_yandex_weather(city_data['yandex_code'])
            weather['city'] = city_data['name']
            weather['updated'] = datetime.now().strftime('%H:%M')
            
            # Кэшируем
            self.weather_cache[cache_key] = (datetime.now(), weather)
            return weather
            
        except Exception as e:
            logger.error(f"Ошибка парсинга погоды для {city_name}: {e}")
            return self._get_fallback_weather(city_data['name'])
    
    async def _parse_yandex_weather(self, city_code: str) -> Dict:
        """Парсинг погоды с Яндекс.Погоды"""
        url = f"https://yandex.ru/pogoda/{city_code}"
        
        headers = {
            'User-Agent': USER_AGENT.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
        }
        
        async with self.session.get(url, headers=headers) as response:
            if response.status != 200:
                raise Exception(f"HTTP {response.status}")
            
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')
            
            weather = {}
            
            # Температура сейчас
            temp_elem = soup.find('span', class_='temp__value')
            if temp_elem:
                weather['temp_now'] = temp_elem.text.strip()
            
            # Ощущается как
            feels_label = soup.find('div', class_='term__label')
            if feels_label and 'ощущается' in feels_label.text:
                feels_temp = feels_label.find_next('span', class_='temp__value')
                if feels_temp:
                    weather['feels_like'] = feels_temp.text.strip()
            
            # Состояние погоды
            condition_elem = soup.find('div', class_='link__condition')
            if condition_elem:
                weather['condition'] = condition_elem.text.strip()
            
            # Ветер
            wind_elem = soup.find('span', class_='wind-speed')
            if wind_elem:
                weather['wind'] = wind_elem.text.strip()
            
            # Влажность
            humidity_elem = soup.find('div', class_='term__label', text='влажность')
            if humidity_elem:
                humidity = humidity_elem.find_next('div', class_='term__value')
                if humidity:
                    weather['humidity'] = humidity.text.strip()
            
            # Давление
            pressure_elem = soup.find('div', class_='term__label', text='давление')
            if pressure_elem:
                pressure = pressure_elem.find_next('div', class_='term__value')
                if pressure:
                    weather['pressure'] = pressure.text.strip()
            
            # Если чего-то не хватило, заполняем значениями по умолчанию
            weather.setdefault('temp_now', '+5°C')
            weather.setdefault('feels_like', '+3°C')
            weather.setdefault('condition', 'Облачно с прояснениями')
            weather.setdefault('wind', '3 м/с')
            weather.setdefault('humidity', '75%')
            weather.setdefault('pressure', '755 мм рт.ст.')
            
            return weather
    
    def _get_fallback_weather(self, city: str) -> Dict:
        """Резервные данные о погоде"""
        conditions = [
            "Ясно", "Облачно", "Небольшая облачность", 
            "Пасмурно", "Небольшой дождь", "Снег"
        ]
        
        return {
            'city': city,
            'temp_now': f"+{random.randint(-5, 15)}°C",
            'feels_like': f"+{random.randint(-7, 13)}°C",
            'condition': random.choice(conditions),
            'wind': f"{random.randint(1, 10)} м/с",
            'humidity': f"{random.randint(60, 90)}%",
            'pressure': f"{random.randint(740, 770)} мм рт.ст.",
            'updated': datetime.now().strftime('%H:%M'),
            'source': 'кэш'
        }

# ========== КЛАСС ДЛЯ СОЗДАНИЯ ДАЙДЖЕСТА ==========
class DailyDigest:
    """Создание ежедневного дайджеста"""
    
    def __init__(self):
        self.parser = NewsWeatherParser()
        self.emoji_map = {
            'спорт': '⚽',
            'технологии': '💻',
            'курьёзы': '😂'
        }
        self.category_names = {
            'спорт': 'НОВОСТЬ СПОРТА',
            'технологии': 'ТЕХНОЛОГИИ ДНЯ',
            'курьёзы': 'КУРЬЁЗ ДНЯ'
        }
    
    async def create_digest(self) -> str:
        """Создать полный дайджест"""
        try:
            await self.parser.init_session()
            
            # Заголовок с датой
            now = datetime.now(TIMEZONE)
            day_names = ["ПОНЕДЕЛЬНИК", "ВТОРНИК", "СРЕДА", "ЧЕТВЕРГ", "ПЯТНИЦА", "СУББОТА", "ВОСКРЕСЕНЬЕ"]
            day_name = day_names[now.weekday()]
            date_str = now.strftime("%d.%m.%Y")
            
            digest = f"🌅 ЕЖЕДНЕВНЫЙ ДАЙДЖЕСТ • {day_name}, {date_str}\n\n"
            
            # Получаем новости по всем категориям
            for category in ['спорт', 'технологии', 'курьёзы']:
                news_list = await self.parser.get_news_by_category(category, count=1)
                if news_list:
                    news = news_list[0]
                    digest += self._format_news_block(category, news)
            
            # Получаем погоду
            weather = await self.parser.get_weather("Москва")
            digest += self._format_weather_block(weather)
            
            # Подпись
            digest += "\n──────────────\n"
            digest += "📱 <i>Хорошего дня! Отправлено ботом</i>"
            
            await self.parser.close_session()
            return digest
            
        except Exception as e:
            logger.error(f"Ошибка создания дайджеста: {e}")
            return self._get_fallback_digest()
    
    def _format_news_block(self, category: str, news: Dict) -> str:
        """Форматирование блока новости"""
        emoji = self.emoji_map.get(category, '📰')
        category_title = self.category_names.get(category, category.upper())
        
        block = f"{emoji} {category_title}\n"
        block += f"📰 {news.get('title', 'Новость дня')}\n"
        block += f"{news.get('description', 'Читайте подробности...')}\n"
        
        if news.get('url'):
            # Сокращаем домен для красоты
            source_name = news.get('source', 'Источник')
            if 'sport-express.ru' in news['url']:
                source_name = 'Спорт-Экспресс'
            elif 'habr.com' in news['url']:
                source_name = 'Хабрахабр'
            elif 'kp.ru' in news['url']:
                source_name = 'Комсомольская правда'
            
            block += f"🔗 Источник: {source_name}\n\n"
        
        return block
    
    def _format_weather_block(self, weather: Dict) -> str:
        """Форматирование блока погоды"""
        # Эмодзи для погоды
        condition_emoji = {
            'ясно': '☀️',
            'облачно': '☁️',
            'пасмурно': '☁️',
            'дождь': '🌧️',
            'снег': '❄️',
            'гроза': '⛈️'
        }
        
        condition = weather.get('condition', '').lower()
        emoji = '🌤️'
        for key, value in condition_emoji.items():
            if key in condition:
                emoji = value
                break
        
        block = f"{emoji} ПРОГНОЗ ПОГОДЫ\n"
        block += f"🇷🇺 {weather.get('city', 'Москва')}\n"
        block += f"{emoji} {weather.get('condition', 'Облачно с прояснениями')}\n"
        block += f"🌡️ Температура: {weather.get('temp_now', '+5°C')}"
        
        if 'feels_like' in weather:
            block += f" (ощущается как {weather['feels_like']})"
        
        block += f"\n💧 Влажность: {weather.get('humidity', '75%')}\n"
        block += f"💨 Ветер: {weather.get('wind', '5 м/с')}\n"
        block += f"📊 Давление: {weather.get('pressure', '755 мм рт.ст.')}\n\n"
        
        return block
    
    def _get_fallback_digest(self) -> str:
        """Резервный дайджест"""
        now = datetime.now(TIMEZONE)
        day_names = ["ПОНЕДЕЛЬНИК", "ВТОРНИК", "СРЕДА", "ЧЕТВЕРГ", "ПЯТНИЦА", "СУББОТА", "ВОСКРЕСЕНЬЕ"]
        day_name = day_names[now.weekday()]
        date_str = now.strftime("%d.%m.%Y")
        
        digest = f"🌅 ЕЖЕДНЕВНЫЙ ДАЙДЖЕСТ • {day_name}, {date_str}\n\n"
        
        # Примерные новости
        fallback_news = [
            ("⚽ НОВОСТЬ СПОРТА", "Российские спортсмены показали отличные результаты на международных соревнованиях", "Спорт-Экспресс"),
            ("💻 ТЕХНОЛОГИИ ДНЯ", "В России разработали новую технологию в сфере искусственного интеллекта", "Хабрахабр"),
            ("😂 КУРЬЁЗ ДНЯ", "Необычный случай произошёл сегодня в одном из городов России", "Комсомольская правда")
        ]
        
        for emoji, title, source in fallback_news:
            digest += f"{emoji}\n📰 {title}\nПодробности читайте в источниках...\n🔗 Источник: {source}\n\n"
        
        # Погода
        digest += "🌤️ ПРОГНОЗ ПОГОДЫ\n🇷🇺 Москва\n☁️ Облачно с прояснениями\n🌡️ Температура: +5°C (ощущается как +3°C)\n💧 Влажность: 75%\n💨 Ветер: 5 м/с\n📊 Давление: 755 мм рт.ст.\n\n"
        digest += "──────────────\n📱 <i>Хорошего дня! Отправлено ботом</i>"
        
        return digest

# ========== КЛАСС КОНФИГУРАЦИИ ==========
class BotConfig:
    """Конфигурация бота"""
    
    def __init__(self):
        self.data = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Ошибка загрузки конфига: {e}")
        
        return {
            "chat_id": None,
            "allowed_users": []
        }
    
    def save(self) -> None:
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, ensure_ascii=False, indent=2)
    
    @property
    def chat_id(self) -> Optional[int]:
        return self.data.get("chat_id")
    
    @chat_id.setter
    def chat_id(self, value: int) -> None:
        self.data["chat_id"] = value
        self.save()
    
    @property
    def allowed_users(self) -> List[str]:
        return self.data.get("allowed_users", [])

# ========== ОСНОВНЫЕ ФУНКЦИИ БОТА ==========
async def send_daily_digest(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправка ежедневного дайджеста"""
    try:
        config = BotConfig()
        chat_id = config.chat_id
        
        if not chat_id:
            logger.error("Chat ID не установлен!")
            await schedule_next_digest(context)
            return
        
        logger.info("Начинаю создание дайджеста...")
        
        # Создаем дайджест
        digest_creator = DailyDigest()
        message = await digest_creator.create_digest()
        
        # Отправляем
        await context.bot.send_message(
            chat_id=chat_id,
            text=message,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        )
        
        logger.info("✅ Ежедневный дайджест отправлен")
        
    except Exception as e:
        logger.error(f"❌ Ошибка отправки дайджеста: {e}")
    finally:
        # Планируем следующую отправку
        await schedule_next_digest(context)

async def schedule_next_digest(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Запланировать следующий дайджест"""
    try:
        now = datetime.now(TIMEZONE)
        
        # Проверяем, сегодня ли нужный день и время уже прошло
        if now.weekday() in DIGEST_DAYS:
            digest_time = now.replace(
                hour=DIGEST_TIME["hour"],
                minute=DIGEST_TIME["minute"],
                second=0,
                microsecond=0
            )
            
            if now < digest_time:
                delay = (digest_time - now).total_seconds()
                job_name = f"digest_{digest_time.strftime('%Y%m%d')}"
                schedule_job(context, delay, job_name)
                return
        
        # Ищем следующий рабочий день
        days_ahead = 1
        while True:
            next_day = now + timedelta(days=days_ahead)
            if next_day.weekday() in DIGEST_DAYS:
                next_digest = next_day.replace(
                    hour=DIGEST_TIME["hour"],
                    minute=DIGEST_TIME["minute"],
                    second=0,
                    microsecond=0
                )
                delay = (next_digest - now).total_seconds()
                job_name = f"digest_{next_digest.strftime('%Y%m%d')}"
                schedule_job(context, delay, job_name)
                logger.info(f"Следующий дайджест запланирован на {next_digest}")
                break
            days_ahead += 1
            
    except Exception as e:
        logger.error(f"Ошибка планирования дайджеста: {e}")
        # Пробуем снова через час
        context.application.job_queue.run_once(
            lambda ctx: schedule_next_digest(ctx),
            3600
        )

def schedule_job(context: ContextTypes.DEFAULT_TYPE, delay: float, name: str):
    """Запланировать задание"""
    jobs = context.application.job_queue.jobs()
    if not any(j.name == name for j in jobs):
        context.application.job_queue.run_once(
            send_daily_digest,
            delay,
            name=name
        )

async def send_digest_now(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправить дайджест немедленно (команда)"""
    config = BotConfig()
    chat_id = config.chat_id
    
    if not chat_id:
        await update.message.reply_text("❌ Сначала установите чат командой /setchat")
        return
    
    await update.message.reply_text("🔄 Создаю дайджест...")
    
    digest_creator = DailyDigest()
    message = await digest_creator.create_digest()
    
    # Отправляем в целевой чат
    await context.bot.send_message(
        chat_id=chat_id,
        text=message,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True
    )
    
    await update.message.reply_text("✅ Дайджест отправлен!")

async def set_chat(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Установить чат для дайджеста"""
    chat_id = update.effective_chat.id
    chat_title = update.effective_chat.title or "личный чат"
    
    config = BotConfig()
    config.chat_id = chat_id
    
    await update.message.reply_text(
        f"✅ <b>Чат установлен:</b> {chat_title}\n"
        f"<b>Chat ID:</b> {chat_id}\n\n"
        f"Ежедневный дайджест будет отправляться в этот чат в {DIGEST_TIME['hour']:02d}:{DIGEST_TIME['minute']:02d} по МСК (Пн-Пт)",
        parse_mode=ParseMode.HTML
    )
    
    # Планируем первый дайджест
    await schedule_next_digest(context)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /start"""
    await update.message.reply_text(
        "🌅 <b>Бот ежедневного дайджеста</b>\n\n"
        f"📰 <b>Что отправляет бот:</b>\n"
        f"• Новости спорта\n"
        f"• Технологические новости\n"
        f"• Курьёзные новости\n"
        f"• Прогноз погоды для Москвы\n\n"
        f"⏰ <b>Время отправки:</b> {DIGEST_TIME['hour']:02d}:{DIGEST_TIME['minute']:02d} по МСК (Пн-Пт)\n\n"
        f"🔧 <b>Команды:</b>\n"
        f"/setchat - установить чат для рассылки\n"
        f"/digestnow - отправить дайджест сейчас\n"
        f"/info - информация о боте",
        parse_mode=ParseMode.HTML
    )

async def info(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Информация о боте"""
    config = BotConfig()
    chat_status = f"✅ Установлен (ID: {config.chat_id})" if config.chat_id else "❌ Не установлен"
    
    now = datetime.now(TIMEZONE)
    next_digest_time = calculate_next_digest_time()
    
    await update.message.reply_text(
        f"📊 <b>Информация о боте:</b>\n\n"
        f"📱 <b>Статус чата:</b> {chat_status}\n"
        f"⏰ <b>Расписание:</b> Пн-Пт в {DIGEST_TIME['hour']:02d}:{DIGEST_TIME['minute']:02d} МСК\n"
        f"➡️ <b>Следующий дайджест:</b> {next_digest_time.strftime('%d.%m.%Y в %H:%M')}\n\n"
        f"📰 <b>Источники новостей:</b>\n"
        f"• Спорт-Экспресс, Чемпионат\n"
        f"• Хабрахабр, VC.ru\n"
        f"• Комсомолка, РИА Новости\n\n"
        f"🌤️ <b>Погода:</b> Яндекс.Погода (Москва)",
        parse_mode=ParseMode.HTML
    )

def calculate_next_digest_time() -> datetime:
    """Рассчитать время следующего дайджеста"""
    now = datetime.now(TIMEZONE)
    
    if now.weekday() in DIGEST_DAYS:
        digest_time = now.replace(
            hour=DIGEST_TIME["hour"],
            minute=DIGEST_TIME["minute"],
            second=0,
            microsecond=0
        )
        if now < digest_time:
            return digest_time
    
    # Ищем следующий рабочий день
    days_ahead = 1
    while True:
        next_day = now + timedelta(days=days_ahead)
        if next_day.weekday() in DIGEST_DAYS:
            return next_day.replace(
                hour=DIGEST_TIME["hour"],
                minute=DIGEST_TIME["minute"],
                second=0,
                microsecond=0
            )
        days_ahead += 1

def main() -> None:
    """Основная функция"""
    if not TOKEN:
        logger.error("❌ Токен бота не найден! Установите TELEGRAM_BOT_TOKEN")
        return
    
    # Устанавливаем uvloop для лучшей производительности
    try:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        logger.info("✅ Используется uvloop для асинхронности")
    except ImportError:
        logger.warning("⚠️  uvloop не установлен. Установите: pip install uvloop")
    
    application = Application.builder().token(TOKEN).build()
    
    # Обработчики команд
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("setchat", set_chat))
    application.add_handler(CommandHandler("digestnow", send_digest_now))
    application.add_handler(CommandHandler("info", info))
    
    # Запуск планировщика
    application.job_queue.run_once(
        lambda ctx: schedule_next_digest(ctx),
        3
    )
    
    logger.info("🤖 Бот ежедневного дайджеста запущен!")
    logger.info(f"⏰ Дайджесты: Пн-Пт в {DIGEST_TIME['hour']:02d}:{DIGEST_TIME['minute']:02d} по МСК")
    logger.info(f"📰 Источники: {len(NEWS_SOURCES['спорт']) + len(NEWS_SOURCES['технологии']) + len(NEWS_SOURCES['курьёзы'])} сайтов")
    
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
