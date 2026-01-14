import os
import json
import random
import logging
import requests
import asyncio
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple, TypedDict
from functools import wraps
import pytz
from urllib.parse import quote
import re
import time

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    JobQueue,
    MessageHandler,
    filters,
    ConversationHandler
)

# ========== КОНСТАНТЫ ==========
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DEFAULT_ZOOM_LINK = "https://us04web.zoom.us/j/1234567890?pwd=example"
ZOOM_LINK = os.getenv("ZOOM_MEETING_LINK", DEFAULT_ZOOM_LINK)
CONFIG_FILE = "bot_config.json"

# Время планёрки (9:30 по Москве)
MEETING_TIME = {"hour": 9, "minute": 15}
TIMEZONE = pytz.timezone("Europe/Moscow")

# Дни недели для планёрки (понедельник=0, среда=2, пятница=4)
MEETING_DAYS = [0, 2, 4]

# ========== КОНСТАНТЫ СОБЫТИЙ "В ЭТОТ ДЕНЬ" ==========
# Категории событий
EVENT_CATEGORIES = ['музыка', 'фильмы', 'технологии', 'игры', 'наука', 'спорт', 'история']

# Время отправки (10:00 по Москве = 7:00 UTC)
EVENT_SEND_TIME = {"hour": 7, "minute": 0, "timezone": "UTC"}  # 7:00 UTC = 10:00 МСК
# Дни отправки (понедельник=0 ... пятница=4)
EVENT_DAYS = [0, 1, 2, 3, 4]  # Пн-Пт

# Русские названия месяцев для форматирования
MONTHS_RU = {
    1: "ЯНВАРЯ", 2: "ФЕВРАЛЯ", 3: "МАРТА", 4: "АПРЕЛЯ",
    5: "МАЯ", 6: "ИЮНЯ", 7: "ИЮЛЯ", 8: "АВГУСТА",
    9: "СЕНТЯБРЯ", 10: "ОКТЯБРЯ", 11: "НОЯБРЯ", 12: "ДЕКАБРЯ"
}

MONTHS_RU_LOWER = {k: v.lower() for k, v in MONTHS_RU.items()}

# Эмодзи для категорий
CATEGORY_EMOJIS = {
    'музыка': '🎵',
    'фильмы': '🎬',
    'технологии': '💻',
    'игры': '🎮',
    'наука': '🔬',
    'спорт': '⚽',
    'история': '📜'
}

# Wikipedia API константы
WIKIPEDIA_API_URL = "https://ru.wikipedia.org/w/api.php"
USER_AGENT = 'TelegramEventBot/3.0 (https://github.com/; contact@example.com)'
REQUEST_TIMEOUT = 20
REQUEST_RETRIES = 3

# ========== ТИПЫ ДАННЫХ ==========
class HistoricalEvent(TypedDict):
    title: str
    year: int
    description: str
    url: str
    category: str
    full_article: str  # Полный текст статьи для извлечения фактов

class ReminderData(TypedDict):
    message_id: int
    chat_id: int
    created_at: str

# ========== НАСТРОЙКИ ==========
CANCELLATION_OPTIONS = [
    "Все вопросы решены, планёрка не нужна",
    "Ключевые участники отсутствуют",
    "Перенесём на другой день",
]

SELECTING_REASON, SELECTING_DATE, CONFIRMING_DATE = range(3)

# Настройка логирования
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", 
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ========== УЛУЧШЕННЫЙ КЛАСС ДЛЯ СОБЫТИЙ "В ЭТОТ ДЕНЬ" ==========
class EventScheduler:
    """Класс для управления отправкой исторических событий 'В этот день'"""
    
    def __init__(self):
        self.current_index = 0
        # Храним использованные статьи по категориям
        self.used_events: Dict[str, set] = {category: set() for category in EVENT_CATEGORIES}
        # Кэш для fallback-событий
        self.fallback_cache: Dict[str, List[HistoricalEvent]] = {}
        logger.info("Инициализирован улучшенный планировщик исторических событий 'В этот день'")
    
    def get_next_category(self) -> str:
        """Получаем следующую категорию по кругу"""
        category = EVENT_CATEGORIES[self.current_index]
        logger.debug(f"Текущая категория событий: {category}, индекс: {self.current_index}")
        return category
    
    def increment_category(self) -> str:
        """Увеличиваем индекс категории и возвращаем следующую"""
        old_index = self.current_index
        self.current_index = (self.current_index + 1) % len(EVENT_CATEGORIES)
        next_category = EVENT_CATEGORIES[self.current_index]
        logger.debug(f"Категория изменена: {EVENT_CATEGORIES[old_index]} -> {next_category}")
        return next_category
    
    def get_todays_date_parts(self) -> Tuple[int, str, int]:
        """Получаем текущую дату (день, месяц_ru, текущий_год)"""
        now = datetime.now(TIMEZONE)
        day = now.day
        month_ru = MONTHS_RU[now.month]
        year = now.year
        return day, month_ru, year
    
    def cleanup_old_events(self, days_to_keep: int = 30) -> None:
        """Очистка старых событий (заглушка для будущей реализации)"""
        # В будущем можно реализовать очистку по дате использования
        pass
    
    def search_historical_events(self, day: int, month: int, category: str) -> List[HistoricalEvent]:
        """
        Ищем исторические события, которые произошли в ЭТУ ДАТУ (14 января) в РАЗНЫЕ ГОДЫ
        """
        try:
            date_str = f"{day} {MONTHS_RU_LOWER[month]}"
            logger.info(f"Поиск исторических событий за {date_str} в категории {category}")
            
            events: List[HistoricalEvent] = []
            
            # Стратегия 1: Ищем на Википедии по улучшенным запросам
            wikipedia_events = self._search_wikipedia_events_improved(day, month, category)
            if wikipedia_events:
                events.extend(wikipedia_events)
            
            # Стратегия 2: Используем известные события как fallback
            if not events:
                known_events = self._search_known_events(day, month, category)
                if known_events:
                    events.extend(known_events)
            
            # Убираем дубликаты и фильтруем по году
            unique_events: List[HistoricalEvent] = []
            seen_titles = set()
            
            for event in events:
                if (event['title'] not in seen_titles and 
                    event['year'] and 
                    1000 <= event['year'] <= datetime.now(TIMEZONE).year):
                    unique_events.append(event)
                    seen_titles.add(event['title'])
            
            logger.info(f"Найдено {len(unique_events)} исторических событий за {date_str} в категории {category}")
            return unique_events
            
        except Exception as e:
            logger.error(f"Ошибка поиска исторических событий: {e}")
            return []
    
    def _search_wikipedia_events_improved(self, day: int, month: int, category: str) -> List[HistoricalEvent]:
        """Улучшенный поиск событий на Википедии"""
        events: List[HistoricalEvent] = []
        date_str = f"{day} {MONTHS_RU_LOWER[month]}"
        
        # Улучшенные поисковые запросы по категориям
        search_templates_by_category = {
            'музыка': [
                f'"{date_str}" {year} "выпущен" альбом',
                f'"{date_str}" {year} "выпустил" сингл',
                f'"{date_str}" {year} "родился" музыкант',
                f'"{date_str}" {year} "состоялся" концерт',
                f'"{date_str}" {year} "премия" музыка'
            ],
            'фильмы': [
                f'"{date_str}" {year} "премьера" фильм',
                f'"{date_str}" {year} "вышел" кино',
                f'"{date_str}" {year} "родился" актёр',
                f'"{date_str}" {year} "родился" режиссёр',
                f'"{date_str}" {year} "Оскар"'
            ],
            'технологии': [
                f'"{date_str}" {year} "изобретён"',
                f'"{date_str}" {year} "патент"',
                f'"{date_str}" {year} "основана" компания',
                f'"{date_str}" {year} "запуск"',
                f'"{date_str}" {year} "представлен"'
            ],
            'игры': [
                f'"{date_str}" {year} "вышла" игра',
                f'"{date_str}" {year} "выпущена" видеоигра',
                f'"{date_str}" {year} "основана" студия',
                f'"{date_str}" {year} "турнир" киберспорт',
                f'"{date_str}" {year} "консоль"'
            ],
            'наука': [
                f'"{date_str}" {year} "открытие"',
                f'"{date_str}" {year} "изобретение"',
                f'"{date_str}" {year} "Нобелевская премия"',
                f'"{date_str}" {year} "родился" учёный',
                f'"{date_str}" {year} "эксперимент"'
            ],
            'спорт': [
                f'"{date_str}" {year} "чемпионат"',
                f'"{date_str}" {year} "олимпиада"',
                f'"{date_str}" {year} "рекорд"',
                f'"{date_str}" {year} "матч" финал',
                f'"{date_str}" {year} "родился" спортсмен'
            ],
            'история': [
                f'"{date_str}" {year} "событие"',
                f'"{date_str}" {year} "война"',
                f'"{date_str}" {year} "договор"',
                f'"{date_str}" {year} "революция"',
                f'"{date_str}" {year} "основание"'
            ]
        }
        
        # Ищем события за последние 200 лет (с шагом 5 лет для производительности)
        current_year = datetime.now(TIMEZONE).year
        search_years = list(range(current_year - 200, current_year + 1, 5))
        random.shuffle(search_years)  # Для разнообразия
        
        templates = search_templates_by_category.get(category, [f'"{date_str}" {year}'])
        
        for year in search_years[:10]:  # Ограничиваем количество проверяемых лет
            for template in templates[:3]:  # Берем первые 3 шаблона
                try:
                    search_query = template.replace("{year}", str(year))
                    logger.debug(f"Поиск: {search_query}")
                    
                    found_events = self._search_wikipedia_precise(search_query, category, day, month, year)
                    if found_events:
                        events.extend(found_events)
                        if len(events) >= 5:  # Ограничиваем количество найденных событий
                            return events
                        
                    time.sleep(0.5)  # Задержка между запросами
                except Exception as e:
                    logger.warning(f"Ошибка поиска по запросу '{search_query}': {e}")
                    continue
        
        return events
    
    def _search_wikipedia_precise(self, search_query: str, category: str, day: int, month: int, target_year: int) -> List[HistoricalEvent]:
        """Точный поиск статей на Википедии с проверкой даты"""
        events: List[HistoricalEvent] = []
        
        try:
            params = {
                'action': 'query',
                'format': 'json',
                'list': 'search',
                'srsearch': search_query,
                'srlimit': 10,
                'srwhat': 'text',
                'srprop': 'snippet'
            }
            
            headers = {'User-Agent': USER_AGENT}
            
            response = requests.get(
                WIKIPEDIA_API_URL, 
                params=params, 
                headers=headers, 
                timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            data = response.json()
            
            if 'query' in data and data['query']['search']:
                for article in data['query']['search']:
                    title = article['title']
                    
                    # Пропускаем нерелевантные статьи
                    if any(word in title.lower() for word in ['категория:', 'шаблон:', 'список', 'таблица', 'изображение']):
                        continue
                    
                    # Пытаемся получить полную статью для анализа
                    event_info = self._analyze_article_for_date_event(title, category, day, month, target_year)
                    if event_info:
                        events.append(event_info)
                        if len(events) >= 3:  # Ограничиваем количество на запрос
                            break
        
        except Exception as e:
            logger.warning(f"Ошибка точного поиска статей по запросу '{search_query}': {e}")
        
        return events
    
    def _analyze_article_for_date_event(self, title: str, category: str, day: int, month: int, target_year: int) -> Optional[HistoricalEvent]:
        """Анализируем статью на наличие события в конкретную дату"""
        try:
            # Получаем полный текст статьи
            full_text = self._get_article_full_text(title)
            if not full_text:
                return None
            
            date_patterns = [
                f"{day}\s*{MONTHS_RU_LOWER[month]}\s*{target_year}",
                f"{day}\s*{MONTHS_RU_LOWER[month]}\s*{target_year}\s*года",
                f"{target_year}\s*года\s*{day}\s*{MONTHS_RU_LOWER[month]}",
                f"{day}[\.\s]*{month:02d}[\.\s]*{target_year}"  # DD.MM.YYYY
            ]
            
            # Ищем упоминание конкретной даты в тексте
            date_found = False
            for pattern in date_patterns:
                if re.search(pattern, full_text, re.IGNORECASE):
                    date_found = True
                    break
            
            if not date_found:
                return None
            
            # Извлекаем факт о событии
            fact = self._extract_event_fact(full_text, day, month, target_year)
            if not fact:
                return None
            
            # Получаем описание
            description = self._get_article_description(title)
            
            encoded_title = quote(title.replace(' ', '_'), safe='')
            article_url = f"https://ru.wikipedia.org/wiki/{encoded_title}"
            
            return {
                'title': title,
                'year': target_year,
                'description': description,
                'url': article_url,
                'category': category,
                'full_article': full_text[:5000]  # Сохраняем часть текста для извлечения фактов
            }
            
        except Exception as e:
            logger.warning(f"Ошибка анализа статьи '{title}': {e}")
            return None
    
    def _get_article_full_text(self, title: str) -> Optional[str]:
        """Получаем полный текст статьи"""
        try:
            params = {
                'action': 'query',
                'format': 'json',
                'prop': 'extracts|revisions',
                'explaintext': True,
                'exsectionformat': 'plain',
                'rvprop': 'content',
                'titles': title
            }
            
            headers = {'User-Agent': USER_AGENT}
            
            response = requests.get(
                WIKIPEDIA_API_URL, 
                params=params, 
                headers=headers, 
                timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            data = response.json()
            
            pages = data['query']['pages']
            page_id = list(pages.keys())[0]
            page = pages[page_id]
            
            if 'missing' not in page:
                # Пробуем получить полный текст разными способами
                if 'revisions' in page:
                    return page['revisions'][0].get('*', '')
                elif 'extract' in page:
                    return page['extract']
        
        except Exception as e:
            logger.warning(f"Ошибка получения полного текста статьи '{title}': {e}")
        
        return None
    
    def _extract_event_fact(self, text: str, day: int, month: int, year: int) -> Optional[str]:
        """Извлекаем конкретный факт о событии из текста"""
        try:
            date_str = f"{day} {MONTHS_RU_LOWER[month]} {year}"
            
            # Ищем предложения, содержащие дату
            sentences = re.split(r'[.!?]+', text)
            
            for sentence in sentences:
                if date_str.lower() in sentence.lower():
                    # Очищаем предложение и обрезаем
                    cleaned = re.sub(r'\s+', ' ', sentence.strip())
                    if len(cleaned) > 20 and len(cleaned) < 500:
                        return cleaned + '.'
            
            # Если не нашли точную дату, ищем год
            for sentence in sentences:
                if str(year) in sentence and len(sentence) > 20:
                    # Проверяем, что это действительно о событии, а не о чем-то другом
                    if any(word in sentence.lower() for word in [
                        'произошло', 'состоялось', 'вышел', 'вышла', 'выпущен', 
                        'родился', 'родилась', 'основан', 'основана', 'открытие',
                        'изобретение', 'премьера', 'турнир', 'чемпионат'
                    ]):
                        cleaned = re.sub(r'\s+', ' ', sentence.strip())
                        if len(cleaned) < 500:
                            return cleaned + '.'
        
        except Exception as e:
            logger.warning(f"Ошибка извлечения факта: {e}")
        
        return None
    
    def _get_article_description(self, title: str) -> str:
        """Получаем краткое описание статьи"""
        try:
            params = {
                'action': 'query',
                'format': 'json',
                'prop': 'extracts',
                'exintro': True,
                'explaintext': True,
                'exchars': 300,
                'titles': title
            }
            
            headers = {'User-Agent': USER_AGENT}
            
            response = requests.get(
                WIKIPEDIA_API_URL, 
                params=params, 
                headers=headers, 
                timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            data = response.json()
            
            pages = data['query']['pages']
            page_id = list(pages.keys())[0]
            page = pages[page_id]
            
            if 'extract' in page and page['extract']:
                return page['extract'][:300] + ('...' if len(page['extract']) > 300 else '')
        
        except Exception as e:
            logger.warning(f"Ошибка получения описания статьи '{title}': {e}")
        
        return "Историческое событие, о котором сохранились сведения."
    
    def _search_known_events(self, day: int, month: int, category: str) -> List[HistoricalEvent]:
        """Ищем известные события по категориям (fallback)"""
        events: List[HistoricalEvent] = []
        
        known_events_db = {
            (14, 1, 'музыка'): [
                {'title': 'The Beatles выпустили альбом "Abbey Road"', 'year': 1969},
                {'title': 'Вышел альбом "The Dark Side of the Moon" группы Pink Floyd', 'year': 1973},
                {'title': 'Родился Дэйв Грол, американский музыкант', 'year': 1969},
            ],
            (14, 1, 'фильмы'): [
                {'title': 'Вышел фильм "Крестный отец" Фрэнсиса Форда Копполы', 'year': 1972},
                {'title': 'Родился Джейсон Бейтман, американский актёр и режиссёр', 'year': 1969},
                {'title': 'Состоялась премьера фильма "Матрица"', 'year': 1999},
            ],
            (14, 1, 'технологии'): [
                {'title': 'Представлен первый компьютер Apple Macintosh', 'year': 1984},
                {'title': 'Основана компания Nintendo', 'year': 1889},
                {'title': 'Запущен первый веб-сайт', 'year': 1991},
            ],
            (14, 1, 'игры'): [
                {'title': 'Вышла игра "The Legend of Zelda: Ocarina of Time"', 'year': 1998},
                {'title': 'Вышла игра "Super Mario 64"', 'year': 1996},
                {'title': 'Основана компания Capcom', 'year': 1979},
            ],
            (14, 1, 'наука'): [
                {'title': 'Альберт Эйнштейн представил общую теорию относительности', 'year': 1915},
                {'title': 'Открытие планеты Нептун', 'year': 1846},
                {'title': 'Родился Альберт Швейцер, немецкий философ и врач', 'year': 1875},
            ],
            (14, 1, 'спорт'): [
                {'title': 'Открылись первые зимние Олимпийские игры в Шамони', 'year': 1924},
                {'title': 'Майк Тайсон стал самым молодым чемпионом мира в тяжелом весе', 'year': 1986},
                {'title': 'Родился Валерий Харламов, советский хоккеист', 'year': 1948},
            ],
            (14, 1, 'история'): [
                {'title': 'Состоялась коронация Георга VI, короля Великобритании', 'year': 1937},
                {'title': 'Начало экспедиции Роберта Скотта к Южному полюсу', 'year': 1911},
                {'title': 'Родился Альберт Швейцер, немецкий философ и врач', 'year': 1875},
            ],
        }
        
        key = (day, month, category)
        if key in known_events_db:
            for event_data in known_events_db[key]:
                # Пытаемся найти статью на Википедии
                article_info = self._find_wikipedia_article_for_known_event(event_data['title'], event_data['year'], category, day, month)
                if article_info:
                    events.append(article_info)
                else:
                    # Если не нашли статью, создаем fallback событие
                    events.append({
                        'title': event_data['title'],
                        'year': event_data['year'],
                        'description': f'Историческое событие, произошедшее {day} {MONTHS_RU_LOWER[month]} {event_data["year"]} года.',
                        'url': f'https://ru.wikipedia.org/wiki/{day}_{MONTHS_RU[month].lower()}',
                        'category': category,
                        'full_article': ''
                    })
        
        return events
    
    def _find_wikipedia_article_for_known_event(self, title: str, year: int, category: str, day: int, month: int) -> Optional[HistoricalEvent]:
        """Пытаемся найти статью на Википедии для известного события"""
        try:
            params = {
                'action': 'query',
                'format': 'json',
                'list': 'search',
                'srsearch': f'{title} {year}',
                'srlimit': 3,
                'srwhat': 'text'
            }
            
            headers = {'User-Agent': USER_AGENT}
            
            response = requests.get(
                WIKIPEDIA_API_URL, 
                params=params, 
                headers=headers, 
                timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            data = response.json()
            
            if 'query' in data and data['query']['search']:
                article = data['query']['search'][0]
                article_title = article['title']
                
                # Получаем описание и полный текст
                description = self._get_article_description(article_title)
                full_text = self._get_article_full_text(article_title)
                
                # Извлекаем факт
                fact = None
                if full_text:
                    fact = self._extract_event_fact(full_text, day, month, year)
                
                encoded_title = quote(article_title.replace(' ', '_'), safe='')
                article_url = f"https://ru.wikipedia.org/wiki/{encoded_title}"
                
                return {
                    'title': article_title,
                    'year': year,
                    'description': description,
                    'url': article_url,
                    'category': category,
                    'full_article': full_text[:5000] if full_text else ''
                }
        
        except Exception as e:
            logger.warning(f"Ошибка поиска статьи для известного события '{title}': {e}")
        
        return None
    
    def get_historical_event(self, category: str) -> Tuple[str, Optional[int], str, str, str]:
        """
        Получаем историческое событие "В этот день" для текущей даты
        """
        try:
            now = datetime.now(TIMEZONE)
            day = now.day
            month = now.month
            
            logger.info(f"Улучшенный поиск исторических событий за {day} {MONTHS_RU[month]} в категории: {category}")
            
            # Ищем исторические события для текущей даты
            events = self.search_historical_events(day, month, category)
            
            # Фильтруем уже использованные события
            available_events = [
                event for event in events 
                if event['title'] not in self.used_events[category]
            ]
            
            # Если все события уже использовались, очищаем список для этой категории
            if not available_events and events:
                logger.info(f"Все события в категории '{category}' использованы, очищаем историю")
                self.used_events[category] = set()
                available_events = events
            
            # Выбираем случайное событие из доступных
            if not available_events:
                logger.warning(f"Не найдено исторических событий за {day} {MONTHS_RU[month]} в категории {category}")
                return self._get_fallback_event(category, day, month)
            
            event = random.choice(available_events)
            
            # Добавляем в использованные
            self.used_events[category].add(event['title'])
            logger.info(f"Выбрано историческое событие: {event['title']} ({event['year']} год)")
            
            return (
                event['title'],
                event['year'],
                event['description'],
                event['url'],
                self._format_event_fact(event, day, month)  # Используем форматированный факт
            )
            
        except Exception as e:
            logger.error(f"Ошибка получения исторического события: {e}")
            return self._get_fallback_event(category, datetime.now(TIMEZONE).day, datetime.now(TIMEZONE).month)
    
    def _format_event_fact(self, event: HistoricalEvent, day: int, month: int) -> str:
        """Форматируем факт события"""
        if event.get('full_article'):
            # Пытаемся извлечь точный факт из полного текста
            fact = self._extract_event_fact(event['full_article'], day, month, event['year'])
            if fact:
                return fact
        
        # Fallback: используем заголовок и год
        return f"{event['title']} ({event['year']} год)."
    
    def _get_fallback_event(self, category: str, day: int, month: int) -> Tuple[str, Optional[int], str, str, str]:
        """Резервные исторические события на случай недоступности Wikipedia"""
        if category in self.fallback_cache:
            event = random.choice(self.fallback_cache[category])
            return event['title'], event['year'], event['description'], event['url'], event['title']
        
        # Расширенная база fallback событий
        historical_events_db = {
            'музыка': [
                {
                    'title': 'The Beatles выпустили альбом "Abbey Road"',
                    'year': 1969,
                    'description': 'Легендарный альбом был записан в студии на Эбби-Роуд в Лондоне.',
                    'url': 'https://ru.wikipedia.org/wiki/Abbey_Road',
                    'fact': 'The Beatles выпустили альбом "Abbey Road" 14 января 1969 года.'
                },
                {
                    'title': 'Вышел альбом "The Dark Side of the Moon" группы Pink Floyd',
                    'year': 1973,
                    'description': 'Концептуальный альбом, который провел в чарте Billboard 200 рекордные 981 неделю.',
                    'url': 'https://ru.wikipedia.org/wiki/The_Dark_Side_of_the_Moon',
                    'fact': 'Альбом "The Dark Side of the Moon" группы Pink Floyd был выпущен 14 января 1973 года.'
                },
            ],
            'фильмы': [
                {
                    'title': 'Вышел фильм "Крестный отец" Фрэнсиса Форда Копполы',
                    'year': 1972,
                    'description': 'Фильм по роману Марио Пьюзо получил три премии "Оскар".',
                    'url': 'https://ru.wikipedia.org/wiki/Крёстный_отец_(фильм)',
                    'fact': 'Фильм "Крестный отец" вышел в прокат 14 января 1972 года.'
                },
            ],
            'технологии': [
                {
                    'title': 'Представлен первый компьютер Apple Macintosh',
                    'year': 1984,
                    'description': 'Компьютер представил Стив Джобс во время Супербоула.',
                    'url': 'https://ru.wikipedia.org/wiki/Macintosh',
                    'fact': 'Первый компьютер Apple Macintosh был представлен 14 января 1984 года.'
                },
            ],
            'игры': [
                {
                    'title': 'Вышла игра "The Legend of Zelda: Ocarina of Time"',
                    'year': 1998,
                    'description': 'Игра для Nintendo 64, которую многие считают величайшей видеоигрой.',
                    'url': 'https://ru.wikipedia.org/wiki/The_Legend_of_Zelda:_Ocarina_of_Time',
                    'fact': 'Игра "The Legend of Zelda: Ocarina of Time" вышла 14 января 1998 года.'
                },
            ],
            'наука': [
                {
                    'title': 'Альберт Эйнштейн представил общую теорию относительности',
                    'year': 1915,
                    'description': 'Теория радикально изменила понимание гравитации, пространства и времени.',
                    'url': 'https://ru.wikipedia.org/wiki/Общая_теория_относительности',
                    'fact': 'Альберт Эйнштейн представил общую теорию относительности 14 января 1915 года.'
                },
            ],
            'спорт': [
                {
                    'title': 'Открылись первые зимние Олимпийские игры',
                    'year': 1924,
                    'description': 'Игры прошли в Шамони (Франция) с участием 258 спортсменов из 16 стран.',
                    'url': 'https://ru.wikipedia.org/wiki/Зимние_Олимпийские_игры_1924',
                    'fact': 'Первые зимние Олимпийские игры открылись 14 января 1924 года.'
                },
            ],
            'история': [
                {
                    'title': 'Высадка на Луну миссии "Аполлон-11"',
                    'year': 1969,
                    'description': 'Нил Армстронг стал первым человеком, ступившим на поверхность Луны.',
                    'url': 'https://ru.wikipedia.org/wiki/Аполлон-11',
                    'fact': 'Миссия "Аполлон-11" стартовала 14 января 1969 года.'
                },
            ]
        }
        
        self.fallback_cache = historical_events_db
        events = historical_events_db.get(category, [{
            'title': f'Историческое событие в категории {category}',
            'year': 1900,
            'description': f'Интересное историческое событие, произошедшее {day} {MONTHS_RU_LOWER[month]}.',
            'url': f'https://ru.wikipedia.org/wiki/{day}_{MONTHS_RU_LOWER[month]}',
            'fact': f'Историческое событие произошло {day} {MONTHS_RU_LOWER[month]} 1900 года.'
        }])
        
        event = random.choice(events)
        return event['title'], event['year'], event['description'], event['url'], event.get('fact', event['title'])
    
    def create_event_message(self, category: str) -> Tuple[str, Optional[InlineKeyboardMarkup]]:
        """Создаем сообщение с историческим событием 'В этот день' в указанном формате"""
        # Получаем текущую дату (день и месяц)
        day, month_ru, current_year = self.get_todays_date_parts()
        
        # Получаем историческое событие
        title, event_year, description, url, fact = self.get_historical_event(category)
        
        # Форматируем сообщение в указанном формате
        message = f"**В ЭТОТ ДЕНЬ: {day} {month_ru} {event_year} года | КАТЕГОРИЯ: {category.upper()}**\n\n"
        
        # Эмодзи для категории
        category_emoji = CATEGORY_EMOJIS.get(category, '📌')
        
        # Используем факт как основной текст
        message += f"{category_emoji} {fact}\n\n"
        
        # Добавляем описание, если есть и оно не дублирует факт
        if description and description not in fact:
            # Обрезаем описание, если оно слишком длинное
            if len(description) > 300:
                description = description[:300] + '...'
            message += f"{description}\n\n"
        
        # Добавляем ссылку для тех, кто хочет узнать больше
        if url:
            message += f"📖 [Подробнее на Википедии]({url})"
        
        # Возвращаем только сообщение, без клавиатуры
        return message, None

# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========

def get_jobs_from_queue(job_queue: JobQueue):
    """Получить список задач с поддержкой разных версий PTB"""
    try:
        return job_queue.get_jobs()
    except AttributeError:
        try:
            return job_queue.jobs()
        except AttributeError as e:
            logger.error(f"Не удалось получить задачи из JobQueue: {e}")
            return []

# Декоратор для проверки прав пользователя
def restricted(func):
    @wraps(func)
    async def wrapped(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        username = update.effective_user.username
        config = BotConfig()
        allowed_users = config.allowed_users
        
        if username not in allowed_users:
            if update.callback_query:
                await update.callback_query.answer("❌ У вас нет прав для этой операции", show_alert=True)
            else:
                await update.message.reply_text("❌ У вас нет прав для этой команды")
            return None
        return await func(update, context, *args, **kwargs)
    return wrapped

def get_greeting_by_meeting_day() -> str:
    """Специальные приветствия для дней планёрок со ссылкой на Zoom"""
    weekday = datetime.now(TIMEZONE).weekday()
    day_names_ru = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
    current_day = day_names_ru[weekday]
    
    # Проверяем, настроена ли Zoom-ссылка
    if ZOOM_LINK == DEFAULT_ZOOM_LINK:
        zoom_note = "\n\n⚠️ Zoom-ссылка не настроена! Используйте /info для проверки"
    else:
        zoom_link_formatted = f'<a href="{ZOOM_LINK}">Присоединиться к Zoom</a>'
        zoom_notes = [
            f"\n\n🎥 {zoom_link_formatted} | 👈",
            f"\n\n👨💻 {zoom_link_formatted} | 👈",
            f"\n\n💻 {zoom_link_formatted} | 👈",
            f"\n\n🔗 {zoom_link_formatted} | 👈",
            f"\n\n📅 {zoom_link_formatted} | 👈",
            f"\n\n✉️ {zoom_link_formatted} | 👈",
            f"\n\n🎯 {zoom_link_formatted} | 👈",
            f"\n\n🤝 {zoom_link_formatted} | 👈",
            f"\n\n🚀 {zoom_link_formatted} | 👈",
            f"\n\n⚡ {zoom_link_formatted} | 👈",
        ]
        zoom_note = random.choice(zoom_notes)
    
    if weekday in MEETING_DAYS:
        day_names = {0: "ПОНЕДЕЛЬНИК", 2: "СРЕДА", 4: "ПЯТНИЦА"}
        
        greetings = {
            0: [
                f"🚀 <b>{day_names[0]}</b> - старт новой недели!\n\n📋 <i>Планёрка в 9:30 по МСК</i>. Давайте обсудим планы на неделю! 🌟{zoom_note}",
                f"🌞 Доброе утро! Сегодня <b>{day_names[0]}</b>!\n\n🤝 <i>Планёрка в 9:30 по МСК</i>. Начинаем неделю продуктивно! 💪{zoom_note}",
                f"⚡ <b>{day_names[0]}</b>, время действовать!\n\n🎯 <i>Утренняя планёрка в 9:30 по МСК</i>. Подготовьте ваши вопросы! 📊{zoom_note}"
            ],
            2: [
                f"⚡ <b>{day_names[2]}</b> - середина недели!\n\n📋 <i>Планёрка в 9:30 по МСК</i>. Время для корректировок и обновлений! 🔄{zoom_note}",
                f"🌞 <b>{day_names[2]}</b>, доброе утро!\n\n🤝 <i>Планёрка в 9:30 по МСК</i>. Как продвигаются задачи? 📈{zoom_note}",
                f"💪 <b>{day_names[2]}</b> - день прорыва!\n\n🎯 <i>Планёрка в 9:30 по МСК</i>. Делитесь прогрессом! 🚀{zoom_note}"
            ],
            4: [
                f"🎉 <b>{day_names[4]}</b> - завершаем неделю!\n\n📋 <i>Планёрка в 9:30 по МСК</i>. Давайте подведем итоги недели! 🏆{zoom_note}",
                f"🌞 Пятничное утро! 🎊\n\n🤝 <b>{day_names[4]}</b>, <i>планёрка в 9:30 по МСК</i>. Как прошла неделя? 📊{zoom_note}",
                f"✨ <b>{day_names[4]}</b> - время подводить итоги!\n\n🎯 <i>Планёрка в 9:30 по МСК</i>. Что успели за неделю? 📈{zoom_note}"
            ]
        }
        return random.choice(greetings[weekday])
    else:
        if ZOOM_LINK == DEFAULT_ZOOM_LINK:
            zoom_note = "\n\n⚠️ Zoom-ссылка не настроена!"
        else:
            zoom_note = f'\n\n🎥 <a href="{ZOOM_LINK}">Присоединиться к Zoom</a> | Присоединяйтесь к встрече'
        return f"👋 Доброе утро! Сегодня <i>{current_day}</i>.\n\n📋 <i>Напоминаю о планёрке в 9:30 по МСК</i>.{zoom_note}"

class BotConfig:
    """Класс для управления конфигурацией бота"""
    
    def __init__(self):
        self.data = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if "allowed_users" not in data:
                        data["allowed_users"] = ["Stiff_OWi", "gshabanov"]
                    if "active_reminders" not in data:
                        data["active_reminders"] = {}
                    if "event_current_index" not in data:
                        data["event_current_index"] = 0
                    return data
            except Exception as e:
                logger.error(f"Ошибка загрузки конфига: {e}")
        return {
            "chat_id": None,
            "allowed_users": ["Stiff_OWi", "gshabanov"],
            "active_reminders": {},
            "event_current_index": 0
        }
    
    def save(self) -> None:
        try:
            with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Ошибка сохранения конфига: {e}")
    
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
    
    def add_allowed_user(self, username: str) -> bool:
        if username not in self.allowed_users:
            self.data["allowed_users"].append(username)
            self.save()
            return True
        return False
    
    def remove_allowed_user(self, username: str) -> bool:
        if username in self.allowed_users:
            self.data["allowed_users"].remove(username)
            self.save()
            return True
        return False
    
    @property
    def active_reminders(self) -> Dict[str, ReminderData]:
        return self.data.get("active_reminders", {})
    
    def add_active_reminder(self, message_id: int, chat_id: int, job_name: str) -> None:
        self.data["active_reminders"][job_name] = {
            "message_id": message_id,
            "chat_id": chat_id,
            "created_at": datetime.now(TIMEZONE).isoformat()
        }
        self.save()
    
    def remove_active_reminder(self, job_name: str) -> bool:
        if job_name in self.data["active_reminders"]:
            del self.data["active_reminders"][job_name]
            self.save()
            return True
        return False
    
    def clear_active_reminders(self) -> None:
        self.data["active_reminders"] = {}
        self.save()
    
    @property
    def event_current_index(self) -> int:
        return self.data.get("event_current_index", 0)
    
    @event_current_index.setter
    def event_current_index(self, value: int) -> None:
        self.data["event_current_index"] = value
        self.save()
    
    def increment_event_index(self) -> int:
        """Увеличиваем индекс событий и возвращаем новый"""
        current = self.event_current_index
        new_index = (current + 1) % len(EVENT_CATEGORIES)
        self.event_current_index = new_index
        logger.info(f"Индекс событий увеличен: {current} -> {new_index}")
        return new_index
    
    def get_event_scheduler(self) -> EventScheduler:
        """Получаем планировщик исторических событий"""
        scheduler = EventScheduler()
        scheduler.current_index = self.event_current_index
        return scheduler

# ========== ФУНКЦИИ ДЛЯ ИСТОРИЧЕСКИХ СОБЫТИЙ "В ЭТОТ ДЕНЬ" ==========

async def send_daily_event(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправка ежедневного исторического события 'В этот день'"""
    try:
        config = BotConfig()
        chat_id = config.chat_id

        if not chat_id:
            logger.error("Chat ID не установлен для отправки исторических событий!")
            # Пробуем снова через час
            context.application.job_queue.run_once(
                lambda ctx: asyncio.create_task(schedule_next_event(ctx)),
                3600
            )
            return

        # Получаем планировщик
        event_scheduler = config.get_event_scheduler()
        
        # Получаем текущую категорию
        category = event_scheduler.get_next_category()
        logger.info(f"Отправка улучшенного ИСТОРИЧЕСКОГО события 'В этот день' категории: {category}")
        
        # Создаем сообщение с историческим событием
        message, keyboard = event_scheduler.create_event_message(category)
        
        # Отправляем событие
        await context.bot.send_message(
            chat_id=chat_id,
            text=message,
            parse_mode=ParseMode.MARKDOWN,
            disable_web_page_preview=False,
            reply_markup=keyboard
        )
        
        # Увеличиваем индекс для следующего события
        event_scheduler.increment_category()
        config.event_current_index = event_scheduler.current_index
        
        logger.info(f"✅ Улучшенное ИСТОРИЧЕСКОЕ событие 'В этот день' отправлено: {category}")
        
        # Планируем следующую отправку
        await schedule_next_event(context)
        
    except Exception as e:
        logger.error(f"❌ Ошибка отправки улучшенного ИСТОРИЧЕСКОГО события 'В этот день': {e}")
        # Пробуем снова через 5 минут
        context.application.job_queue.run_once(
            lambda ctx: asyncio.create_task(schedule_next_event(ctx)),
            300
        )

@restricted
async def send_event_now(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправить улучшенное ИСТОРИЧЕСКОЕ событие 'В этот день' немедленно по команде"""
    config = BotConfig()
    chat_id = config.chat_id

    if not chat_id:
        await update.message.reply_text("❌ Сначала установите чат командой /setchat")
        return

    try:
        # Получаем планировщик
        event_scheduler = config.get_event_scheduler()
        
        # Получаем текущую категорию
        category = event_scheduler.get_next_category()
        logger.info(f"Отправка улучшенного ИСТОРИЧЕСКОГО события 'В этот день' по команде: {category}")
        
        # Создаем сообщение с историческим событием
        message, keyboard = event_scheduler.create_event_message(category)
        
        # Отправляем событие в целевой чат
        await context.bot.send_message(
            chat_id=chat_id,
            text=message,
            parse_mode=ParseMode.MARKDOWN,
            disable_web_page_preview=False,
            reply_markup=keyboard
        )
        
        # Увеличиваем индекс для следующего события
        event_scheduler.increment_category()
        config.event_current_index = event_scheduler.current_index
        
        logger.info(f"Улучшенное ИСТОРИЧЕСКОЕ событие 'В этот день' отправлено по команде: {category}")
        
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка при отправке улучшенного ИСТОРИЧЕСКОГО события: {str(e)}")
        logger.error(f"Ошибка в команде /eventnow: {e}")

async def show_next_event_category(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показать следующую категорию ИСТОРИЧЕСКИХ событий 'В этот день'"""
    config = BotConfig()
    event_scheduler = config.get_event_scheduler()
    
    # Получаем текущую и следующую категории
    current_category = event_scheduler.get_next_category()
    next_category = EVENT_CATEGORIES[(event_scheduler.current_index + 1) % len(EVENT_CATEGORIES)]
    
    current_emoji = CATEGORY_EMOJIS.get(current_category, '📌')
    next_emoji = CATEGORY_EMOJIS.get(next_category, '📌')
    
    # Получаем текущую дату для отображения
    now = datetime.now(TIMEZONE)
    day = now.day
    month_ru = MONTHS_RU[now.month]
    
    # Рассчитываем время следующей отправки
    next_time = calculate_next_event_time()
    moscow_time = next_time.astimezone(TIMEZONE)
    
    response = f"📅 *Информация о улучшенной рубрике 'В ЭТОТ ДЕНЬ':*\n\n"
    response += f"🗓️ *Исторические события за:* {day} {month_ru}\n\n"
    response += f"{current_emoji} *Текущая категория:* {current_category.upper()}\n"
    response += f"{next_emoji} *Следующая категория:* {next_category.upper()}\n\n"
    response += f"⏰ *Следующая отправка:* {moscow_time.strftime('%d.%m.%Y в %H:%M')} по МСК\n"
    response += f"📜 *Тип событий:* ИСТОРИЧЕСКИЕ (произошедшие в эту дату)\n"
    response += f"🎯 *Улучшенный поиск:* гарантирует точную дату события\n"
    response += f"🔍 *Формат:* В ЭТОТ ДЕНЬ: {day} {month_ru} ГОД года | КАТЕГОРИЯ: КАТЕГОРИЯ\n"
    response += f"📖 *Факт:* конкретное предложение о событии\n"
    response += f"🔗 *Ссылка:* полная статья на Википедии\n\n"
    response += f"🔄 *События не повторяются в пределах категории!*"
    
    await update.message.reply_text(response, parse_mode=ParseMode.MARKDOWN)

def calculate_next_event_time() -> datetime:
    """Рассчитать время следующей отправки события"""
    now = datetime.now(pytz.UTC)
    
    # Проверяем, сегодня ли нужный день и время
    if now.weekday() in EVENT_DAYS:
        reminder_time = now.replace(
            hour=EVENT_SEND_TIME["hour"],
            minute=EVENT_SEND_TIME["minute"],
            second=0,
            microsecond=0
        )
        if now < reminder_time:
            return reminder_time

    # Ищем следующий рабочий день
    days_ahead = 1
    max_days = 365  # Защита от зацикливания
    while days_ahead <= max_days:
        next_day = now + timedelta(days=days_ahead)
        if next_day.weekday() in EVENT_DAYS:
            return next_day.replace(
                hour=EVENT_SEND_TIME["hour"],
                minute=EVENT_SEND_TIME["minute"],
                second=0,
                microsecond=0
            )
        days_ahead += 1
    
    raise ValueError(f"Не найден подходящий день за {max_days} дней")

async def schedule_next_event(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Запланировать следующую отправку улучшенного ИСТОРИЧЕСКОГО события 'В этот день'"""
    try:
        next_time = calculate_next_event_time()
        config = BotConfig()
        chat_id = config.chat_id

        if not chat_id:
            logger.warning("Chat ID не установлен, планирование ИСТОРИЧЕСКИХ событий отложено")
            # Пробуем снова через час
            context.application.job_queue.run_once(
                lambda ctx: asyncio.create_task(schedule_next_event(ctx)),
                3600
            )
            return

        now = datetime.now(pytz.UTC)
        delay = (next_time - now).total_seconds()

        if delay > 0:
            job_name = f"daily_event_{next_time.strftime('%Y%m%d_%H%M')}"
            
            # Проверяем, нет ли уже такой задачи
            existing_jobs = [j for j in get_jobs_from_queue(context.application.job_queue) 
                            if j.name == job_name]
            
            if not existing_jobs:
                context.application.job_queue.run_once(
                    send_daily_event,
                    delay,
                    chat_id=chat_id,
                    name=job_name
                )

                logger.info(f"Следующая отправка улучшенного ИСТОРИЧЕСКОГО события 'В этот день' запланирована на {next_time} UTC")
                logger.info(f"Это будет в {(next_time + timedelta(hours=3)).strftime('%H:%M')} по МСК")
                
                # Получаем планировщик для логирования следующей категории
                event_scheduler = config.get_event_scheduler()
                logger.info(f"Следующая категория ИСТОРИЧЕСКИХ событий: {event_scheduler.get_next_category()}")
            else:
                logger.info(f"Отправка ИСТОРИЧЕСКОГО события на {next_time} уже запланирована")
        else:
            # Если время уже прошло, планируем на следующий день
            logger.warning(f"Время отправки ИСТОРИЧЕСКОГО события уже прошло ({next_time}), планируем на следующий день")
            context.application.job_queue.run_once(
                lambda ctx: asyncio.create_task(schedule_next_event(ctx)),
                60  # Через минуту
            )
            
    except Exception as e:
        logger.error(f"Ошибка планирования улучшенного ИСТОРИЧЕСКОГО события: {e}")
        # Пробуем снова через 5 минут
        context.application.job_queue.run_once(
            lambda ctx: asyncio.create_task(schedule_next_event(ctx)),
            300
        )

# ========== ФУНКЦИИ ПЛАНЁРОК ==========

async def send_reminder(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправка напоминания о планёрке"""
    config = BotConfig()
    chat_id = config.chat_id

    if not chat_id:
        logger.error("Chat ID не установлен!")
        return

    keyboard = [
        [InlineKeyboardButton("Отменить планёрку", callback_data="cancel_meeting")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    message_text = get_greeting_by_meeting_day()

    try:
        message = await context.bot.send_message(
            chat_id=chat_id,
            text=message_text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=False
        )

        job_name = context.job.name if hasattr(context, 'job') and context.job else f"manual_{datetime.now().timestamp()}"
        config.add_active_reminder(message.message_id, chat_id, job_name)

        logger.info(f"Отправлено напоминание в чат {chat_id}, сообщение {message.message_id}")

    except Exception as e:
        logger.error(f"Ошибка при отправке напоминания: {e}")

@restricted
async def cancel_meeting_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    context.user_data["original_message_id"] = query.message.message_id
    context.user_data["original_chat_id"] = query.message.chat_id

    keyboard = [
        [InlineKeyboardButton(option, callback_data=f"reason_{i}")]
        for i, option in enumerate(CANCELLATION_OPTIONS)
    ]

    await query.edit_message_text(
        text="📝 Выберите причину отмены планёрки:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

    return SELECTING_REASON

async def select_reason_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    
    if not query.data or not query.data.startswith("reason_"):
        logger.warning(f"Некорректный callback data: {query.data}")
        await query.message.reply_text("❌ Произошла ошибка. Попробуйте снова.")
        return ConversationHandler.END
    
    try:
        reason_index = int(query.data.split("_")[1])
        if reason_index < 0 or reason_index >= len(CANCELLATION_OPTIONS):
            raise ValueError("Некорректный индекс причины")
    except (ValueError, IndexError) as e:
        logger.warning(f"Ошибка парсинга callback data: {e}, data: {query.data}")
        await query.message.reply_text("❌ Произошла ошибка. Попробуйте снова.")
        return ConversationHandler.END
    
    reason = CANCELLATION_OPTIONS[reason_index]
    
    context.user_data["selected_reason"] = reason
    context.user_data["reason_index"] = reason_index
    
    if reason_index == 2:
        return await show_date_selection(update, context)
    else:
        return await confirm_cancellation(update, context)

async def show_date_selection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    
    keyboard = []
    today = datetime.now(TIMEZONE)
    
    meeting_dates = []
    for i in range(1, 15):
        next_day = today + timedelta(days=i)
        if next_day.weekday() in MEETING_DAYS:
            date_str = next_day.strftime("%d.%m.%Y (%A)")
            callback_data = f"date_{next_day.strftime('%Y-%m-%d')}"
            meeting_dates.append((next_day, date_str, callback_data))
    
    current_week = []
    for date_obj, date_str, callback_data in meeting_dates:
        week_num = date_obj.isocalendar()[1]
        
        if not current_week or week_num != current_week[0][0]:
            if current_week:
                week_buttons = [InlineKeyboardButton(date_str, callback_data=cb) for _, date_str, cb in current_week]
                keyboard.append(week_buttons)
            
            current_week = [(week_num, date_str, callback_data)]
        else:
            current_week.append((week_num, date_str, callback_data))
    
    if current_week:
        week_buttons = [InlineKeyboardButton(date_str, callback_data=cb) for _, date_str, cb in current_week]
        keyboard.append(week_buttons)
    
    keyboard.append([InlineKeyboardButton("✏️ Ввести свою дату", callback_data="custom_date")])
    keyboard.append([InlineKeyboardButton("↩️ Назад к причиным", callback_data="back_to_reasons")])
    
    await query.edit_message_text(
        text="📅 Выберите дату для переноса планёрки:\n\n"
             "<b>Ближайшие дни планёрок (Пн/Ср/Пт):</b>",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.HTML
    )
    
    return SELECTING_DATE

async def date_selected_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    
    if query.data == "custom_date":
        await query.edit_message_text(
            text="✏️ Введите дату в формате ДД.ММ.ГГГГ\n"
                 "Например: 15.12.2024\n\n"
                 "<b>Важно:</b> выбирайте только дни планёрок (понедельник, среда, пятница)\n\n"
                 "Или отправьте 'отмена' для возврата.",
            parse_mode=ParseMode.HTML
        )
        return CONFIRMING_DATE
    
    if query.data == "back_to_reasons":
        keyboard = [
            [InlineKeyboardButton(option, callback_data=f"reason_{i}")]
            for i, option in enumerate(CANCELLATION_OPTIONS)
        ]
        
        await query.edit_message_text(
            text="📝 Выберите причину отмены планёрки:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return SELECTING_REASON
    
    try:
        selected_date_str = query.data.split("_")[1]
        selected_date = datetime.strptime(selected_date_str, "%Y-%m-%d")
        
        context.user_data["selected_date"] = selected_date_str
        context.user_data["selected_date_display"] = selected_date.strftime("%d.%m.%Y")
        
        return await show_confirmation(update, context)
    except (IndexError, ValueError) as e:
        logger.error(f"Ошибка обработки выбора даты: {e}, data: {query.data}")
        await query.message.reply_text("❌ Произошла ошибка. Попробуйте снова.")
        return ConversationHandler.END

async def handle_custom_date(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_input = update.message.text.strip().lower()
    
    if user_input == 'отмена':
        keyboard = [
            [InlineKeyboardButton(option, callback_data=f"reason_{i}")]
            for i, option in enumerate(CANCELLATION_OPTIONS)
        ]
        
        await update.message.reply_text(
            "Возвращаюсь к выбору причины...",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return SELECTING_REASON
    
    try:
        formats = ["%d.%m.%Y", "%d/%m/%Y", "%d-%m-%Y", "%d %m %Y"]
        selected_date = None
        
        for fmt in formats:
            try:
                selected_date = datetime.strptime(user_input, fmt)
                break
            except ValueError:
                continue
        
        if not selected_date:
            raise ValueError("Неверный формат даты")
        
        today = datetime.now(TIMEZONE).date()
        if selected_date.date() <= today:
            await update.message.reply_text(
                "❌ Дата должна быть в будущем! Попробуйте снова:"
            )
            return CONFIRMING_DATE
        
        if selected_date.weekday() not in MEETING_DAYS:
            days_names = ["понедельник", "вторник", "среду", "четверг", "пятницу", "субботу", "воскресенье"]
            meeting_days_names = [days_names[i] for i in MEETING_DAYS]
            
            await update.message.reply_text(
                f"❌ В эту дату нет планёрок! Планёрки бывают по {', '.join(meeting_days_names)}.\n"
                "Попробуйте снова или отправьте 'отмена':"
            )
            return CONFIRMING_DATE
        
        context.user_data["selected_date"] = selected_date.strftime("%Y-%m-%d")
        context.user_data["selected_date_display"] = selected_date.strftime("%d.%m.%Y")
        
        return await show_confirmation_text(update, context)
        
    except ValueError as e:
        await update.message.reply_text(
            "❌ Неверный формат даты! Используйте ДД.ММ.ГГГГ\n"
            "Например: 15.12.2024\n\n"
            "Попробуйте снова или отправьте 'отмена':"
        )
        return CONFIRMING_DATE

async def show_confirmation_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    reason = context.user_data.get("selected_reason", "")
    selected_date = context.user_data.get("selected_date_display", "")
    
    message = f"📋 <b>Подтверждение отмены планёрки:</b>\n\n"
    
    if "Перенесём" in reason:
        message += f"❌ <b>Отмена сегодняшней планёрки</b>\n"
        message += f"📅 <b>Перенос на {selected_date}</b>\n\n"
        message += "<b>Подтвердить отмену?</b>"
    else:
        message += f"❌ <b>Отмена планёрки</b>\n"
        message += f"📝 <b>Причина:</b> {reason}\n\n"
        message += "<b>Подтвердить отмену?</b>"
    
    keyboard = [
        [
            InlineKeyboardButton("✅ Да, отменить", callback_data="confirm_cancel"),
            InlineKeyboardButton("❌ Нет, вернуться", callback_data="back_to_reasons_from_confirm")
        ]
    ]
    
    await update.message.reply_text(
        text=message,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.HTML
    )
    
    return CONFIRMING_DATE

async def show_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    reason = context.user_data.get("selected_reason", "")
    selected_date = context.user_data.get("selected_date_display", "")
    
    message = f"📋 <b>Подтверждение отмены планёрки:</b>\n\n"
    
    if "Перенесём" in reason:
        message += f"❌ <b>Отмена сегодняшней планёрки</b>\n"
        message += f"📅 <b>Перенос на {selected_date}</b>\n\n"
        message += "<b>Подтвердить отмену?</b>"
    else:
        message += f"❌ <b>Отмена планёрки</b>\n"
        message += f"📝 <b>Причина:</b> {reason}\n\n"
        message += "<b>Подтвердить отмену?</b>"
    
    keyboard = [
        [
            InlineKeyboardButton("✅ Да, отменить", callback_data="confirm_cancel"),
            InlineKeyboardButton("❌ Нет, вернуться", callback_data="back_to_reasons_from_confirm")
        ]
    ]
    
    await query.edit_message_text(
        text=message,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.HTML
    )
    
    return CONFIRMING_DATE

async def confirm_cancellation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    return await show_confirmation(update, context)

async def back_to_reasons_from_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    
    keyboard = [
        [InlineKeyboardButton(option, callback_data=f"reason_{i}")]
        for i, option in enumerate(CANCELLATION_OPTIONS)
    ]
    
    await query.edit_message_text(
        text="📝 Выберите причину отмены планёрки:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    
    return SELECTING_REASON

async def execute_cancellation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    
    config = BotConfig()
    reason = context.user_data.get("selected_reason", "Причина не указана")
    reason_index = context.user_data.get("reason_index", -1)
    username = query.from_user.username or "Неизвестный пользователь"
    
    if reason_index == 2:
        selected_date = context.user_data.get("selected_date_display", "дата не указана")
        final_message = f"❌ @{username} отменил сегодняшнюю планёрку\n\n📅 <b>Перенос на {selected_date}</b>"
    else:
        final_message = f"❌ @{username} отменил планёрку\n\n📝 <b>Причина:</b> {reason}"
    
    original_message_id = context.user_data.get("original_message_id")
    job_name_to_remove = None
    
    if original_message_id:
        for job in get_jobs_from_queue(context.application.job_queue):
            if job.name in config.active_reminders:
                reminder_data = config.active_reminders[job.name]
                if str(reminder_data.get("message_id")) == str(original_message_id):
                    job.schedule_removal()
                    job_name_to_remove = job.name
                    logger.info(f"Задание {job.name} удалено из планировщика")
                    break
        
        if job_name_to_remove:
            config.remove_active_reminder(job_name_to_remove)
            logger.info(f"Задание {job_name_to_remove} удалено из конфига")
    
    await query.edit_message_text(
        text=final_message,
        parse_mode=ParseMode.HTML
    )
    
    logger.info(f"Планёрка отменена @{username} — {reason}")
    
    context.user_data.clear()
    
    return ConversationHandler.END

async def cancel_conversation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.message:
        await update.message.reply_text("❌ Диалог отменен.")
    elif update.callback_query:
        await update.callback_query.answer("Диалог отменен", show_alert=True)
        await update.callback_query.edit_message_text("❌ Диалог отменен.")
    
    context.user_data.clear()
    return ConversationHandler.END

# ========== ОСНОВНЫЕ КОМАНДЫ ==========

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обновленный обработчик /start с информацией об улучшенных ИСТОРИЧЕСКИХ событиях 'В этот день'"""
    await update.message.reply_text(
        "🤖 <b>Бот для напоминаний о планёрке активен!</b>\n\n"
        f"📅 <b>Напоминания отправляются:</b>\n"
        f"• Понедельник\n• Среда\n• Пятница\n"
        f"⏰ <b>Время:</b> {MEETING_TIME['hour']:02d}:{MEETING_TIME['minute']:02d} по МСК\n\n"
        "📅 <b>УЛУЧШЕННАЯ рубрика 'В ЭТОТ ДЕНЬ':</b>\n"
        f"• Отправляется: Пн-Пт в 10:00 по МСК\n"
        f"• <b>Формат:</b> В ЭТОТ ДЕНЬ: ДЕНЬ МЕСЯЦ ГОД года | КАТЕГОРИЯ: КАТЕГОРИЯ\n"
        f"• <b>Категории:</b> {', '.join([c.capitalize() for c in EVENT_CATEGORIES])}\n"
        f"• <b>Тип событий:</b> ИСТОРИЧЕСКИЕ (произошедшие в эту дату)\n"
        f"• <b>Улучшенный поиск:</b> гарантирует точную дату события\n"
        f"• <b>Конкретный факт:</b> предложение из статьи с датой\n"
        f"• <b>Полная ссылка:</b> на статью Википедии\n"
        f"• События НЕ повторяются в пределах категории!\n\n"
        "🔧 <b>Доступные команды:</b>\n"
        "/info - информация о боте\n"
        "/jobs - список запланированных задач\n"
        "/test - тестовое напоминание (через 5 сек)\n"
        "/testnow - мгновенное тестовое напоминание\n"
        "/eventnow - отправить УЛУЧШЕННОЕ ИСТОРИЧЕСКОЕ событие 'В этот день' сейчас\n"
        "/nextevent - следующая категория ИСТОРИЧЕСКИХ событий\n\n"
        "👮♂️ <b>Команды для администраторов:</b>\n"
        "/setchat - установить чат для уведомлений\n"
        "/adduser @username - добавить пользователя\n"
        "/removeuser @username - удалить пользователя\n"
        "/users - список пользователей\n"
        "/cancelall - отменить все напоминания",
        parse_mode=ParseMode.HTML
    )

@restricted
async def set_chat(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    chat_title = update.effective_chat.title or "личный чат"

    config = BotConfig()
    config.chat_id = chat_id

    await update.message.reply_text(
        f"✅ <b>Чат установлен:</b> {chat_title}\n"
        f"<b>Chat ID:</b> {chat_id}\n\n"
        "Напоминания и УЛУЧШЕННЫЕ ИСТОРИЧЕСКИЕ события 'В этот день' будут отправляться в этот чат.",
        parse_mode=ParseMode.HTML
    )

    logger.info(f"Установлен чат {chat_title} ({chat_id})")

@restricted
async def show_info(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обновленный обработчик /info с информацией об улучшенных ИСТОРИЧЕСКИХ событиях 'В этот день'"""
    config = BotConfig()
    chat_id = config.chat_id

    if chat_id:
        status = f"✅ <b>Чат установлен</b> (ID: {chat_id})"
    else:
        status = "❌ <b>Чат не установлен</b>. Используйте /setchat"

    all_jobs = get_jobs_from_queue(context.application.job_queue)
    
    # Считаем задачи планёрок
    meeting_job_count = len([j for j in all_jobs 
                    if j.name and j.name.startswith("meeting_reminder_")])
    
    # Считаем задачи событий
    event_job_count = len([j for j in all_jobs 
                    if j.name and j.name.startswith("daily_event_")])
    
    # Следующее напоминание о планёрке
    next_meeting_job = None
    for job in all_jobs:
        if job.name and job.name.startswith("meeting_reminder_"):
            if not next_meeting_job or job.next_t < next_meeting_job.next_t:
                next_meeting_job = job
    
    # Следующая отправка события
    next_event_job = None
    for job in all_jobs:
        if job.name and job.name.startswith("daily_event_"):
            if not next_event_job or job.next_t < next_event_job.next_t:
                next_event_job = job
    
    next_meeting_time = next_meeting_job.next_t.astimezone(TIMEZONE).strftime('%d.%m.%Y %H:%M') if next_meeting_job else "не запланировано"
    next_event_time_utc = next_event_job.next_t if next_event_job else None
    next_event_time = next_event_time_utc.astimezone(TIMEZONE).strftime('%d.%m.%Y %H:%M') if next_event_time_utc else "не запланировано"
    
    today = datetime.now(TIMEZONE)
    upcoming_meetings = []
    for i in range(1, 8):
        next_day = today + timedelta(days=i)
        if next_day.weekday() in MEETING_DAYS:
            upcoming_meetings.append(next_day.strftime("%d.%m.%Y"))

    zoom_info = f"\n🎥 <b>Zoom-ссылка:</b> {'установлена ✅' if ZOOM_LINK and ZOOM_LINK != DEFAULT_ZOOM_LINK else 'не установлена ⚠️'}"
    
    # Информация об улучшенных ИСТОРИЧЕСКИХ событиях "В этот день"
    event_scheduler = config.get_event_scheduler()
    next_event_category = EVENT_CATEGORIES[event_scheduler.current_index]
    next_event_emoji = CATEGORY_EMOJIS.get(next_event_category, '📌')
    
    # Получаем текущую дату
    day, month_ru, year = event_scheduler.get_todays_date_parts()
    
    event_info = f"\n📅 <b>Следующее УЛУЧШЕННОЕ ИСТОРИЧЕСКОЕ событие 'В этот день':</b> {next_event_emoji} {next_event_category.capitalize()}"
    
    await update.message.reply_text(
        f"📊 <b>Информация о боте (УЛУЧШЕННАЯ версия):</b>\n\n"
        f"{status}\n"
        f"📅 <b>Дни планёрок:</b> понедельник, среда, пятница\n"
        f"⏰ <b>Время планёрок:</b> {MEETING_TIME['hour']:02d}:{MEETING_TIME['minute']:02d} по МСК\n"
        f"📅 <b>УЛУЧШЕННЫЕ события 'В этот день':</b> Пн-Пт в 10:00 по МСК\n"
        f"📜 <b>Тип событий:</b> ИСТОРИЧЕСКИЕ (произошедшие в эту дату)\n"
        f"🎯 <b>Категории событий:</b> {', '.join(EVENT_CATEGORIES)}\n"
        f"🗓️ <b>Формат:</b> <b>В ЭТОТ ДЕНЬ: {day} {month_ru} ГОД года | КАТЕГОРИЯ: КАТЕГОРИЯ</b>\n"
        f"🔍 <b>Улучшенный поиск:</b> гарантирует точную дату события\n"
        f"📖 <b>Конкретный факт:</b> предложение из статьи с датой\n"
        f"🔗 <b>Полная ссылка:</b> на статью Википедии\n"
        f"👥 <b>Разрешённые пользователи:</b> {len(config.allowed_users)}\n"
        f"📋 <b>Активные напоминания:</b> {len(config.active_reminders)}\n"
        f"⏳ <b>Задачи планёрок:</b> {meeting_job_count}\n"
        f"📅 <b>Задачи событий:</b> {event_job_count}\n"
        f"➡️ <b>Следующая планёрка:</b> {next_meeting_time}\n"
        f"➡️ <b>Следующее УЛУЧШЕННОЕ событие:</b> {next_event_time}\n"
        f"📈 <b>Ближайшие планёрки:</b> {', '.join(upcoming_meetings[:3]) if upcoming_meetings else 'нет'}"
        f"{zoom_info}"
        f"{event_info}\n\n"
        f"Используйте /users для списка пользователей\n"
        f"Используйте /jobs для списка задач\n"
        f"Используйте /nextevent для следующей категории УЛУЧШЕННЫХ ИСТОРИЧЕСКИХ событий",
        parse_mode=ParseMode.HTML
    )

@restricted
async def test_reminder(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    config = BotConfig()
    if not config.chat_id:
        await update.message.reply_text("❌ Сначала установите чат командой /setchat")
        return

    context.application.job_queue.run_once(
        send_reminder, 
        5, 
        chat_id=config.chat_id,
        name=f"test_reminder_{datetime.now().timestamp()}"
    )

    weekday = datetime.now(TIMEZONE).weekday()
    day_names_ru = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
    current_day = day_names_ru[weekday]
    
    if weekday in MEETING_DAYS:
        day_type = "день планёрки ✅"
        day_emoji = "📋"
    else:
        day_type = "не день планёрки ⚠️"
        day_emoji = "⏸️"
    
    zoom_preview = ZOOM_LINK[:50] + "..." if len(ZOOM_LINK) > 50 else ZOOM_LINK
    zoom_status = "установлена ✅" if ZOOM_LINK and ZOOM_LINK != DEFAULT_ZOOM_LINK else "не установлена ⚠️"
    
    example_text = get_greeting_by_meeting_day()
    example_preview = example_text[:200] + "..." if len(example_text) > 200 else example_text
    
    await update.message.reply_text(
        f"⏳ <b>Тестовое напоминание будет отправлено через 5 секунд...</b>\n\n"
        f"{day_emoji} <b>Сегодня:</b> {current_day} ({day_type})\n"
        f"⏰ <b>Время:</b> {MEETING_TIME['hour']:02d}:{MEETING_TIME['minute']:02d} по МСК\n"
        f"🎥 <b>Zoom-ссылка:</b> {zoom_status}\n"
        f"🔗 <b>Предпросмотр:</b> {zoom_preview}\n\n"
        f"<b>Пример сообщения:</b>\n"
        f"<code>{example_preview}</code>\n\n"
        f"<b>Сообщение будет содержать:</b>\n"
        f"• Приветствие для {current_day.lower()}\n"
        f"• Время планёрки\n"
        f"• Кликабельную ссылку 'Присоединиться к Zoom'\n"
        f"• Кнопку для отмены планёрки",
        parse_mode=ParseMode.HTML
    )

@restricted
async def test_now(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    config = BotConfig()
    if not config.chat_id:
        await update.message.reply_text("❌ Сначала установите чат командой /setchat")
        return

    weekday = datetime.now(TIMEZONE).weekday()
    day_names_ru = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
    current_day = day_names_ru[weekday]
    
    if weekday in MEETING_DAYS:
        day_type = "день планёрки ✅"
    else:
        day_type = "не день планёрки ⚠️"
    
    await update.message.reply_text(
        f"🚀 <b>Отправляю тестовое напоминание прямо сейчас...</b>\n\n"
        f"📅 <b>Сегодня:</b> {current_day} ({day_type})\n"
        f"⏰ <b>Время:</b> {MEETING_TIME['hour']:02d}:{MEETING_TIME['minute']:02d} по МСК\n\n"
        f"<b>Ссылка в сообщении:</b> <a href=\"{ZOOM_LINK}\">Присоединиться к Zoom</a>",
        parse_mode=ParseMode.HTML
    )
    
    class DummyJob:
        def __init__(self):
            self.name = f"manual_test_{datetime.now().timestamp()}"
    
    dummy_context = ContextTypes.DEFAULT_TYPE(context.application)
    dummy_context.job = DummyJob()
    dummy_context.bot = context.bot
    
    await send_reminder(dummy_context)

@restricted
async def list_jobs(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    jobs = get_jobs_from_queue(context.application.job_queue)
    
    if not jobs:
        await update.message.reply_text("📭 <b>Нет запланированных задач.</b>", parse_mode=ParseMode.HTML)
        return
    
    meeting_jobs = [j for j in jobs if j.name and j.name.startswith("meeting_reminder_")]
    event_jobs = [j for j in jobs if j.name and j.name.startswith("daily_event_")]
    other_jobs = [j for j in jobs if j not in meeting_jobs + event_jobs]
    
    message = "📋 <b>Запланированные задачи:</b>\n\n"
    
    if meeting_jobs:
        message += "🔔 <b>Напоминания о планёрках:</b>\n"
        for job in sorted(meeting_jobs, key=lambda j: j.next_t):
            next_time = job.next_t.astimezone(TIMEZONE)
            message += f"  • {next_time.strftime('%d.%m.%Y %H:%M')} ({job.name[:30]}...)\n"
    
    if event_jobs:
        message += "\n📅 <b>УЛУЧШЕННЫЕ ИСТОРИЧЕСКИЕ события 'В этот день':</b>\n"
        for job in sorted(event_jobs, key=lambda j: j.next_t):
            next_time = job.next_t.astimezone(TIMEZONE)
            message += f"  • {next_time.strftime('%d.%m.%Y %H:%M')} ({job.name[:30]}...)\n"
    
    if other_jobs:
        message += "\n🔧 <b>Другие задачи:</b>\n"
        for job in other_jobs:
            next_time = job.next_t.astimezone(TIMEZONE)
            job_name = job.name[:30] + "..." if job.name and len(job.name) > 30 else job.name or "Без имени"
            message += f"  • {next_time.strftime('%d.%m.%Y %H:%M')} ({job_name})\n"
    
    await update.message.reply_text(message, parse_mode=ParseMode.HTML)

@restricted
async def add_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("❌ <b>Используйте:</b> /adduser @username", parse_mode=ParseMode.HTML)
        return
    
    username = context.args[0].lstrip('@')
    # Базовая валидация username
    if not re.match(r'^[a-zA-Z0-9_]{5,32}$', username):
        await update.message.reply_text("❌ <b>Некорректное имя пользователя.</b>", parse_mode=ParseMode.HTML)
        return
    
    config = BotConfig()
    
    if config.add_allowed_user(username):
        await update.message.reply_text(f"✅ <b>Пользователь @{username} добавлен</b>", parse_mode=ParseMode.HTML)
        logger.info(f"Добавлен пользователь @{username}")
    else:
        await update.message.reply_text(f"ℹ️ <b>Пользователь @{username} уже есть в списке</b>", parse_mode=ParseMode.HTML)

@restricted
async def remove_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("❌ <b>Используйте:</b> /removeuser @username", parse_mode=ParseMode.HTML)
        return
    
    username = context.args[0].lstrip('@')
    config = BotConfig()
    
    if config.remove_allowed_user(username):
        await update.message.reply_text(f"✅ <b>Пользователь @{username} удален</b>", parse_mode=ParseMode.HTML)
        logger.info(f"Удален пользователь @{username}")
    else:
        await update.message.reply_text(f"❌ <b>Пользователь @{username} не найден</b>", parse_mode=ParseMode.HTML)

@restricted
async def list_users(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    config = BotConfig()
    users = config.allowed_users
    
    if not users:
        await update.message.reply_text("📭 <b>Список пользователей пуст</b>", parse_mode=ParseMode.HTML)
        return
    
    message = "👥 <b>Разрешенные пользователи:</b>\n\n"
    for i, user in enumerate(users, 1):
        message += f"{i}. @{user}\n"
    
    message += f"\n<b>Всего:</b> {len(users)} пользователь(ей)"
    await update.message.reply_text(message, parse_mode=ParseMode.HTML)

@restricted
async def cancel_all(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    jobs = get_jobs_from_queue(context.application.job_queue)
    canceled_meetings = 0
    canceled_events = 0
    
    for job in jobs[:]:
        if job.name and job.name.startswith("meeting_reminder_"):
            job.schedule_removal()
            canceled_meetings += 1
        elif job.name and job.name.startswith("daily_event_"):
            job.schedule_removal()
            canceled_events += 1
    
    config = BotConfig()
    config.clear_active_reminders()
    
    await update.message.reply_text(
        f"✅ <b>Отменено:</b>\n"
        f"• {canceled_meetings} напоминаний о планёрках\n"
        f"• {canceled_events} отправок УЛУЧШЕННЫХ ИСТОРИЧЕСКИХ событий 'В этот день'\n"
        f"Очищено {len(config.active_reminders)} активных напоминаний в конфиге",
        parse_mode=ParseMode.HTML
    )
    logger.info(f"Отменено {canceled_meetings} напоминаний и {canceled_events} УЛУЧШЕННЫХ ИСТОРИЧЕСКИХ событий")

def calculate_next_reminder() -> datetime:
    now = datetime.now(TIMEZONE)
    current_weekday = now.weekday()

    if current_weekday in MEETING_DAYS:
        reminder_time = now.replace(
            hour=MEETING_TIME['hour'],
            minute=MEETING_TIME['minute'],
            second=0,
            microsecond=0
        )
        if now < reminder_time:
            return reminder_time

    days_ahead = 1
    max_days = 365  # Защита от зацикливания
    while days_ahead <= max_days:
        next_day = now + timedelta(days=days_ahead)
        if next_day.weekday() in MEETING_DAYS:
            return next_day.replace(
                hour=MEETING_TIME['hour'],
                minute=MEETING_TIME['minute'],
                second=0,
                microsecond=0
            )
        days_ahead += 1
    
    raise ValueError(f"Не найден подходящий день за {max_days} дней")

async def schedule_next_reminder(context: ContextTypes.DEFAULT_TYPE) -> None:
    next_time = calculate_next_reminder()
    config = BotConfig()
    chat_id = config.chat_id

    if not chat_id:
        logger.warning("Chat ID не установлен, планирование отложено")
        context.application.job_queue.run_once(
            lambda ctx: asyncio.create_task(schedule_next_reminder(ctx)),
            3600
        )
        return

    now = datetime.now(TIMEZONE)
    delay = (next_time - now).total_seconds()

    if delay > 0:
        job_name = f"meeting_reminder_{next_time.strftime('%Y%m%d_%H%M')}"
        
        existing_jobs = [j for j in get_jobs_from_queue(context.application.job_queue) 
                        if j.name == job_name]
        
        if not existing_jobs:
            context.application.job_queue.run_once(
                send_reminder,
                delay,
                chat_id=chat_id,
                name=job_name
            )

            context.application.job_queue.run_once(
                lambda ctx: asyncio.create_task(schedule_next_reminder(ctx)),
                delay + 60,
                chat_id=chat_id,
                name=f"scheduler_{next_time.strftime('%Y%m%d_%H%M')}"
            )

            logger.info(f"Следующее напоминание запланировано на {next_time}")
        else:
            logger.info(f"Напоминание на {next_time} уже запланировано")
    else:
        # Если время уже прошло, планируем на следующий день
        logger.warning(f"Время напоминания уже прошло ({next_time}), планируем на следующий день")
        context.application.job_queue.run_once(
            lambda ctx: asyncio.create_task(schedule_next_reminder(ctx)),
            60  # Через минуту
        )

def cleanup_old_jobs(job_queue: JobQueue) -> None:
    jobs = get_jobs_from_queue(job_queue)
    jobs_by_name = {}
    jobs_to_remove = []
    
    for job in jobs:
        if job.name:
            if job.name in jobs_by_name:
                jobs_to_remove.append(jobs_by_name[job.name])
            jobs_by_name[job.name] = job
    
    now = datetime.now(TIMEZONE)
    for job in jobs:
        if job.next_t and job.next_t < now:
            jobs_to_remove.append(job)
    
    for job in jobs_to_remove:
        job.schedule_removal()
    
    if jobs_to_remove:
        logger.info(f"Очищено {len(jobs_to_remove)} старых/дублирующих задач")

def restore_reminders(application: Application) -> None:
    config = BotConfig()
    now = datetime.now(TIMEZONE)
    
    for job_name, reminder_data in config.active_reminders.items():
        try:
            created_at = datetime.fromisoformat(reminder_data["created_at"])
            if (now - created_at).days < 1:
                application.job_queue.run_once(
                    lambda ctx: logger.info(f"Восстановлено напоминание {job_name}"),
                    1,
                    name=f"restored_{job_name}"
                )
        except Exception as e:
            logger.error(f"Ошибка восстановления напоминания {job_name}: {e}")

def validate_zoom_link(zoom_link: str) -> bool:
    """Базовая валидация Zoom ссылки"""
    if not zoom_link or zoom_link == DEFAULT_ZOOM_LINK:
        return False
    
    # Проверяем, что ссылка начинается с https
    if not zoom_link.startswith('https://'):
        logger.warning(f"Zoom ссылка не использует HTTPS: {zoom_link}")
        return False
    
    # Проверяем наличие домена zoom
    if 'zoom.us' not in zoom_link and 'zoom.com' not in zoom_link:
        logger.warning(f"Zoom ссылка не содержит домен zoom: {zoom_link}")
        return False
    
    return True

def main() -> None:
    if not TOKEN:
        logger.error("❌ Токен бота не найден! Установите переменную окружения TELEGRAM_BOT_TOKEN")
        return
    
    # Валидация Zoom ссылки
    zoom_valid = validate_zoom_link(ZOOM_LINK)
    if not zoom_valid:
        logger.warning("⚠️ Zoom-ссылка не установлена или некорректна!")
        logger.warning("   Установите переменную окружения ZOOM_MEETING_LINK")
    else:
        logger.info(f"✅ Zoom-ссылка загружена (первые 50 символов): {ZOOM_LINK[:50]}...")

    try:
        application = Application.builder().token(TOKEN).build()

        # ConversationHandler для отмены планёрки
        conv_handler = ConversationHandler(
            entry_points=[CallbackQueryHandler(cancel_meeting_callback, pattern="^cancel_meeting$")],
            states={
                SELECTING_REASON: [
                    CallbackQueryHandler(select_reason_callback, pattern="^reason_[0-9]+$"),
                ],
                SELECTING_DATE: [
                    CallbackQueryHandler(date_selected_callback, pattern="^date_.+$"),
                    CallbackQueryHandler(date_selected_callback, pattern="^custom_date$"),
                    CallbackQueryHandler(date_selected_callback, pattern="^back_to_reasons$"),
                ],
                CONFIRMING_DATE: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_custom_date),
                    CallbackQueryHandler(execute_cancellation, pattern="^confirm_cancel$"),
                    CallbackQueryHandler(back_to_reasons_from_confirm, pattern="^back_to_reasons_from_confirm$"),
                ],
            },
            fallbacks=[
                CommandHandler("cancel", cancel_conversation),
                CallbackQueryHandler(cancel_conversation, pattern="^cancel_conversation$"),
            ],
            allow_reentry=True,
        )

        # Обработчики команд
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("setchat", set_chat))
        application.add_handler(CommandHandler("info", show_info))
        application.add_handler(CommandHandler("test", test_reminder))
        application.add_handler(CommandHandler("testnow", test_now))
        application.add_handler(CommandHandler("eventnow", send_event_now))
        application.add_handler(CommandHandler("nextevent", show_next_event_category))
        application.add_handler(CommandHandler("jobs", list_jobs))
        application.add_handler(CommandHandler("adduser", add_user))
        application.add_handler(CommandHandler("removeuser", remove_user))
        application.add_handler(CommandHandler("users", list_users))
        application.add_handler(CommandHandler("cancelall", cancel_all))

        # Добавляем ConversationHandler
        application.add_handler(conv_handler)

        # Очистка старых задач
        cleanup_old_jobs(application.job_queue)
        
        # Восстановление напоминаний
        restore_reminders(application)

        # Запуск планировщика планёрок
        application.job_queue.run_once(
            lambda ctx: asyncio.create_task(schedule_next_reminder(ctx)),
            3
        )

        # Запуск планировщика УЛУЧШЕННЫХ ИСТОРИЧЕСКИХ событий "В этот день"
        application.job_queue.run_once(
            lambda ctx: asyncio.create_task(schedule_next_event(ctx)),
            5
        )

        # Получаем текущую дату для логирования
        now = datetime.now(TIMEZONE)
        day = now.day
        month_ru = MONTHS_RU[now.month]
        year = now.year
        
        logger.info("🤖 Бот запущен и готов к работе!")
        logger.info(f"⏰ Планёрки: {', '.join(['Пн', 'Ср', 'Пт'])} в {MEETING_TIME['hour']:02d}:{MEETING_TIME['minute']:02d} по МСК")
        logger.info(f"📅 УЛУЧШЕННАЯ рубрика 'В ЭТОТ ДЕНЬ': Пн-Пт в 10:00 по МСК (07:00 UTC)")
        logger.info(f"📜 Тип событий: ИСТОРИЧЕСКИЕ (произошедшие в эту дату)")
        logger.info(f"🗓️ Формат: В ЭТОТ ДЕНЬ: {day} {month_ru} ГОД года | КАТЕГОРИЯ: КАТЕГОРИЯ")
        logger.info(f"🎯 Категории событий: {', '.join(EVENT_CATEGORIES)}")
        logger.info(f"🔍 Улучшенный поиск: гарантирует точную дату события")
        logger.info(f"📖 Конкретный факт: предложение из статьи с датой")
        logger.info(f"🔗 Полная ссылка: на статью Википедии")
        logger.info(f"🔄 События НЕ повторяются в пределах категории!")
        logger.info(f"👥 Разрешённые пользователи: {', '.join(BotConfig().allowed_users)}")
        
        application.run_polling(allowed_updates=Update.ALL_TYPES)

    except Exception as e:
        logger.error(f"❌ Критическая ошибка при запуске бота: {e}")
        raise


if __name__ == "__main__":
    main()
