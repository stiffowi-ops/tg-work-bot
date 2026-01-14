import os
import json
import random
import logging
import requests
import asyncio
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple
from functools import wraps
import pytz
from urllib.parse import quote

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

# ========== НАСТРОЙКИ ==========
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ZOOM_LINK = os.getenv("ZOOM_MEETING_LINK", "https://us04web.zoom.us/j/1234567890?pwd=example")
CONFIG_FILE = "bot_config.json"

# Время планёрки (9:30 по Москве)
MEETING_TIME = {"hour": 9, "minute": 15}
TIMEZONE = pytz.timezone("Europe/Moscow")

# Дни недели для планёрки (понедельник=0, среда=2, пятница=4)
MEETING_DAYS = [0, 2, 4]

# ========== НАСТРОЙКИ СОБЫТИЙ "В ЭТОТ ДЕНЬ" ==========
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

# ========== ОСТАЛЬНЫЕ НАСТРОЙКИ ==========
CANCELLATION_OPTIONS = [
    "Все вопросы решены, планёрка не нужна",
    "Ключевые участники отсутствуют",
    "Перенесём на другой день",
]

SELECTING_REASON, SELECTING_DATE, CONFIRMING_DATE = range(3)

# Настройка логирования
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# ========== КЛАСС ДЛЯ СОБЫТИЙ "В ЭТОТ ДЕНЬ" ==========
class EventScheduler:
    """Класс для управления отправкой событий 'В этот день'"""
    
    def __init__(self):
        self.current_index = 0
        # Храним использованные статьи по категориям
        self.used_events = {category: set() for category in EVENT_CATEGORIES}
        # Кэш для fallback-событий
        self.fallback_cache = {}
        logger.info("Инициализирован планировщик событий 'В этот день'")
    
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
        """Получаем текущую дату (день, месяц_ru, год)"""
        now = datetime.now(TIMEZONE)
        day = now.day
        month_ru = MONTHS_RU[now.month]
        year = now.year
        return day, month_ru, year
    
    def search_wikipedia_events(self, day: int, month: int, category: str, lang: str = 'ru') -> List[Dict[str, Any]]:
        """
        Ищем ВСЕМИРНЫЕ события на Wikipedia для указанной даты и категории
        
        Возвращает список событий: [{'title': ..., 'year': ..., 'description': ..., 'url': ...}, ...]
        """
        try:
            # Форматируем дату для поиска (14 января события, 14 января история, January 14 events)
            date_formats = [
                f"{day} {MONTHS_RU[month].lower()}",
                f"{day} января",
                f"{day} {month}",
                f"{day}.{month:02d}",
            ]
            
            events = []
            
            # Ищем на русской Википедии (всемирные события на русском языке)
            ru_events = self._search_wikipedia_ru(day, month, category, date_formats)
            if ru_events:
                events.extend(ru_events)
            
            # Если не нашли на русской, ищем на английской и переводим
            if not events:
                en_events = self._search_wikipedia_en(day, month, category)
                if en_events:
                    events.extend(en_events)
            
            return events
            
        except Exception as e:
            logger.error(f"Ошибка поиска событий: {e}")
            return []
    
    def _search_wikipedia_ru(self, day: int, month: int, category: str, date_formats: List[str]) -> List[Dict[str, Any]]:
        """Ищем события на русской Википедии (всемирные события)"""
        events = []
        url = "https://ru.wikipedia.org/w/api.php"
        
        headers = {
            'User-Agent': 'TelegramEventBot/1.0 (https://t.me/; contact@example.com)'
        }
        
        # Ключевые слова для поиска всемирных событий
        world_keywords = {
            'музыка': ['релиз альбома', 'концерт', 'премия', 'фестиваль', 'хит', 'чарт', 'тур'],
            'фильмы': ['премьера фильма', 'кинофестиваль', 'оскар', 'награда', 'съёмки', 'релиз'],
            'технологии': ['изобретение', 'патент', 'запуск', 'презентация', 'компания', 'гаджет'],
            'игры': ['релиз игры', 'конференция', 'чемпионат', 'турнир', 'консоль', 'разработка'],
            'наука': ['открытие', 'изобретение', 'нобелевская премия', 'исследование', 'эксперимент'],
            'спорт': ['чемпионат мира', 'олимпиада', 'рекорд', 'матч', 'турнир', 'кубок'],
            'история': ['событие', 'война', 'мирный договор', 'революция', 'открытие', 'основание']
        }
        
        keywords = world_keywords.get(category, ['событие', 'история'])
        
        for date_format in date_formats:
            for keyword in keywords:
                params = {
                    'action': 'query',
                    'format': 'json',
                    'list': 'search',
                    'srsearch': f"{date_format} {keyword}",
                    'srlimit': 20,
                    'srwhat': 'text',
                    'srprop': 'snippet'
                }
                
                try:
                    response = requests.get(url, params=params, headers=headers, timeout=15)
                    response.raise_for_status()
                    data = response.json()
                    
                    if 'query' in data and data['query']['search']:
                        articles = data['query']['search']
                        
                        for article in articles:
                            title = article['title']
                            
                            # Пропускаем общие страницы
                            if any(common in title.lower() for common in [
                                f"{day} января", 
                                f"{day} {MONTHS_RU[month].lower()}",
                                "категория:",
                                "шаблон:",
                                "список",
                                "таблица"
                            ]):
                                continue
                            
                            # Извлекаем год из заголовка (если есть)
                            year_match = self._extract_year_from_title(title)
                            
                            # Получаем описание статьи
                            desc_params = {
                                'action': 'query',
                                'format': 'json',
                                'prop': 'extracts|info',
                                'inprop': 'url',
                                'exchars': 400,
                                'explaintext': True,
                                'exintro': True,
                                'titles': title
                            }
                            
                            desc_response = requests.get(url, params=desc_params, headers=headers, timeout=10)
                            desc_response.raise_for_status()
                            desc_data = desc_response.json()
                            
                            pages = desc_data['query']['pages']
                            page_id = list(pages.keys())[0]
                            page = pages[page_id]
                            
                            if 'missing' not in page:
                                description = page.get('extract', '')
                                # Очищаем описание
                                if description:
                                    description = self._clean_description(description)
                                
                                # Формируем URL
                                encoded_title = quote(title.replace(' ', '_'), safe='')
                                article_url = f"https://ru.wikipedia.org/wiki/{encoded_title}"
                                
                                events.append({
                                    'title': title,
                                    'year': year_match,
                                    'description': description,
                                    'url': article_url,
                                    'category': category,
                                    'lang': 'ru'
                                })
                    
                    if events:
                        break
                
                except Exception as e:
                    logger.warning(f"Ошибка при поиске на русской Википедии: {e}")
                    continue
            
            if events:
                break
        
        return events
    
    def _search_wikipedia_en(self, day: int, month: int, category: str) -> List[Dict[str, Any]]:
        """Ищем события на английской Википедии и получаем русскую версию"""
        events = []
        
        # Английские названия месяцев
        months_en = {
            1: "January", 2: "February", 3: "March", 4: "April",
            5: "May", 6: "June", 7: "July", 8: "August",
            9: "September", 10: "October", 11: "November", 12: "December"
        }
        
        month_en = months_en.get(month, "")
        if not month_en:
            return events
        
        url = "https://en.wikipedia.org/w/api.php"
        headers = {
            'User-Agent': 'TelegramEventBot/1.0 (https://t.me/; contact@example.com)'
        }
        
        # Ключевые слова на английском
        en_keywords = {
            'музыка': ['album release', 'concert', 'award', 'festival', 'hit single', 'chart'],
            'фильмы': ['film premiere', 'movie release', 'oscar', 'award', 'production'],
            'технологии': ['invention', 'patent', 'launch', 'presentation', 'company founded'],
            'игры': ['game release', 'video game', 'championship', 'tournament', 'console'],
            'наука': ['discovery', 'invention', 'nobel prize', 'experiment', 'research'],
            'спорт': ['championship', 'olympics', 'world cup', 'record', 'tournament'],
            'история': ['event', 'war', 'treaty', 'revolution', 'discovery', 'founding']
        }
        
        keywords = en_keywords.get(category, ['event', 'history'])
        
        for keyword in keywords:
            search_query = f"{month_en} {day} {keyword}"
            params = {
                'action': 'query',
                'format': 'json',
                'list': 'search',
                'srsearch': search_query,
                'srlimit': 15,
                'srwhat': 'text',
                'srprop': 'snippet'
            }
            
            try:
                response = requests.get(url, params=params, headers=headers, timeout=15)
                response.raise_for_status()
                data = response.json()
                
                if 'query' in data and data['query']['search']:
                    articles = data['query']['search']
                    
                    for article in articles:
                        title = article['title']
                        
                        # Пропускаем общие страницы
                        if any(common in title.lower() for common in [
                            f"{month_en} {day}",
                            "category:",
                            "template:",
                            "list of",
                            "timeline"
                        ]):
                            continue
                        
                        # Пробуем найти русскую версию статьи
                        ru_title = self._get_russian_version(title)
                        if ru_title:
                            # Используем русский заголовок
                            title = ru_title
                            lang = 'ru'
                        else:
                            # Оставляем английский, но переводим описание
                            lang = 'en'
                        
                        # Извлекаем год
                        year_match = self._extract_year_from_title(title)
                        
                        # Получаем описание
                        description = self._get_article_description(title, lang)
                        
                        # Формируем URL
                        if lang == 'ru':
                            encoded_title = quote(title.replace(' ', '_'), safe='')
                            article_url = f"https://ru.wikipedia.org/wiki/{encoded_title}"
                        else:
                            encoded_title = quote(title.replace(' ', '_'), safe='')
                            article_url = f"https://en.wikipedia.org/wiki/{encoded_title}"
                        
                        events.append({
                            'title': title,
                            'year': year_match,
                            'description': description,
                            'url': article_url,
                            'category': category,
                            'lang': lang
                        })
                
                if events:
                    break
            
            except Exception as e:
                logger.warning(f"Ошибка при поиске на английской Википедии: {e}")
                continue
        
        return events
    
    def _get_russian_version(self, en_title: str) -> Optional[str]:
        """Пытаемся получить русскую версию английской статьи"""
        try:
            url = "https://en.wikipedia.org/w/api.php"
            headers = {
                'User-Agent': 'TelegramEventBot/1.0 (https://t.me/; contact@example.com)'
            }
            
            params = {
                'action': 'query',
                'format': 'json',
                'prop': 'langlinks',
                'lllang': 'ru',
                'titles': en_title
            }
            
            response = requests.get(url, params=params, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            pages = data.get('query', {}).get('pages', {})
            for page in pages.values():
                langlinks = page.get('langlinks', [])
                for link in langlinks:
                    if link.get('lang') == 'ru':
                        return link.get('*')
            
            return None
            
        except Exception as e:
            logger.warning(f"Ошибка получения русской версии: {e}")
            return None
    
    def _get_article_description(self, title: str, lang: str) -> str:
        """Получаем описание статьи на указанном языке"""
        try:
            url = f"https://{lang}.wikipedia.org/w/api.php"
            headers = {
                'User-Agent': 'TelegramEventBot/1.0 (https://t.me/; contact@example.com)'
            }
            
            params = {
                'action': 'query',
                'format': 'json',
                'prop': 'extracts',
                'exchars': 300,
                'explaintext': True,
                'exintro': True,
                'titles': title
            }
            
            response = requests.get(url, params=params, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            pages = data.get('query', {}).get('pages', {})
            for page in pages.values():
                if 'extract' in page:
                    description = page['extract']
                    return self._clean_description(description)
            
            return ""
            
        except Exception as e:
            logger.warning(f"Ошибка получения описания: {e}")
            return ""
    
    def _extract_year_from_title(self, title: str) -> Optional[int]:
        """Извлекаем год из заголовка статьи"""
        import re
        # Ищем год в формате (1969) или 1969 год
        patterns = [
            r'\((\d{4})\)',           # (1969)
            r'\b(\d{4})\s+год',       # 1969 год
            r'\b(\d{4})\b',           # 1969
            r'в\s+(\d{4})\s+',        # в 1969 году
        ]
        
        for pattern in patterns:
            match = re.search(pattern, title)
            if match:
                try:
                    year = int(match.group(1))
                    if 1000 <= year <= 2100:  # Разумный диапазон лет
                        return year
                except (ValueError, IndexError):
                    continue
        
        return None
    
    def _clean_description(self, description: str) -> str:
        """Очищаем и форматируем описание"""
        if not description:
            return ""
        
        # Заменяем переносы строк
        description = description.replace('\n', ' ').replace('  ', ' ').strip()
        
        # Разбиваем на предложения
        sentences = description.split('. ')
        
        # Фильтруем и берем первые 2-3 осмысленных предложения
        filtered_sentences = []
        for sentence in sentences:
            sentence = sentence.strip()
            if len(sentence) > 20:  # Минимальная длина
                # Пропускаем скучные определения
                lower_sentence = sentence.lower()
                if not (lower_sentence.startswith('это ') or 
                       lower_sentence.startswith('является ') or
                       'это ' in lower_sentence[:50]):
                    filtered_sentences.append(sentence)
            
            if len(filtered_sentences) >= 3:
                break
        
        if filtered_sentences:
            result = '. '.join(filtered_sentences)
            if not result.endswith('.'):
                result += '.'
            return result
        
        return description
    
    def get_todays_event(self, category: str) -> Tuple[str, Optional[int], str, str, str]:
        """
        Получаем ВСЕМИРНОЕ событие "В этот день" для текущей даты и категории
        
        Возвращает: (заголовок, год, описание, ссылка, название_статьи)
        """
        try:
            now = datetime.now(TIMEZONE)
            day = now.day
            month = now.month
            
            logger.info(f"Поиск ВСЕМИРНЫХ событий для {day} {MONTHS_RU[month]} в категории: {category}")
            
            # Ищем события для текущей даты
            events = self.search_wikipedia_events(day, month, category)
            
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
                logger.warning(f"Не найдено ВСЕМИРНЫХ событий для {day} {MONTHS_RU[month]} в категории {category}")
                return self._get_fallback_event(category, day, month)
            
            event = random.choice(available_events)
            
            # Добавляем в использованные
            self.used_events[category].add(event['title'])
            logger.info(f"Выбрано ВСЕМИРНОЕ событие: {event['title']} ({event['year']})")
            
            return (
                event['title'],
                event['year'],
                event['description'],
                event['url'],
                event['title']
            )
            
        except Exception as e:
            logger.error(f"Ошибка получения события: {e}")
            return self._get_fallback_event(category, datetime.now(TIMEZONE).day, datetime.now(TIMEZONE).month)
    
    def _get_fallback_event(self, category: str, day: int, month: int) -> Tuple[str, Optional[int], str, str, str]:
        """Резервные ВСЕМИРНЫЕ события на случай недоступности Wikipedia"""
        if category in self.fallback_cache:
            event = random.choice(self.fallback_cache[category])
            return event['title'], event['year'], event['description'], event['url'], event['title']
        
        # ВСЕМИРНЫЕ fallback-события для каждой категории
        fallback_events = {
            'музыка': [
                {
                    'title': 'The Beatles выпустили альбом "Abbey Road"',
                    'year': 1969,
                    'description': 'Легендарный альбом был записан в студии на Эбби-Роуд в Лондоне. Это последняя совместная работа группы, а обложка с переходом через пешеходный переход стала одной из самых знаменитых в истории музыки.',
                    'url': 'https://ru.wikipedia.org/wiki/Abbey_Road'
                },
                {
                    'title': 'Майкл Джексон выпустил альбом "Thriller"',
                    'year': 1982,
                    'description': 'Альбом стал самым продаваемым в истории, разойдясь тиражом более 66 миллионов копий. Синглы "Billie Jean", "Beat It" и "Thriller" возглавляли чарты по всему миру.',
                    'url': 'https://ru.wikipedia.org/wiki/Thriller'
                },
                {
                    'title': 'Состоялся концерт Live Aid',
                    'year': 1985,
                    'description': 'Масштабный благотворительный концерт прошел одновременно в Лондоне и Филадельфии. В нем участвовали Queen, U2, Дэвид Боуи, Пол Маккартни и многие другие звезды мировой музыки.',
                    'url': 'https://ru.wikipedia.org/wiki/Live_Aid'
                }
            ],
            'фильмы': [
                {
                    'title': 'Состоялась премьера фильма "Звездные войны"',
                    'year': 1977,
                    'description': 'Фильм Джорджа Лукаса положил начало одной из самых успешных медиафраншиз в истории. "Звездные войны" революционизировали кинематограф спецэффектов и научной фантастики.',
                    'url': 'https://ru.wikipedia.org/wiki/Звёздные_войны'
                },
                {
                    'title': 'Выход фильма "Крестный отец"',
                    'year': 1972,
                    'description': 'Фильм Фрэнсиса Форда Копполы по роману Марио Пьюзо получил три премии "Оскар". Картина считается одним из величайших фильмов в истории кинематографа.',
                    'url': 'https://ru.wikipedia.org/wiki/Крёстный_отец_(фильм)'
                }
            ],
            'технологии': [
                {
                    'title': 'Стив Джобс представил первый iPhone',
                    'year': 2007,
                    'description': 'На конференции Macworld в Сан-Франциско был представлен смартфон, который революционизировал мобильную индустрию. Джобс назвал его "революционным продуктом".',
                    'url': 'https://ru.wikipedia.org/wiki/IPhone_(1-го_поколения)'
                },
                {
                    'title': 'Запуск первого веб-сайта',
                    'year': 1991,
                    'description': 'Тим Бернерс-Ли создал первый в мире веб-сайт info.cern.ch, который объяснял, что такое Всемирная паутина. Это положило начало современному интернету.',
                    'url': 'https://ru.wikipedia.org/wiki/Первый_веб-сайт'
                }
            ],
            'игры': [
                {
                    'title': 'Вышла игра "Super Mario Bros."',
                    'year': 1985,
                    'description': 'Платформер для Nintendo Entertainment System стал одной из самых влиятельных игр в истории. Марио стал самым узнаваемым персонажем видеоигр в мире.',
                    'url': 'https://ru.wikipedia.org/wiki/Super_Mario_Bros.'
                },
                {
                    'title': 'Релиз игры "Minecraft"',
                    'year': 2011,
                    'description': 'Игра, созданная Маркусом Перссоном, стала самой продаваемой видеоигрой в истории. В 2014 году Microsoft купила Mojang за 2,5 миллиарда долларов.',
                    'url': 'https://ru.wikipedia.org/wiki/Minecraft'
                }
            ],
            'наука': [
                {
                    'title': 'Альберт Эйнштейн опубликовал общую теорию относительности',
                    'year': 1915,
                    'description': 'Теория радикально изменила понимание гравитации, пространства и времени. Предсказания Эйнштейна были подтверждены многочисленными экспериментами.',
                    'url': 'https://ru.wikipedia.org/wiki/Общая_теория_относительности'
                },
                {
                    'title': 'Открытие пенициллина Александром Флемингом',
                    'year': 1928,
                    'description': 'Случайное открытие первого антибиотика спасло миллионы жизней. Флеминг обнаружил, что плесень Penicillium notatum подавляет рост бактерий.',
                    'url': 'https://ru.wikipedia.org/wiki/Пенициллин'
                }
            ],
            'спорт': [
                {
                    'title': 'Открылись первые современные Олимпийские игры',
                    'year': 1896,
                    'description': 'Игры прошли в Афинах с участием 241 спортсмена из 14 стран. Это возрождение олимпийского движения после 1500-летнего перерыва.',
                    'url': 'https://ru.wikipedia.org/wiki/Летние_Олимпийские_игры_1896'
                },
                {
                    'title': 'Первый чемпионат мира по футболу',
                    'year': 1930,
                    'description': 'Турнир прошел в Уругвае, и победителем стала сборная хозяев. В финале Уругвай обыграл Аргентину со счетом 4:2.',
                    'url': 'https://ru.wikipedia.org/wiki/Чемпионат_мира_по_футболу_1930'
                }
            ],
            'история': [
                {
                    'title': 'Высадка на Луну миссии "Аполлон-11"',
                    'year': 1969,
                    'description': 'Нил Армстронг стал первым человеком, ступившим на поверхность Луны. Его слова "Это один маленький шаг для человека, но гигантский скачок для человечества" вошли в историю.',
                    'url': 'https://ru.wikipedia.org/wiki/Аполлон-11'
                },
                {
                    'title': 'Падение Берлинской стены',
                    'year': 1989,
                    'description': 'Событие стало символом окончания Холодной войны. Тысячи восточных немцев перешли границу, и начался процесс воссоединения Германии.',
                    'url': 'https://ru.wikipedia.org/wiki/Берлинская_стена'
                }
            ]
        }
        
        self.fallback_cache = fallback_events
        events = fallback_events.get(category, [{
            'title': f'Всемирное историческое событие в категории {category}',
            'year': 2000,
            'description': 'Интересное всемирное событие, изменившее историю.',
            'url': 'https://ru.wikipedia.org'
        }])
        
        event = random.choice(events)
        return event['title'], event['year'], event['description'], event['url'], event['title']
    
    def create_event_message(self, category: str) -> Tuple[str, Optional[InlineKeyboardMarkup]]:
        """Создаем сообщение с событием 'В этот день' в указанном формате"""
        # Получаем текущую дату
        day, month_ru, current_year = self.get_todays_date_parts()
        
        # Получаем событие
        title, event_year, description, url, clean_title = self.get_todays_event(category)
        
        # Форматируем заголовок с годом события
        year_display = f" {event_year}" if event_year else ""
        
        # Эмодзи для категории
        category_emoji = CATEGORY_EMOJIS.get(category, '📌')
        
        # Форматируем сообщение в указанном формате
        message = f"**В ЭТОТ ДЕНЬ: {day} {month_ru}{year_display} |КАТЕГОРИЯ: {category.upper()}**\n\n"
        message += f"{category_emoji} **{clean_title}**\n\n"
        
        # Добавляем описание, если есть
        if description:
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
    if ZOOM_LINK == "https://us04web.zoom.us/j/1234567890?pwd=example":
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
        if ZOOM_LINK == "https://us04web.zoom.us/j/1234567890?pwd=example":
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
    def allowed_users(self) -> list:
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
    def active_reminders(self) -> Dict[str, Any]:
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
        """Получаем планировщик событий"""
        scheduler = EventScheduler()
        scheduler.current_index = self.event_current_index
        return scheduler

# ========== ФУНКЦИИ ДЛЯ СОБЫТИЙ "В ЭТОТ ДЕНЬ" ==========

async def send_daily_event(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправка ежедневного события 'В этот день'"""
    try:
        config = BotConfig()
        chat_id = config.chat_id

        if not chat_id:
            logger.error("Chat ID не установлен для отправки событий!")
            # Пробуем снова через час
            context.application.job_queue.run_once(
                schedule_next_event,
                3600
            )
            return

        # Получаем планировщик
        event_scheduler = config.get_event_scheduler()
        
        # Получаем текущую категорию
        category = event_scheduler.get_next_category()
        logger.info(f"Отправка ВСЕМИРНОГО события 'В этот день' категории: {category}, индекс: {event_scheduler.current_index}")
        
        # Создаем сообщение с событием
        message, keyboard = event_scheduler.create_event_message(category)
        
        # Отправляем событие
        await context.bot.send_message(
            chat_id=chat_id,
            text=message,
            parse_mode=ParseMode.MARKDOWN,
            disable_web_page_preview=False,
            reply_markup=keyboard  # Может быть None
        )
        
        # Увеличиваем индекс для следующего события
        event_scheduler.increment_category()
        config.event_current_index = event_scheduler.current_index
        
        logger.info(f"✅ ВСЕМИРНОЕ событие 'В этот день' отправлено: {category}. Следующий индекс: {event_scheduler.current_index}")
        
        # Планируем следующую отправку
        await schedule_next_event(context)
        
    except Exception as e:
        logger.error(f"❌ Ошибка отправки ВСЕМИРНОГО события 'В этот день': {e}")
        # Пробуем снова через 5 минут
        context.application.job_queue.run_once(
            schedule_next_event,
            300,
            chat_id=context.job.chat_id if hasattr(context, 'job') else None
        )

@restricted
async def send_event_now(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправить ВСЕМИРНОЕ событие 'В этот день' немедленно по команде"""
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
        logger.info(f"Отправка ВСЕМИРНОГО события 'В этот день' по команде: {category}, индекс: {event_scheduler.current_index}")
        
        # Создаем сообщение с событием
        message, keyboard = event_scheduler.create_event_message(category)
        
        # Отправляем событие в целевой чат
        await context.bot.send_message(
            chat_id=chat_id,
            text=message,
            parse_mode=ParseMode.MARKDOWN,
            disable_web_page_preview=False,
            reply_markup=keyboard  # Может быть None
        )
        
        # Увеличиваем индекс для следующего события
        event_scheduler.increment_category()
        config.event_current_index = event_scheduler.current_index
        
        logger.info(f"ВСЕМИРНОЕ событие 'В этот день' отправлено по команде: {category}. Следующий индекс: {event_scheduler.current_index}")
        
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка при отправке ВСЕМИРНОГО события: {str(e)}")
        logger.error(f"Ошибка в команде /eventnow: {e}")

async def show_next_event_category(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показать следующую категорию ВСЕМИРНЫХ событий 'В этот день'"""
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
    
    response = f"📅 *Информация о рубрике 'В ЭТОТ ДЕНЬ':*\n\n"
    response += f"🗓️ *Сегодня:* {day} {month_ru}\n\n"
    response += f"{current_emoji} *Текущая категория:* {current_category.upper()}\n"
    response += f"{next_emoji} *Следующая категория:* {next_category.upper()}\n\n"
    response += f"⏰ *Следующая отправка:* {moscow_time.strftime('%d.%m.%Y в %H:%M')} по МСК\n"
    response += f"🌍 *Тип событий:* ВСЕМИРНЫЕ (на русском языке)\n"
    response += f"🎯 *Всего категорий:* {len(EVENT_CATEGORIES)}\n"
    response += f"🔁 *Порядок:* {', '.join(EVENT_CATEGORIES)}\n\n"
    response += f"📖 *События не повторяются в пределах категории!*"
    
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
    while True:
        next_day = now + timedelta(days=days_ahead)
        if next_day.weekday() in EVENT_DAYS:
            return next_day.replace(
                hour=EVENT_SEND_TIME["hour"],
                minute=EVENT_SEND_TIME["minute"],
                second=0,
                microsecond=0
            )
        days_ahead += 1

async def schedule_next_event(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Запланировать следующую отправку ВСЕМИРНОГО события 'В этот день'"""
    try:
        next_time = calculate_next_event_time()
        config = BotConfig()
        chat_id = config.chat_id

        if not chat_id:
            logger.warning("Chat ID не установлен, планирование ВСЕМИРНЫХ событий отложено")
            # Пробуем снова через час
            context.application.job_queue.run_once(
                schedule_next_event,
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

                logger.info(f"Следующая отправка ВСЕМИРНОГО события 'В этот день' запланирована на {next_time} UTC")
                logger.info(f"Это будет в {(next_time + timedelta(hours=3)).strftime('%H:%M')} по МСК")
                
                # Получаем планировщик для логирования следующей категории
                event_scheduler = config.get_event_scheduler()
                logger.info(f"Следующая категория ВСЕМИРНЫХ событий: {event_scheduler.get_next_category()}")
            else:
                logger.info(f"Отправка ВСЕМИРНОГО события на {next_time} уже запланирована")
        else:
            # Если время уже прошло, планируем на следующий день
            logger.warning(f"Время отправки ВСЕМИРНОГО события уже прошло ({next_time}), планируем на следующий день")
            context.application.job_queue.run_once(
                schedule_next_event,
                60,  # Через минуту
                chat_id=chat_id
            )
            
    except Exception as e:
        logger.error(f"Ошибка планирования ВСЕМИРНОГО события: {e}")
        # Пробуем снова через 5 минут
        context.application.job_queue.run_once(
            schedule_next_event,
            300,
            chat_id=context.job.chat_id if hasattr(context, 'job') else None
        )

# ========== ФУНКЦИИ ПЛАНЁРОК (БЕЗ ИЗМЕНЕНИЙ) ==========

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
    keyboard.append([InlineKeyboardButton("↩️ Назад к причинам", callback_data="back_to_reasons")])
    
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
    """Обновленный обработчик /start с информацией о ВСЕМИРНЫХ событиях 'В этот день'"""
    await update.message.reply_text(
        "🤖 <b>Бот для напоминаний о планёрке активен!</b>\n\n"
        f"📅 <b>Напоминания отправляются:</b>\n"
        f"• Понедельник\n• Среда\n• Пятница\n"
        f"⏰ <b>Время:</b> {MEETING_TIME['hour']:02d}:{MEETING_TIME['minute']:02d} по МСК\n\n"
        "📅 <b>Ежедневная рубрика 'В ЭТОТ ДЕНЬ':</b>\n"
        f"• Отправляется: Пн-Пт в 10:00 по МСК\n"
        f"• Формат: <b>В ЭТОТ ДЕНЬ: ДЕНЬ МЕСЯЦ ГОД |КАТЕГОРИЯ: КАТЕГОРИЯ</b>\n"
        f"• Категории: {', '.join([c.capitalize() for c in EVENT_CATEGORIES])}\n"
        f"• <b>События ВСЕМИРНЫЕ</b> (на русском языке)\n"
        f"• События НЕ повторяются в пределах категории!\n\n"
        "🔧 <b>Доступные команды:</b>\n"
        "/info - информация о боте\n"
        "/jobs - список запланированных задач\n"
        "/test - тестовое напоминание (через 5 сек)\n"
        "/testnow - мгновенное тестовое напоминание\n"
        "/eventnow - отправить ВСЕМИРНОЕ событие 'В этот день' сейчас\n"
        "/nextevent - следующая категория ВСЕМИРНЫХ событий\n\n"
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
        "Напоминания и ВСЕМИРНЫЕ события 'В этот день' будут отправляться в этот чат.",
        parse_mode=ParseMode.HTML
    )

    logger.info(f"Установлен чат {chat_title} ({chat_id})")

@restricted
async def show_info(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обновленный обработчик /info с информацией о ВСЕМИРНЫХ событиях 'В этот день'"""
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
        if job.name and j.name and j.name.startswith("daily_event_"):
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

    zoom_info = f"\n🎥 <b>Zoom-ссылка:</b> {'установлена ✅' if ZOOM_LINK and ZOOM_LINK != 'https://us04web.zoom.us/j/1234567890?pwd=example' else 'не установлена ⚠️'}"
    
    # Информация о ВСЕМИРНЫХ событиях "В этот день"
    event_scheduler = config.get_event_scheduler()
    next_event_category = EVENT_CATEGORIES[event_scheduler.current_index]
    next_event_emoji = CATEGORY_EMOJIS.get(next_event_category, '📌')
    
    # Получаем текущую дату
    day, month_ru, year = event_scheduler.get_todays_date_parts()
    event_info = f"\n📅 <b>Следующее ВСЕМИРНОЕ событие 'В этот день':</b> {next_event_emoji} {next_event_category.capitalize()}"
    
    await update.message.reply_text(
        f"📊 <b>Информация о боте:</b>\n\n"
        f"{status}\n"
        f"📅 <b>Дни планёрок:</b> понедельник, среда, пятница\n"
        f"⏰ <b>Время планёрок:</b> {MEETING_TIME['hour']:02d}:{MEETING_TIME['minute']:02d} по МСК\n"
        f"📅 <b>События 'В этот день':</b> Пн-Пт в 10:00 по МСК\n"
        f"🌍 <b>Тип событий:</b> ВСЕМИРНЫЕ (на русском языке)\n"
        f"🎯 <b>Категории событий:</b> {', '.join(EVENT_CATEGORIES)}\n"
        f"🗓️ <b>Формат:</b> <b>В ЭТОТ ДЕНЬ: {day} {month_ru} ГОД |КАТЕГОРИЯ: КАТЕГОРИЯ</b>\n"
        f"🔄 <b>События не повторяются</b> в пределах категории!\n"
        f"👥 <b>Разрешённые пользователи:</b> {len(config.allowed_users)}\n"
        f"📋 <b>Активные напоминания:</b> {len(config.active_reminders)}\n"
        f"⏳ <b>Задачи планёрок:</b> {meeting_job_count}\n"
        f"📅 <b>Задачи событий:</b> {event_job_count}\n"
        f"➡️ <b>Следующая планёрка:</b> {next_meeting_time}\n"
        f"➡️ <b>Следующее ВСЕМИРНОЕ событие:</b> {next_event_time}\n"
        f"📈 <b>Ближайшие планёрки:</b> {', '.join(upcoming_meetings[:3]) if upcoming_meetings else 'нет'}"
        f"{zoom_info}"
        f"{event_info}\n\n"
        f"Используйте /users для списка пользователей\n"
        f"Используйте /jobs для списка задач\n"
        f"Используйте /nextevent для следующей категории ВСЕМИРНЫХ событий",
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
    zoom_status = "установлена ✅" if ZOOM_LINK and ZOOM_LINK != "https://us04web.zoom.us/j/1234567890?pwd=example" else "не установлена ⚠️"
    
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
        message += "\n📅 <b>ВСЕМИРНЫЕ события 'В этот день':</b>\n"
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
        f"• {canceled_events} отправок ВСЕМИРНЫХ событий 'В этот день'\n"
        f"Очищено {len(config.active_reminders)} активных напоминаний в конфиге",
        parse_mode=ParseMode.HTML
    )
    logger.info(f"Отменено {canceled_meetings} напоминаний и {canceled_events} ВСЕМИРНЫХ событий")

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
    while True:
        next_day = now + timedelta(days=days_ahead)
        if next_day.weekday() in MEETING_DAYS:
            return next_day.replace(
                hour=MEETING_TIME['hour'],
                minute=MEETING_TIME['minute'],
                second=0,
                microsecond=0
            )
        days_ahead += 1

async def schedule_next_reminder(context: ContextTypes.DEFAULT_TYPE) -> None:
    next_time = calculate_next_reminder()
    config = BotConfig()
    chat_id = config.chat_id

    if not chat_id:
        logger.warning("Chat ID не установлен, планирование отложено")
        context.application.job_queue.run_once(
            schedule_next_reminder,
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
                schedule_next_reminder,
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
            schedule_next_reminder,
            60,  # Через минуту
            chat_id=chat_id
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

def main() -> None:
    if not TOKEN:
        logger.error("❌ Токен бота не найден! Установите переменную окружения TELEGRAM_BOT_TOKEN")
        return
    
    if not ZOOM_LINK or ZOOM_LINK == "https://us04web.zoom.us/j/1234567890?pwd=example":
        logger.warning("⚠️  Zoom-ссылка не установлена или используется значение по умолчанию!")
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
            lambda context: schedule_next_reminder(context),
            3
        )

        # Запуск планировщика ВСЕМИРНЫХ событий "В этот день"
        application.job_queue.run_once(
            lambda context: schedule_next_event(context),
            5
        )

        # Получаем текущую дату для логирования
        now = datetime.now(TIMEZONE)
        day = now.day
        month_ru = MONTHS_RU[now.month]
        year = now.year
        
        logger.info("🤖 Бот запущен и готов к работе!")
        logger.info(f"⏰ Планёрки: {', '.join(['Пн', 'Ср', 'Пт'])} в {MEETING_TIME['hour']:02d}:{MEETING_TIME['minute']:02d} по МСК")
        logger.info(f"📅 Рубрика 'В ЭТОТ ДЕНЬ': Пн-Пт в 10:00 по МСК (07:00 UTC)")
        logger.info(f"🌍 Тип событий: ВСЕМИРНЫЕ (на русском языке)")
        logger.info(f"🗓️ Формат: В ЭТОТ ДЕНЬ: {day} {month_ru} ГОД |КАТЕГОРИЯ: КАТЕГОРИЯ")
        logger.info(f"🎯 Категории событий: {', '.join(EVENT_CATEGORIES)}")
        logger.info(f"🔄 События НЕ повторяются в пределах категории!")
        logger.info(f"👥 Разрешённые пользователи: {', '.join(BotConfig().allowed_users)}")
        
        application.run_polling(allowed_updates=Update.ALL_TYPES)

    except Exception as e:
        logger.error(f"❌ Критическая ошибка при запуске бота: {e}")
        raise


if __name__ == "__main__":
    main()
