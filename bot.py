import os
import json
import random
import logging
import requests
import asyncio
import html
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple, TypedDict
from functools import wraps
import pytz
from urllib.parse import quote
import re
import time
from collections import Counter, defaultdict

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
CATEGORY_STATS_FILE = "category_stats.json"

# Время планёрки (9:30 по Москве)
MEETING_TIME = {"hour": 9, "minute": 15}
TIMEZONE = pytz.timezone("Europe/Moscow")

# Дни недели для планёрки (понедельник=0, среда=2, пятница=4)
MEETING_DAYS = [0, 2, 4]

# ========== КОНСТАНТЫ СОБЫТИЙ "В ЭТОТ ДЕНЬ" ==========
EVENT_CATEGORIES = ['музыка', 'фильмы', 'технологии', 'игры', 'наука', 'спорт', 'история']

DAY_CATEGORY_PREFERENCES = {
    0: ['технологии', 'наука', 'история'],
    1: ['музыка', 'фильмы', 'игры'],
    2: ['спорт', 'история', 'технологии'],
    3: ['наука', 'музыка', 'фильмы'],
    4: ['игры', 'музыка', 'спорт'],
}

SEASONAL_PREFERENCES = {
    1: ['история', 'наука', 'спорт'],
    2: ['история', 'наука'],
    3: ['наука', 'технологии'],
    4: ['спорт', 'музыка'],
    5: ['фильмы', 'игры'],
    6: ['спорт', 'музыка'],
    7: ['игры', 'технологии'],
    8: ['история', 'спорт'],
    9: ['наука', 'фильмы'],
    10: ['игры', 'музыка'],
    11: ['история', 'фильмы'],
    12: ['музыка', 'фильмы', 'игры'],
}

# Время отправки (10:00 по Москве = 7:00 UTC)
EVENT_SEND_TIME = {"hour": 7, "minute": 0, "timezone": "UTC"}
EVENT_DAYS = [0, 1, 2, 3, 4]

# Русские названия месяцев
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

# Описания категорий
CATEGORY_DESCRIPTIONS = {
    'музыка': 'Знаменательные события в мире музыки',
    'фильмы': 'Кинопремьеры и события из мира кино',
    'технологии': 'Изобретения и технологические прорывы',
    'игры': 'Выпуски игр и события индустрии',
    'наука': 'Научные открытия и достижения',
    'спорт': 'Спортивные рекорды и события',
    'история': 'Исторические события и даты'
}

# Wikipedia API
WIKIPEDIA_API_URL = "https://ru.wikipedia.org/w/api.php"
USER_AGENT = 'TelegramEventBot/4.1 (https://github.com/; contact@example.com)'
REQUEST_TIMEOUT = 20
REQUEST_RETRIES = 3

# ========== ТИПЫ ДАННЫХ ==========
class HistoricalEvent(TypedDict):
    title: str
    year: int
    description: str
    url: str
    category: str
    full_article: str
    fact: str  # Добавили поле для факта

class ReminderData(TypedDict):
    message_id: int
    chat_id: int
    created_at: str

class CategoryStats(TypedDict):
    sent_count: int
    engagement_score: float
    last_sent: str
    popularity_score: float
    feedback_counts: Dict[str, int]

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
    """Класс для управления отправкой исторических событий 'В этот день' с адаптивными категориями"""
    
    def __init__(self):
        self.current_index = 0
        self.used_events: Dict[str, set] = {category: set() for category in EVENT_CATEGORIES}
        self.fallback_cache: Dict[str, List[HistoricalEvent]] = {}
        
        # Статистика категорий
        self.category_stats = self._load_category_stats()
        
        # История выбора категорий
        self.category_history: List[str] = []
        self.max_history_size = 100
        
        # Веса категорий для адаптивного выбора
        self.category_weights = self._calculate_initial_weights()
        
        logger.info("Инициализирован адаптивный планировщик исторических событий 'В этот день'")
    
    def _load_category_stats(self) -> Dict[str, CategoryStats]:
        """Загружаем статистику категорий из файла"""
        if os.path.exists(CATEGORY_STATS_FILE):
            try:
                with open(CATEGORY_STATS_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for category in EVENT_CATEGORIES:
                        if category not in data:
                            data[category] = {
                                'sent_count': 0,
                                'engagement_score': 0.5,
                                'last_sent': '',
                                'popularity_score': 0.5,
                                'feedback_counts': {'likes': 0, 'dislikes': 0, 'skips': 0}
                            }
                    return data
            except Exception as e:
                logger.error(f"Ошибка загрузки статистики категорий: {e}")
        
        stats = {}
        for category in EVENT_CATEGORIES:
            stats[category] = {
                'sent_count': 0,
                'engagement_score': 0.5,
                'last_sent': '',
                'popularity_score': 0.5,
                'feedback_counts': {'likes': 0, 'dislikes': 0, 'skips': 0}
            }
        return stats
    
    def _save_category_stats(self) -> None:
        """Сохраняем статистика категорий в файл"""
        try:
            with open(CATEGORY_STATS_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.category_stats, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Ошибка сохранения статистики категорий: {e}")
    
    def _calculate_initial_weights(self) -> Dict[str, float]:
        """Рассчитываем начальные веса категорий"""
        weights = {}
        base_weight = 1.0 / len(EVENT_CATEGORIES)
        
        for category in EVENT_CATEGORIES:
            weights[category] = base_weight
        
        return weights
    
    def _update_category_weights(self) -> None:
        """Обновляем веса категорий на основе статистики и контекста"""
        now = datetime.now(TIMEZONE)
        weekday = now.weekday()
        month = now.month
        
        # 1. Вес на основе статистики вовлеченности
        engagement_weights = {}
        total_engagement = sum(stats['engagement_score'] for stats in self.category_stats.values())
        
        for category in EVENT_CATEGORIES:
            if total_engagement > 0:
                engagement_weights[category] = self.category_stats[category]['engagement_score'] / total_engagement
            else:
                engagement_weights[category] = 1.0 / len(EVENT_CATEGORIES)
        
        # 2. Вес на основе дня недели
        day_weights = {}
        if weekday in DAY_CATEGORY_PREFERENCES:
            preferred = DAY_CATEGORY_PREFERENCES[weekday]
            for category in EVENT_CATEGORIES:
                if category in preferred:
                    day_weights[category] = 1.5
                else:
                    day_weights[category] = 1.0
        else:
            for category in EVENT_CATEGORIES:
                day_weights[category] = 1.0
        
        # 3. Вес на основе сезона/месяца
        seasonal_weights = {}
        if month in SEASONAL_PREFERENCES:
            preferred = SEASONAL_PREFERENCES[month]
            for category in EVENT_CATEGORIES:
                if category in preferred:
                    seasonal_weights[category] = 1.3
                else:
                    seasonal_weights[category] = 1.0
        else:
            for category in EVENT_CATEGORIES:
                seasonal_weights[category] = 1.0
        
        # 4. Вес на основе времени с последней отправки
        recency_weights = {}
        for category in EVENT_CATEGORIES:
            last_sent = self.category_stats[category]['last_sent']
            if last_sent:
                try:
                    last_sent_date = datetime.fromisoformat(last_sent)
                    days_passed = (now - last_sent_date).days
                    recency_weights[category] = min(2.0, 1.0 + (days_passed / 30.0))
                except:
                    recency_weights[category] = 2.0
            else:
                recency_weights[category] = 2.0
        
        # 5. Комбинируем все веса
        for category in EVENT_CATEGORIES:
            combined_weight = (
                engagement_weights[category] *
                day_weights[category] *
                seasonal_weights[category] *
                recency_weights[category]
            )
            self.category_weights[category] = combined_weight
        
        # Нормализуем веса
        total_weight = sum(self.category_weights.values())
        if total_weight > 0:
            for category in EVENT_CATEGORIES:
                self.category_weights[category] /= total_weight
        
        logger.debug(f"Обновленные веса категорий: {self.category_weights}")
    
    def get_next_category(self) -> str:
        """Получаем следующую категорию с учетом адаптивных весов"""
        self._update_category_weights()
        
        categories = list(self.category_weights.keys())
        weights = list(self.category_weights.values())
        
        selected_category = random.choices(categories, weights=weights, k=1)[0]
        
        self.category_history.append(selected_category)
        if len(self.category_history) > self.max_history_size:
            self.category_history.pop(0)
        
        logger.info(f"Выбрана адаптивная категория: {selected_category} (вес: {self.category_weights[selected_category]:.3f})")
        return selected_category
    
    def record_category_feedback(self, category: str, feedback_type: str = 'neutral') -> None:
        """Записываем обратную связь по категории"""
        if category not in self.category_stats:
            return
        
        stats = self.category_stats[category]
        
        if feedback_type in ['like', 'dislike', 'skip']:
            if feedback_type not in stats['feedback_counts']:
                stats['feedback_counts'][feedback_type] = 0
            stats['feedback_counts'][feedback_type] += 1
        
        # Пересчитываем engagement_score
        total_feedback = sum(stats['feedback_counts'].values())
        if total_feedback > 0:
            likes = stats['feedback_counts'].get('like', 0)
            dislikes = stats['feedback_counts'].get('dislike', 0)
            
            if likes + dislikes > 0:
                stats['engagement_score'] = likes / (likes + dislikes)
            else:
                stats['engagement_score'] = 0.5
        
        self._save_category_stats()
        logger.info(f"Записан фидбэк для категории {category}: {feedback_type}")
    
    def increment_category(self) -> str:
        """Увеличиваем индекс категории и возвращаем следующую"""
        old_category = self.get_next_category()
        
        now = datetime.now(TIMEZONE).isoformat()
        self.category_stats[old_category]['sent_count'] += 1
        self.category_stats[old_category]['last_sent'] = now
        
        total_sent = sum(stats['sent_count'] for stats in self.category_stats.values())
        if total_sent > 0:
            for category in EVENT_CATEGORIES:
                self.category_stats[category]['popularity_score'] = (
                    self.category_stats[category]['sent_count'] / total_sent
                )
        
        self._save_category_stats()
        
        next_category = self.get_next_category()
        logger.info(f"Категория изменена: {old_category} -> {next_category}")
        return next_category
    
    def get_category_stats_message(self) -> str:
        """Получаем статистику категорий в читаемом формате"""
        message = "📊 *Статистика категорий:*\n\n"
        
        sorted_categories = sorted(
            self.category_stats.items(),
            key=lambda x: x[1]['popularity_score'],
            reverse=True
        )
        
        for category, stats in sorted_categories:
            emoji = CATEGORY_EMOJIS.get(category, '📌')
            sent_count = stats['sent_count']
            engagement = stats['engagement_score']
            
            engagement_bar = self._create_progress_bar(engagement, 10)
            
            total_feedback = sum(stats['feedback_counts'].values())
            if total_feedback > 0:
                likes = stats['feedback_counts'].get('like', 0)
                likes_percent = (likes / total_feedback) * 100
            else:
                likes_percent = 0
            
            message += (
                f"{emoji} *{category.upper()}*\n"
                f"• Отправлено: {sent_count} раз\n"
                f"• Вовлеченность: {engagement_bar} ({engagement:.1%})\n"
                f"• Лайков: {likes_percent:.0f}%\n"
                f"• Последний раз: {self._format_last_sent(stats['last_sent'])}\n\n"
            )
        
        total_sent = sum(stats['sent_count'] for stats in self.category_stats.values())
        message += f"📈 *Всего отправлено:* {total_sent} событий\n"
        
        popular_categories = sorted_categories[:3]
        if popular_categories:
            popular_names = [f"{CATEGORY_EMOJIS.get(cat, '')} {cat}" for cat, _ in popular_categories]
            message += f"🏆 *Топ-3:* {', '.join(popular_names)}\n"
        
        next_category = self.get_next_category()
        next_emoji = CATEGORY_EMOJIS.get(next_category, '📌')
        message += f"🔮 *Следующая (предположительно):* {next_emoji} {next_category}"
        
        return message
    
    def _create_progress_bar(self, value: float, length: int = 10) -> str:
        """Создаем текстовый прогресс-бар"""
        filled = int(value * length)
        empty = length - filled
        return '█' * filled + '░' * empty
    
    def _format_last_sent(self, last_sent_str: str) -> str:
        """Форматируем дату последней отправки"""
        if not last_sent_str:
            return "никогда"
        
        try:
            last_sent = datetime.fromisoformat(last_sent_str)
            now = datetime.now(TIMEZONE)
            days_passed = (now - last_sent).days
            
            if days_passed == 0:
                return "сегодня"
            elif days_passed == 1:
                return "вчера"
            elif days_passed < 7:
                return f"{days_passed} дней назад"
            elif days_passed < 30:
                weeks = days_passed // 7
                return f"{weeks} недель назад"
            else:
                months = days_passed // 30
                return f"{months} месяцев назад"
        except:
            return "неизвестно"
    
    def get_todays_date_parts(self) -> Tuple[int, str, int]:
        """Получаем текущую дату (день, месяц_ru, текущий_год)"""
        now = datetime.now(TIMEZONE)
        day = now.day
        month_ru = MONTHS_RU[now.month]
        year = now.year
        return day, month_ru, year
    
    def search_historical_events(self, day: int, month: int, category: str) -> List[HistoricalEvent]:
        """
        Ищем исторические события, которые произошли в ЭТУ ДАТУ в РАЗНЫЕ ГОДЫ
        """
        try:
            date_str = f"{day} {MONTHS_RU_LOWER[month]}"
            logger.info(f"УЛУЧШЕННЫЙ поиск исторических событий за {date_str} в категории {category}")
            
            events: List[HistoricalEvent] = []
            
            # 1. Ищем через Википедию
            wikipedia_events = self._search_wikipedia_events(day, month, category)
            if wikipedia_events:
                events.extend(wikipedia_events)
                logger.info(f"Найдено {len(wikipedia_events)} событий в Википедии")
            
            # 2. Если не нашли, пробуем известные события
            if not events:
                known_events = self._search_known_events(day, month, category)
                if known_events:
                    events.extend(known_events)
                    logger.info(f"Найдено {len(known_events)} известных событий")
            
            # 3. Уникальные события
            unique_events: List[HistoricalEvent] = []
            seen_titles = set()
            
            for event in events:
                if (event['title'] not in seen_titles and 
                    event['year'] and 
                    1000 <= event['year'] <= datetime.now(TIMEZONE).year):
                    unique_events.append(event)
                    seen_titles.add(event['title'])
            
            logger.info(f"Итого найдено {len(unique_events)} уникальных исторических событий за {date_str} в категории {category}")
            return unique_events
            
        except Exception as e:
            logger.error(f"Ошибка поиска исторических событий: {e}")
            return []
    
    def _search_wikipedia_events(self, day: int, month: int, category: str) -> List[HistoricalEvent]:
        """Улучшенный поиск событий на Википедии"""
        events: List[HistoricalEvent] = []
        
        try:
            # Стратегия 1: Поиск по точной дате
            date_formats = [
                f"{day} {MONTHS_RU_LOWER[month]}",
                f"{day} {MONTHS_RU[month].lower()}",
                f"{day:02d}.{month:02d}",
                f"{day}/{month}"
            ]
            
            for date_format in date_formats:
                for year in range(1800, datetime.now(TIMEZONE).year + 1):
                    try:
                        search_query = f"{date_format} {year} {self._get_category_keywords(category)}"
                        logger.debug(f"Поиск в Википедии: {search_query}")
                        
                        params = {
                            'action': 'query',
                            'format': 'json',
                            'list': 'search',
                            'srsearch': search_query,
                            'srlimit': 5,
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
                                
                                # Пропускаем служебные страницы
                                if any(word in title.lower() for word in ['категория:', 'шаблон:', 'список:', 'изображение:', 'файл:']):
                                    continue
                                
                                # Получаем полную статью
                                event = self._get_event_from_article(title, day, month, year, category)
                                if event and event['fact']:
                                    events.append(event)
                                    logger.info(f"Найдено событие: {title} ({year})")
                                    
                                    if len(events) >= 3:
                                        return events
                        
                        time.sleep(0.3)  # Задержка, чтобы не перегружать API
                        
                    except Exception as e:
                        logger.debug(f"Ошибка при поиске {year}: {e}")
                        continue
            
            return events
            
        except Exception as e:
            logger.error(f"Ошибка поиска в Википедии: {e}")
            return []
    
    def _get_category_keywords(self, category: str) -> str:
        """Ключевые слова для поиска по категории"""
        keywords = {
            'музыка': 'альбом сингл концерт музыкант группа премия',
            'фильмы': 'фильм кино премьера актёр режиссёр Оскар',
            'технологии': 'изобретение патент компания запуск представлен',
            'игры': 'игра выпуск студия консоль турнир',
            'наука': 'открытие изобретение учёный эксперимент премия',
            'спорт': 'чемпионат олимпиада рекорд матч спортсмен',
            'история': 'событие война договор революция основание'
        }
        return keywords.get(category, '')
    
    def _get_event_from_article(self, title: str, day: int, month: int, year: int, category: str) -> Optional[HistoricalEvent]:
        """Получаем событие из статьи Википедии"""
        try:
            # Получаем полный текст статьи
            full_text = self._get_article_full_text(title)
            if not full_text:
                return None
            
            # Ищем факт с датой
            fact = self._extract_event_fact_improved(full_text, day, month, year)
            if not fact:
                return None
            
            # Получаем описание
            description = self._get_article_description(title)
            
            # URL статьи
            encoded_title = quote(title.replace(' ', '_'), safe='')
            article_url = f"https://ru.wikipedia.org/wiki/{encoded_title}"
            
            return {
                'title': title,
                'year': year,
                'description': description,
                'url': article_url,
                'category': category,
                'full_article': full_text[:5000],
                'fact': fact
            }
            
        except Exception as e:
            logger.warning(f"Ошибка получения события из статьи '{title}': {e}")
            return None
    
    def _get_article_full_text(self, title: str) -> Optional[str]:
        """Получаем полный текст статьи"""
        try:
            params = {
                'action': 'query',
                'format': 'json',
                'prop': 'extracts',
                'explaintext': True,
                'exsectionformat': 'plain',
                'exchars': 10000,
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
            
            if 'missing' not in page and 'extract' in page:
                return page['extract']
        
        except Exception as e:
            logger.warning(f"Ошибка получения полного текста статьи '{title}': {e}")
        
        return None
    
    def _extract_event_fact_improved(self, text: str, day: int, month: int, year: int) -> Optional[str]:
        """Улучшенное извлечение факта о событии из текста"""
        try:
            # Варианты написания даты
            date_patterns = [
                f"{day}\s+{MONTHS_RU_LOWER[month]}\s+{year}",
                f"{day}\s+{MONTHS_RU_LOWER[month]}\s+{year}\s+года",
                f"{year}\s+года\s+{day}\s+{MONTHS_RU_LOWER[month]}",
                f"{day:02d}[\.\s]+{month:02d}[\.\s]+{year}",
                f"{year}[\.\s]+{month:02d}[\.\s]+{day:02d}"
            ]
            
            # Разделяем на предложения
            sentences = re.split(r'[.!?]+', text)
            
            for sentence in sentences:
                sentence = sentence.strip()
                if not sentence:
                    continue
                
                # Проверяем все варианты даты
                for pattern in date_patterns:
                    if re.search(pattern, sentence, re.IGNORECASE):
                        # Убираем лишние пробелы и обрезаем
                        cleaned = re.sub(r'\s+', ' ', sentence).strip()
                        if 30 <= len(cleaned) <= 500:  # Разумная длина
                            return cleaned + '.'
            
            # Если точной даты нет, ищем упоминание года и события
            year_str = str(year)
            for sentence in sentences:
                sentence = sentence.strip()
                if not sentence or len(sentence) < 30:
                    continue
                
                if year_str in sentence:
                    # Проверяем, что это действительно о событии
                    event_keywords = [
                        'произошло', 'состоялось', 'вышел', 'вышла', 'выпущен',
                        'родился', 'родилась', 'основан', 'основана', 'открытие',
                        'изобретение', 'премьера', 'турнир', 'чемпионат', 'начало',
                        'создан', 'создана', 'запущен', 'запущена'
                    ]
                    
                    if any(keyword in sentence.lower() for keyword in event_keywords):
                        cleaned = re.sub(r'\s+', ' ', sentence).strip()
                        if len(cleaned) <= 500:
                            return cleaned + '.'
            
            return None
            
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
        
        # Расширенная база известных событий
        known_events_db = {
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
                    'description': 'Один из самых продаваемых альбомов в истории музыки.',
                    'url': 'https://ru.wikipedia.org/wiki/The_Dark_Side_of_the_Moon',
                    'fact': 'Альбом "The Dark Side of the Moon" группы Pink Floyd вышел 14 января 1973 года.'
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
                {
                    'title': 'Состоялась премьера фильма "Матрица"',
                    'year': 1999,
                    'description': 'Научно-фантастический фильм братьев Вачовски.',
                    'url': 'https://ru.wikipedia.org/wiki/Матрица_(фильм)',
                    'fact': 'Премьера фильма "Матрица" состоялась 14 января 1999 года.'
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
                {
                    'title': 'Запущен первый веб-сайт',
                    'year': 1991,
                    'description': 'Сайт создан Тимом Бернерсом-Ли для CERN.',
                    'url': 'https://ru.wikipedia.org/wiki/Всемирная_паутина',
                    'fact': 'Первый веб-сайт был запущен 14 января 1991 года.'
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
                {
                    'title': 'Вышла игра "Super Mario 64"',
                    'year': 1996,
                    'description': 'Первая 3D-игра про Марио для Nintendo 64.',
                    'url': 'https://ru.wikipedia.org/wiki/Super_Mario_64',
                    'fact': 'Игра "Super Mario 64" вышла 14 января 1996 года.'
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
                {
                    'title': 'Открытие планеты Нептун',
                    'year': 1846,
                    'description': 'Планета была открыта по математическим расчётам.',
                    'url': 'https://ru.wikipedia.org/wiki/Нептун',
                    'fact': 'Планета Нептун была открыта 14 января 1846 года.'
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
                {
                    'title': 'Майк Тайсон стал самым молодым чемпионом мира в тяжелом весе',
                    'year': 1986,
                    'description': 'Тайсон победил Тревора Бербика и стал чемпионом в возрасте 20 лет.',
                    'url': 'https://ru.wikipedia.org/wiki/Тайсон,_Майк',
                    'fact': 'Майк Тайсон стал самым молодым чемпионом мира в тяжелом весе 14 января 1986 года.'
                },
            ],
            'история': [
                {
                    'title': 'Состоялась коронация Георга VI, короля Великобритании',
                    'year': 1937,
                    'description': 'Коронация прошла в Вестминстерском аббатстве.',
                    'url': 'https://ru.wikipedia.org/wiki/Георг_VI',
                    'fact': 'Коронация Георга VI состоялась 14 января 1937 года.'
                },
                {
                    'title': 'Начало экспедиции Роберта Скотта к Южному полюсу',
                    'year': 1911,
                    'description': 'Британская антарктическая экспедиция под руководством Роберта Скотта.',
                    'url': 'https://ru.wikipedia.org/wiki/Экспедиция_Скотта_(1910—1912)',
                    'fact': 'Экспедиция Роберта Скотта к Южному полюсу началась 14 января 1911 года.'
                },
            ]
        }
        
        # Проверяем текущую дату
        current_day = datetime.now(TIMEZONE).day
        current_month = datetime.now(TIMEZONE).month
        
        # Если сегодня 14 января - используем известные события
        if day == current_day and month == current_month:
            if category in known_events_db:
                for event_data in known_events_db[category]:
                    events.append({
                        'title': event_data['title'],
                        'year': event_data['year'],
                        'description': event_data['description'],
                        'url': event_data['url'],
                        'category': category,
                        'full_article': '',
                        'fact': event_data['fact']
                    })
        
        return events
    
    def get_historical_event_for_category(self, category: str) -> Tuple[str, Optional[int], str, str, str]:
        """
        Получаем историческое событие "В этот день" для текущей даты
        Возвращает: (title, year, description, url, fact)
        """
        try:
            now = datetime.now(TIMEZONE)
            day = now.day
            month = now.month
            
            logger.info(f"УЛУЧШЕННЫЙ поиск исторических событий за {day} {MONTHS_RU[month]} в категории: {category}")
            
            # Ищем события
            events = self.search_historical_events(day, month, category)
            
            # Фильтруем уже использованные
            available_events = [
                event for event in events 
                if event['title'] not in self.used_events[category]
            ]
            
            # Если все использованы, очищаем историю
            if not available_events and events:
                logger.info(f"Все события в категории '{category}' использованы, очищаем историю")
                self.used_events[category] = set()
                available_events = events
            
            # Если ничего не нашли, используем fallback
            if not available_events:
                logger.warning(f"Не найдено исторических событий за {day} {MONTHS_RU[month]} в категории {category}")
                return self._get_fallback_event(category, day, month)
            
            # Выбираем случайное событие
            event = random.choice(available_events)
            
            # Добавляем в использованные
            self.used_events[category].add(event['title'])
            
            logger.info(f"Выбрано историческое событие: {event['title']} ({event['year']} год)")
            logger.info(f"Факт: {event.get('fact', 'Нет факта')[:100]}...")
            
            return (
                event['title'],
                event['year'],
                event['description'],
                event['url'],
                event.get('fact', f"{event['title']} ({event['year']} год).")
            )
            
        except Exception as e:
            logger.error(f"Ошибка получения исторического события: {e}")
            return self._get_fallback_event(category, datetime.now(TIMEZONE).day, datetime.now(TIMEZONE).month)
    
    def _get_fallback_event(self, category: str, day: int, month: int) -> Tuple[str, Optional[int], str, str, str]:
        """Резервные исторические события на случай недоступности Wikipedia"""
        # Если в кэше есть события для этой категории
        if category in self.fallback_cache:
            event = random.choice(self.fallback_cache[category])
            return (
                event['title'], 
                event['year'], 
                event['description'], 
                event['url'], 
                event.get('fact', event['title'])
            )
        
        # Или создаем простой fallback
        fallback_events = {
            'музыка': {
                'title': 'Знаменательное событие в мире музыки',
                'year': 1900 + random.randint(0, 120),
                'description': 'Интересное музыкальное событие произошло в этот день.',
                'url': f'https://ru.wikipedia.org/wiki/{day}_{MONTHS_RU_LOWER[month]}',
                'fact': f'Музыкальное событие произошло {day} {MONTHS_RU_LOWER[month]} в мире искусства.'
            },
            'фильмы': {
                'title': 'Кинематографическое событие',
                'year': 1900 + random.randint(0, 120),
                'description': 'Важное событие в истории кино.',
                'url': f'https://ru.wikipedia.org/wiki/{day}_{MONTHS_RU_LOWER[month]}',
                'fact': f'Кинематографическое событие произошло {day} {MONTHS_RU_LOWER[month]}.'
            },
            'технологии': {
                'title': 'Технологическое достижение',
                'year': 1900 + random.randint(0, 120),
                'description': 'Прорыв в области технологий.',
                'url': f'https://ru.wikipedia.org/wiki/{day}_{MONTHS_RU_LOWER[month]}',
                'fact': f'Технологическое достижение было зафиксировано {day} {MONTHS_RU_LOWER[month]}.'
            },
            'игры': {
                'title': 'Событие в игровой индустрии',
                'year': 1980 + random.randint(0, 40),
                'description': 'Важное событие в мире видеоигр.',
                'url': f'https://ru.wikipedia.org/wiki/{day}_{MONTHS_RU_LOWER[month]}',
                'fact': f'Событие в игровой индустрии произошло {day} {MONTHS_RU_LOWER[month]}.'
            },
            'наука': {
                'title': 'Научное открытие',
                'year': 1800 + random.randint(0, 220),
                'description': 'Важное научное достижение.',
                'url': f'https://ru.wikipedia.org/wiki/{day}_{MONTHS_RU_LOWER[month]}',
                'fact': f'Научное открытие было сделано {day} {MONTHS_RU_LOWER[month]}.'
            },
            'спорт': {
                'title': 'Спортивное достижение',
                'year': 1900 + random.randint(0, 120),
                'description': 'Рекорд или важное спортивное событие.',
                'url': f'https://ru.wikipedia.org/wiki/{day}_{MONTHS_RU_LOWER[month]}',
                'fact': f'Спортивное достижение было установлено {day} {MONTHS_RU_LOWER[month]}.'
            },
            'история': {
                'title': 'Историческое событие',
                'year': 1000 + random.randint(0, 1000),
                'description': 'Важное событие в мировой истории.',
                'url': f'https://ru.wikipedia.org/wiki/{day}_{MONTHS_RU_LOWER[month]}',
                'fact': f'Историческое событие произошло {day} {MONTHS_RU_LOWER[month]}.'
            }
        }
        
        event_data = fallback_events.get(category, fallback_events['история'])
        
        return (
            event_data['title'],
            event_data['year'],
            event_data['description'],
            event_data['url'],
            event_data['fact']
        )
    
    def create_event_message(self, category: str) -> Tuple[str, Optional[InlineKeyboardMarkup]]:
        """Создаем сообщение с историческим событием 'В этот день' в формате HTML"""
        day, month_ru, current_year = self.get_todays_date_parts()
        
        # ВАЖНО: Используем правильный метод для получения события
        title, event_year, description, url, fact = self.get_historical_event_for_category(category)
        
        # Форматируем в HTML
        message = f"<b>В ЭТОТ ДЕНЬ: {day} {month_ru} {event_year} года</b>\n\n"
        
        category_emoji = CATEGORY_EMOJIS.get(category, '📌')
        category_description = CATEGORY_DESCRIPTIONS.get(category, '')
        
        message += f"{category_emoji} {category_description}\n\n"
        
        # Экранируем HTML-сущности в факте
        safe_fact = html.escape(fact)
        message += f"{safe_fact}\n\n"
        
        if description and description not in fact:
            if len(description) > 300:
                description = description[:300] + '...'
            # Экранируем HTML-сущности в описании
            safe_description = html.escape(description)
            message += f"{safe_description}\n\n"
        
        if url:
            # HTML ссылка - будет работать без проблем
            safe_url = html.escape(url)
            message += f'📖 <a href="{safe_url}">Подробнее на Википедии</a>'
        
        # Только кнопки обратной связи, без статистики
        keyboard = [
            [
                InlineKeyboardButton("👍 Понравилось", callback_data=f"feedback_like_{category}"),
                InlineKeyboardButton("👎 Не понравилось", callback_data=f"feedback_dislike_{category}")
            ],
            [
                InlineKeyboardButton("⏭️ Пропустить", callback_data=f"feedback_skip_{category}")
            ]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        return message, reply_markup

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
    
    # Используем формат HTML для кликабельной ссылки
    if ZOOM_LINK == DEFAULT_ZOOM_LINK:
        zoom_note = "\n\n⚠️ Zoom-ссылка не настроена! Используйте /info для проверки"
    else:
        zoom_link_formatted = f'<a href="{ZOOM_LINK}">Присоединиться к Zoom</a>'
        zoom_notes = [
            f"\n\n🎥 {zoom_link_formatted} | 👈",
            f"\n\n👨💻 {zoom_link_formatted} | 👈",
            f"\n\n💻 {zoom_link_formatted} | 👈",
            f"\n\n🔗 {zoom_link_formatted} | 👈",
        ]
        zoom_note = random.choice(zoom_notes)
    
    if weekday in MEETING_DAYS:
        day_names = {0: "ПОНЕДЕЛЬНИК", 2: "СРЕДА", 4: "ПЯТНИЦА"}
        
        greetings = {
            0: [
                f"🚀 <b>{day_names[0]}</b> - старт новой недели!\n\n📋 <i>Планёрка в 9:30 по МСК</i>. Давайте обсудим планы на неделю! 🌟{zoom_note}",
                f"🌞 Доброе утро! Сегодня <b>{day_names[0]}</b>!\n\n🤝 <i>Планёрка в 9:30 по МСК</i>. Начинаем неделю продуктивно! 💪{zoom_note}",
            ],
            2: [
                f"⚡ <b>{day_names[2]}</b> - середина недели!\n\n📋 <i>Планёрка в 9:30 по МСК</i>. Время для корректировок и обновлений! 🔄{zoom_note}",
                f"🌞 <b>{day_names[2]}</b>, доброе утро!\n\n🤝 <i>Планёрка в 9:30 по МСК</i>. Как продвигаются задачи? 📈{zoom_note}",
            ],
            4: [
                f"🎉 <b>{day_names[4]}</b> - завершаем неделю!\n\n📋 <i>Планёрка в 9:30 по МСК</i>. Давайте подведем итоги недели! 🏆{zoom_note}",
                f"🌞 Пятничное утро! 🎊\n\n🤝 <b>{day_names[4]}</b>, <i>планёрка в 9:30 по МСК</i>. Как прошла неделя? 📊{zoom_note}",
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
    """Отправка ежедневного исторического события 'В этот день' в формате HTML"""
    try:
        config = BotConfig()
        chat_id = config.chat_id

        if not chat_id:
            logger.error("Chat ID не установлен для отправки исторических событий!")
            context.application.job_queue.run_once(
                lambda ctx: asyncio.create_task(schedule_next_event(ctx)),
                3600
            )
            return

        event_scheduler = config.get_event_scheduler()
        
        category = event_scheduler.get_next_category()
        logger.info(f"ОТПРАВКА АДАПТИВНОГО события 'В этот день' категории: {category}")
        
        message, keyboard = event_scheduler.create_event_message(category)
        
        await context.bot.send_message(
            chat_id=chat_id,
            text=message,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=False,
            reply_markup=keyboard
        )
        
        event_scheduler.increment_category()
        config.event_current_index = event_scheduler.current_index
        
        logger.info(f"✅ АДАПТИВНОЕ событие 'В этот день' отправлено: {category}")
        
        await schedule_next_event(context)
        
    except Exception as e:
        logger.error(f"❌ Ошибка отправки АДАПТИВНОГО события 'В этот день': {e}")
        context.application.job_queue.run_once(
            lambda ctx: asyncio.create_task(schedule_next_event(ctx)),
            300
        )

@restricted
async def send_event_now(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправить АДАПТИВНОЕ историческое событие 'В этот день' немедленно по команде"""
    config = BotConfig()
    chat_id = config.chat_id

    if not chat_id:
        await update.message.reply_text("❌ Сначала установите чат командой /setchat")
        return

    try:
        event_scheduler = config.get_event_scheduler()
        
        category = event_scheduler.get_next_category()
        logger.info(f"ОТПРАВКА ПО КОМАНДЕ АДАПТИВНОГО события 'В этот день' категории: {category}")
        
        message, keyboard = event_scheduler.create_event_message(category)
        
        await context.bot.send_message(
            chat_id=chat_id,
            text=message,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=False,
            reply_markup=keyboard
        )
        
        event_scheduler.increment_category()
        config.event_current_index = event_scheduler.current_index
        
        logger.info(f"✅ АДАПТИВНОЕ событие 'В этот день' отправлено по команде: {category}")
        
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка при отправке АДАПТИВНОГО события: {str(e)}")
        logger.error(f"Ошибка в команде /eventnow: {e}")

async def show_next_event_category(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показать следующую категорию АДАПТИВНЫХ событий 'В этот день'"""
    config = BotConfig()
    event_scheduler = config.get_event_scheduler()
    
    current_category = event_scheduler.get_next_category()
    
    now = datetime.now(TIMEZONE)
    day = now.day
    month_ru = MONTHS_RU[now.month]
    weekday = now.weekday()
    
    next_time = calculate_next_event_time()
    moscow_time = next_time.astimezone(TIMEZONE)
    
    category_stats_message = event_scheduler.get_category_stats_message()
    
    day_names = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
    current_day_name = day_names[weekday]
    
    if weekday in DAY_CATEGORY_PREFERENCES:
        preferred = DAY_CATEGORY_PREFERENCES[weekday]
        preferred_emojis = [CATEGORY_EMOJIS.get(cat, '') for cat in preferred]
        preferred_str = ', '.join([f"{emoji} {cat}" for emoji, cat in zip(preferred_emojis, preferred)])
        day_info = f"\n📅 *Сегодня {current_day_name}* - предпочтительные категории: {preferred_str}"
    else:
        day_info = f"\n📅 *Сегодня {current_day_name}*"
    
    month = now.month
    if month in SEASONAL_PREFERENCES:
        seasonal = SEASONAL_PREFERENCES[month]
        seasonal_emojis = [CATEGORY_EMOJIS.get(cat, '') for cat in seasonal]
        seasonal_str = ', '.join([f"{emoji} {cat}" for emoji, cat in zip(seasonal_emojis, seasonal)])
        month_info = f"\n🌦️ *Сезонные предпочтения ({MONTHS_RU_LOWER[month]}):* {seasonal_str}"
    else:
        month_info = ""
    
    current_emoji = CATEGORY_EMOJIS.get(current_category, '📌')
    
    response = (
        f"📊 *АДАПТИВНАЯ система категорий 'В ЭТОТ ДЕНЬ':*\n\n"
        f"🗓️ *Исторические события за:* {day} {month_ru}\n"
        f"{day_info}"
        f"{month_info}\n\n"
        f"🎯 *Текущая категория:* {current_emoji} {current_category.upper()}\n"
        f"⏰ *Следующая отправка:* {moscow_time.strftime('%d.%m.%Y в %H:%M')} по МСК\n\n"
        f"{category_stats_message}"
    )
    
    await update.message.reply_text(response, parse_mode=ParseMode.MARKDOWN)

@restricted
async def show_category_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показать детальную статистику категорий (только для разрешенных пользователей)"""
    config = BotConfig()
    event_scheduler = config.get_event_scheduler()
    
    stats_message = event_scheduler.get_category_stats_message()
    
    await update.message.reply_text(stats_message, parse_mode=ParseMode.MARKDOWN)

async def handle_feedback_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработка обратной связи по категориям"""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    logger.info(f"Получен фидбэк: {data}")
    
    if data.startswith("feedback_"):
        try:
            parts = data.split("_")
            if len(parts) >= 3:
                feedback_type = parts[1]
                category = "_".join(parts[2:])
                
                config = BotConfig()
                event_scheduler = config.get_event_scheduler()
                
                event_scheduler.record_category_feedback(category, feedback_type)
                
                emoji = "👍" if feedback_type == "like" else "👎" if feedback_type == "dislike" else "⏭️"
                feedback_texts = {
                    "like": "понравилось",
                    "dislike": "не понравилось",
                    "skip": "пропущено"
                }
                
                category_emoji = CATEGORY_EMOJIS.get(category, '📌')
                response = f"{emoji} Спасибо! Ваш отзыв ({feedback_texts.get(feedback_type, '')}) записан для категории {category_emoji} {category}."
                
                await query.edit_message_text(
                    text=query.message.text + f"\n\n{response}",
                    parse_mode=ParseMode.HTML,
                    reply_markup=None
                )
            else:
                await query.answer("❌ Ошибка обработки фидбэка", show_alert=True)
                
        except Exception as e:
            logger.error(f"Ошибка обработки фидбэка: {e}")
            await query.answer("❌ Произошла ошибка", show_alert=True)

def calculate_next_event_time() -> datetime:
    """Рассчитать время следующей отправки события"""
    now = datetime.now(pytz.UTC)
    
    if now.weekday() in EVENT_DAYS:
        reminder_time = now.replace(
            hour=EVENT_SEND_TIME["hour"],
            minute=EVENT_SEND_TIME["minute"],
            second=0,
            microsecond=0
        )
        if now < reminder_time:
            return reminder_time

    days_ahead = 1
    max_days = 365
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
    """Запланировать следующую отправку АДАПТИВНОГО события 'В этот день'"""
    try:
        next_time = calculate_next_event_time()
        config = BotConfig()
        chat_id = config.chat_id

        if not chat_id:
            logger.warning("Chat ID не установлен, планирование АДАПТИВНЫХ событий отложено")
            context.application.job_queue.run_once(
                lambda ctx: asyncio.create_task(schedule_next_event(ctx)),
                3600
            )
            return

        now = datetime.now(pytz.UTC)
        delay = (next_time - now).total_seconds()

        if delay > 0:
            job_name = f"daily_event_{next_time.strftime('%Y%m%d_%H%M')}"
            
            existing_jobs = [j for j in get_jobs_from_queue(context.application.job_queue) 
                            if j.name == job_name]
            
            if not existing_jobs:
                context.application.job_queue.run_once(
                    send_daily_event,
                    delay,
                    chat_id=chat_id,
                    name=job_name
                )

                logger.info(f"Следующая отправка АДАПТИВНОГО события 'В этот день' запланирована на {next_time} UTC")
                logger.info(f"Это будет в {(next_time + timedelta(hours=3)).strftime('%H:%M')} по МСК")
                
                event_scheduler = config.get_event_scheduler()
                next_category = event_scheduler.get_next_category()
                logger.info(f"Следующая АДАПТИВНАЯ категория: {next_category}")
            else:
                logger.info(f"Отправка АДАПТИВНОГО события на {next_time} уже запланирована")
        else:
            logger.warning(f"Время отправки АДАПТИВНОГО события уже прошло ({next_time}), планируем на следующий день")
            context.application.job_queue.run_once(
                lambda ctx: asyncio.create_task(schedule_next_event(ctx)),
                60
            )
            
    except Exception as e:
        logger.error(f"Ошибка планирования АДАПТИВНОГО события: {e}")
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
            days_names = ["понедельник", "вторник", "среду", "четверг", "пятницу", "суббота", "воскресенье"]
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
    """Обновленный обработчик /start"""
    await update.message.reply_text(
        "🤖 <b>Бот для напоминаний о планёрке с АДАПТИВНОЙ рубрикой 'В этот день'!</b>\n\n"
        f"📅 <b>Напоминания отправляются:</b>\n"
        f"• Понедельник\n• Среда\n• Пятница\n"
        f"⏰ <b>Время:</b> {MEETING_TIME['hour']:02d}:{MEETING_TIME['minute']:02d} по МСК\n\n"
        "📅 <b>АДАПТИВНАЯ рубрика 'В ЭТОТ ДЕНЬ':</b>\n"
        f"• Отправляется: Пн-Пт в 10:00 по МСК\n"
        f"• <b>Умная система категорий:</b> адаптируется под ваши предпочтения\n"
        f"• <b>Контекстный выбор:</b> учитывает день недели и сезон\n"
        f"• <b>Обратная связь:</b> оценивайте события 👍/👎\n"
        f"• Категории: {', '.join([c.capitalize() for c in EVENT_CATEGORIES])}\n"
        f"• <b>УЛУЧШЕННЫЙ поиск фактов из Википедии!</b>\n"
        f"• События НЕ повторяются в пределах категории!\n\n"
        "🔧 <b>Доступные команды:</b>\n"
        "/info - информация о боте\n"
        "/jobs - список запланированных задач\n"
        "/test - тестовое напоминание (через 5 сек)\n"
        "/testnow - мгновенное тестовое напоминание\n"
        "/eventnow - отправить АДАПТИВНОЕ событие 'В этот день' сейчас\n"
        "/nextevent - следующая категория АДАПТИВНЫХ событий\n\n"
        "👮♂️ <b>Команды для администраторов:</b>\n"
        "/setchat - установить чат для уведомлений\n"
        "/adduser @username - добавить пользователя\n"
        "/removeuser @username - удалить пользователя\n"
        "/users - список пользователей\n"
        "/stats - статистика категорий (только для админов)\n"
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
        "Напоминания и АДАПТИВНЫЕ события 'В этот день' будут отправляться в этот чат.",
        parse_mode=ParseMode.HTML
    )

    logger.info(f"Установлен чат {chat_title} ({chat_id})")

@restricted
async def show_info(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обновленный обработчик /info"""
    config = BotConfig()
    chat_id = config.chat_id

    if chat_id:
        status = f"✅ <b>Чат установлен</b> (ID: {chat_id})"
    else:
        status = "❌ <b>Чат не установлен</b>. Используйте /setchat"

    all_jobs = get_jobs_from_queue(context.application.job_queue)
    
    meeting_job_count = len([j for j in all_jobs 
                    if j.name and j.name.startswith("meeting_reminder_")])
    
    event_job_count = len([j for j in all_jobs 
                    if j.name and j.name.startswith("daily_event_")])
    
    next_meeting_job = None
    for job in all_jobs:
        if job.name and job.name.startswith("meeting_reminder_"):
            if not next_meeting_job or job.next_t < next_meeting_job.next_t:
                next_meeting_job = job
    
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
    
    event_scheduler = config.get_event_scheduler()
    next_event_category = event_scheduler.get_next_category()
    next_event_emoji = CATEGORY_EMOJIS.get(next_event_category, '📌')
    
    day, month_ru, year = event_scheduler.get_todays_date_parts()
    
    now = datetime.now(TIMEZONE)
    weekday = now.weekday()
    month = now.month
    
    day_names = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
    current_day = day_names[weekday]
    
    context_info = ""
    if weekday in DAY_CATEGORY_PREFERENCES:
        preferred = DAY_CATEGORY_PREFERENCES[weekday]
        context_info = f"\n📅 <b>Сегодня {current_day}</b> - предпочтения: {', '.join(preferred)}"
    
    if month in SEASONAL_PREFERENCES:
        seasonal = SEASONAL_PREFERENCES[month]
        context_info += f"\n🌦️ <b>Сезон ({MONTHS_RU_LOWER[month]}):</b> предпочтение к {', '.join(seasonal[:2])}"
    
    event_info = f"\n📅 <b>Следующее АДАПТИВНОЕ событие 'В этот день':</b> {next_event_emoji} {next_event_category.capitalize()}"
    
    await update.message.reply_text(
        f"📊 <b>Информация о боте (АДАПТИВНАЯ версия):</b>\n\n"
        f"{status}\n"
        f"📅 <b>Дни планёрок:</b> понедельник, среда, пятница\n"
        f"⏰ <b>Время планёрок:</b> {MEETING_TIME['hour']:02d}:{MEETING_TIME['minute']:02d} по МСК\n"
        f"📅 <b>АДАПТИВНЫЕ события 'В этот день':</b> Пн-Пт в 10:00 по МСК\n"
        f"🎯 <b>Умная система категорий:</b> адаптируется под предпочтения\n"
        f"📈 <b>Контекстный выбор:</b> день недели + сезонные предпочтения\n"
        f"📊 <b>Статистика вовлеченности:</b> учитывает обратную связь 👍/👎\n"
        f"🌐 <b>УЛУЧШЕННЫЙ поиск:</b> факты из Википедии + локальная база\n"
        f"👥 <b>Разрешённые пользователи:</b> {len(config.allowed_users)}\n"
        f"📋 <b>Активные напоминания:</b> {len(config.active_reminders)}\n"
        f"⏳ <b>Задачи планёрок:</b> {meeting_job_count}\n"
        f"📅 <b>Задачи событий:</b> {event_job_count}\n"
        f"➡️ <b>Следующая планёрка:</b> {next_meeting_time}\n"
        f"➡️ <b>Следующее АДАПТИВНОЕ событие:</b> {next_event_time}"
        f"{context_info}"
        f"{zoom_info}"
        f"{event_info}\n\n"
        f"Используйте /stats для статистики категорий (админы)\n"
        f"Используйте /users для списка пользователей\n"
        f"Используйте /jobs для списка задач\n"
        f"Используйте /nextevent для следующей категории АДАПТИВНЫХ событий",
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
        message += "\n📅 <b>АДАПТИВНЫЕ события 'В этот день':</b>\n"
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
        f"• {canceled_events} отправок АДАПТИВНЫХ событий 'В этот день'\n"
        f"Очищено {len(config.active_reminders)} активных напоминаний в конфиге",
        parse_mode=ParseMode.HTML
    )
    logger.info(f"Отменено {canceled_meetings} напоминаний и {canceled_events} АДАПТИВНЫХ событий")

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
    max_days = 365
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
        logger.warning(f"Время напоминания уже прошло ({next_time}), планируем на следующий день")
        context.application.job_queue.run_once(
            lambda ctx: asyncio.create_task(schedule_next_reminder(ctx)),
            60
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
    
    if not zoom_link.startswith('https://'):
        logger.warning(f"Zoom ссылка не использует HTTPS: {zoom_link}")
        return False
    
    if 'zoom.us' not in zoom_link and 'zoom.com' not in zoom_link:
        logger.warning(f"Zoom ссылка не содержит домен zoom: {zoom_link}")
        return False
    
    return True

def main() -> None:
    if not TOKEN:
        logger.error("❌ Токен бота не найден! Установите переменную окружения TELEGRAM_BOT_TOKEN")
        return
    
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
        application.add_handler(CommandHandler("stats", show_category_stats))
        application.add_handler(CommandHandler("test", test_reminder))
        application.add_handler(CommandHandler("testnow", test_now))
        application.add_handler(CommandHandler("eventnow", send_event_now))
        application.add_handler(CommandHandler("nextevent", show_next_event_category))
        application.add_handler(CommandHandler("jobs", list_jobs))
        application.add_handler(CommandHandler("adduser", add_user))
        application.add_handler(CommandHandler("removeuser", remove_user))
        application.add_handler(CommandHandler("users", list_users))
        application.add_handler(CommandHandler("cancelall", cancel_all))

        # Обработчик фидбэка для категорий
        application.add_handler(CallbackQueryHandler(handle_feedback_callback, pattern="^feedback_"))

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

        # Запуск планировщика АДАПТИВНЫХ событий "В этот день"
        application.job_queue.run_once(
            lambda ctx: asyncio.create_task(schedule_next_event(ctx)),
            5
        )

        # Получаем текущую дату для логирования
        now = datetime.now(TIMEZONE)
        day = now.day
        month_ru = MONTHS_RU[now.month]
        year = now.year
        weekday = now.weekday()
        
        day_names = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Саббота", "Воскресенье"]
        current_day = day_names[weekday]
        
        logger.info("🤖 Бот запущен и готов к работе!")
        logger.info(f"⏰ Планёрки: {', '.join(['Пн', 'Ср', 'Пт'])} в {MEETING_TIME['hour']:02d}:{MEETING_TIME['minute']:02d} по МСК")
        logger.info(f"📅 АДАПТИВНАЯ рубрика 'В ЭТОТ ДЕНЬ': Пн-Пт в 10:00 по МСК (07:00 UTC)")
        logger.info(f"🗓️ Сегодня: {current_day}, {day} {month_ru} {year}")
        logger.info(f"🎯 Умная система категорий: адаптивный выбор на основе статистики")
        logger.info(f"📈 Контекстный выбор: учитывает день недели и сезонные предпочтения")
        logger.info(f"📊 Обратная связь: система фидбэка 👍/👎 для улучшения подбора")
        logger.info(f"🌐 УЛУЧШЕННЫЙ поиск: факты из Википедии + локальная база")
        logger.info(f"🔄 События НЕ повторяются в пределах категории!")
        logger.info(f"👥 Разрешённые пользователи: {', '.join(BotConfig().allowed_users)}")
        
        if weekday in DAY_CATEGORY_PREFERENCES:
            preferred = DAY_CATEGORY_PREFERENCES[weekday]
            logger.info(f"📅 Предпочтения для {current_day}: {', '.join(preferred)}")
        
        if now.month in SEASONAL_PREFERENCES:
            seasonal = SEASONAL_PREFERENCES[now.month]
            logger.info(f"🌦️ Сезонные предпочтения ({MONTHS_RU_LOWER[now.month]}): {', '.join(seasonal)}")
        
        application.run_polling(allowed_updates=Update.ALL_TYPES)

    except Exception as e:
        logger.error(f"❌ Критическая ошибка при запуске бота: {e}")
        raise


if __name__ == "__main__":
    main()
