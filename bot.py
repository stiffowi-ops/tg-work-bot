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
INDUSTRY_ZOOM_LINK = os.getenv("INDUSTRY_MEETING_LINK", DEFAULT_ZOOM_LINK)
CONFIG_FILE = "bot_config.json"

# Время планёрки (9:30 по Москве)
MEETING_TIME = {"hour": 9, "minute": 15}
TIMEZONE = pytz.timezone("Europe/Moscow")

# Дни недели для планёрки (понедельник=0, среда=2, пятница=4)
MEETING_DAYS = [0, 2, 4]

# Время отправки утреннего приветствия с гороскопом (9:00 по МСК, Пн-Пт)
MORNING_GREETING_TIME = {"hour": 9, "minute": 0}
MORNING_DAYS = [0, 1, 2, 3, 4]  # Пн-Пт

# Время отправки исторических событий "В этот день" (10:00 по МСК, Пн-Пт)
EVENT_SEND_TIME = {"hour": 10, "minute": 0}
EVENT_DAYS = [0, 1, 2, 3, 4]  # Пн-Пт

# Время отраслевой встречи (вторник 12:00 по МСК)
INDUSTRY_MEETING_TIME = {"hour": 12, "minute": 0}
INDUSTRY_MEETING_DAY = [1]  # Вторник

# Русские названия месяцев
MONTHS_RU = {
    1: "ЯНВАРЯ", 2: "ФЕВРАЛЯ", 3: "МАРТА", 4: "АПРЕЛЯ",
    5: "МАЯ", 6: "ИЮНЯ", 7: "ИЮЛЯ", 8: "АВГУСТА",
    9: "СЕНТЯБРЯ", 10: "ОКТЯБРЯ", 11: "НОЯБРЯ", 12: "ДЕКАБРЯ"
}

# Знаки зодиака с русскими переводами
ZODIAC_SIGNS = {
    'aries': {'ru': '♈ Овен', 'emoji': '♈', 'en': 'Aries'},
    'taurus': {'ru': '♉ Телец', 'emoji': '♉', 'en': 'Taurus'},
    'gemini': {'ru': '♊ Близнецы', 'emoji': '♊', 'en': 'Gemini'},
    'cancer': {'ru': '♋ Рак', 'emoji': '♋', 'en': 'Cancer'},
    'leo': {'ru': '♌ Лев', 'emoji': '♌', 'en': 'Leo'},
    'virgo': {'ru': '♍ Дева', 'emoji': '♍', 'en': 'Virgo'},
    'libra': {'ru': '♎ Весы', 'emoji': '♎', 'en': 'Libra'},
    'scorpio': {'ru': '♏ Скорпион', 'emoji': '♏', 'en': 'Scorpio'},
    'sagittarius': {'ru': '♐ Стрелец', 'emoji': '♐', 'en': 'Sagittarius'},
    'capricorn': {'ru': '♑ Козерог', 'emoji': '♑', 'en': 'Capricorn'},
    'aquarius': {'ru': '♒ Водолей', 'emoji': '♒', 'en': 'Aquarius'},
    'pisces': {'ru': '♓ Рыбы', 'emoji': '♓', 'en': 'Pisces'}
}

# Соответствие знаков зодиака и сабреддитов для мемов (с приоритетом русских мемов)
ZODIAC_TO_MEME = {
    'aries': ['Pikabu', 'ru_Anime', 'dankmemes', 'programmingmemes', 'motivation'],
    'taurus': ['Pikabu', 'ru_Anime', 'wholesomememes', 'food', 'memes'],
    'gemini': ['Pikabu', 'ru_Anime', 'funny', 'dankmemes', 'memes'],
    'cancer': ['Pikabu', 'ru_Anime', 'wholesomememes', 'memes', 'MadeMeSmile'],
    'leo': ['Pikabu', 'ru_Anime', 'dankmemes', 'memes', 'motivation'],
    'virgo': ['Pikabu', 'ru_Anime', 'programmingmemes', 'memes', 'wholesomememes'],
    'libra': ['Pikabu', 'ru_Anime', 'wholesomememes', 'memes', 'funny'],
    'scorpio': ['Pikabu', 'ru_Anime', 'dankmemes', 'memes', 'programmingmemes'],
    'sagittarius': ['Pikabu', 'ru_Anime', 'dankmemes', 'memes', 'funny'],
    'capricorn': ['Pikabu', 'ru_Anime', 'programmingmemes', 'memes', 'wholesomememes'],
    'aquarius': ['Pikabu', 'ru_Anime', 'programmingmemes', 'dankmemes', 'memes'],
    'pisces': ['Pikabu', 'ru_Anime', 'wholesomememes', 'memes', 'MadeMeSmile']
}

# Русскоязычные сабреддиты (будем пытаться сначала их)
RUSSIAN_SUBREDDITS = ['Pikabu', 'ru_Anime', 'RU_Memes', 'russian', 'RussNews']

# Утренние приветствия
MORNING_GREETINGS = [
    "Оу, еще спишь? 😴 Давай посмотрим, что говорят звезды о тебе сегодня! ✨",
    "☀️ Хочешь узнать, что приготовили для тебя звезды? 🔮",
    "👋 Готов(а) узнать свой гороскоп на сегодня? Давай заглянем в будущее! 🌟"
]

# Текст для отраслевой встречи
INDUSTRY_MEETING_TEXTS = [
    "🏢 𝗢ТРАСЛЕВАЯ ВСТРЕЧА\n\n🎯 Что делаем:\n• Обсудим итоги за неделю\n• Новые тренды и инсайты\n• Обмен опытом с коллегами\n• Запланируем мероприятия на следующую\n\n🕐 Начало: 12:00 по МСК\n📍 Формат: Zoom-конференция\n\n🔗 Всех причастных ждём! {zoom_link} | 👈",
    "🏢 𝗢ТРАСЛЕВАЯ ВСТРЕЧА\n\n📊 Сегодня на повестке:\n• Анализ недельных результатов\n• Выявление ключевых трендов\n• Коллективный разбор кейсов\n• Планирование активностей\n\n🕐 Старт: 12:00 (МСК)\n🎥 Онлайн в Zoom\n\n🔗 Присоединяйтесь: {zoom_link} ← переход",
    "🏢 𝗢ТРАСЛЕВАЯ ВСТРЕЧА\n\n✨ На повестке дня:\n• Итоги рабочей недели\n• Прогнозы и инсайты\n•Планы на неделю\n\n⏰ Время: 12:00 по Москве\n💻 Платформа: Zoom\n\n🔗 Подключайтесь: {zoom_link} | 👈"
]

# Wikipedia API
WIKIPEDIA_API_URL = "https://ru.wikipedia.org/w/api.php"
USER_AGENT = 'TelegramEventBot/7.0 (https://github.com/; contact@example.com)'

# Meme API
MEME_API_URL = "https://meme-api.com/gimme"
REQUEST_TIMEOUT = 10

# ========== ТИПЫ ДАННЫХ ==========
class HistoricalEvent(TypedDict):
    title: str
    year: int
    text: str
    url: str
    category: str
    score: float

class Horoscope(TypedDict):
    sign: str
    date: str
    prediction: str
    mood: str
    color: str
    lucky_number: str
    lucky_time: str
    compatibility: str

class MemeData(TypedDict):
    url: str
    title: str
    subreddit: str
    post_url: str

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

INDUSTRY_CANCELLATION_OPTIONS = [
    "Основные спикеры не смогут участвовать",
    "Переносим на другую дату",
    "Актуальные вопросы решены вне встречи",
]

SELECTING_REASON, SELECTING_DATE, CONFIRMING_DATE = range(3)
SELECTING_INDUSTRY_REASON = 4

# Настройка логирования
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", 
    level=logging.INFO
)
logger = logging.getLogger(__name__)

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

def get_zodiac_meme(zodiac_sign: str) -> Optional[MemeData]:
    """Получаем тематический мем для знака зодиака с приоритетом русских мемов"""
    try:
        # Получаем подходящие сабреддиты для знака зодиака
        subreddits = ZODIAC_TO_MEME.get(zodiac_sign, ['Pikabu', 'ru_Anime', 'memes'])
        
        # Сначала пробуем русскоязычные сабреддиты
        for subreddit in subreddits:
            if subreddit in RUSSIAN_SUBREDDITS:
                try:
                    response = requests.get(
                        f"{MEME_API_URL}/{subreddit}",
                        headers={"User-Agent": USER_AGENT},
                        timeout=REQUEST_TIMEOUT
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        if data.get('nsfw', False) or data.get('spoiler', False):
                            continue
                        
                        return {
                            'url': data.get('url'),
                            'title': data.get('title', 'Без названия'),
                            'subreddit': data.get('subreddit', 'memes'),
                            'post_url': data.get('postLink', '')
                        }
                except Exception as e:
                    logger.debug(f"Не удалось получить мем из русского сабреддита {subreddit}: {e}")
        
        # Если русские мемы не найдены, пробуем английские
        for subreddit in subreddits:
            if subreddit not in RUSSIAN_SUBREDDITS:
                try:
                    response = requests.get(
                        f"{MEME_API_URL}/{subreddit}",
                        headers={"User-Agent": USER_AGENT},
                        timeout=REQUEST_TIMEOUT
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        if data.get('nsfw', False) or data.get('spoiler', False):
                            continue
                        
                        return {
                            'url': data.get('url'),
                            'title': data.get('title', 'Без названия'),
                            'subreddit': data.get('subreddit', 'memes'),
                            'post_url': data.get('postLink', '')
                        }
                except Exception as e:
                    logger.debug(f"Не удалось получить мем из {subreddit}: {e}")
        
        # Если не получилось, пробуем общий запрос
        response = requests.get(
            MEME_API_URL,
            headers={"User-Agent": USER_AGENT},
            timeout=REQUEST_TIMEOUT
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get('nsfw', False) or data.get('spoiler', False):
                raise ValueError("NSFW или спойлер-контент")
            
            return {
                'url': data.get('url'),
                'title': data.get('title', 'Без названия'),
                'subreddit': data.get('subreddit', 'memes'),
                'post_url': data.get('postLink', '')
            }
            
    except Exception as e:
        logger.error(f"Ошибка получения мема: {e}")
    
    return None

def get_backup_meme() -> MemeData:
    """Резервные мемы на случай ошибки API"""
    backup_memes = [
        {
            'url': 'https://i.imgflip.com/30b1gx.jpg',
            'title': 'Созерцающий кот',
            'subreddit': 'memes',
            'post_url': 'https://imgflip.com/i/30b1gx'
        },
        {
            'url': 'https://i.imgflip.com/1ur9b0.jpg',
            'title': 'Дрейк одобряет',
            'subreddit': 'dankmemes',
            'post_url': 'https://imgflip.com/i/1ur9b0'
        },
        {
            'url': 'https://i.imgflip.com/3vzej.jpg',
            'title': 'Программист за работой',
            'subreddit': 'programmingmemes',
            'post_url': 'https://imgflip.com/i/3vzej'
        }
    ]
    
    return random.choice(backup_memes)

def get_industry_meeting_text() -> str:
    """Получаем текст для отраслевой встречи с ссылкой"""
    zoom_link = INDUSTRY_ZOOM_LINK
    
    if zoom_link == DEFAULT_ZOOM_LINK:
        zoom_link_formatted = f'<a href="{zoom_link}">[НЕ НАСТРОЕНА - настройте INDUSTRY_MEETING_LINK]</a>'
    else:
        zoom_link_formatted = f'<a href="{zoom_link}">Присоединиться к Zoom</a>'
    
    text = random.choice(INDUSTRY_MEETING_TEXTS)
    return text.format(zoom_link=zoom_link_formatted)

def calculate_event_score(event_text: str, event_year: int) -> float:
    """Рассчитываем рейтинг события (0-100)"""
    text_lower = event_text.lower()
    score = 50  # Базовый балл
    
    # Проверка на жесткие запреты
    hard_forbidden = ['убийство', 'терроризм', 'казнь', 'погибло', 'погибли']
    for forbidden in hard_forbidden:
        if forbidden in text_lower:
            return 0  # Сразу отбрасываем
    
    # Положительные ключевые слова
    positive_keywords = {
        'наука': ['открытие', 'изобретение', 'ученый', 'научный', 'эксперимент'],
        'технологии': ['компьютер', 'интернет', 'программа', 'гаджет', 'патент'],
        'музыка': ['песня', 'альбом', 'концерт', 'группа', 'исполнитель'],
        'фильмы': ['фильм', 'кино', 'актер', 'режиссер', 'премьера'],
        'спорт': ['чемпионат', 'олимпиада', 'матч', 'спортсмен', 'рекорд'],
        'история': ['договор', 'основание', 'событие', 'закон', 'конституция']
    }
    
    # Бонусы за положительные ключевые слова
    for category, keywords in positive_keywords.items():
        for keyword in keywords:
            if keyword in text_lower:
                score += 5
    
    # Бонус за современность
    current_year = datetime.now().year
    if 1900 <= event_year <= current_year:
        recency_factor = (event_year - 1900) / (current_year - 1900)
        score += recency_factor * 20
    
    # Бонус за длину текста
    text_length = len(event_text)
    if 50 <= text_length <= 300:
        score += 10
    elif text_length > 300:
        score += 5
    
    # Штраф за упоминание войн
    negative_words = ['война', 'битва', 'сражение', 'конфликт', 'революция']
    for word in negative_words:
        if word in text_lower:
            score -= 15
    
    return min(max(score, 0), 100)

def get_events_for_today() -> List[HistoricalEvent]:
    """Получаем исторические события для сегодня"""
    now = datetime.now(TIMEZONE)
    day = now.day
    month = now.month
    
    all_events = []
    
    try:
        # Используем Wikipedia API
        params = {
            "action": "query",
            "format": "json",
            "prop": "onthisday",
            "onthistype": "events",
            "onthisday": f"{month:02d}-{day:02d}"
        }

        response = requests.get(
            WIKIPEDIA_API_URL,
            params=params,
            headers={"User-Agent": USER_AGENT},
            timeout=REQUEST_TIMEOUT
        )
        
        if response.status_code == 200:
            data = response.json()
            events_data = data.get("query", {}).get("onthisday", {}).get("events", [])
            
            for item in events_data:
                text = item.get("text", "").strip()
                year = item.get("year", 0)
                
                if not text or year < 1000 or year > datetime.now().year:
                    continue
                
                # Пропускаем события с войнами и смертями
                if any(word in text.lower() for word in ['война', 'битва', 'умер', 'погиб']):
                    continue
                
                score = calculate_event_score(text, year)
                if score < 20:
                    continue
                
                pages = item.get("pages", [])
                if pages:
                    title = pages[0]["title"]
                    url = f"https://ru.wikipedia.org/wiki/{quote(title.replace(' ', '_'))}"
                    
                    event: HistoricalEvent = {
                        "title": title,
                        "year": year,
                        "text": text,
                        "url": url,
                        "category": "история",
                        "score": score
                    }
                    
                    all_events.append(event)
        
    except Exception as e:
        logger.error(f"Ошибка получения событий: {e}")
    
    # Если нет событий, используем запасной вариант
    if not all_events:
        backup_events = [
            {"year": 2001, "title": "Википедия", "text": "Запущена Википедия — свободная интернет-энциклопедия."},
            {"year": 1998, "title": "Google", "text": "Основана компания Google."},
            {"year": 2007, "title": "iPhone", "text": "Представлен первый iPhone."},
        ]
        
        for event_data in backup_events:
            all_events.append({
                "title": event_data["title"],
                "year": event_data["year"],
                "text": event_data["text"],
                "url": f"https://ru.wikipedia.org/wiki/{event_data['title']}",
                "category": "история",
                "score": 80.0
            })
    
    all_events.sort(key=lambda x: x['score'], reverse=True)
    return all_events

def build_event_message(event: HistoricalEvent) -> str:
    """Создаем сообщение с историческим событием"""
    now = datetime.now(TIMEZONE)
    day = now.day
    month = MONTHS_RU[now.month]
    
    fact = html.escape(f"В {event['year']} году — {event['text']}")
    
    return (
        f"<b>В ЭТОТ ДЕНЬ — {day} {month}</b>\n\n"
        f"📜 <b>ИСТОРИЧЕСКОЕ СОБЫТИЕ</b>\n\n"
        f"{fact}\n\n"
        f"📖 <a href=\"{event['url']}\">Подробнее на Википедии</a>"
    )

def translate_simple(text: str) -> str:
    """Простой перевод для гороскопов (без внешних библиотек)"""
    # Словарь для перевода ключевых слов гороскопов
    translation_dict = {
        # Настроения
        'happy': 'Счастливое',
        'excited': 'Взволнованное',
        'romantic': 'Романтичное',
        'calm': 'Спокойное',
        'energetic': 'Энергичное',
        'creative': 'Творческое',
        'optimistic': 'Оптимистичное',
        'adventurous': 'Приключенческое',
        
        # Цвета
        'red': 'Красный',
        'blue': 'Синий',
        'green': 'Зеленый',
        'yellow': 'Желтый',
        'purple': 'Фиолетовый',
        'orange': 'Оранжевый',
        'pink': 'Розовый',
        'gold': 'Золотой',
        'silver': 'Серебряный',
        'white': 'Белый',
        'black': 'Черный',
        
        # Совместимость
        'aries': 'Овен',
        'taurus': 'Телец',
        'gemini': 'Близнецы',
        'cancer': 'Рак',
        'leo': 'Лев',
        'virgo': 'Дева',
        'libra': 'Весы',
        'scorpio': 'Скорпион',
        'sagittarius': 'Стрелец',
        'capricorn': 'Козерог',
        'aquarius': 'Водолей',
        'pisces': 'Рыбы',
        
        # Общие слова
        'today': 'сегодня',
        'day': 'день',
        'good': 'хороший',
        'great': 'отличный',
        'excellent': 'превосходный',
        'opportunity': 'возможность',
        'chance': 'шанс',
        'love': 'любовь',
        'money': 'деньги',
        'success': 'успех',
        'work': 'работа',
        'family': 'семья',
        'friends': 'друзья',
    }
    
    # Простой перевод - заменяем известные слова
    result = text
    for eng, rus in translation_dict.items():
        result = re.sub(rf'\b{eng}\b', rus, result, flags=re.IGNORECASE)
    
    return result

def get_horoscope_from_api(sign: str) -> Optional[Dict]:
    """Получаем гороскоп из работающего API (Horoscope API)"""
    try:
        # Преобразуем знак в русское название для API
        sign_translations = {
            'aries': 'oven',
            'taurus': 'telec',
            'gemini': 'bliznecy',
            'cancer': 'rak',
            'leo': 'lev',
            'virgo': 'deva',
            'libra': 'vesy',
            'scorpio': 'skorpion',
            'sagittarius': 'strelec',
            'capricorn': 'kozerog',
            'aquarius': 'vodoley',
            'pisces': 'ryby'
        }
        
        api_sign = sign_translations.get(sign.lower())
        if not api_sign:
            logger.error(f"Неизвестный знак зодиака: {sign}")
            return None
        
        # Используем альтернативный API
        response = requests.get(
            f"https://horoscope-api.vercel.app/api/horoscope/today/{api_sign}",
            headers={"User-Agent": USER_AGENT},
            timeout=REQUEST_TIMEOUT
        )
        
        if response.status_code == 200:
            data = response.json()
            
            # API возвращает данные на русском языке
            horoscope_data = {
                'sign': ZODIAC_SIGNS[sign]['ru'],
                'date': datetime.now(TIMEZONE).strftime('%d.%m.%Y'),
                'prediction': data.get('prediction', 'Нет предсказания на сегодня'),
                'mood': data.get('mood', 'Нейтральное'),
                'color': data.get('color', 'Неизвестно'),
                'lucky_number': str(data.get('lucky_number', '7')),
                'lucky_time': data.get('lucky_time', 'День'),
                'compatibility': ZODIAC_SIGNS.get(
                    data.get('compatibility', 'aries').lower(), 
                    {'ru': 'Овен'}
                )['ru']
            }
            
            return horoscope_data
            
    except Exception as e:
        logger.error(f"Ошибка получения гороскопа для {sign}: {e}")
    
    return None

def get_backup_horoscope(sign: str) -> Dict:
    """Резервный гороскоп, если API не работает"""
    predictions = [
        "Сегодня звезды благоволят вам. Ожидайте приятных сюрпризов!",
        "День подходит для новых начинаний. Доверяйте своей интуиции.",
        "Сегодня хороший день для общения и знакомств.",
        "Время для творчества и самовыражения.",
        "Финансовая удача сегодня на вашей стороне.",
        "День гармонии и спокойствия. Наслаждайтесь моментом.",
        "Сегодня вы сможете решить давние проблемы.",
        "Удачный день для планирования будущего.",
        "Ждите интересных предложений сегодня.",
        "День полон возможностей - будьте внимательны!"
    ]
    
    moods = ['Радостное', 'Спокойное', 'Энергичное', 'Романтичное', 'Творческое']
    colors = ['Красный', 'Синий', 'Зеленый', 'Золотой', 'Фиолетовый']
    times = ['Утро', 'День', 'Вечер', 'Полдень']
    numbers = ['7', '3', '11', '22', '5']
    
    # Выбираем случайную совместимость
    compatible_signs = list(ZODIAC_SIGNS.values())
    compatibility = random.choice([s['ru'] for s in compatible_signs])
    
    return {
        'sign': ZODIAC_SIGNS[sign]['ru'],
        'date': datetime.now(TIMEZONE).strftime('%d.%m.%Y'),
        'prediction': random.choice(predictions),
        'mood': random.choice(moods),
        'color': random.choice(colors),
        'lucky_number': random.choice(numbers),
        'lucky_time': random.choice(times),
        'compatibility': compatibility
    }

def build_horoscope_message(horoscope: Dict, meme: Optional[MemeData] = None) -> str:
    """Создаем красивое сообщение с гороскопом и мемом"""
    horoscope_text = (
        f"✨ <b>ГОРОСКОП НА СЕГОДНЯ</b> ✨\n\n"
        f"<b>{horoscope['sign']}</b>\n"
        f"📅 {horoscope['date']}\n\n"
        f"🔮 <b>Предсказание:</b>\n"
        f"{horoscope['prediction']}\n\n"
        f"😊 <b>Настроение:</b> {horoscope['mood']}\n"
        f"🎨 <b>Цвет дня:</b> {horoscope['color']}\n"
        f"🍀 <b>Счастливое число:</b> {horoscope['lucky_number']}\n"
        f"⏰ <b>Благоприятное время:</b> {horoscope['lucky_time']}\n"
        f"💞 <b>Совместимость:</b> {horoscope['compatibility']}\n\n"
    )
    
    if meme:
        # Определяем источник мема
        source = "🇷🇺 Русский мем" if meme['subreddit'] in RUSSIAN_SUBREDDITS else "🌍 Мем"
        
        horoscope_text += (
            f"🎭 <b>Мем дня:</b> {source}\n"
            f"<i>«{html.escape(meme['title'])}»</i>\n"
            f"📁 <a href=\"{meme['post_url']}\">r/{meme['subreddit']}</a>\n\n"
        )
    
    horoscope_text += f"<i>Хорошего дня! 🌟</i>"
    
    return horoscope_text

async def send_horoscope_with_meme(chat_id: int, horoscope: Dict, context: ContextTypes.DEFAULT_TYPE, 
                                  sign_key: str) -> None:
    """Отправляет гороскоп с прикреплённым мемом"""
    try:
        # Получаем мем для знака зодиака (с приоритетом русских)
        meme = get_zodiac_meme(sign_key) or get_backup_meme()
        
        # Строим текстовое сообщение с информацией о меме
        message_text = build_horoscope_message(horoscope, meme)
        
        # Отправляем мем как фото
        await context.bot.send_photo(
            chat_id=chat_id,
            photo=meme['url'],
            caption=message_text,
            parse_mode=ParseMode.HTML
        )
        
        logger.info(f"✅ Автоматический гороскоп с мемом отправлен в чат {chat_id} ({horoscope['sign']})")
        
    except Exception as e:
        logger.error(f"Ошибка отправки гороскопа с мемом: {e}")
        # Если не удалось отправить фото, отправляем текстовый гороскоп
        fallback_text = build_horoscope_message(horoscope)
        await context.bot.send_message(
            chat_id=chat_id,
            text=fallback_text,
            parse_mode=ParseMode.HTML
        )

def create_zodiac_keyboard() -> InlineKeyboardMarkup:
    """Создаем клавиатуру со знаками зодиака в 3 колонки"""
    keyboard = []
    signs_list = list(ZODIAC_SIGNS.items())
    
    # Разбиваем на 3 колонки по 4 знака
    for i in range(0, len(signs_list), 4):
        row = []
        for j in range(4):
            if i + j < len(signs_list):
                sign_key, sign_data = signs_list[i + j]
                row.append(
                    InlineKeyboardButton(
                        f"{sign_data['emoji']}",
                        callback_data=f"horoscope_{sign_key}"
                    )
                )
        keyboard.append(row)
    
    return InlineKeyboardMarkup(keyboard)

def get_greeting_by_meeting_day() -> str:
    """Специальные приветствия для дней планёрок"""
    weekday = datetime.now(TIMEZONE).weekday()
    day_names_ru = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Саббота", "Воскресенье"]
    current_day = day_names_ru[weekday]
    
    if ZOOM_LINK == DEFAULT_ZOOM_LINK:
        zoom_note = "\n\n⚠️ Zoom-ссылка не настроена!"
    else:
        zoom_link_formatted = f'<a href="{ZOOM_LINK}">Присоединиться к Zoom</a>'
        zoom_note = f"\n\n🎥 {zoom_link_formatted} | 👈"
    
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
                f"🎉 <b>{day_names[4]}</b> - завершаем недели!\n\n📋 <i>Планёрка в 9:30 по МСК</i>. Давайте подведем итоги недели! 🏆{zoom_note}",
                f"🌞 Пятничное утро! 🎊\n\n🤝 <b>{day_names[4]}</b>, <i>планёрка в 9:30 по МСК</i>. Как прошла неделя? 📊{zoom_note}",
            ]
        }
        return random.choice(greetings[weekday])
    else:
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
                    if "user_zodiacs" not in data:
                        data["user_zodiacs"] = {}
                    if "horoscope_requests" not in data:
                        data["horoscope_requests"] = {}
                    return data
            except Exception as e:
                logger.error(f"Ошибка загрузки конфига: {e}")
        return {
            "chat_id": None,
            "allowed_users": ["Stiff_OWi", "gshabanov"],
            "active_reminders": {},
            "user_zodiacs": {},
            "horoscope_requests": {}
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
    def user_zodiacs(self) -> Dict[str, str]:
        """Словарь user_id -> знак зодиака"""
        return self.data.get("user_zodiacs", {})
    
    def set_user_zodiac(self, user_id: int, zodiac: str) -> None:
        self.data["user_zodiacs"][str(user_id)] = zodiac
        self.save()
    
    def get_user_zodiac(self, user_id: int) -> Optional[str]:
        return self.data.get("user_zodiacs", {}).get(str(user_id))
    
    @property
    def horoscope_requests(self) -> Dict[str, Dict[str, str]]:
        """Словарь user_id -> информация о последнем запросе гороскопа"""
        return self.data.get("horoscope_requests", {})
    
    def cleanup_old_requests(self) -> None:
        """Очищает старые записи о запросах (старше 7 дней)"""
        today = datetime.now(TIMEZONE)
        week_ago = (today - timedelta(days=7)).strftime('%Y-%m-%d')
        
        updated_requests = {}
        for user_id, request_data in self.horoscope_requests.items():
            if request_data.get('last_request_date', '') >= week_ago:
                updated_requests[user_id] = request_data
        
        self.data["horoscope_requests"] = updated_requests
        self.save()

# ========== ФУНКЦИИ ДЛЯ ГОРОСКОПОВ С МЕМАМИ ==========

async def send_morning_greeting(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправка утреннего приветствия и автоматического гороскопа с мемом"""
    try:
        config = BotConfig()
        chat_id = config.chat_id

        if not chat_id:
            logger.error("Chat ID не установлен для отправки утреннего приветствия!")
            await schedule_next_morning_greeting(context)
            return

        # Отправляем случайное приветствие
        greeting = random.choice(MORNING_GREETINGS)
        
        # Отправляем приветствие
        await context.bot.send_message(
            chat_id=chat_id,
            text=greeting,
            parse_mode=ParseMode.HTML
        )

        logger.info(f"✅ Утреннее приветствие отправлено в чат {chat_id}")
        
        # Ждем 1 секунду перед отправкой гороскопа
        await asyncio.sleep(1)
        
        # Выбираем случайный знак зодиака для группового гороскопа
        # Можно сделать один знак для всех или разные варианты
        # Для простоты выберем случайный знак
        sign_key = random.choice(list(ZODIAC_SIGNS.keys()))
        sign_name = ZODIAC_SIGNS[sign_key]['ru']
        
        # Получаем гороскоп из API
        horoscope = get_horoscope_from_api(sign_key)
        
        # Если API не вернуло данные, используем резервный
        if not horoscope:
            horoscope = get_backup_horoscope(sign_key)
            logger.warning(f"API не вернуло гороскоп, используется резервный для {sign_name}")
        else:
            logger.info(f"Гороскоп получен из API для {sign_name}")
        
        # Отправляем гороскоп с мемом
        await send_horoscope_with_meme(
            chat_id=chat_id,
            horoscope=horoscope,
            context=context,
            sign_key=sign_key
        )
        
        logger.info(f"✅ Автоматический гороскоп отправлен в чат {chat_id} ({sign_name})")
        
        # Планируем следующее приветствие
        await schedule_next_morning_greeting(context)
        
    except Exception as e:
        logger.error(f"❌ Ошибка отправки утреннего приветствия: {e}")
        await schedule_next_morning_greeting(context)

def calculate_next_morning_time() -> datetime:
    """Рассчитать время следующего утреннего приветствия"""
    now = datetime.now(TIMEZONE)
    
    # Сегодняшнее время отправки
    today_target = now.replace(
        hour=MORNING_GREETING_TIME["hour"],
        minute=MORNING_GREETING_TIME["minute"],
        second=0,
        microsecond=0
    )

    # Если сегодня рабочий день и время еще не наступило
    if now < today_target and now.weekday() in MORNING_DAYS:
        return today_target

    # Ищем следующий рабочий день
    for i in range(1, 8):
        next_day = now + timedelta(days=i)
        if next_day.weekday() in MORNING_DAYS:
            return next_day.replace(
                hour=MORNING_GREETING_TIME["hour"],
                minute=MORNING_GREETING_TIME["minute"],
                second=0,
                microsecond=0
            )
    
    raise ValueError("Не найден подходящий день для утреннего приветствия")

async def schedule_next_morning_greeting(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Запланировать следующее утреннее приветствие"""
    try:
        next_time = calculate_next_morning_time()
        config = BotConfig()
        chat_id = config.chat_id

        if not chat_id:
            logger.warning("Chat ID не установлен, планирование утренних приветствий отложено")
            context.application.job_queue.run_once(
                lambda ctx: asyncio.create_task(schedule_next_morning_greeting(ctx)),
                3600
            )
            return

        now = datetime.now(TIMEZONE)
        delay = (next_time - now).total_seconds()

        if delay > 0:
            job_name = f"morning_greeting_{next_time.strftime('%Y%m%d_%H%M')}"
            
            existing_jobs = [j for j in get_jobs_from_queue(context.application.job_queue) 
                            if j.name == job_name]
            
            if not existing_jobs:
                context.application.job_queue.run_once(
                    send_morning_greeting,
                    delay,
                    chat_id=chat_id,
                    name=job_name
                )

                logger.info(f"Следующее утреннее приветствие запланировано на {next_time}")
            else:
                logger.info(f"Утреннее приветствие на {next_time} уже запланировано")
        else:
            logger.warning(f"Время утреннего приветствия уже прошло ({next_time}), планируем на следующий день")
            context.application.job_queue.run_once(
                lambda ctx: asyncio.create_task(schedule_next_morning_greeting(ctx)),
                60
            )
            
    except Exception as e:
        logger.error(f"Ошибка планирования утреннего приветствия: {e}")
        context.application.job_queue.run_once(
            lambda ctx: asyncio.create_task(schedule_next_morning_greeting(ctx)),
            300
        )

# ========== ФУНКЦИИ ДЛЯ ИСТОРИЧЕСКИХ СОБЫТИЙ ==========

async def send_daily_event(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправка ежедневного исторического события"""
    try:
        config = BotConfig()
        chat_id = config.chat_id

        if not chat_id:
            logger.error("Chat ID не установлен!")
            await schedule_next_event(context)
            return

        events = get_events_for_today()
        
        if not events:
            logger.warning("Не найдено событий на сегодня")
            await schedule_next_event(context)
            return

        event = events[0]
        message = build_event_message(event)

        await context.bot.send_message(
            chat_id=chat_id,
            text=message,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=False
        )

        logger.info(f"✅ Событие 'В этот день' отправлено: {event['year']} - {event['title']}")
        
        await schedule_next_event(context)
        
    except Exception as e:
        logger.error(f"❌ Ошибка отправки события: {e}")
        await schedule_next_event(context)

@restricted
async def send_event_now(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправить историческое событие немедленно"""
    config = BotConfig()
    chat_id = config.chat_id

    if not chat_id:
        await update.message.reply_text("❌ Сначала установите чат командой /setchat")
        return

    try:
        events = get_events_for_today()
        
        if not events:
            await update.message.reply_text("❌ Не найдено исторических событий на сегодня")
            return

        event = events[0]
        message = build_event_message(event)

        await context.bot.send_message(
            chat_id=chat_id,
            text=message,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=False
        )

        logger.info(f"✅ Событие отправлено по команде: {event['year']} - {event['title']}")
        
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")
        logger.error(f"Ошибка в команде /eventnow: {e}")

def calculate_next_event_time() -> datetime:
    """Рассчитать время следующего события"""
    now = datetime.now(TIMEZONE)
    
    today_target = now.replace(
        hour=EVENT_SEND_TIME["hour"],
        minute=EVENT_SEND_TIME["minute"],
        second=0,
        microsecond=0
    )

    if now < today_target and now.weekday() in EVENT_DAYS:
        return today_target

    for i in range(1, 8):
        next_day = now + timedelta(days=i)
        if next_day.weekday() in EVENT_DAYS:
            return next_day.replace(
                hour=EVENT_SEND_TIME["hour"],
                minute=EVENT_SEND_TIME["minute"],
                second=0,
                microsecond=0
            )
    
    raise ValueError("Не найден подходящий день для отправки событий")

async def schedule_next_event(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Запланировать следующее событие"""
    try:
        next_time = calculate_next_event_time()
        config = BotConfig()
        chat_id = config.chat_id

        if not chat_id:
            logger.warning("Chat ID не установлен, планирование событий отложено")
            context.application.job_queue.run_once(
                lambda ctx: asyncio.create_task(schedule_next_event(ctx)),
                3600
            )
            return

        now = datetime.now(TIMEZONE)
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

                logger.info(f"Следующее событие запланировано на {next_time}")
            else:
                logger.info(f"Событие на {next_time} уже запланировано")
        else:
            logger.warning(f"Время события уже прошло ({next_time}), планируем на следующий день")
            context.application.job_queue.run_once(
                lambda ctx: asyncio.create_task(schedule_next_event(ctx)),
                60
            )
            
    except Exception as e:
        logger.error(f"Ошибка планирования события: {e}")
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

# ========== ФУНКЦИИ ДЛЯ ОТРАСЛЕВОЙ ВСТРЕЧИ ==========

async def send_industry_reminder(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправка напоминания об отраслевой встрече"""
    config = BotConfig()
    chat_id = config.chat_id

    if not chat_id:
        logger.error("Chat ID не установлен!")
        return

    keyboard = [
        [InlineKeyboardButton("Отменить встречу", callback_data="cancel_industry")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    message_text = get_industry_meeting_text()

    try:
        message = await context.bot.send_message(
            chat_id=chat_id,
            text=message_text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=False
        )

        job_name = context.job.name if hasattr(context, 'job') and context.job else f"industry_{datetime.now().timestamp()}"
        config.add_active_reminder(message.message_id, chat_id, job_name)

        logger.info(f"Отправлено напоминание об отраслевой встрече в чат {chat_id}")

    except Exception as e:
        logger.error(f"Ошибка при отправке напоминания об отраслевой встрече: {e}")

def calculate_next_industry_time() -> datetime:
    """Рассчитать время следующей отраслевой встречи"""
    now = datetime.now(TIMEZONE)
    
    # Сегодняшнее время отправки
    today_target = now.replace(
        hour=INDUSTRY_MEETING_TIME["hour"],
        minute=INDUSTRY_MEETING_TIME["minute"],
        second=0,
        microsecond=0
    )

    # Если сегодня вторник и время еще не наступило
    if now < today_target and now.weekday() in INDUSTRY_MEETING_DAY:
        return today_target

    # Ищем следующий вторник
    for i in range(1, 8):
        next_day = now + timedelta(days=i)
        if next_day.weekday() in INDUSTRY_MEETING_DAY:
            return next_day.replace(
                hour=INDUSTRY_MEETING_TIME["hour"],
                minute=INDUSTRY_MEETING_TIME["minute"],
                second=0,
                microsecond=0
            )
    
    raise ValueError("Не найден подходящий день для отраслевой встречи")

async def schedule_next_industry_reminder(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Запланировать следующее напоминание об отраслевой встрече"""
    try:
        next_time = calculate_next_industry_time()
        config = BotConfig()
        chat_id = config.chat_id

        if not chat_id:
            logger.warning("Chat ID не установлен, планирование отраслевых встреч отложено")
            context.application.job_queue.run_once(
                lambda ctx: asyncio.create_task(schedule_next_industry_reminder(ctx)),
                3600
            )
            return

        now = datetime.now(TIMEZONE)
        delay = (next_time - now).total_seconds()

        if delay > 0:
            job_name = f"industry_meeting_{next_time.strftime('%Y%m%d_%H%M')}"
            
            existing_jobs = [j for j in get_jobs_from_queue(context.application.job_queue) 
                            if j.name == job_name]
            
            if not existing_jobs:
                context.application.job_queue.run_once(
                    send_industry_reminder,
                    delay,
                    chat_id=chat_id,
                    name=job_name
                )
                logger.info(f"Напоминание об отраслевой встрече запланировано на {next_time}")
            else:
                logger.info(f"Отраслевая встреча на {next_time} уже запланирована")
        else:
            logger.warning(f"Время отраслевой встречи уже прошло ({next_time}), планируем на следующий вторник")
            context.application.job_queue.run_once(
                lambda ctx: asyncio.create_task(schedule_next_industry_reminder(ctx)),
                60
            )
            
    except Exception as e:
        logger.error(f"Ошибка планирования отраслевой встречи: {e}")
        context.application.job_queue.run_once(
            lambda ctx: asyncio.create_task(schedule_next_industry_reminder(ctx)),
            300
        )

# ========== КОНВЕРСАЦИИ ДЛЯ ОТМЕНЫ ВСТРЕЧ ==========

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

@restricted
async def cancel_industry_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    context.user_data["original_message_id"] = query.message.message_id
    context.user_data["original_chat_id"] = query.message.chat_id
    context.user_data["meeting_type"] = "industry"

    keyboard = [
        [InlineKeyboardButton(option, callback_data=f"industry_reason_{i}")]
        for i, option in enumerate(INDUSTRY_CANCELLATION_OPTIONS)
    ]

    await query.edit_message_text(
        text="📝 Выберите причину отмены отраслевой встречи:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

    return SELECTING_INDUSTRY_REASON

async def select_reason_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    
    try:
        reason_index = int(query.data.split("_")[1])
        reason = CANCELLATION_OPTIONS[reason_index]
        
        context.user_data["selected_reason"] = reason
        context.user_data["reason_index"] = reason_index
        
        final_message = f"❌ @{query.from_user.username or 'Пользователь'} отменил планёрку\n\n📝 <b>Причина:</b> {reason}"
        
        config = BotConfig()
        original_message_id = context.user_data.get("original_message_id")
        
        if original_message_id:
            for job in get_jobs_from_queue(context.application.job_queue):
                if job.name in config.active_reminders:
                    reminder_data = config.active_reminders[job.name]
                    if str(reminder_data.get("message_id")) == str(original_message_id):
                        job.schedule_removal()
                        config.remove_active_reminder(job.name)
                        break
        
        await query.edit_message_text(
            text=final_message,
            parse_mode=ParseMode.HTML
        )
        
        logger.info(f"Планёрка отменена @{query.from_user.username} — {reason}")
        
        context.user_data.clear()
        
    except Exception as e:
        logger.error(f"Ошибка отмены планёрки: {e}")
        await query.message.reply_text("❌ Произошла ошибка")
    
    return ConversationHandler.END

async def select_industry_reason_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    
    try:
        reason_index = int(query.data.split("_")[2])
        reason = INDUSTRY_CANCELLATION_OPTIONS[reason_index]
        
        context.user_data["selected_reason"] = reason
        context.user_data["reason_index"] = reason_index
        
        final_message = f"❌ @{query.from_user.username or 'Пользователь'} отменил отраслевую встречу\n\n📝 <b>Причина:</b> {reason}"
        
        config = BotConfig()
        original_message_id = context.user_data.get("original_message_id")
        
        if original_message_id:
            for job in get_jobs_from_queue(context.application.job_queue):
                if job.name in config.active_reminders:
                    reminder_data = config.active_reminders[job.name]
                    if str(reminder_data.get("message_id")) == str(original_message_id):
                        job.schedule_removal()
                        config.remove_active_reminder(job.name)
                        break
        
        await query.edit_message_text(
            text=final_message,
            parse_mode=ParseMode.HTML
        )
        
        logger.info(f"Отраслевая встреча отменена @{query.from_user.username} — {reason}")
        
        context.user_data.clear()
        
    except Exception as e:
        logger.error(f"Ошибка отмены отраслевой встречи: {e}")
        await query.message.reply_text("❌ Произошла ошибка")
    
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
    """Обработчик /start"""
    await update.message.reply_text(
        "🤖 <b>Бот для планёрок, отраслевых встреч, гороскопов и исторических событий!</b>\n\n"
        f"📅 <b>Утренние гороскопы:</b>\n"
        f"• Пн-Пт в 9:00 по МСК\n"
        f"• 3 разных приветствия\n"
        f"• <i>🎭 + персональный мем к каждому гороскопу!</i>\n"
        f"• <i>🇷🇺 С приоритетом русских мемов</i>\n\n"
        f"📅 <b>Планёрки:</b>\n"
        f"• Пн, Ср, Пт в 9:30 по МСК\n"
        f"• Возможность отмены\n\n"
        f"📅 <b>Отраслевые встречи:</b>\n"
        f"• Вт в 12:00 по МСК\n"
        f"• Обсуждение трендов и инсайтов\n"
        f"• Нетворкинг с коллегами\n\n"
        f"📅 <b>Исторические события:</b>\n"
        f"• Пн-Пт в 10:00 по МСК\n\n"
        f"🔧 <b>Основные команды:</b>\n"
        "/eventnow - историческое событие сейчас\n"
        "/info - информация о боте\n"
        "/setchat - установить чат\n"
        "/testmorning - тест утреннего приветствия\n"
        "/testindustry - тест отраслевой встречи\n"
        "/jobs - список запланированных задач\n\n"
        f"✨ <b>Каждое утро в 9:00 бот присылает приветствие и гороскоп с мемом!</b>\n"
        f"🇷🇺 <i>Русские мемы имеют приоритет при поиске</i>",
        parse_mode=ParseMode.HTML
    )

@restricted
async def set_chat(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    chat_title = update.effective_chat.title or "личный чат"

    config = BotConfig()
    config.chat_id = chat_id

    await update.message.reply_text(
        f"✅ <b>Чат установлен:</b> {chat_title}\n\n"
        f"Теперь бот будет отправлять:\n"
        f"• Утренние гороскопы (9:00, Пн-Пт)\n"
        f"• Планёрки (9:30, Пн/Ср/Пт)\n"
        f"• Отраслевые встречи (12:00, Вт)\n"
        f"• Исторические события (10:00, Пн-Пт)\n\n"
        f"🎭 <i>Каждый гороскоп с персональным мемом!</i>\n"
        f"🇷🇺 <i>Русские мемы имеют приоритет при поиске</i>",
        parse_mode=ParseMode.HTML
    )

    logger.info(f"Установлен чат {chat_title} ({chat_id})")

@restricted
async def show_info(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Информация о боте"""
    config = BotConfig()
    chat_id = config.chat_id

    if chat_id:
        status = f"✅ <b>Чат установлен</b> (ID: {chat_id})"
    else:
        status = "❌ <b>Чат не установлен</b>. Используйте /setchat"

    all_jobs = get_jobs_from_queue(context.application.job_queue)
    
    morning_jobs = len([j for j in all_jobs if j.name and j.name.startswith("morning_greeting_")])
    meeting_jobs = len([j for j in all_jobs if j.name and j.name.startswith("meeting_reminder_")])
    industry_jobs = len([j for j in all_jobs if j.name and j.name.startswith("industry_meeting_")])
    event_jobs = len([j for j in all_jobs if j.name and j.name.startswith("daily_event_")])
    
    now = datetime.now(TIMEZONE)
    weekday = now.weekday()
    day_names = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Саббота", "Воскресенье"]
    current_day = day_names[weekday]
    
    is_morning_day = weekday in MORNING_DAYS
    is_meeting_day = weekday in MEETING_DAYS
    is_industry_day = weekday in INDUSTRY_MEETING_DAY
    
    # Очищаем старые записи о запросах
    config.cleanup_old_requests()
    
    # Проверяем настройку ссылок
    zoom_status = "✅" if ZOOM_LINK != DEFAULT_ZOOM_LINK else "❌"
    industry_zoom_status = "✅" if INDUSTRY_ZOOM_LINK != DEFAULT_ZOOM_LINK else "❌"
    
    await update.message.reply_text(
        f"📊 <b>Информация о боте:</b>\n\n"
        f"{status}\n\n"
        f"⏰ <b>Расписание:</b>\n"
        f"• Гороскопы: 9:00 (Пн-Пт) {'✅ сегодня' if is_morning_day else '❌ не сегодня'}\n"
        f"• Планёрки: 9:30 (Пн/Ср/Пт) {'✅ сегодня' if is_meeting_day else '❌ не сегодня'}\n"
        f"• Отраслевые: 12:00 (Вт) {'✅ сегодня' if is_industry_day else '❌ не сегодня'}\n"
        f"• События: 10:00 (Пн-Пт) {'✅ сегодня' if is_morning_day else '❌ не сегодня'}\n\n"
        f"🔗 <b>Настройка ссылок:</b>\n"
        f"• Планёрки: {zoom_status}\n"
        f"• Отраслевые: {industry_zoom_status}\n\n"
        f"📋 <b>Активные задачи:</b>\n"
        f"• Гороскопы: {morning_jobs}\n"
        f"• Планёрки: {meeting_jobs}\n"
        f"• Отраслевые: {industry_jobs}\n"
        f"• События: {event_jobs}\n\n"
        f"🎭 <b>Мемы:</b>\n"
        f"• API: Meme API\n"
        f"• Приоритет: Русские мемы 🇷🇺\n"
        f"• Резерв: встроенные мемы\n\n"
        f"🔮 <b>Гороскопы:</b>\n"
        f"• Источник: Horoscope API\n"
        f"• Язык: Русский\n"
        f"• Резерв: встроенные предсказания\n\n"
        f"📅 <b>Сегодня:</b> {current_day}, {now.day} {MONTHS_RU[now.month]} {now.year}\n\n"
        f"✨ <b>Гороскопы приходят автоматически в 9:00 каждый будний день!</b>\n"
        f"🎭 <i>Каждый гороскоп сопровождается тематическим мемом</i>\n"
        f"🇷🇺 <i>Приоритет отдается русскоязычным мемам</i>",
        parse_mode=ParseMode.HTML
    )

async def list_jobs(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    jobs = get_jobs_from_queue(context.application.job_queue)
    
    if not jobs:
        await update.message.reply_text("📭 <b>Нет запланированных задач.</b>", parse_mode=ParseMode.HTML)
        return
    
    message = "📋 <b>Запланированные задачи:</b>\n\n"
    
    for job in sorted(jobs, key=lambda j: j.next_t):
        next_time = job.next_t.astimezone(TIMEZONE)
        job_name = job.name or "Без имени"
        
        # Определяем тип задачи для иконки
        if "morning_greeting" in job_name:
            icon = "🌅"
        elif "meeting_reminder" in job_name:
            icon = "🤝"
        elif "industry_meeting" in job_name:
            icon = "🏢"
        elif "daily_event" in job_name:
            icon = "📜"
        else:
            icon = "🔧"
        
        message += f"{icon} {next_time.strftime('%d.%m.%Y %H:%M')} - {job_name[:30]}\n"
    
    await update.message.reply_text(message, parse_mode=ParseMode.HTML)

@restricted
async def test_morning(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Тестовая отправка утреннего приветствия"""
    config = BotConfig()
    if not config.chat_id:
        await update.message.reply_text("❌ Сначала установите чат командой /setchat")
        return

    await update.message.reply_text("⏳ <b>Отправляю тестовое утреннее приветствие и гороскоп...</b>", parse_mode=ParseMode.HTML)
    await send_morning_greeting(context)

@restricted
async def test_industry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Тестовая отправка отраслевой встречи"""
    config = BotConfig()
    if not config.chat_id:
        await update.message.reply_text("❌ Сначала установите чат командой /setchat")
        return

    await update.message.reply_text("⏳ <b>Отправляю тестовое уведомление об отраслевой встрече...</b>", parse_mode=ParseMode.HTML)
    await send_industry_reminder(context)

def calculate_next_reminder() -> datetime:
    """Рассчитать время следующего напоминания о планёрке"""
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
    while days_ahead <= 7:
        next_day = now + timedelta(days=days_ahead)
        if next_day.weekday() in MEETING_DAYS:
            return next_day.replace(
                hour=MEETING_TIME['hour'],
                minute=MEETING_TIME['minute'],
                second=0,
                microsecond=0
            )
        days_ahead += 1
    
    raise ValueError("Не найден подходящий день для планёрки")

async def schedule_next_reminder(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Запланировать следующее напоминание о планёрке"""
    next_time = calculate_next_reminder()
    config = BotConfig()
    chat_id = config.chat_id

    if not chat_id:
        logger.warning("Chat ID не установлен")
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
            logger.info(f"Напоминание о планёрке запланировано на {next_time}")

def main() -> None:
    if not TOKEN:
        logger.error("❌ Токен бота не найден!")
        return
    
    try:
        application = Application.builder().token(TOKEN).build()

        # ConversationHandler для отмены планёрки
        conv_handler = ConversationHandler(
            entry_points=[
                CallbackQueryHandler(cancel_meeting_callback, pattern="^cancel_meeting$"),
                CallbackQueryHandler(cancel_industry_callback, pattern="^cancel_industry$")
            ],
            states={
                SELECTING_REASON: [
                    CallbackQueryHandler(select_reason_callback, pattern="^reason_[0-9]+$"),
                ],
                SELECTING_INDUSTRY_REASON: [
                    CallbackQueryHandler(select_industry_reason_callback, pattern="^industry_reason_[0-9]+$"),
                ],
            },
            fallbacks=[
                CommandHandler("cancel", cancel_conversation),
                CallbackQueryHandler(cancel_conversation, pattern="^cancel_conversation$"),
            ],
        )

        # Основные обработчики (убрали /horoscope)
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("setchat", set_chat))
        application.add_handler(CommandHandler("info", show_info))
        application.add_handler(CommandHandler("eventnow", send_event_now))
        application.add_handler(CommandHandler("testmorning", test_morning))
        application.add_handler(CommandHandler("testindustry", test_industry))
        application.add_handler(CommandHandler("jobs", list_jobs))

        # Убрали обработчики callback для гороскопов
        # application.add_handler(CallbackQueryHandler(handle_horoscope_callback, pattern="^horoscope_"))

        # Добавляем ConversationHandler
        application.add_handler(conv_handler)

        # Запуск планировщиков
        application.job_queue.run_once(
            lambda ctx: asyncio.create_task(schedule_next_morning_greeting(ctx)),
            3
        )
        
        application.job_queue.run_once(
            lambda ctx: asyncio.create_task(schedule_next_reminder(ctx)),
            5
        )
        
        application.job_queue.run_once(
            lambda ctx: asyncio.create_task(schedule_next_industry_reminder(ctx)),
            7
        )
        
        application.job_queue.run_once(
            lambda ctx: asyncio.create_task(schedule_next_event(ctx)),
            9
        )

        # Очистка старых записей при запуске
        config = BotConfig()
        config.cleanup_old_requests()
        
        # Логирование при запуске
        now = datetime.now(TIMEZONE)
        logger.info("🤖 Бот запущен и готов к работе!")
        logger.info(f"✨ Утренние гороскопы: Пн-Пт в 9:00 по МСК")
        logger.info(f"🎭 Каждый гороскоп теперь с персональным мемом!")
        logger.info(f"🇷🇺 Приоритет русских мемов при поиске")
        logger.info(f"🔮 API гороскопов: Horoscope API (рабочее)")
        logger.info(f"🚫 Нет ручных запросов гороскопов - только автоматические!")
        logger.info(f"📅 Планёрки: Пн/Ср/Пт в 9:30 по МСК")
        logger.info(f"🏢 Отраслевые встречи: Вт в 12:00 по МСК")
        logger.info(f"📜 Исторические события: Пн-Пт в 10:00 по МСК")
        logger.info(f"🔗 Ссылка для планёрок: {'Настроена' if ZOOM_LINK != DEFAULT_ZOOM_LINK else 'НЕ настроена'}")
        logger.info(f"🔗 Ссылка для отраслевых: {'Настроена' if INDUSTRY_ZOOM_LINK != DEFAULT_ZOOM_LINK else 'НЕ настроена'}")
        logger.info(f"🗓️ Сегодня: {now.strftime('%d.%m.%Y')}")
        
        application.run_polling(allowed_updates=Update.ALL_TYPES)

    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        raise


if __name__ == "__main__":
    main()
