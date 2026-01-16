import os
import json
import random
import logging
import asyncio
import re
import html
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

import pytz
import aiohttp
import feedparser
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
)

# ========== КОНСТАНТЫ ==========
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DEFAULT_ZOOM_LINK = "https://us04web.zoom.us/j/1234567890?pwd=example"
ZOOM_LINK = os.getenv("ZOOM_MEETING_LINK", DEFAULT_ZOOM_LINK)
INDUSTRY_ZOOM_LINK = os.getenv("INDUSTRY_MEETING_LINK", DEFAULT_ZOOM_LINK)

# Приватные ссылки для помощи
YA_CRM_LINK = os.getenv("YA_CRM_LINK", "https://crm.example.com")
WIKI_LINK = os.getenv("WIKI_LINK", "https://wiki.example.com")
HELPY_BOT_LINK = os.getenv("HELPY_BOT_LINK", "https://t.me/helpy_bot")

# Файлы бота
CONFIG_FILE = "bot_config.json"
HELP_DATA_FILE = "help_data.json"

# Время планёрки (9:15 по Москве)
MEETING_TIME = {"hour": 9, "minute": 15}
TIMEZONE = pytz.timezone("Europe/Moscow")

# Дни недели для планёрки (понедельник=0, среда=2, пятница=4)
MEETING_DAYS = [0, 2, 4]

# Время отправки утреннего приветствия с гороскопом (9:00 по МСК, Пн-Пт)
MORNING_GREETING_TIME = {"hour": 9, "minute": 0}
MORNING_DAYS = [0, 1, 2, 3, 4]  # Пн-Пт

# Время отраслевой встречи (вторник 12:00 по МСК)
INDUSTRY_MEETING_TIME = {"hour": 12, "minute": 0}
INDUSTRY_MEETING_DAY = [1]  # Вторник

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

# Утренние приветствия
MORNING_GREETINGS = [
    "Оу, еще спишь? 😴 Давай посмотрим, что говорят звезды о тебе сегодня! ✨",
    "☀️ Хочешь узнать, что приготовили для тебя звезды? 🔮",
    "👋 Готов(а) узнать свой гороскоп на сегодня? Давай заглянем в будущее! 🌟"
]

# Тексты для планёрок
PLANERKA_TEXTS = [
    "🎯 𝗣ЛАНЁРКА\n\n📋 Повестка дня:\n• Отчёт по задачам\n• Планы на день\n• Вопросы и обсуждения\n\n🕐 Начало: 9:30 по МСК\n📍 Формат: Zoom-конференция\n\n🔗 Подключаемся: {zoom_link} | 👈",
    "🎯 𝗣ЛАНЁРКА\n\n✨ Что будет:\n• Обсуждение текущих задач\n• Координация командной работы\n• Решение операционных вопросов\n\n⏰ Время: 9:30 (МСК)\n💻 Онлайн в Zoom\n\n🔗 Всех ждём! {zoom_link} ← переход",
    "🎯 𝗣ЛАНЁРКА\n\n📊 На сегодня:\n• Статус по проектам\n• Приоритеты дня\n• Синхронизация команд\n\n🕐 Старт: 9:30 по Москве\n🎥 Конференция Zoom\n\n🔗 Присоединяйтесь: {zoom_link} | 👈"
]

# Текст для отраслевой встречи
INDUSTRY_MEETING_TEXTS = [
    "🏢 𝗢ТРАСЛЕВАЯ ВСТРЕЧА\n\n🎯 Что делаем:\n• Обсудим итоги за неделю\n• Новые тренды и инсайты\n• Обмен опытом с коллегами\n• Запланируем мероприятия на следующую\n\n🕐 Начало: 12:00 по МСК\n📍 Формат: Zoom-конференция\n\n🔗 Всех причастных ждём! {zoom_link} | 👈",
    "🏢 𝗢ТРАСЛЕВАЯ ВСТРЕЧА\n\n📊 Сегодня на повестке:\n• Анализ недельных результатов\n• Выявление ключевых трендов\n• Коллективный разбор кейсов\n• Планирование активностей\n\n🕐 Старт: 12:00 (МСК)\n🎥 Онлайн в Zoom\n\n🔗 Присоединяйтесь: {zoom_link} ← переход",
    "🏢 𝗢ТРАСЛЕВАЯ ВСТРЕЧА\n\n✨ На повестке дня:\n• Итоги рабочей недели\n• Прогнозы и инсайты\n•Планы на неделю\n\n⏰ Время: 12:00 по Москве\n💻 Платформа: Zoom\n\n🔗 Подключайтесь: {zoom_link} | 👈"
]

# Опции для отмены встреч
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

# Настройка логирования
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", 
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Состояния для диалогов
ADDING_FILE_NAME, ADDING_FILE_DESCRIPTION = range(2)

# ========== ПАРСЕР ГОРОСКОПОВ ==========

class HoroscopeParser:
    """Парсер гороскопов из различных источников"""
    
    def __init__(self):
        self.sources = {
            'rambler': {
                'base_url': 'https://horoscopes.rambler.ru/rss/{sign}/',
                'type': 'rss'
            },
            'mail_ru': {
                'base_url': 'https://horo.mail.ru/rss/{sign}/today.xml',
                'type': 'rss'
            },
            'yandex': {
                'base_url': 'https://news.yandex.ru/horoscope/index.rss',
                'type': 'rss_multi'
            },
            'astrology_com': {
                'base_url': 'https://www.astrology.com/horoscope/daily/today.html?rss=true',
                'type': 'rss_multi'
            }
        }
        
        # Маппинг для разных источников
        self.sign_mapping = {
            'aries': {'rambler': 'oven', 'mail_ru': 'oven', 'yandex': 'Овен'},
            'taurus': {'rambler': 'telec', 'mail_ru': 'telec', 'yandex': 'Телец'},
            'gemini': {'rambler': 'bliznecy', 'mail_ru': 'bliznecy', 'yandex': 'Близнецы'},
            'cancer': {'rambler': 'rak', 'mail_ru': 'rak', 'yandex': 'Рак'},
            'leo': {'rambler': 'lev', 'mail_ru': 'lev', 'yandex': 'Лев'},
            'virgo': {'rambler': 'deva', 'mail_ru': 'deva', 'yandex': 'Дева'},
            'libra': {'rambler': 'vesy', 'mail_ru': 'vesy', 'yandex': 'Весы'},
            'scorpio': {'rambler': 'skorpion', 'mail_ru': 'skorpion', 'yandex': 'Скорпион'},
            'sagittarius': {'rambler': 'strelec', 'mail_ru': 'strelec', 'yandex': 'Стрелец'},
            'capricorn': {'rambler': 'kozerog', 'mail_ru': 'kozerog', 'yandex': 'Козерог'},
            'aquarius': {'rambler': 'vodoley', 'mail_ru': 'vodoley', 'yandex': 'Водолей'},
            'pisces': {'rambler': 'ryby', 'mail_ru': 'ryby', 'yandex': 'Рыбы'}
        }
    
    async def _clean_text(self, text: str) -> str:
        """Очистка текста от HTML тегов и лишних символов"""
        if not text:
            return ""
        
        # Удаляем HTML теги
        text = re.sub(r'<[^>]+>', '', text)
        
        # Заменяем HTML сущности
        text = html.unescape(text)
        
        # Удаляем лишние пробелы и переносы
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Ограничиваем длину
        if len(text) > 1000:
            text = text[:997] + "..."
        
        return text
    
    async def parse_rambler_horoscope(self, sign: str) -> Optional[Dict]:
        """Парсим гороскоп с Rambler"""
        try:
            sign_key = self.sign_mapping[sign]['rambler']
            url = self.sources['rambler']['base_url'].format(sign=sign_key)
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as response:
                    if response.status != 200:
                        return None
                    
                    content = await response.text()
                    feed = feedparser.parse(content)
                    
                    if not feed.entries:
                        return None
                    
                    entry = feed.entries[0]
                    prediction = await self._clean_text(entry.get('summary', ''))
                    
                    if not prediction:
                        return None
                    
                    return {
                        'sign': ZODIAC_SIGNS[sign]['ru'],
                        'date': datetime.now(TIMEZONE).strftime('%d.%m.%Y'),
                        'prediction': prediction,
                        'source': 'Rambler',
                        'url': entry.get('link', '')
                    }
                    
        except Exception as e:
            logger.error(f"Ошибка парсинга Rambler для {sign}: {e}")
            return None
    
    async def parse_mail_ru_horoscope(self, sign: str) -> Optional[Dict]:
        """Парсим гороскоп с Mail.ru"""
        try:
            sign_key = self.sign_mapping[sign]['mail_ru']
            url = self.sources['mail_ru']['base_url'].format(sign=sign_key)
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as response:
                    if response.status != 200:
                        return None
                    
                    content = await response.text()
                    feed = feedparser.parse(content)
                    
                    if not feed.entries:
                        return None
                    
                    entry = feed.entries[0]
                    prediction = await self._clean_text(entry.get('summary', ''))
                    
                    if not prediction:
                        return None
                    
                    return {
                        'sign': ZODIAC_SIGNS[sign]['ru'],
                        'date': datetime.now(TIMEZONE).strftime('%d.%m.%Y'),
                        'prediction': prediction,
                        'source': 'Mail.ru',
                        'url': entry.get('link', '')
                    }
                    
        except Exception as e:
            logger.error(f"Ошибка парсинга Mail.ru для {sign}: {e}")
            return None
    
    async def parse_yandex_horoscope(self, sign: str) -> Optional[Dict]:
        """Парсим гороскоп с Яндекс"""
        try:
            url = self.sources['yandex']['base_url']
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as response:
                    if response.status != 200:
                        return None
                    
                    content = await response.text()
                    feed = feedparser.parse(content)
                    
                    if not feed.entries:
                        return None
                    
                    sign_ru = self.sign_mapping[sign]['yandex']
                    
                    for entry in feed.entries:
                        title = entry.get('title', '')
                        if sign_ru in title:
                            prediction = await self._clean_text(entry.get('summary', ''))
                            if prediction:
                                return {
                                    'sign': ZODIAC_SIGNS[sign]['ru'],
                                    'date': datetime.now(TIMEZONE).strftime('%d.%m.%Y'),
                                    'prediction': prediction,
                                    'source': 'Яндекс',
                                    'url': entry.get('link', '')
                                }
                    
                    return None
                    
        except Exception as e:
            logger.error(f"Ошибка парсинга Яндекс для {sign}: {e}")
            return None
    
    async def get_horoscope(self, sign: str) -> Optional[Dict]:
        """Получить гороскоп для указанного знака"""
        try:
            # Пробуем разные источники в порядке приоритета
            sources = [
                self.parse_rambler_horoscope,
                self.parse_mail_ru_horoscope,
                self.parse_yandex_horoscope
            ]
            
            for source_func in sources:
                try:
                    horoscope = await source_func(sign)
                    if horoscope:
                        logger.info(f"Успешно получен гороскоп для {sign} из {horoscope['source']}")
                        return horoscope
                except Exception as e:
                    logger.debug(f"Источник не сработал: {e}")
                    continue
            
            logger.warning(f"Не удалось получить гороскоп для {sign} из всех источников")
            return None
            
        except Exception as e:
            logger.error(f"Ошибка получения гороскопа для {sign}: {e}")
            return None

# Глобальный парсер
horoscope_parser = HoroscopeParser()

# ========== КЛАСС КОНФИГА ==========

class BotConfig:
    """Класс для управления конфигурацией бота"""
    
    def __init__(self):
        self.config_file = CONFIG_FILE
        self.help_data_file = HELP_DATA_FILE
        self.data = self._load_config()
        self.help_data = self._load_help_data()
    
    def _load_config(self) -> Dict[str, Any]:
        """Загрузить основные данные"""
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if "allowed_users" not in data:
                        data["allowed_users"] = ["Stiff_OWi", "gshabanov"]
                    if "active_reminders" not in data:
                        data["active_reminders"] = {}
                    if "pending_files" not in data:
                        data["pending_files"] = {}
                    if "admins" not in data:
                        data["admins"] = ["Stiff_OWi", "gshabanov"]
                    if "chat_id" not in data:
                        data["chat_id"] = None
                    if "user_zodiacs" not in data:
                        data["user_zodiacs"] = {}
                    return data
            except Exception as e:
                logger.error(f"Ошибка загрузки конфига: {e}")
        return {
            "chat_id": None,
            "allowed_users": ["Stiff_OWi", "gshabanov"],
            "admins": ["Stiff_OWi", "gshabanov"],
            "active_reminders": {},
            "pending_files": {},
            "user_zodiacs": {}
        }
    
    def _load_help_data(self) -> Dict[str, Any]:
        """Загрузить данные помощи"""
        default_data = {
            "files": {},  # Пустой словарь - файлы будут добавляться через бота
            "links": {
                "ya_crm": {
                    "name": "YA CRM",
                    "url": YA_CRM_LINK,
                    "description": "Корпоративная CRM система"
                },
                "wiki": {
                    "name": "WIKI Отрасли",
                    "url": WIKI_LINK,
                    "description": "Презентации и спичи по отраслям"
                },
                "helpy_bot": {
                    "name": "Бот Helpy",
                    "url": HELPY_BOT_LINK,
                    "description": "Помощник по внутренним вопросам"
                }
            },
            "categories": {
                "documents": {
                    "name": "📄 Документы",
                    "description": "Корпоративные документы и спичи"
                },
                "links": {
                    "name": "🔗 Полезные ссылки",
                    "description": "Важные внутренние ресурсы"
                }
            }
        }
        
        if os.path.exists(self.help_data_file):
            try:
                with open(self.help_data_file, 'r', encoding='utf-8') as f:
                    loaded_data = json.load(f)
                    
                    # Обновляем ссылки из переменных окружения
                    if "links" in loaded_data:
                        if "ya_crm" in loaded_data["links"]:
                            loaded_data["links"]["ya_crm"]["url"] = YA_CRM_LINK
                        if "wiki" in loaded_data["links"]:
                            loaded_data["links"]["wiki"]["url"] = WIKI_LINK
                        if "helpy_bot" in loaded_data["links"]:
                            loaded_data["links"]["helpy_bot"]["url"] = HELPY_BOT_LINK
                    
                    return loaded_data
            except Exception as e:
                logger.error(f"Ошибка загрузки данных помощи: {e}")
        
        return default_data
    
    def save(self) -> None:
        """Сохранить основные данные"""
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Ошибка сохранения конфига: {e}")
    
    def save_help_data(self) -> None:
        """Сохранить данные помощи"""
        try:
            with open(self.help_data_file, 'w', encoding='utf-8') as f:
                json.dump(self.help_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Ошибка сохранения данных помощи: {e}")
    
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
    
    @property
    def admins(self) -> List[str]:
        return self.data.get("admins", [])
    
    def is_admin(self, username: str) -> bool:
        return username in self.admins
    
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
    def user_zodiacs(self) -> Dict[str, str]:
        """Словарь user_id -> знак зодиака"""
        return self.data.get("user_zodiacs", {})
    
    def set_user_zodiac(self, user_id: int, zodiac: str) -> None:
        self.data["user_zodiacs"][str(user_id)] = zodiac
        self.save()
    
    def get_user_zodiac(self, user_id: int) -> Optional[str]:
        return self.data.get("user_zodiacs", {}).get(str(user_id))
    
    def get_pending_file(self, user_id: int) -> Optional[Dict]:
        return self.data["pending_files"].get(str(user_id))
    
    def start_adding_file(self, user_id: int) -> None:
        self.data["pending_files"][str(user_id)] = {"state": "waiting_file"}
        self.save()
    
    def save_file_data(self, user_id: int, file_id: str, file_name: str) -> None:
        if str(user_id) in self.data["pending_files"]:
            self.data["pending_files"][str(user_id)] = {
                "state": "waiting_name",
                "file_id": file_id,
                "file_name": file_name
            }
            self.save()
    
    def save_file_name(self, user_id: int, display_name: str) -> None:
        if str(user_id) in self.data["pending_files"]:
            self.data["pending_files"][str(user_id)]["state"] = "waiting_description"
            self.data["pending_files"][str(user_id)]["display_name"] = display_name
            self.save()
    
    def add_file(self, user_id: int, file_id: str, file_name: str, description: str) -> bool:
        """Добавить новый файл"""
        try:
            # Очищаем временные данные
            if str(user_id) in self.data["pending_files"]:
                del self.data["pending_files"][str(user_id)]
                self.save()
            
            # Создаем ключ для файла
            file_key = file_name.lower().replace(' ', '_').replace('(', '').replace(')', '').replace('/', '_').replace('\\', '_')
            
            # Если ключ уже существует, добавляем номер
            original_key = file_key
            counter = 1
            while file_key in self.help_data["files"]:
                file_key = f"{original_key}_{counter}"
                counter += 1
            
            self.help_data["files"][file_key] = {
                "name": file_name,
                "description": description,
                "file_id": file_id,
                "category": "documents",
                "added_date": datetime.now().isoformat()
            }
            
            self.save_help_data()
            logger.info(f"Файл добавлен: {file_name} (ID: {file_key})")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка добавления файла: {e}")
            return False
    
    def delete_file(self, file_id: str) -> bool:
        """Удалить файл"""
        if file_id in self.help_data["files"]:
            deleted_name = self.help_data["files"][file_id]["name"]
            del self.help_data["files"][file_id]
            self.save_help_data()
            logger.info(f"Файл удален: {deleted_name} (ID: {file_id})")
            return True
        return False

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

# ========== ФУНКЦИИ ДЛЯ ГОРОСКОПОВ ==========

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

def build_horoscope_message(horoscope: Dict) -> str:
    """Создаем красивое сообщение с гороскопом"""
    horoscope_text = (
        f"✨ <b>ГОРОСКОП НА СЕГОДНЯ</b> ✨\n\n"
        f"<b>{horoscope['sign']}</b>\n"
        f"📅 {horoscope['date']}\n\n"
        f"🔮 <b>Предсказание:</b>\n"
        f"{horoscope['prediction']}\n\n"
        f"📌 <i>Источник: {horoscope['source']}</i>\n\n"
        f"<i>Хорошего дня! 🌟</i>"
    )
    
    return horoscope_text

async def send_horoscope(chat_id: int, horoscope: Dict, context: ContextTypes.DEFAULT_TYPE, 
                        user_id: Optional[int] = None) -> None:
    """Отправляет гороскоп"""
    try:
        # Строим текстовое сообщение
        message_text = build_horoscope_message(horoscope)
        
        # Отправляем сообщение
        await context.bot.send_message(
            chat_id=chat_id,
            text=message_text,
            parse_mode=ParseMode.HTML
        )
        
        if user_id:
            logger.info(f"✅ Гороскоп отправлен пользователю {user_id} ({horoscope['sign']})")
        else:
            logger.info(f"✅ Гороскоп отправлен в чат {chat_id} ({horoscope['sign']})")
        
    except Exception as e:
        logger.error(f"Ошибка отправки гороскопа: {e}")
        fallback_text = build_horoscope_message(horoscope)
        await context.bot.send_message(
            chat_id=chat_id,
            text=fallback_text,
            parse_mode=ParseMode.HTML
        )

async def get_backup_horoscope(sign: str) -> Dict:
    """Резервный гороскоп, если парсинг не работает"""
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
    
    return {
        'sign': ZODIAC_SIGNS[sign]['ru'],
        'date': datetime.now(TIMEZONE).strftime('%d.%m.%Y'),
        'prediction': random.choice(predictions),
        'source': 'Резервный гороскоп',
        'url': ''
    }

# ========== УТРЕННЯЯ РАССЫЛКА ГОРОСКОПОВ ==========

async def send_morning_horoscopes(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправка утренних гороскопов для всех пользователей"""
    try:
        config = BotConfig()
        chat_id = config.chat_id

        if not chat_id:
            logger.error("Chat ID не установлен для утренней рассылки!")
            await schedule_next_morning(context)
            return
        
        # Отправляем утреннее приветствие
        greeting = random.choice(MORNING_GREETINGS)
        await context.bot.send_message(
            chat_id=chat_id,
            text=greeting,
            parse_mode=ParseMode.HTML
        )

        logger.info(f"✅ Утреннее приветствие отправлено в чат {chat_id}")
        
        # Ждем 1 секунду перед отправкой гороскопов
        await asyncio.sleep(1)
        
        # Получаем всех пользователей с их знаками зодиака
        user_zodiacs = config.user_zodiacs
        
        if not user_zodiacs:
            logger.warning("Нет пользователей с выбранными знаками зодиака")
            await context.bot.send_message(
                chat_id=chat_id,
                text="📝 <i>Напоминание: выберите свой знак зодиака с помощью /start, чтобы получать персональные гороскопы!</i>",
                parse_mode=ParseMode.HTML
            )
        else:
            logger.info(f"Отправляю гороскопы для {len(user_zodiacs)} пользователей")
            
            # Для каждого пользователя отправляем его персональный гороскоп
            for user_id_str, sign_key in user_zodiacs.items():
                try:
                    # Получаем гороскоп через парсинг
                    horoscope = await horoscope_parser.get_horoscope(sign_key)
                    
                    # Если парсинг не вернул данные, используем резервный
                    if not horoscope:
                        horoscope = await get_backup_horoscope(sign_key)
                        logger.warning(f"Используется резервный гороскоп для {sign_key}")
                    
                    # Отправляем гороскоп в групповой чат
                    await send_horoscope(
                        chat_id=chat_id,
                        horoscope=horoscope,
                        context=context,
                        user_id=int(user_id_str) if user_id_str.isdigit() else None
                    )
                    
                    # Небольшая задержка между отправками
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    logger.error(f"Ошибка отправки гороскопа для пользователя {user_id_str}: {e}")
                    continue
        
        logger.info(f"✅ Утренние гороскопы отправлены в чат {chat_id}")
        
        # Планируем следующую рассылку
        await schedule_next_morning(context)
        
    except Exception as e:
        logger.error(f"❌ Ошибка отправки утренних гороскопов: {e}")
        await schedule_next_morning(context)

def calculate_next_morning_time() -> datetime:
    """Рассчитать время следующей утренней рассылки"""
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
    
    raise ValueError("Не найден подходящий день для утренней рассылки")

async def schedule_next_morning(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Запланировать следующую утреннюю рассылку"""
    try:
        next_time = calculate_next_morning_time()
        config = BotConfig()
        chat_id = config.chat_id

        if not chat_id:
            logger.warning("Chat ID не установлен, планирование утренних рассылок отложено")
            context.application.job_queue.run_once(
                lambda ctx: asyncio.create_task(schedule_next_morning(ctx)),
                3600
            )
            return

        now = datetime.now(TIMEZONE)
        delay = (next_time - now).total_seconds()

        if delay > 0:
            job_name = f"morning_horoscopes_{next_time.strftime('%Y%m%d_%H%M')}"
            
            existing_jobs = [j for j in get_jobs_from_queue(context.application.job_queue) 
                            if j.name == job_name]
            
            if not existing_jobs:
                context.application.job_queue.run_once(
                    send_morning_horoscopes,
                    delay,
                    chat_id=chat_id,
                    name=job_name
                )

                logger.info(f"Следующая утренняя рассылка запланирована на {next_time}")
            else:
                logger.info(f"Утренняя рассылка на {next_time} уже запланирована")
        else:
            logger.warning(f"Время утренней рассылки уже прошло ({next_time}), планируем на следующий день")
            context.application.job_queue.run_once(
                lambda ctx: asyncio.create_task(schedule_next_morning(ctx)),
                60
            )
            
    except Exception as e:
        logger.error(f"Ошибка планирования утренней рассылки: {e}")
        context.application.job_queue.run_once(
            lambda ctx: asyncio.create_task(schedule_next_morning(ctx)),
            300
        )

# ========== ФУНКЦИИ ДЛЯ НАПОМИНАНИЙ ==========

def get_planerka_text() -> str:
    """Получить текст для планёрки"""
    zoom_link = ZOOM_LINK
    
    if zoom_link == DEFAULT_ZOOM_LINK:
        zoom_link_formatted = f'<a href="{zoom_link}">[НЕ НАСТРОЕНА - настройте ZOOM_MEETING_LINK]</a>'
    else:
        zoom_link_formatted = f'<a href="{zoom_link}">Присоединиться к Zoom</a>'
    
    text = random.choice(PLANERKA_TEXTS)
    return text.format(zoom_link=zoom_link_formatted)

def get_industry_meeting_text() -> str:
    """Получить текст для отраслевой встречи"""
    zoom_link = INDUSTRY_ZOOM_LINK
    
    if zoom_link == DEFAULT_ZOOM_LINK:
        zoom_link_formatted = f'<a href="{zoom_link}">[НЕ НАСТРОЕНА - настройте INDUSTRY_MEETING_LINK]</a>'
    else:
        zoom_link_formatted = f'<a href="{zoom_link}">Присоединиться к Zoom</a>'
    
    text = random.choice(INDUSTRY_MEETING_TEXTS)
    return text.format(zoom_link=zoom_link_formatted)

def calculate_next_planerka_time() -> datetime:
    """Рассчитать время следующей планёрки"""
    now = datetime.now(TIMEZONE)
    
    # Сегодняшнее время планёрки
    today_target = now.replace(
        hour=MEETING_TIME["hour"],
        minute=MEETING_TIME["minute"],
        second=0,
        microsecond=0
    )

    # Если сегодня день планёрки и время еще не наступило
    if now < today_target and now.weekday() in MEETING_DAYS:
        return today_target

    # Ищем следующий день планёрки
    for i in range(1, 8):
        next_day = now + timedelta(days=i)
        if next_day.weekday() in MEETING_DAYS:
            return next_day.replace(
                hour=MEETING_TIME["hour"],
                minute=MEETING_TIME["minute"],
                second=0,
                microsecond=0
            )
    
    raise ValueError("Не найден подходящий день для планёрки")

def calculate_next_industry_meeting_time() -> datetime:
    """Рассчитать время следующей отраслевой встречи"""
    now = datetime.now(TIMEZONE)
    
    # Сегодняшнее время встречи
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

async def send_planerka_reminder(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправка напоминания о планёрке"""
    try:
        config = BotConfig()
        chat_id = config.chat_id

        if not chat_id:
            logger.error("Chat ID не установлен для отправки напоминания о планёрке!")
            await schedule_next_planerka(context)
            return

        text = get_planerka_text()
        
        keyboard = [
            [
                InlineKeyboardButton("❌ Отменить планёрку", callback_data="planerka_cancel")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await context.bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML
        )
        
        logger.info(f"✅ Напоминание о планёрке отправлено в чат {chat_id}")
        
        # Планируем следующую планёрку
        await schedule_next_planerka(context)
        
    except Exception as e:
        logger.error(f"❌ Ошибка отправки напоминания о планёрке: {e}")
        await schedule_next_planerka(context)

async def schedule_next_planerka(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Запланировать следующую планёрку"""
    try:
        next_time = calculate_next_planerka_time()
        config = BotConfig()
        chat_id = config.chat_id

        if not chat_id:
            logger.warning("Chat ID не установлен, планирование планёрок отложено")
            context.application.job_queue.run_once(
                lambda ctx: asyncio.create_task(schedule_next_planerka(ctx)),
                3600
            )
            return

        now = datetime.now(TIMEZONE)
        delay = (next_time - now).total_seconds()

        if delay > 0:
            job_name = f"planerka_reminder_{next_time.strftime('%Y%m%d_%H%M')}"
            
            # Проверяем, нет ли уже такой задачи
            existing_jobs = [j for j in get_jobs_from_queue(context.application.job_queue) 
                            if j.name == job_name]
            
            if not existing_jobs:
                context.application.job_queue.run_once(
                    send_planerka_reminder,
                    delay,
                    chat_id=chat_id,
                    name=job_name
                )

                logger.info(f"Следующая планёрка запланирована на {next_time}")
            else:
                logger.info(f"Планёрка на {next_time} уже запланирована")
        else:
            logger.warning(f"Время планёрки уже прошло ({next_time}), планируем на следующий день")
            context.application.job_queue.run_once(
                lambda ctx: asyncio.create_task(schedule_next_planerka(ctx)),
                60
            )
            
    except Exception as e:
        logger.error(f"Ошибка планирования планёрки: {e}")
        context.application.job_queue.run_once(
            lambda ctx: asyncio.create_task(schedule_next_planerka(ctx)),
            300
        )

async def send_industry_meeting_reminder(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправка напоминания об отраслевой встрече"""
    try:
        config = BotConfig()
        chat_id = config.chat_id

        if not chat_id:
            logger.error("Chat ID не установлен для отправки напоминания об отраслевой встрече!")
            await schedule_next_industry_meeting(context)
            return

        text = get_industry_meeting_text()
        
        keyboard = [
            [
                InlineKeyboardButton("❌ Отменить встречу", callback_data="industry_cancel")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await context.bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML
        )
        
        logger.info(f"✅ Напоминание об отраслевой встрече отправлено в чат {chat_id}")
        
        # Планируем следующую встречу
        await schedule_next_industry_meeting(context)
        
    except Exception as e:
        logger.error(f"❌ Ошибка отправки напоминания об отраслевой встрече: {e}")
        await schedule_next_industry_meeting(context)

async def schedule_next_industry_meeting(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Запланировать следующую отраслевую встречу"""
    try:
        next_time = calculate_next_industry_meeting_time()
        config = BotConfig()
        chat_id = config.chat_id

        if not chat_id:
            logger.warning("Chat ID не установлен, планирование отраслевых встреч отложено")
            context.application.job_queue.run_once(
                lambda ctx: asyncio.create_task(schedule_next_industry_meeting(ctx)),
                3600
            )
            return

        now = datetime.now(TIMEZONE)
        delay = (next_time - now).total_seconds()

        if delay > 0:
            job_name = f"industry_meeting_{next_time.strftime('%Y%m%d_%H%M')}"
            
            # Проверяем, нет ли уже такой задачи
            existing_jobs = [j for j in get_jobs_from_queue(context.application.job_queue) 
                            if j.name == job_name]
            
            if not existing_jobs:
                context.application.job_queue.run_once(
                    send_industry_meeting_reminder,
                    delay,
                    chat_id=chat_id,
                    name=job_name
                )

                logger.info(f"Следующая отраслевая встреча запланирована на {next_time}")
            else:
                logger.info(f"Отраслевая встреча на {next_time} уже запланирована")
        else:
            logger.warning(f"Время отраслевой встречи уже прошло ({next_time}), планируем на следующий вторник")
            context.application.job_queue.run_once(
                lambda ctx: asyncio.create_task(schedule_next_industry_meeting(ctx)),
                60
            )
            
    except Exception as e:
        logger.error(f"Ошибка планирования отраслевой встречи: {e}")
        context.application.job_queue.run_once(
            lambda ctx: asyncio.create_task(schedule_next_industry_meeting(ctx)),
            300
        )

# ========== ФУНКЦИИ ДЛЯ СИСТЕМЫ ПОМОЩИ ==========

def get_help_main_menu() -> InlineKeyboardMarkup:
    """Получить главное меню помощи"""
    keyboard = []
    
    for cat_id, cat_data in config.help_data["categories"].items():
        keyboard.append([
            InlineKeyboardButton(cat_data["name"], callback_data=f"help_cat_{cat_id}")
        ])
    
    # Кнопка настроек (только для админов)
    keyboard.append([
        InlineKeyboardButton("⚙️ Настройки", callback_data="help_settings")
    ])
    
    return InlineKeyboardMarkup(keyboard)

def get_help_category_menu(category_id: str) -> InlineKeyboardMarkup:
    """Получить меню категории помощи"""
    keyboard = []
    
    if category_id == "documents":
        # Показываем файлы (если они есть)
        if config.help_data["files"]:
            for file_id, file_data in config.help_data["files"].items():
                if file_data["category"] == category_id:
                    keyboard.append([
                        InlineKeyboardButton(
                            f"📋 {file_data['name']}",
                            callback_data=f"help_file_{file_id}"
                        )
                    ])
        else:
            # Если файлов нет, показываем сообщение
            keyboard.append([
                InlineKeyboardButton("📭 Нет файлов", callback_data="no_files")
            ])
    
    elif category_id == "links":
        # Показываем ссылки
        for link_id, link_data in config.help_data["links"].items():
            keyboard.append([
                InlineKeyboardButton(
                    f"🔗 {link_data['name']}",
                    callback_data=f"help_link_{link_id}"
                )
            ])
    
    # Кнопка назад
    keyboard.append([
        InlineKeyboardButton("🔙 Назад", callback_data="help_back")
    ])
    
    return InlineKeyboardMarkup(keyboard)

def get_help_settings_menu() -> InlineKeyboardMarkup:
    """Получить меню настроек помощи (для админов)"""
    keyboard = [
        [InlineKeyboardButton("➕ Добавить файл", callback_data="help_add_file")],
        [InlineKeyboardButton("🗑️ Удалить файл", callback_data="help_delete_file")],
        [InlineKeyboardButton("📊 Статистика", callback_data="help_stats")],
        [InlineKeyboardButton("🔙 Главное меню", callback_data="help_back")]
    ]
    
    return InlineKeyboardMarkup(keyboard)

def get_help_delete_files_menu() -> InlineKeyboardMarkup:
    """Получить меню для удаления файлов"""
    keyboard = []
    
    if not config.help_data["files"]:
        keyboard.append([
            InlineKeyboardButton("📭 Нет файлов для удаления", callback_data="help_settings")
        ])
    else:
        for file_id, file_data in config.help_data["files"].items():
            keyboard.append([
                InlineKeyboardButton(
                    f"🗑️ {file_data['name']}",
                    callback_data=f"help_delete_{file_id}"
                )
            ])
    
    keyboard.append([
        InlineKeyboardButton("🔙 Назад", callback_data="help_settings")
    ])
    
    return InlineKeyboardMarkup(keyboard)

# ========== КОМАНДЫ ==========

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик /start - выбор знака зодиака"""
    await update.message.reply_text(
        "🔮 <b>Выберите ваш знак зодиака:</b>\n\n"
        "Бот запомнит ваш выбор и будет отправлять персональный гороскоп каждое утро в 9:00!",
        reply_markup=create_zodiac_keyboard(),
        parse_mode=ParseMode.HTML
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик команды /help"""
    config = BotConfig()
    files_count = len(config.help_data["files"])
    
    text = (
        "📚 *ЦЕНТР ПОМОЩИ СОТРУДНИКАМ*\n\n"
        "Здесь вы найдете все необходимые материалы для работы:\n\n"
        f"• 📄 *Документы* – корпоративные спичи и шаблоны ({files_count} файлов)\n"
        "• 🔗 *Полезные ссылки* – внутренние ресурсы и системы\n\n"
        "Выберите категорию:"
    )
    
    await update.message.reply_text(
        text=text,
        reply_markup=get_help_main_menu(),
        parse_mode=ParseMode.MARKDOWN
    )

async def set_chat(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Установка группового чата для рассылки"""
    config = BotConfig()
    config.chat_id = update.effective_chat.id
    
    await update.message.reply_text(
        f"✅ <b>Чат установлен!</b>\n\n"
        f"Теперь бот будет отправлять:\n"
        f"• Утренние гороскопы в 9:00 (Пн-Пт)\n"
        f"• Планёрки: Пн, Ср, Пт в 9:15 по МСК\n"
        f"• Отраслевые встречи: Вт в 12:00 по МСК\n\n"
        f"<i>Попросите всех участников выбрать знак зодиака с помощью /start</i>",
        parse_mode=ParseMode.HTML
    )
    
    logger.info(f"Установлен чат {update.effective_chat.id}")
    
    # Запускаем планировщики
    await schedule_next_morning(context)
    await schedule_next_planerka(context)
    await schedule_next_industry_meeting(context)

async def test_morning(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Тестовая отправка утренней рассылки"""
    config = BotConfig()
    if not config.chat_id:
        await update.message.reply_text("❌ Сначала установите чат командой /setchat")
        return
    
    await update.message.reply_text("⏳ <b>Отправляю тестовую утреннюю рассылку...</b>", parse_mode=ParseMode.HTML)
    await send_morning_horoscopes(context)

async def test_planerka_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Тестовая отправка напоминания о планёрке"""
    config = BotConfig()
    chat_id = config.chat_id
    if not chat_id:
        await update.message.reply_text("❌ Сначала установите чат командой /setchat")
        return
    
    await update.message.reply_text("⏳ Отправляю тестовое напоминание о планёрке...")
    await send_planerka_reminder(context)

async def test_industry_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Тестовая отправка напоминания об отраслевой встрече"""
    config = BotConfig()
    chat_id = config.chat_id
    if not chat_id:
        await update.message.reply_text("❌ Сначала установите чат командой /setchat")
        return
    
    await update.message.reply_text("⏳ Отправляю тестовое напоминание об отраслевой встрече...")
    await send_industry_meeting_reminder(context)

# ========== ОБРАБОТЧИКИ КНОПОК ==========

async def handle_horoscope_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработка выбора знака зодиака (для персонального выбора)"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    config = BotConfig()
    
    # Показываем "загрузку"
    await query.edit_message_text(
        text="🔮 <i>Спрашиваю у звезд...</i>",
        parse_mode=ParseMode.HTML
    )
    
    try:
        # Получаем выбранный знак из callback_data
        sign_key = query.data.replace("horoscope_", "")
        
        if sign_key not in ZODIAC_SIGNS:
            await query.edit_message_text(
                text="❌ Ошибка: неверный знак зодиака",
                parse_mode=ParseMode.HTML
            )
            return
        
        # Получаем гороскоп через парсинг
        horoscope = await horoscope_parser.get_horoscope(sign_key)
        
        # Если парсинг не вернул данные, используем резервный
        if not horoscope:
            horoscope = await get_backup_horoscope(sign_key)
            logger.warning(f"API не вернуло гороскоп, используется резервный для {sign_key}")
        
        # Сохраняем выбор пользователя
        config.set_user_zodiac(user_id, sign_key)
        
        # Отправляем гороскоп
        await send_horoscope(
            chat_id=user_id,
            horoscope=horoscope,
            context=context,
            user_id=user_id
        )
        
        # Удаляем сообщение с выбором знака
        try:
            await query.delete_message()
        except:
            pass
        
    except Exception as e:
        logger.error(f"Ошибка обработки гороскопа: {e}")
        await query.edit_message_text(
            text="❌ Произошла ошибка при получении гороскопа. Попробуйте позже.",
            parse_mode=ParseMode.HTML
        )

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик всех callback-кнопок"""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    user = query.from_user.username
    user_id = query.from_user.id
    config = BotConfig()
    
    # ========== ОБРАБОТКА НАПОМИНАНИЙ ==========
    
    if data == "planerka_cancel":
        keyboard = [
            [InlineKeyboardButton(reason, callback_data=f"cancel_planerka_{i}")]
            for i, reason in enumerate(CANCELLATION_OPTIONS)
        ]
        keyboard.append([InlineKeyboardButton("↩️ Назад", callback_data="cancel_back_planerka")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            text="<b>Выберите причину отмены планёрки:</b>",
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML
        )
    
    elif data == "industry_cancel":
        keyboard = [
            [InlineKeyboardButton(reason, callback_data=f"cancel_industry_{i}")]
            for i, reason in enumerate(INDUSTRY_CANCELLATION_OPTIONS)
        ]
        keyboard.append([InlineKeyboardButton("↩️ Назад", callback_data="cancel_back_industry")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            text="<b>Выберите причину отмены отраслевой встречи:</b>",
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML
        )
    
    # Обработка причин отмены планёрки
    elif data.startswith("cancel_planerka_"):
        try:
            reason_index = int(data.replace("cancel_planerka_", ""))
            reason = CANCELLATION_OPTIONS[reason_index]
            
            await query.edit_message_text(
                text=f"<b>Планёрка отменена!</b>\n\nПричина: {reason}\n\n"
                     f"Следующая планёрка запланирована на {calculate_next_planerka_time().strftime('%A, %d %B в %H:%M')}",
                parse_mode=ParseMode.HTML
            )
            logger.info(f"Планёрка отменена пользователем {user}. Причина: {reason}")
        except (ValueError, IndexError) as e:
            logger.error(f"Ошибка обработки отмены планёрки: {e}")
            await query.answer("Ошибка при обработке запроса", show_alert=True)
    
    # Обработка причин отмены отраслевой встречи
    elif data.startswith("cancel_industry_"):
        try:
            reason_index = int(data.replace("cancel_industry_", ""))
            reason = INDUSTRY_CANCELLATION_OPTIONS[reason_index]
            
            await query.edit_message_text(
                text=f"<b>Отраслевая встреча отменена!</b>\n\nПричина: {reason}\n\n"
                     f"Следующая встреча запланирована на {calculate_next_industry_meeting_time().strftime('%A, %d %B в %H:%M')}",
                parse_mode=ParseMode.HTML
            )
            logger.info(f"Отраслевая встреча отменена пользователем {user}. Причина: {reason}")
        except (ValueError, IndexError) as e:
            logger.error(f"Ошибка обработки отмены встречи: {e}")
            await query.answer("Ошибка при обработке запроса", show_alert=True)
    
    # Кнопка "Назад" для отмены планёрки
    elif data == "cancel_back_planerka":
        await query.edit_message_text(
            text=query.message.text,  # Восстанавливаем оригинальное сообщение
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Отменить планёрку", callback_data="planerka_cancel")]
            ]),
            parse_mode=ParseMode.HTML
        )
    
    # Кнопка "Назад" для отмены отраслевой встречи
    elif data == "cancel_back_industry":
        await query.edit_message_text(
            text=query.message.text,  # Восстанавливаем оригинальное сообщение
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Отменить встречу", callback_data="industry_cancel")]
            ]),
            parse_mode=ParseMode.HTML
        )
    
    # ========== ОБРАБОТКА СИСТЕМЫ ПОМОЩИ ==========
    
    elif data == "help_back":
        files_count = len(config.help_data["files"])
        text = (
            "📚 *ЦЕНТР ПОМОЩИ СОТРУДНИКАМ*\n\n"
            "Здесь вы найдете все необходимые материалы для работы:\n\n"
            f"• 📄 *Документы* – корпоративные спичи и шаблоны ({files_count} файлов)\n"
            "• 🔗 *Полезные ссылки* – внутренние ресурсы и системы\n\n"
            "Выберите категорию:"
        )
        await query.edit_message_text(
            text=text,
            reply_markup=get_help_main_menu(),
            parse_mode=ParseMode.MARKDOWN
        )
    
    # Категории помощи
    elif data.startswith("help_cat_"):
        category_id = data.replace("help_cat_", "")
        category = config.help_data["categories"][category_id]
        
        text = f"*{category['name']}*\n\n{category['description']}\n\nВыберите нужный материал:"
        
        await query.edit_message_text(
            text=text,
            reply_markup=get_help_category_menu(category_id),
            parse_mode=ParseMode.MARKDOWN
        )
    
    # Файлы помощи
    elif data.startswith("help_file_"):
        file_id = data.replace("help_file_", "")
        file_data = config.help_data["files"].get(file_id)
        
        if file_data and file_data["file_id"]:
            try:
                # Отправляем файл
                await context.bot.send_document(
                    chat_id=query.message.chat_id,
                    document=file_data["file_id"],
                    caption=f"📁 *{file_data['name']}*\n\n{file_data['description']}",
                    parse_mode=ParseMode.MARKDOWN
                )
                
                # Показываем кнопку "Назад"
                keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=f"help_cat_{file_data['category']}")]]
                await query.edit_message_reply_markup(
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                
            except Exception as e:
                logger.error(f"Ошибка отправки файла: {e}")
                await query.edit_message_text(
                    text="❌ Ошибка при отправке файла. Возможно, файл был перезагружен.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="help_back")]])
                )
        else:
            await query.edit_message_text(
                text="❌ Файл не загружен. Обратитесь к администратору.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="help_back")]])
            )
    
    # Сообщение "Нет файлов"
    elif data == "no_files":
        await query.answer("📭 Пока нет файлов в этой категории", show_alert=True)
    
    # Ссылки помощи
    elif data.startswith("help_link_"):
        link_id = data.replace("help_link_", "")
        link_data = config.help_data["links"].get(link_id)
        
        if link_data:
            text = (
                f"🔗 *{link_data['name']}*\n\n"
                f"{link_data['description']}\n\n"
                f"*Ссылка:* {link_data['url']}"
            )
            
            keyboard = [
                [InlineKeyboardButton("🌐 Открыть ссылку", url=link_data["url"])],
                [InlineKeyboardButton("🔙 Назад", callback_data="help_cat_links")]
            ]
            
            await query.edit_message_text(
                text=text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.MARKDOWN
            )
    
    # Настройки помощи
    elif data == "help_settings":
        if config.is_admin(user):
            text = "⚙️ *Панель администратора (Помощь)*\n\nВыберите действие:"
            await query.edit_message_text(
                text=text,
                reply_markup=get_help_settings_menu(),
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await query.answer("❌ У вас нет прав доступа!", show_alert=True)
    
    # Добавление файла помощи
    elif data == "help_add_file":
        if config.is_admin(user):
            # Начинаем процесс добавления файла
            config.start_adding_file(user_id)
            
            text = (
                "📤 *Добавление нового файла*\n\n"
                "1. *Отправьте мне файл* (PDF, Word, Excel, картинку и т.д.)\n"
                "2. Затем я спрошу название файла\n"
                "3. Добавьте описание\n\n"
                "❌ Для отмены отправьте /cancel"
            )
            
            # Отправляем новое сообщение с инструкцией
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=text,
                parse_mode=ParseMode.MARKDOWN
            )
            
            # Закрываем старое меню
            await query.edit_message_reply_markup(reply_markup=None)
            
        else:
            await query.answer("❌ У вас нет прав доступа!", show_alert=True)
    
    # Удаление файла помощи
    elif data == "help_delete_file":
        if config.is_admin(user):
            if not config.help_data["files"]:
                await query.edit_message_text(
                    text="📭 *Нет файлов для удаления*\n\n"
                         "База файлов пуста.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="help_settings")]]),
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                await query.edit_message_text(
                    text="🗑️ *Удаление файла*\n\n"
                         "Выберите файл для удаления:",
                    reply_markup=get_help_delete_files_menu(),
                    parse_mode=ParseMode.MARKDOWN
                )
        else:
            await query.answer("❌ У вас нет прав доступа!", show_alert=True)
    
    # Подтверждение удаления файла помощи
    elif data.startswith("help_delete_"):
        if config.is_admin(user):
            file_id = data.replace("help_delete_", "")
            file_data = config.help_data["files"].get(file_id)
            
            if file_data:
                keyboard = [
                    [
                        InlineKeyboardButton("✅ Да, удалить", callback_data=f"help_confirm_delete_{file_id}"),
                        InlineKeyboardButton("❌ Нет, отмена", callback_data="help_delete_file")
                    ]
                ]
                
                await query.edit_message_text(
                    text=f"⚠️ *Подтверждение удаления*\n\n"
                         f"Вы уверены, что хотите удалить файл:\n"
                         f"*{file_data['name']}*?\n\n"
                         f"Описание: {file_data['description']}",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode=ParseMode.MARKDOWN
                )
    
    # Подтвержденное удаление файла помощи
    elif data.startswith("help_confirm_delete_"):
        if config.is_admin(user):
            file_id = data.replace("help_confirm_delete_", "")
            
            if config.delete_file(file_id):
                await query.edit_message_text(
                    text="✅ *Файл успешно удален!*",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 В настройки", callback_data="help_settings")]])
                )
            else:
                await query.edit_message_text(
                    text="❌ *Ошибка при удалении файла*",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 В настройки", callback_data="help_settings")]])
                )
    
    # Статистика помощи
    elif data == "help_stats":
        if config.is_admin(user):
            files_count = len(config.help_data["files"])
            links_count = len(config.help_data["links"])
            
            text = (
                "📊 *Статистика системы помощи*\n\n"
                f"📁 Файлов в базе: *{files_count}*\n"
                f"🔗 Ссылок в базе: *{links_count}*\n"
                f"📂 Категорий: *{len(config.help_data['categories'])}*\n\n"
            )
            
            if files_count > 0:
                text += "*Доступные файлы:*\n"
                for file_id, file_data in config.help_data["files"].items():
                    added_date = file_data.get("added_date", "неизвестно")
                    if added_date:
                        added_date = added_date[:10]
                    text += f"• {file_data['name']} (добавлен: {added_date})\n"
            else:
                text += "*Файлов пока нет.* Добавьте первый файл через панель администратора."
            
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="help_settings")]]
            
            await query.edit_message_text(
                text=text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.MARKDOWN
            )

# ========== ОБРАБОТЧИКИ ДЛЯ ДОБАВЛЕНИЯ ФАЙЛОВ ==========

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик получения документа для добавления"""
    config = BotConfig()
    user = update.message.from_user.username
    user_id = update.message.from_user.id
    
    if not config.is_admin(user):
        await update.message.reply_text("❌ У вас нет прав для добавления файлов.")
        return
    
    pending_data = config.get_pending_file(user_id)
    
    if not pending_data or pending_data.get("state") != "waiting_file":
        # Пользователь не в процессе добавления файла
        return
    
    # Сохраняем информацию о файле
    document = update.message.document
    file_id = document.file_id
    file_name = document.file_name or "Без названия"
    
    config.save_file_data(user_id, file_id, file_name)
    
    await update.message.reply_text(
        f"📁 *Файл получен:* {file_name}\n\n"
        f"Теперь введите *название файла* для отображения в меню:\n\n"
        f"❌ *Отмена:* /cancel",
        parse_mode=ParseMode.MARKDOWN
    )

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик текстовых сообщений для добавления файла"""
    config = BotConfig()
    user = update.message.from_user.username
    user_id = update.message.from_user.id
    
    if not config.is_admin(user):
        return
    
    pending_data = config.get_pending_file(user_id)
    
    if not pending_data:
        # Пользователь не в процессе добавления файла
        return
    
    text = update.message.text
    
    # Если пользователь отменяет
    if text.lower() == "/cancel":
        if str(user_id) in config.data["pending_files"]:
            del config.data["pending_files"][str(user_id)]
            config.save()
        await update.message.reply_text("❌ Добавление файла отменено.")
        return
    
    state = pending_data.get("state")
    
    if state == "waiting_name":
        # Пользователь отправляет название файла
        config.save_file_name(user_id, text)
        
        await update.message.reply_text(
            f"✅ *Название сохранено:* {text}\n\n"
            f"Теперь введите *описание файла*:\n\n"
            f"❌ *Отмена:* /cancel",
            parse_mode=ParseMode.MARKDOWN
        )
    
    elif state == "waiting_description":
        # Пользователь отправляет описание файла
        file_id = pending_data.get("file_id")
        display_name = pending_data.get("display_name")
        
        if file_id and display_name:
            # Добавляем файл в систему
            success = config.add_file(user_id, file_id, display_name, text)
            
            if success:
                await update.message.reply_text(
                    f"✅ *Файл успешно добавлен!*\n\n"
                    f"📁 *Название:* {display_name}\n"
                    f"📝 *Описание:* {text}\n\n"
                    f"Файл теперь доступен в разделе 📄 Документы.\n\n"
                    f"Используйте /help чтобы увидеть его в меню.",
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                await update.message.reply_text(
                    "❌ *Ошибка при добавлении файла*\n\n"
                    "Попробуйте еще раз или обратитесь к разработчику.",
                    parse_mode=ParseMode.MARKDOWN
                )
        else:
            await update.message.reply_text(
                "❌ *Ошибка: данные файла потеряны*\n\n"
                "Пожалуйста, начните процесс добавления заново.",
                parse_mode=ParseMode.MARKDOWN
            )

async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик команды /cancel"""
    config = BotConfig()
    user = update.message.from_user.username
    user_id = update.message.from_user.id
    
    if not config.is_admin(user):
        return
    
    # Удаляем данные о процессе добавления файла
    if str(user_id) in config.data["pending_files"]:
        del config.data["pending_files"][str(user_id)]
        config.save()
    
    await update.message.reply_text(
        "❌ *Добавление файла отменено.*",
        parse_mode=ParseMode.MARKDOWN
    )

# ========== ГЛАВНАЯ ФУНКЦИЯ ==========

def main() -> None:
    if not TOKEN:
        logger.error("❌ Токен бота не найден!")
        return
    
    try:
        application = Application.builder().token(TOKEN).build()
        
        # Основные обработчики
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("help", help_command))
        application.add_handler(CommandHandler("setchat", set_chat))
        application.add_handler(CommandHandler("testmorning", test_morning))
        application.add_handler(CommandHandler("testplanerka", test_planerka_command))
        application.add_handler(CommandHandler("testindustry", test_industry_command))
        application.add_handler(CommandHandler("cancel", cancel_command))
        
        # Обработчики callback
        application.add_handler(CallbackQueryHandler(handle_horoscope_callback, pattern="^horoscope_"))
        application.add_handler(CallbackQueryHandler(handle_callback))
        
        # Обработчики для добавления файлов
        application.add_handler(MessageHandler(
            filters.Document.ALL & filters.ChatType.PRIVATE,
            handle_document
        ))
        
        application.add_handler(MessageHandler(
            filters.TEXT & filters.ChatType.PRIVATE & ~filters.COMMAND,
            handle_text
        ))
        
        # Запуск планировщиков
        application.job_queue.run_once(
            lambda ctx: asyncio.create_task(schedule_next_morning(ctx)),
            3
        )
        
        # Логирование при запуске
        logger.info("🤖 Бот запущен и готов к работе!")
        logger.info(f"✨ Утренние гороскопы: Пн-Пт в 9:00 по МСК")
        logger.info(f"📡 Парсинг гороскопов из RSS/XML источников")
        logger.info(f"🏢 Планёрки: Пн, Ср, Пт в 9:15 по МСК")
        logger.info(f"🎯 Отраслевые встречи: Вт в 12:00 по МСК")
        logger.info(f"📚 Система помощи с файлами и ссылками")
        
        # Создаем экземпляр конфига для инициализации
        config = BotConfig()
        
        # Если чат уже установлен, запускаем дополнительные планировщики
        if config.chat_id:
            logger.info(f"Чат установлен: {config.chat_id}")
            application.job_queue.run_once(
                lambda ctx: asyncio.create_task(schedule_next_planerka(ctx)),
                3
            )
            application.job_queue.run_once(
                lambda ctx: asyncio.create_task(schedule_next_industry_meeting(ctx)),
                3
            )
        
        application.run_polling(allowed_updates=Update.ALL_TYPES)
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        raise

if __name__ == "__main__":
    main()
