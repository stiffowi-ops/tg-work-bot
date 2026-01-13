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

# Время планёрки (9:15 по Москве)
MEETING_TIME = {"hour": 9, "minute": 15}
TIMEZONE = pytz.timezone("Europe/Moscow")

# Дни недели для планёрки (понедельник=0, среда=2, пятница=4)
MEETING_DAYS = [0, 2, 4]

# ========== НАСТРОЙКИ ФАКТОВ ==========
# Категории фактов
FACT_CATEGORIES = ['музыка', 'фильмы', 'технологии', 'игры']
# Время отправки фактов (10:00 по Москве = 7:00 UTC)
FACT_SEND_TIME = {"hour": 7, "minute": 0, "timezone": "UTC"}  # 7:00 UTC = 10:00 МСК
# Реакции под фактами
FACT_REACTIONS = ['👍', '👎', '💩', '🔥', '🧠💥']

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

# ========== КЛАСС ДЛЯ ФАКТОВ ИЗ ВИКИПЕДИИ (ИСПРАВЛЕННЫЙ) ==========
class FactScheduler:
    """Класс для управления отправкой фактов из Википедии"""
    
    def __init__(self):
        self.current_index = 0
        self.last_fact_data = {}  # Кэш последних фактов
        logger.info("Инициализирован планировщик фактов")
    
    def get_next_category(self) -> str:
        """Получаем следующую категорию по кругу"""
        category = FACT_CATEGORIES[self.current_index]
        self.current_index = (self.current_index + 1) % len(FACT_CATEGORIES)
        logger.debug(f"Следующая категория фактов: {category}")
        return category
    
    def get_wikipedia_fact(self, category: str, lang: str = 'ru') -> Tuple[str, str, str]:
        """
        Получаем случайный факт из Википедии по категории
        
        Возвращает: (факт, ссылка, название_статьи)
        """
        try:
            logger.info(f"Запрос факта для категории: {category}")
            
            # Шаг 1: Получаем статьи из категории
            url = f"https://{lang}.wikipedia.org/w/api.php"
            
            # Ключевые слова для поиска по категориям
            category_keywords = {
                'музыка': ['музыка', 'песня', 'альбом', 'исполнитель', 'группа'],
                'фильмы': ['фильм', 'кинематограф', 'режиссёр', 'актёр', 'кино'],
                'технологии': ['технология', 'компьютер', 'программа', 'интернет', 'наука'],
                'игры': ['игра', 'видеоигра', 'компьютерная игра', 'разработчик', 'игрок']
            }
            
            # Используем более простой запрос для поиска статей
            search_keyword = random.choice(category_keywords.get(category, [category]))
            
            params = {
                'action': 'query',
                'format': 'json',
                'list': 'search',
                'srsearch': search_keyword,
                'srlimit': 50,
                'srwhat': 'text',
                'srinfo': 'totalhits',
                'srprop': 'snippet'
            }
            
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            if 'query' not in data or not data['query']['search']:
                logger.warning(f"Не найдено статей для поиска: {search_keyword}")
                return self._get_fallback_fact(category), "", "Статья не найдена"
            
            # Выбираем случайную статью из результатов поиска
            articles = data['query']['search']
            article = random.choice(articles)
            title = article['title']
            logger.debug(f"Выбрана статья: {title}")
            
            # Шаг 2: Получаем содержание статьи (только первые 500 символов для краткости)
            params = {
                'action': 'query',
                'format': 'json',
                'prop': 'extracts|info',
                'inprop': 'url',
                'exchars': 500,  # Ограничиваем количество символов
                'explaintext': True,
                'titles': title
            }
            
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            pages = data['query']['pages']
            page_id = list(pages.keys())[0]
            page = pages[page_id]
            
            if 'missing' in page:
                logger.warning(f"Статья отсутствует: {title}")
                return self._get_fallback_fact(category), "", title
            
            # Извлекаем факт
            fact = page.get('extract', 'Нет описания')
            
            # Если факт слишком короткий, добавляем больше информации
            if len(fact) < 100:
                # Пробуем получить больше текста
                params['exchars'] = 1000
                response = requests.get(url, params=params, timeout=15)
                if response.status_code == 200:
                    data = response.json()
                    pages = data['query']['pages']
                    page_id = list(pages.keys())[0]
                    page = pages[page_id]
                    fact = page.get('extract', fact)
            
            # Формируем URL (заменяем пробелы на подчеркивания и кодируем)
            encoded_title = title.replace(' ', '_')
            article_url = f"https://{lang}.wikipedia.org/wiki/{encoded_title}"
            
            logger.info(f"Успешно получен факт: {title}")
            
            # Кэшируем факт
            self.last_fact_data[category] = {
                'title': title,
                'fact': fact,
                'url': article_url,
                'timestamp': datetime.now().isoformat()
            }
            
            return fact, article_url, title
            
        except requests.exceptions.Timeout:
            logger.error(f"Таймаут при запросе категории: {category}")
            # Пробуем вернуть кэшированный факт
            if category in self.last_fact_data:
                data = self.last_fact_data[category]
                return data['fact'], data['url'], data['title']
            return self._get_fallback_fact(category), "", "Ошибка загрузки"
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка сети: {e}")
            if category in self.last_fact_data:
                data = self.last_fact_data[category]
                return data['fact'], data['url'], data['title']
            return self._get_fallback_fact(category), "", "Ошибка сети"
        except Exception as e:
            logger.error(f"Неожиданная ошибка: {e}")
            if category in self.last_fact_data:
                data = self.last_fact_data[category]
                return data['fact'], data['url'], data['title']
            return self._get_fallback_fact(category), "", "Ошибка"
    
    def _get_fallback_fact(self, category: str) -> str:
        """Резервные факты на случай недоступности Wikipedia"""
        fallback_facts = {
            'музыка': [
                "Бетховен продолжал сочинять музыку даже после того, как полностью потерял слух.",
                "Группа The Beatles удерживает рекорд по количеству проданных альбомов в истории музыки.",
                "Скрипка Страдивари может стоить более 15 миллионов долларов.",
                "Песня 'Happy Birthday to You' изначально называлась 'Good Morning to All'.",
                "В опере 'Волшебная флейта' Моцарта используется музыкальная тема, основанная на масонских символах."
            ],
            'фильмы': [
                "Первый в истории полнометражный фильм был снят в 1906 году и длился около 60 минут.",
                "Альфред Хичкок никогда не получал премию 'Оскар' за лучшую режиссуру.",
                "Фильм 'Аватар' Джеймса Кэмерона является самым кассовым фильмом в истории кино.",
                "Для съёмок 'Властелина колец' было изготовлено более 48 000 предметов реквизита.",
                "Мэрилин Монро имела IQ 168, что считается уровнем гения."
            ],
            'технологии': [
                "Первый компьютерный вирус был создан в 1983 году и назывался 'Elk Cloner'.",
                "QR-коды были изобретены в Японии в 1994 году для отслеживания запчастей автомобилей.",
                "Средний смартфон сегодня имеет больше вычислительной мощности, чем компьютеры NASA в 1969 году.",
                "Первое в мире SMS было отправлено в 1992 году и содержало текст 'Счастливого Рождества!'",
                "Искусственный интеллект впервые обыграл чемпиона мира по шахматам в 1997 году."
            ],
            'игры': [
                "Первая в мире видеоигра была создана в 1958 году и называлась 'Tennis for Two'.",
                "Персонаж Марио изначально назывался 'Прыгающий человек' и появился в игре 'Donkey Kong'.",
                "Игра Minecraft является самой продаваемой видеоигрой в истории.",
                "Разработчики игры The Legend of Zelda вдохновлялись детскими воспоминаниями о лесах Киото.",
                "Самый дорогой предмет в истории игр - космический корабль в Eve Online, проданный за 330 тысяч долларов."
            ]
        }
        
        facts = fallback_facts.get(category, ["Интересный факт будет в следующий раз!"])
        return random.choice(facts)
    
    def create_fact_message(self, category: str) -> Tuple[str, InlineKeyboardMarkup]:
        """Создаем сообщение с фактом и inline-кнопками"""
        fact, url, title = self.get_wikipedia_fact(category)
        
        # Форматируем сообщение
        message = f"📚 *ФАКТ ДНЯ* • {category.upper()}\n\n"
        message += f"*{title}*\n\n"
        message += f"{fact}\n\n"
        
        # Ссылка встроена в текст "Читать подробнее"
        if url:
            message += f"[Читать подробнее]({url})"
        
        # Создаем inline-кнопки с реакциями
        keyboard = []
        row = []
        for emoji in FACT_REACTIONS:
            callback_data = f"react_fact_{emoji}_{category}"
            row.append(
                InlineKeyboardButton(text=emoji, callback_data=callback_data)
            )
            if len(row) == 5:  # 5 кнопок в ряд
                keyboard.append(row)
                row = []
        
        if row:  # Если остались кнопки
            keyboard.append(row)
        
        return message, InlineKeyboardMarkup(keyboard)

# ========== ОСТАЛЬНЫЙ ВАШ КОД ==========

# Вспомогательная функция для совместимости версий PTB
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
        config = load_config()
        allowed_users = config.get("allowed_users", [])
        
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
    
    if weekday in MEETING_DAYS:
        day_names = {0: "ПОНЕДЕЛЬНИК", 2: "СРЕДА", 4: "ПЯТНИЦА"}
        zoom_link_formatted = f'<a href="{ZOOM_LINK}">Присоединиться к Zoom</a>'
        zoom_notes = [
            f"\n\n🎥 {zoom_link_formatted} | 👈",
            f"\n\n👨‍💻 {zoom_link_formatted} | 👈",
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
        
        greetings = {
            0: [
                f"🚀 <b>{day_names[0]}</b> - старт новой недели!\n\n📋 <i>Планёрка в 9:30 по МСК</i>.\nДавайте обсудим планы на неделю! 🌟{zoom_note}",
                f"🌞 Доброе утро! Сегодня <b>{day_names[0]}</b>!\n\n🤝 <i>Планёрка в 9:30 по МСК</i>.\nНачинаем неделю продуктивно! 💪{zoom_note}",
                f"⚡ <b>{day_names[0]}</b>, время действовать!\n\n🎯 <i>Утренняя планёрка в 9:30 по МСК</i>.\nПодготовьте ваши вопросы! 📊{zoom_note}"
            ],
            2: [
                f"⚡ <b>{day_names[2]}</b> - середина недели!\n\n📋 <i>Планёрка в 9:30 по МСК</i>.\nВремя для корректировок и обновлений! 🔄{zoom_note}",
                f"🌞 <b>{day_names[2]}</b>, доброе утро!\n\n🤝 <i>Планёрка в 9:30 по МСК</i>.\nКак продвигаются задачи? 📈{zoom_note}",
                f"💪 <b>{day_names[2]}</b> - день прорыва!\n\n🎯 <i>Планёрка в 9:30 по МСК</i>.\nДелитесь прогрессом! 🚀{zoom_note}"
            ],
            4: [
                f"🎉 <b>{day_names[4]}</b> - завершаем неделю!\n\n📋 <i>Планёрка в 9:30 по МСК</i>.\nДавайте подведем итоги недели! 🏆{zoom_note}",
                f"🌞 Пятничное утро! 🎊\n\n🤝 <b>{day_names[4]}</b>, <i>планёрка в 9:30 по МСК</i>.\nКак прошла неделя? 📊{zoom_note}",
                f"✨ <b>{day_names[4]}</b> - время подводить итоги!\n\n🎯 <i>Планёрка в 9:30 по МСК</i>.\nЧто успели за неделю? 📈{zoom_note}"
            ]
        }
        return random.choice(greetings[weekday])
    else:
        zoom_link_formatted = f'<a href="{ZOOM_LINK}">Присоединиться к Zoom</a>'
        return f"👋 Доброе утро! Сегодня <i>{current_day}</i>.\n\n📋 <i>Напоминаю о планёрке в 9:30 по МСК</i>.\n🎥 {zoom_link_formatted} | Присоединяйтесь к встрече"

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
                    return data
            except Exception as e:
                logger.error(f"Ошибка загрузки конфига: {e}")
        return {
            "chat_id": None,
            "allowed_users": ["Stiff_OWi", "gshabanov"],
            "active_reminders": {}
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

def load_config() -> Dict[str, Any]:
    config = BotConfig()
    return config.data

def save_config(config: Dict[str, Any]) -> None:
    bot_config = BotConfig()
    bot_config.data = config
    bot_config.save()

# ========== НОВЫЕ ФУНКЦИИ ДЛЯ ФАКТОВ (ИСПРАВЛЕННЫЕ) ==========

async def send_daily_fact(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправка ежедневного факта"""
    config = BotConfig()
    chat_id = config.chat_id

    if not chat_id:
        logger.error("Chat ID не установлен для отправки фактов!")
        return

    try:
        # Инициализируем планировщик фактов
        fact_scheduler = FactScheduler()
        category = fact_scheduler.get_next_category()
        message, keyboard = fact_scheduler.create_fact_message(category)
        
        # Отправляем факт
        await context.bot.send_message(
            chat_id=chat_id,
            text=message,
            parse_mode=ParseMode.MARKDOWN,
            disable_web_page_preview=False,
            reply_markup=keyboard
        )
        
        logger.info(f"✅ Факт отправлен: {category} в {datetime.now(TIMEZONE).strftime('%H:%M')}")
        
    except Exception as e:
        logger.error(f"❌ Ошибка отправки факта: {e}")

async def handle_fact_reaction(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработка реакций на факты"""
    query = update.callback_query
    await query.answer()
    
    try:
        # Парсим callback_data: react_fact_🎵_музыка
        parts = query.data.split('_')
        if len(parts) >= 4:
            emoji = parts[2]
            category = parts[3]
            
            # Словарь названий реакций
            reaction_names = {
                '👍': 'Лайк',
                '👎': 'Дизлайк', 
                '💩': 'Какашка',
                '🔥': 'Огонь',
                '🧠💥': 'Взрыв мозга'
            }
            
            # Emoji для категорий
            emoji_map = {
                'музыка': '🎵',
                'фильмы': '🎬',
                'технологии': '💻',
                'игры': '🎮'
            }
            
            category_emoji = emoji_map.get(category, '📌')
            reaction_name = reaction_names.get(emoji, 'Реакция')
            
            # Отправляем всплывающее уведомление
            await query.answer(
                text=f"{emoji} {reaction_name} на факт {category_emoji} {category.upper()}!",
                show_alert=False
            )
            
            logger.debug(f"Реакция на факт: {emoji} на категорию {category}")
            
    except Exception as e:
        logger.error(f"Ошибка обработки реакции на факт: {e}")
        await query.answer(text="⚠️ Ошибка обработки реакции", show_alert=False)

@restricted
async def send_fact_now(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправить факт немедленно по команде"""
    config = BotConfig()
    chat_id = config.chat_id

    if not chat_id:
        await update.message.reply_text("❌ Сначала установите чат командой /setchat")
        return

    try:
        fact_scheduler = FactScheduler()
        category = fact_scheduler.get_next_category()
        message, keyboard = fact_scheduler.create_fact_message(category)
        
        # Отправляем факт в целевой чат
        await context.bot.send_message(
            chat_id=chat_id,
            text=message,
            parse_mode=ParseMode.MARKDOWN,
            disable_web_page_preview=False,
            reply_markup=keyboard
        )
        
        logger.info(f"Факт отправлен по команде: {category}")
        # НЕ отправляем подтверждение пользователю
        
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка при отправке факта: {str(e)}")
        logger.error(f"Ошибка в команде /factnow: {e}")

async def show_next_fact_category(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показать следующую категорию фактов"""
    fact_scheduler = FactScheduler()
    category = FACT_CATEGORIES[fact_scheduler.current_index]
    
    emoji_map = {
        'музыка': '🎵',
        'фильмы': '🎬', 
        'технологии': '💻',
        'игры': '🎮'
    }
    
    category_emoji = emoji_map.get(category, '📌')
    
    response = f"{category_emoji} *Следующая категория фактов:* {category.upper()}\n\n"
    response += f"📅 Будет отправлена сегодня в 10:00 по МСК"
    
    await update.message.reply_text(response, parse_mode=ParseMode.HTML)

def calculate_next_fact_time() -> datetime:
    """Рассчитать время следующей отправки факта"""
    now = datetime.now(pytz.UTC)  # Используем UTC для планирования
    
    # Дни отправки фактов (понедельник=0 ... пятница=4)
    FACT_DAYS = [0, 1, 2, 3, 4]  # Пн-Пт
    
    # Проверяем, сегодня ли нужный день и время
    if now.weekday() in FACT_DAYS:
        reminder_time = now.replace(
            hour=FACT_SEND_TIME["hour"],
            minute=FACT_SEND_TIME["minute"],
            second=0,
            microsecond=0
        )
        if now < reminder_time:
            return reminder_time

    # Ищем следующий рабочий день
    days_ahead = 1
    while True:
        next_day = now + timedelta(days=days_ahead)
        if next_day.weekday() in FACT_DAYS:
            return next_day.replace(
                hour=FACT_SEND_TIME["hour"],
                minute=FACT_SEND_TIME["minute"],
                second=0,
                microsecond=0
            )
        days_ahead += 1

async def schedule_next_fact(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Запланировать следующую отправку факта"""
    next_time = calculate_next_fact_time()
    config = BotConfig()
    chat_id = config.chat_id

    if not chat_id:
        logger.warning("Chat ID не установлен, планирование фактов отложено")
        # Пробуем снова через час
        context.application.job_queue.run_once(
            schedule_next_fact,
            3600
        )
        return

    now = datetime.now(pytz.UTC)
    delay = (next_time - now).total_seconds()

    if delay > 0:
        job_name = f"daily_fact_{next_time.strftime('%Y%m%d_%H%M')}"
        
        # Проверяем, нет ли уже такой задачи
        existing_jobs = [j for j in get_jobs_from_queue(context.application.job_queue) 
                        if j.name == job_name]
        
        if not existing_jobs:
            context.application.job_queue.run_once(
                send_daily_fact,
                delay,
                chat_id=chat_id,
                name=job_name
            )

            # Планируем следующую отправку после текущей
            context.application.job_queue.run_once(
                schedule_next_fact,
                delay + 60,  # Через минуту после отправки
                chat_id=chat_id,
                name=f"fact_scheduler_{next_time.strftime('%Y%m%d_%H%M')}"
            )

            logger.info(f"Следующая отправка факта запланирована на {next_time} UTC")
            logger.info(f"Это будет в {(next_time + timedelta(hours=3)).strftime('%H:%M')} по МСК")
        else:
            logger.info(f"Отправка факта на {next_time} уже запланирована")

# ========== ОСТАЛЬНЫЕ ФУНКЦИИ ВАШЕГО БОТА ==========

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

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обновленный обработчик /start с информацией о фактах"""
    await update.message.reply_text(
        "🤖 <b>Бот для напоминаний о планёрке активен!</b>\n\n"
        f"📅 <b>Напоминания отправляются:</b>\n"
        f"• Понедельник\n• Среда\n• Пятница\n"
        f"⏰ <b>Время:</b> {MEETING_TIME['hour']:02d}:{MEETING_TIME['minute']:02d} по МСК\n\n"
        "📚 <b>Ежедневные факты из Википедии:</b>\n"
        f"• Отправляются: Пн-Пт в 10:00 по МСК\n"
        f"• Категории: {', '.join([c.capitalize() for c in FACT_CATEGORIES])}\n\n"
        "🔧 <b>Доступные команды:</b>\n"
        "/info - информация о боте\n"
        "/jobs - список запланированных задач\n"
        "/test - тестовое напоминание (через 5 сек)\n"
        "/testnow - мгновенное тестовое напоминание\n"
        "/factnow - отправить факт сейчас\n"
        "/nextfact - следующая категория фактов\n\n"
        "👮‍♂️ <b>Команды для администраторов:</b>\n"
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
        "Напоминания и факты будут отправляться в этот чат.",
        parse_mode=ParseMode.HTML
    )

    logger.info(f"Установлен чат {chat_title} ({chat_id})")

@restricted
async def show_info(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обновленный обработчик /info с информацией о фактах"""
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
    
    # Считаем задачи фактов
    fact_job_count = len([j for j in all_jobs 
                    if j.name and j.name.startswith("daily_fact_")])
    
    # Следующее напоминание о планёрке
    next_meeting_job = None
    for job in all_jobs:
        if job.name and job.name.startswith("meeting_reminder_"):
            if not next_meeting_job or job.next_t < next_meeting_job.next_t:
                next_meeting_job = job
    
    # Следующая отправка факта
    next_fact_job = None
    for job in all_jobs:
        if job.name and job.name.startswith("daily_fact_"):
            if not next_fact_job or job.next_t < next_fact_job.next_t:
                next_fact_job = job
    
    next_meeting_time = next_meeting_job.next_t.astimezone(TIMEZONE) if next_meeting_job else "не запланировано"
    next_fact_time_utc = next_fact_job.next_t if next_fact_job else None
    next_fact_time = next_fact_time_utc.astimezone(TIMEZONE) if next_fact_time_utc else "не запланировано"
    
    today = datetime.now(TIMEZONE)
    upcoming_meetings = []
    for i in range(1, 8):
        next_day = today + timedelta(days=i)
        if next_day.weekday() in MEETING_DAYS:
            upcoming_meetings.append(next_day.strftime("%d.%m.%Y"))

    zoom_info = f"\n🎥 <b>Zoom-ссылка:</b> {'установлена ✅' if ZOOM_LINK and ZOOM_LINK != 'https://us04web.zoom.us/j/1234567890?pwd=example' else 'не установлена ⚠️'}"
    
    # Информация о фактах
    fact_scheduler = FactScheduler()
    next_fact_category = FACT_CATEGORIES[fact_scheduler.current_index]
    fact_info = f"\n📚 <b>Следующий факт:</b> {next_fact_category.capitalize()}"
    
    await update.message.reply_text(
        f"📊 <b>Информация о боте:</b>\n\n"
        f"{status}\n"
        f"📅 <b>Дни планёрок:</b> понедельник, среда, пятница\n"
        f"⏰ <b>Время планёрок:</b> {MEETING_TIME['hour']:02d}:{MEETING_TIME['minute']:02d} по МСК\n"
        f"📚 <b>Факты из Википедии:</b> Пн-Пт в 10:00 по МСК\n"
        f"🎯 <b>Категории фактов:</b> {', '.join(FACT_CATEGORIES)}\n"
        f"👥 <b>Разрешённые пользователи:</b> {len(config.allowed_users)}\n"
        f"📋 <b>Активные напоминания:</b> {len(config.active_reminders)}\n"
        f"⏳ <b>Задачи планёрок:</b> {meeting_job_count}\n"
        f"📖 <b>Задачи фактов:</b> {fact_job_count}\n"
        f"➡️ <b>Следующая планёрка:</b> {next_meeting_time}\n"
        f"➡️ <b>Следующий факт:</b> {next_fact_time}\n"
        f"📈 <b>Ближайшие планёрки:</b> {', '.join(upcoming_meetings[:3]) if upcoming_meetings else 'нет'}"
        f"{zoom_info}"
        f"{fact_info}\n\n"
        f"Используйте /users для списка пользователей\n"
        f"Используйте /jobs для списка задач\n"
        f"Используйте /nextfact для следующей категории фактов",
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
    fact_jobs = [j for j in jobs if j.name and j.name.startswith("daily_fact_")]
    other_jobs = [j for j in jobs if j not in meeting_jobs + fact_jobs]
    
    message = "📋 <b>Запланированные задачи:</b>\n\n"
    
    if meeting_jobs:
        message += "🔔 <b>Напоминания о планёрках:</b>\n"
        for job in sorted(meeting_jobs, key=lambda j: j.next_t):
            next_time = job.next_t.astimezone(TIMEZONE)
            message += f"  • {next_time.strftime('%d.%m.%Y %H:%M')} ({job.name})\n"
    
    if fact_jobs:
        message += "\n📚 <b>Факты из Википедии:</b>\n"
        for job in sorted(fact_jobs, key=lambda j: j.next_t):
            next_time = job.next_t.astimezone(TIMEZONE)
            message += f"  • {next_time.strftime('%d.%m.%Y %H:%M')} ({job.name})\n"
    
    if other_jobs:
        message += "\n🔧 <b>Другие задачи:</b>\n"
        for job in other_jobs:
            next_time = job.next_t.astimezone(TIMEZONE)
            job_name = job.name or "Без имени"
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
    canceled_facts = 0
    
    for job in jobs[:]:
        if job.name and job.name.startswith("meeting_reminder_"):
            job.schedule_removal()
            canceled_meetings += 1
        elif job.name and job.name.startswith("daily_fact_"):
            job.schedule_removal()
            canceled_facts += 1
    
    config = BotConfig()
    config.clear_active_reminders()
    
    await update.message.reply_text(
        f"✅ <b>Отменено:</b>\n"
        f"• {canceled_meetings} напоминаний о планёрках\n"
        f"• {canceled_facts} отправок фактов\n"
        f"Очищено {len(config.active_reminders)} активных напоминаний в конфиге",
        parse_mode=ParseMode.HTML
    )
    logger.info(f"Отменено {canceled_meetings} напоминаний и {canceled_facts} фактов")

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
        application.add_handler(CommandHandler("factnow", send_fact_now))
        application.add_handler(CommandHandler("nextfact", show_next_fact_category))
        application.add_handler(CommandHandler("jobs", list_jobs))
        application.add_handler(CommandHandler("adduser", add_user))
        application.add_handler(CommandHandler("removeuser", remove_user))
        application.add_handler(CommandHandler("users", list_users))
        application.add_handler(CommandHandler("cancelall", cancel_all))

        # Добавляем ConversationHandler
        application.add_handler(conv_handler)

        # Обработчик реакций на факты (ИСПРАВЛЕННЫЙ PATTERN)
        application.add_handler(
            CallbackQueryHandler(handle_fact_reaction, pattern="^react_fact_.+$")
        )

        # Очистка старых задач
        cleanup_old_jobs(application.job_queue)
        
        # Восстановление напоминаний
        restore_reminders(application)

        # Запуск планировщика планёрок
        application.job_queue.run_once(
            lambda context: schedule_next_reminder(context),
            3
        )

        # Запуск планировщика фактов
        application.job_queue.run_once(
            lambda context: schedule_next_fact(context),
            5
        )

        logger.info("🤖 Бот запущен и готов к работе!")
        logger.info(f"⏰ Планёрки: {', '.join(['Пн', 'Ср', 'Пт'])} в {MEETING_TIME['hour']:02d}:{MEETING_TIME['minute']:02d} по МСК")
        logger.info(f"📚 Факты: Пн-Пт в 10:00 по МСК (07:00 UTC)")
        logger.info(f"🎯 Категории фактов: {', '.join(FACT_CATEGORIES)}")
        
        application.run_polling(allowed_updates=Update.ALL_TYPES)

    except Exception as e:
        logger.error(f"❌ Критическая ошибка при запуске бота: {e}")
        raise


if __name__ == "__main__":
    main()
