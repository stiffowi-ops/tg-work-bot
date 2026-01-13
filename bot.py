import os
import logging
import json
import random
import urllib.request
import urllib.parse
import urllib.error
import ssl
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
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

# Настройка логирования
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# Состояния для ConversationHandler
SELECTING_REASON, SELECTING_DATE, CONFIRMING_DATE = range(3)

# Конфигурация
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")  # Токен бота из переменных окружения
ZOOM_LINK = os.getenv("ZOOM_MEETING_LINK", "https://us04web.zoom.us/j/1234567890?pwd=example")  # Ссылка на Zoom
CONFIG_FILE = "bot_config.json"  # Файл для хранения настроек

# Время планёрки (9:15 по Москве) и время отправки фактов (10:00 по Москве)
MEETING_TIME = {"hour": 9, "minute": 15}
FACTS_TIME = {"hour": 10, "minute": 0}
TIMEZONE = pytz.timezone("Europe/Moscow")

# Категории фактов
FACT_CATEGORIES = {
    "Наука": "science",
    "Технологии": "technology", 
    "Кино": "movies",
    "Музыка": "music",
    "Игры": "games"
}

# Дни недели для планёрки (понедельник=0, среда=2, пятница=4)
MEETING_DAYS = [0, 2, 4]

# Варианты отмены планёрки
CANCELLATION_OPTIONS = [
    "Все вопросы решены, планёрка не нужна",
    "Ключевые участники отсутствуют",
    "Перенесём на другой день",
]

# API источники для разных категорий фактов (ВСЕ БЕСПЛАТНЫЕ, БЕЗ API КЛЮЧЕЙ)
FACT_APIS = {
    "Наука": [
        {
            "name": "Useless Facts",
            "url": "https://uselessfacts.jsph.pl/api/v2/facts/random",
            "params": {"language": "ru"},
            "parser": lambda data: data["text"] if "text" in data else None,
            "priority": 1
        },
        {
            "name": "Numbers API",
            "url": "http://numbersapi.com/random/trivia",
            "params": {},
            "parser": lambda data: f"Числовой факт: {data}" if data else None,
            "priority": 2
        }
    ],
    "Технологии": [
        {
            "name": "Useless Facts",
            "url": "https://uselessfacts.jsph.pl/api/v2/facts/random",
            "params": {"language": "ru"},
            "parser": lambda data: data["text"] if "text" in data else None,
            "priority": 1
        },
        {
            "name": "Numbers Math",
            "url": "http://numbersapi.com/random/math",
            "params": {},
            "parser": lambda data: f"Математический факт: {data}" if data else None,
            "priority": 2
        }
    ],
    "Кино": [
        {
            "name": "Useless Facts",
            "url": "https://uselessfacts.jsph.pl/api/v2/facts/random",
            "params": {"language": "ru"},
            "parser": lambda data: data["text"] if "text" in data else None,
            "priority": 1
        },
        {
            "name": "Movie Quotes",
            "url": "https://movie-quote-api.vercel.app/v1/quote/random",
            "params": {},
            "parser": lambda data: f"Кинофакт: \"{data.get('quote', '')}\" - {data.get('show', 'Неизвестный фильм/сериал')}" if data else None,
            "priority": 2
        }
    ],
    "Музыка": [
        {
            "name": "Useless Facts",
            "url": "https://uselessfacts.jsph.pl/api/v2/facts/random",
            "params": {"language": "ru"},
            "parser": lambda data: data["text"] if "text" in data else None,
            "priority": 1
        },
        {
            "name": "Genrenator",
            "url": "https://binaryjazz.us/wp-json/genrenator/v1/story/",
            "params": {},
            "parser": lambda data: f"Музыкальный факт: {data}" if data else None,
            "priority": 2
        }
    ],
    "Игры": [
        {
            "name": "Useless Facts",
            "url": "https://uselessfacts.jsph.pl/api/v2/facts/random",
            "params": {"language": "ru"},
            "parser": lambda data: data["text"] if "text" in data else None,
            "priority": 1
        },
        {
            "name": "GamerPower",
            "url": "https://www.gamerpower.com/api/giveaways",
            "params": {"platform": "pc", "type": "game"},
            "parser": lambda data: f"Игровой факт: Сейчас на GamerPower доступно {len(data) if isinstance(data, list) else 'несколько'} игровых раздач для PC!" if data else None,
            "priority": 2
        }
    ]
}

# Резервные факты на случай недоступности API
BACKUP_FACTS = {
    "Наука": [
        "Среднее человеческое тело содержит достаточно углерода, чтобы изготовить 9000 карандашей.",
        "Осьминоги имеют три сердца: два перекачивают кровь через жабры, а одно через тело.",
        "Свету от Солнца требуется около 8 минут и 20 секунд, чтобы достичь Земли.",
        "Человеческий мозг на 60% состоит из жира, что делает его самым жирным органом в теле.",
        "Венера - единственная планета в Солнечной системе, которая вращается в обратном направлении."
    ],
    "Технологии": [
        "Первый компьютерный вирус был создан в 1983 году и назывался 'Brain'.",
        "Первое сообщение электронной почты было отправлено в 1971 году Рэем Томлинсоном.",
        "Средний человек проверяет свой телефон около 150 раз в день.",
        "Первый веб-сайт до сих пор работает: http://info.cern.ch",
        "Изначально у Google было название 'Backrub'."
    ],
    "Кино": [
        "В фильме 'Криминальное чтиво' использована не настоящая кровь, а смесь сиропа и пищевого красителя.",
        "Актёру Джонни Деппу заплатили $3 миллиона за роль в 'Пиратах Карибского моря', что составило около $60,000 за каждую минуту экранного времени.",
        "Фильм 'Аватар' является самым кассовым фильмом в истории, собрав более $2.8 миллиардов.",
        "Сцена с зеркалом в 'Контакт' была снята без CGI - использовалось настоящее зеркало длиной 12 метров.",
        "В 'Звёздных войнах' звук светового меча создан комбинацией жужжания проектора и помех от телевизора."
    ],
    "Музыка": [
        "Бетховен был почти полностью глухим, когда написал свою Девятую симфонию.",
        "Самая длинная песня в мире - 'The Rise and Fall of Bossanova' длительностью 13 часов 23 минуты 32 секунды.",
        "Гитара Fender Stratocaster - самая копируемая модель гитары в мире.",
        "Моцарт начал сочинять музыку в возрасте 5 лет.",
        "Битлз первоначально назывались 'The Quarrymen'."
    ],
    "Игры": [
        "Minecraft - самая продаваемая игра в истории с более чем 238 миллионами проданных копий.",
        "Первая в мире компьютерная игра 'Spacewar!' была создана в 1962 году.",
        "Создание The Legend of Zelda: Ocarina of Time заняло 5 лет и участие более 100 человек.",
        "В Tetris нет конца - игра продолжается до тех пор, пока игрок не проиграет.",
        "Персонаж Марио изначально назывался 'Прыгун' и появился в игре Donkey Kong."
    ]
}

# Вспомогательная функция для совместимости версий PTB
def get_jobs_from_queue(job_queue: JobQueue):
    """Получить список задач с поддержкой разных версий PTB"""
    try:
        # Пробуем новый метод (PTB >= 20)
        return job_queue.get_jobs()
    except AttributeError:
        try:
            # Используем старый метод (PTB < 20)
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


def get_daily_fact_sync(category: str) -> str:
    """Синхронная версия получения факта дня"""
    try:
        # Сортируем API по приоритету (от наивысшего к наименьшему)
        if category in FACT_APIS:
            api_configs = sorted(FACT_APIS[category], key=lambda x: x.get("priority", 10))
            
            for api_config in api_configs:
                try:
                    url = api_config["url"]
                    params = api_config.get("params", {})
                    
                    # Формируем URL с параметрами
                    if params:
                        query_string = urllib.parse.urlencode(params)
                        url = f"{url}?{query_string}"
                    
                    # Создаем запрос
                    req = urllib.request.Request(
                        url,
                        headers={"User-Agent": "TelegramFactsBot/1.0"}
                    )
                    
                    # Отправляем запрос с обработкой SSL
                    context = ssl._create_unverified_context()
                    with urllib.request.urlopen(req, context=context, timeout=10) as response:
                        if response.status == 200:
                            data = json.loads(response.read().decode())
                            fact = api_config["parser"](data)
                            if fact:
                                logger.info(f"Успешно получен факт из {api_config['name']} для категории {category}")
                                return fact
                except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, json.JSONDecodeError) as e:
                    logger.warning(f"Ошибка при запросе к API {api_config['name']} ({url}): {e}")
                    continue
        
        # Если все API не сработали, используем резервные факты
        if category in BACKUP_FACTS:
            logger.info(f"Используем резервный факт для категории {category}")
            return random.choice(BACKUP_FACTS[category])
            
    except Exception as e:
        logger.error(f"Ошибка при получении факта для категории {category}: {e}")
    
    # Факт по умолчанию
    return "Сегодняшний факт временно недоступен. Попробуйте позже!"


async def get_daily_fact(category: str) -> str:
    """Асинхронная обертка для получения факта дня"""
    # Используем синхронную версию в отдельном потоке
    import asyncio
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, get_daily_fact_sync, category)


async def send_daily_fact(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправка факта дня"""
    config = BotConfig()
    chat_id = config.chat_id

    if not chat_id:
        logger.error("Chat ID не установлен для отправки факта!")
        return

    try:
        # Выбираем случайную категорию
        category = random.choice(list(FACT_CATEGORIES.keys()))
        
        # Получаем факт
        fact = await get_daily_fact(category)
        
        # Форматируем сообщение
        message_text = (
            f"<b>📊 Факт дня из мира {category}</b>\n\n"
            f"{fact}\n\n"
            f"#фактдня #{FACT_CATEGORIES[category]}"
        )
        
        # Создаем клавиатуру с реакциями
        keyboard = [
            [
                InlineKeyboardButton("👍", callback_data=f"fact_like_{category}"),
                InlineKeyboardButton("👎", callback_data=f"fact_dislike_{category}"),
                InlineKeyboardButton("💩", callback_data=f"fact_poop_{category}")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Отправляем сообщение
        message = await context.bot.send_message(
            chat_id=chat_id,
            text=message_text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        )
        
        # Сохраняем информацию о факте
        job_name = context.job.name if hasattr(context, 'job') and context.job else f"fact_{datetime.now().timestamp()}"
        config.add_active_fact(message.message_id, chat_id, job_name, category)
        
        logger.info(f"Отправлен факт дня категории '{category}' в чат {chat_id}")
        
    except Exception as e:
        logger.error(f"Ошибка при отправке факта дня: {e}")


async def fact_reaction_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик реакций на факты"""
    query = update.callback_query
    await query.answer()
    
    # Извлекаем данные из callback_data
    data_parts = query.data.split('_')
    if len(data_parts) != 3:
        return
    
    reaction_type = data_parts[1]
    category = data_parts[2]
    
    # Определяем текст реакции
    reaction_texts = {
        "like": "👍 Спасибо за лайк!",
        "dislike": "👎 Ваше мнение учтено!",
        "poop": "💩 Хм... интересная реакция!"
    }
    
    reaction_text = reaction_texts.get(reaction_type, "Реакция получена!")
    
    # Отвечаем пользователю
    await query.answer(reaction_text, show_alert=True)
    
    # Логируем реакцию
    username = query.from_user.username or query.from_user.first_name
    logger.info(f"Реакция на факт: @{username} - {reaction_type} для категории {category}")


def get_greeting_by_meeting_day() -> str:
    """Специальные приветствия для дней планёрок со ссылкой на Zoom"""
    weekday = datetime.now(TIMEZONE).weekday()
    day_names_ru = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
    current_day = day_names_ru[weekday]
    
    # Только для дней планёрок (пн/ср/пт)
    if weekday in MEETING_DAYS:
        day_names = {
            0: "ПОНЕДЕЛЬНИК",
            2: "СРЕДА", 
            4: "ПЯТНИЦА"
        }
        
        # Форматируем ссылку на Zoom для HTML
        zoom_link_formatted = f'<a href="{ZOOM_LINK}">Присоединиться к Zoom</a>'
        
        # Формулировки с новой ссылкой
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
        # Если почему-то напоминание отправлено не в день планёрки
        zoom_link_formatted = f'<a href="{ZOOM_LINK}">Присоединиться к Zoom</a>'
        return f"👋 Доброе утро! Сегодня <i>{current_day}</i>.\n\n📋 <i>Напоминаю о планёрке в 9:30 по МСК</i>.\n🎥 {zoom_link_formatted} | Присоединяйтесь к встрече"


class BotConfig:
    """Класс для управления конфигурацией бота"""
    
    def __init__(self):
        self.data = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Загрузка конфигурации из файла"""
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # Инициализация структуры конфига при первой загрузке
                    if "allowed_users" not in data:
                        data["allowed_users"] = ["Stiff_OWi", "gshabanov"]
                    if "active_reminders" not in data:
                        data["active_reminders"] = {}
                    if "daily_facts_enabled" not in data:
                        data["daily_facts_enabled"] = True
                    if "last_fact_date" not in data:
                        data["last_fact_date"] = None
                    if "active_facts" not in data:
                        data["active_facts"] = {}
                    return data
            except Exception as e:
                logger.error(f"Ошибка загрузки конфига: {e}")
        return {
            "chat_id": None,
            "allowed_users": ["Stiff_OWi", "gshabanov"],
            "active_reminders": {},
            "daily_facts_enabled": True,
            "last_fact_date": None,
            "active_facts": {}
        }
    
    def save(self) -> None:
        """Сохранение конфигурации в файл"""
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
        """Добавить пользователя в список разрешенных"""
        if username not in self.allowed_users:
            self.data["allowed_users"].append(username)
            self.save()
            return True
        return False
    
    def remove_allowed_user(self, username: str) -> bool:
        """Удалить пользователя из списка разрешенных"""
        if username in self.allowed_users:
            self.data["allowed_users"].remove(username)
            self.save()
            return True
        return False
    
    @property
    def active_reminders(self) -> Dict[str, Any]:
        return self.data.get("active_reminders", {})
    
    def add_active_reminder(self, message_id: int, chat_id: int, job_name: str) -> None:
        """Добавить активное напоминание"""
        self.data["active_reminders"][job_name] = {
            "message_id": message_id,
            "chat_id": chat_id,
            "created_at": datetime.now(TIMEZONE).isoformat()
        }
        self.save()
    
    def remove_active_reminder(self, job_name: str) -> bool:
        """Удалить активное напоминание"""
        if job_name in self.data["active_reminders"]:
            del self.data["active_reminders"][job_name]
            self.save()
            return True
        return False
    
    def clear_active_reminders(self) -> None:
        """Очистить все активные напоминания"""
        self.data["active_reminders"] = {}
        self.save()
    
    @property
    def daily_facts_enabled(self) -> bool:
        return self.data.get("daily_facts_enabled", True)
    
    @daily_facts_enabled.setter
    def daily_facts_enabled(self, value: bool) -> None:
        self.data["daily_facts_enabled"] = value
        self.save()
    
    @property
    def last_fact_date(self) -> Optional[str]:
        return self.data.get("last_fact_date")
    
    @last_fact_date.setter
    def last_fact_date(self, value: str) -> None:
        self.data["last_fact_date"] = value
        self.save()
    
    @property
    def active_facts(self) -> Dict[str, Any]:
        return self.data.get("active_facts", {})
    
    def add_active_fact(self, message_id: int, chat_id: int, job_name: str, category: str) -> None:
        """Добавить активный факт"""
        self.data["active_facts"][job_name] = {
            "message_id": message_id,
            "chat_id": chat_id,
            "category": category,
            "created_at": datetime.now(TIMEZONE).isoformat()
        }
        self.save()
    
    def remove_active_fact(self, job_name: str) -> bool:
        """Удалить активный факт"""
        if job_name in self.data["active_facts"]:
            del self.data["active_facts"][job_name]
            self.save()
            return True
        return False


def load_config() -> Dict[str, Any]:
    """Утилитарная функция для обратной совместимости"""
    config = BotConfig()
    return config.data


def save_config(config: Dict[str, Any]) -> None:
    """Утилитарная функция для обратной совместимости"""
    bot_config = BotConfig()
    bot_config.data = config
    bot_config.save()


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

    # Используем наше улучшенное приветствие с Zoom-ссылкой
    message_text = get_greeting_by_meeting_day()

    try:
        message = await context.bot.send_message(
            chat_id=chat_id,
            text=message_text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=False  # Показываем превью для Zoom-ссылки
        )

        # Сохраняем информацию о напоминании
        job_name = context.job.name if hasattr(context, 'job') and context.job else f"manual_{datetime.now().timestamp()}"
        config.add_active_reminder(message.message_id, chat_id, job_name)

        logger.info(f"Отправлено напоминание в чат {chat_id}, сообщение {message.message_id}")

    except Exception as e:
        logger.error(f"Ошибка при отправке напоминания: {e}")


@restricted
async def cancel_meeting_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработчик нажатия кнопки отмены планёрки - начало Conversation"""
    query = update.callback_query
    await query.answer()

    # Сохраняем ID сообщения для редактирования
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
    """Обработчик выбора причины"""
    query = update.callback_query
    await query.answer()
    
    # Добавляем проверку на корректность данных
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
    
    # Сохраняем выбранную причину
    context.user_data["selected_reason"] = reason
    context.user_data["reason_index"] = reason_index
    
    if reason_index == 2:  # "Перенесём на другой день"
        # Показываем выбор даты
        return await show_date_selection(update, context)
    else:
        # Автоматические причины - сразу подтверждаем
        return await confirm_cancellation(update, context)


async def show_date_selection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Показать выбор даты для переноса"""
    query = update.callback_query
    
    # Создаем клавиатуру с датами на ближайшие 2 недели
    keyboard = []
    today = datetime.now(TIMEZONE)
    
    # Добавляем ближайшие дни планёрок
    meeting_dates = []
    for i in range(1, 15):  # 2 недели вперед
        next_day = today + timedelta(days=i)
        if next_day.weekday() in MEETING_DAYS:
            date_str = next_day.strftime("%d.%m.%Y (%A)")
            callback_data = f"date_{next_day.strftime('%Y-%m-%d')}"
            meeting_dates.append((next_day, date_str, callback_data))
    
    # Группируем по неделям
    current_week = []
    for date_obj, date_str, callback_data in meeting_dates:
        week_num = date_obj.isocalendar()[1]
        
        # Начинаем новую неделю
        if not current_week or week_num != current_week[0][0]:
            if current_week:
                # Добавляем предыдущую неделю как строку кнопок
                week_buttons = [InlineKeyboardButton(date_str, callback_data=cb) for _, date_str, cb in current_week]
                keyboard.append(week_buttons)
            
            current_week = [(week_num, date_str, callback_data)]
        else:
            current_week.append((week_num, date_str, callback_data))
    
    # Добавляем последнюю неделю
    if current_week:
        week_buttons = [InlineKeyboardButton(date_str, callback_data=cb) for _, date_str, cb in current_week]
        keyboard.append(week_buttons)
    
    # Кнопка для ввода своей даты
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
    """Обработчик выбора даты"""
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
    
    # Обработка выбранной даты
    try:
        selected_date_str = query.data.split("_")[1]
        selected_date = datetime.strptime(selected_date_str, "%Y-%m-%d")
        
        # Сохраняем выбранную дату
        context.user_data["selected_date"] = selected_date_str
        context.user_data["selected_date_display"] = selected_date.strftime("%d.%m.%Y")
        
        # Переходим к подтверждению
        return await show_confirmation(update, context)
    except (IndexError, ValueError) as e:
        logger.error(f"Ошибка обработки выбора даты: {e}, data: {query.data}")
        await query.message.reply_text("❌ Произошла ошибка. Попробуйте снова.")
        return ConversationHandler.END


async def handle_custom_date(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработка ввода пользовательской даты"""
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
        # Пробуем разные форматы дат
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
        
        # Проверяем, что дата в будущем
        today = datetime.now(TIMEZONE).date()
        if selected_date.date() <= today:
            await update.message.reply_text(
                "❌ Дата должна быть в будущем! Попробуйте снова:"
            )
            return CONFIRMING_DATE
        
        # Проверяем, что это день планёрки
        if selected_date.weekday() not in MEETING_DAYS:
            days_names = ["понедельник", "вторник", "среду", "четверг", "пятницу", "субботу", "воскресенье"]
            meeting_days_names = [days_names[i] for i in MEETING_DAYS]
            
            await update.message.reply_text(
                f"❌ В эту дату нет планёрок! Планёрки бывают по {', '.join(meeting_days_names)}.\n"
                "Попробуйте снова или отправьте 'отмена':"
            )
            return CONFIRMING_DATE
        
        # Сохраняем дату
        context.user_data["selected_date"] = selected_date.strftime("%Y-%m-%d")
        context.user_data["selected_date_display"] = selected_date.strftime("%d.%m.%Y")
        
        # Переходим к подтверждению
        return await show_confirmation_text(update, context)
        
    except ValueError as e:
        await update.message.reply_text(
            "❌ Неверный формат даты! Используйте ДД.ММ.ГГГГ\n"
            "Например: 15.12.2024\n\n"
            "Попробуйте снова или отправьте 'отмена':"
        )
        return CONFIRMING_DATE


async def show_confirmation_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Показать подтверждение отмены для текстового ввода"""
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
    """Показать подтверждение отмены для callback"""
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
    """Показать подтверждение отмены после выбора причины/даты"""
    return await show_confirmation(update, context)


async def back_to_reasons_from_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Вернуться к выбору причины из подтверждения"""
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
    """Выполнить отмену планёрки"""
    query = update.callback_query
    await query.answer()
    
    config = BotConfig()
    reason = context.user_data.get("selected_reason", "Причина не указана")
    reason_index = context.user_data.get("reason_index", -1)
    username = query.from_user.username or "Неизвестный пользователь"
    
    # Формируем финальное сообщение
    if reason_index == 2:  # "Перенесём на другой день"
        selected_date = context.user_data.get("selected_date_display", "дата не указана")
        final_message = f"❌ @{username} отменил сегодняшнюю планёрку\n\n📅 <b>Перенос на {selected_date}</b>"
    else:
        final_message = f"❌ @{username} отменил планёрку\n\n📝 <b>Причина:</b> {reason}"
    
    # Удаляем задание из планировщика
    original_message_id = context.user_data.get("original_message_id")
    job_name_to_remove = None
    
    # Ищем и удаляем соответствующие задания
    if original_message_id:
        # ИСПРАВЛЕНО: Используем нашу вспомогательную функцию
        for job in get_jobs_from_queue(context.application.job_queue):
            if job.name in config.active_reminders:
                reminder_data = config.active_reminders[job.name]
                if str(reminder_data.get("message_id")) == str(original_message_id):
                    job.schedule_removal()
                    job_name_to_remove = job.name
                    logger.info(f"Задание {job.name} удалено из планировщика")
                    break
        
        # Удаляем из конфига
        if job_name_to_remove:
            config.remove_active_reminder(job_name_to_remove)
            logger.info(f"Задание {job_name_to_remove} удалено из конфига")
    
    # Отправляем финальное сообщение
    await query.edit_message_text(
        text=final_message,
        parse_mode=ParseMode.HTML
    )
    
    logger.info(f"Планёрка отменена @{username} — {reason}")
    
    # Очищаем user_data
    context.user_data.clear()
    
    return ConversationHandler.END


async def cancel_conversation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отмена диалога"""
    if update.message:
        await update.message.reply_text("❌ Диалог отменен.")
    elif update.callback_query:
        await update.callback_query.answer("Диалог отменен", show_alert=True)
        await update.callback_query.edit_message_text("❌ Диалог отменен.")
    
    context.user_data.clear()
    return ConversationHandler.END


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик команды /start"""
    await update.message.reply_text(
        "🤖 <b>Бот для напоминаний о планёрке активен!</b>\n\n"
        f"📅 <b>Напоминания отправляются:</b>\n"
        f"• Понедельник\n• Среда\n• Пятница\n"
        f"⏰ <b>Время:</b> {MEETING_TIME['hour']:02d}:{MEETING_TIME['minute']:02d} по МСК\n\n"
        f"📊 <b>Факты дня отправляются:</b> каждый день в {FACTS_TIME['hour']:02d}:{FACTS_TIME['minute']:02d} по МСК\n"
        f"📚 <b>Категории:</b> Наука, Технологии, Кино, Музыка, Игры\n\n"
        "🔧 <b>Доступные команды:</b>\n"
        "/info - информация о боте\n"
        "/jobs - список запланированных задач\n"
        "/test - тестовое напоминание (через 5 сек)\n"
        "/testnow - мгновенное тестовое напоминание\n"
        "/testfact - тестовый факт (сейчас)\n\n"
        "👮‍♂️ <b>Команды для администраторов:</b>\n"
        "/setchat - установить чат для уведомлений\n"
        "/adduser @username - добавить пользователя\n"
        "/removeuser @username - удалить пользователя\n"
        "/users - список пользователей\n"
        "/togglefacts - вкл/выкл отправку фактов\n"
        "/cancelall - отменить все напоминания",
        parse_mode=ParseMode.HTML
    )


@restricted
async def set_chat(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Установка чата для уведомлений"""
    chat_id = update.effective_chat.id
    chat_title = update.effective_chat.title or "личный чат"

    config = BotConfig()
    config.chat_id = chat_id

    await update.message.reply_text(
        f"✅ <b>Чат установлен:</b> {chat_title}\n"
        f"<b>Chat ID:</b> {chat_id}\n\n"
        "Напоминания будут отправляться в этот чат.",
        parse_mode=ParseMode.HTML
    )

    logger.info(f"Установлен чат {chat_title} ({chat_id})")


@restricted
async def test_fact(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправка тестового факта"""
    config = BotConfig()
    if not config.chat_id:
        await update.message.reply_text("❌ Сначала установите чат командой /setchat")
        return

    # Получаем тестовый факт
    category = random.choice(list(FACT_CATEGORIES.keys()))
    fact = await get_daily_fact(category)
    
    # Форматируем сообщение
    message_text = (
        f"<b>🧪 Тестовый факт из мира {category}</b>\n\n"
        f"{fact}\n\n"
        f"#тест #фактдня #{FACT_CATEGORIES[category]}"
    )
    
    # Создаем клавиатуру с реакциями
    keyboard = [
        [
            InlineKeyboardButton("👍", callback_data=f"fact_like_{category}"),
            InlineKeyboardButton("👎", callback_data=f"fact_dislike_{category}"),
            InlineKeyboardButton("💩", callback_data=f"fact_poop_{category}")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        text=message_text,
        reply_markup=reply_markup,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True
    )
    
    logger.info(f"Отправлен тестовый факт категории '{category}'")


@restricted
async def toggle_facts(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Включить/выключить отправку фактов"""
    config = BotConfig()
    config.daily_facts_enabled = not config.daily_facts_enabled
    
    status = "✅ включена" if config.daily_facts_enabled else "❌ отключена"
    
    await update.message.reply_text(
        f"📊 <b>Отправка фактов дня {status}</b>\n\n"
        f"⏰ Время отправки: {FACTS_TIME['hour']:02d}:{FACTS_TIME['minute']:02d} по МСК\n"
        f"📚 Категории: {', '.join(FACT_CATEGORIES.keys())}",
        parse_mode=ParseMode.HTML
    )
    
    logger.info(f"Отправка фактов дня {status}")


@restricted
async def show_info(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показать информацию о боте"""
    config = BotConfig()
    chat_id = config.chat_id

    if chat_id:
        status = f"✅ <b>Чат установлен</b> (ID: {chat_id})"
    else:
        status = "❌ <b>Чат не установлен</b>. Используйте /setchat"

    # Подсчет запланированных задач
    all_jobs = get_jobs_from_queue(context.application.job_queue)
    meeting_job_count = len([j for j in all_jobs 
                           if j.name and j.name.startswith("meeting_reminder_")])
    fact_job_count = len([j for j in all_jobs 
                         if j.name and j.name.startswith("daily_fact_")])
    
    # Статус отправки фактов
    facts_status = "✅ включена" if config.daily_facts_enabled else "❌ отключена"
    
    await update.message.reply_text(
        f"📊 <b>Информация о боте:</b>\n\n"
        f"{status}\n"
        f"📅 <b>Дни планёрок:</b> понедельник, среда, пятница\n"
        f"⏰ <b>Время планёрок:</b> {MEETING_TIME['hour']:02d}:{MEETING_TIME['minute']:02d} по МСК\n"
        f"📚 <b>Факты дня:</b> {facts_status}\n"
        f"⏰ <b>Время фактов:</b> {FACTS_TIME['hour']:02d}:{FACTS_TIME['minute']:02d} по МСК\n"
        f"👥 <b>Разрешённые пользователи:</b> {len(config.allowed_users)}\n"
        f"📋 <b>Активные напоминания:</b> {len(config.active_reminders)}\n"
        f"⏳ <b>Запланировано напоминаний:</b> {meeting_job_count}\n"
        f"📝 <b>Запланировано фактов:</b> {fact_job_count}\n\n"
        f"<b>Категории фактов:</b>\n"
        f"{', '.join(FACT_CATEGORIES.keys())}\n\n"
        f"Используйте /users для списка пользователей\n"
        f"Используйте /jobs для списка задач\n"
        f"Используйте /togglefacts для управления фактами",
        parse_mode=ParseMode.HTML
    )


@restricted
async def test_reminder(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправка тестового напоминания через 5 секунд"""
    config = BotConfig()
    if not config.chat_id:
        await update.message.reply_text("❌ Сначала установите чат командой /setchat")
        return

    # Создаем задачу, которая вызовет send_reminder через 5 секунд
    context.application.job_queue.run_once(
        send_reminder, 
        5, 
        chat_id=config.chat_id,
        name=f"test_reminder_{datetime.now().timestamp()}"
    )

    # Получаем информацию о текущем дне
    weekday = datetime.now(TIMEZONE).weekday()
    day_names_ru = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
    current_day = day_names_ru[weekday]
    
    # Проверяем, является ли сегодня день планёрки
    if weekday in MEETING_DAYS:
        day_type = "день планёрки ✅"
        day_emoji = "📋"
    else:
        day_type = "не день планёрки ⚠️"
        day_emoji = "⏸️"
    
    # Форматируем Zoom-ссылку для предпросмотра
    zoom_preview = ZOOM_LINK[:50] + "..." if len(ZOOM_LINK) > 50 else ZOOM_LINK
    zoom_status = "установлена ✅" if ZOOM_LINK and ZOOM_LINK != "https://us04web.zoom.us/j/1234567890?pwd=example" else "не установлена ⚠️"
    
    # Показываем пример сообщения
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
    """Мгновенная отправка тестового уведомления"""
    config = BotConfig()
    if not config.chat_id:
        await update.message.reply_text("❌ Сначала установите чат командой /setchat")
        return

    # Получаем информацию о текущем дне
    weekday = datetime.now(TIMEZONE).weekday()
    day_names_ru = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
    current_day = day_names_ru[weekday]
    
    # Проверяем, является ли сегодня день планёрки
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
    
    # Создаем контекст для отправки
    class DummyJob:
        def __init__(self):
            self.name = f"manual_test_{datetime.now().timestamp()}"
    
    dummy_context = ContextTypes.DEFAULT_TYPE(context.application)
    dummy_context.job = DummyJob()
    dummy_context.bot = context.bot
    
    await send_reminder(dummy_context)


@restricted
async def list_jobs(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показать список запланированных задач"""
    # ИСПРАВЛЕНО: Используем нашу вспомогательную функцию
    jobs = get_jobs_from_queue(context.application.job_queue)
    
    if not jobs:
        await update.message.reply_text("📭 <b>Нет запланированных задач.</b>", parse_mode=ParseMode.HTML)
        return
    
    meeting_jobs = [j for j in jobs if j.name and j.name.startswith("meeting_reminder_")]
    fact_jobs = [j for j in jobs if j.name and j.name.startswith("daily_fact_")]
    other_jobs = [j for j in jobs if j not in meeting_jobs and j not in fact_jobs]
    
    message = "📋 <b>Запланированные задачи:</b>\n\n"
    
    if meeting_jobs:
        message += "🔔 <b>Напоминания о планёрках:</b>\n"
        for job in sorted(meeting_jobs, key=lambda j: j.next_t):
            next_time = job.next_t.astimezone(TIMEZONE)
            message += f"  • {next_time.strftime('%d.%m.%Y %H:%M')} ({job.name})\n"
    
    if fact_jobs:
        message += "\n📊 <b>Факты дня:</b>\n"
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
    """Добавить пользователя в список разрешенных"""
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
    """Удалить пользователя из списка разрешенных"""
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
    """Показать список разрешенных пользователей"""
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
    """Отменить все запланированные напоминания"""
    # ИСПРАВЛЕНО: Используем нашу вспомогательную функцию
    jobs = get_jobs_from_queue(context.application.job_queue)
    canceled_count = 0
    
    for job in jobs[:]:  # Копируем список для безопасного удаления
        if job.name and (job.name.startswith("meeting_reminder_") or job.name.startswith("daily_fact_")):
            job.schedule_removal()
            canceled_count += 1
    
    # Очищаем активные напоминания в конфиге
    config = BotConfig()
    config.clear_active_reminders()
    
    await update.message.reply_text(
        f"✅ <b>Отменено {canceled_count} напоминаний(я)</b>\n"
        f"Очищены активные напоминания в конфиге",
        parse_mode=ParseMode.HTML
    )
    logger.info(f"Отменено {canceled_count} напоминаний")


def calculate_next_reminder() -> datetime:
    """Рассчитать время следующего напоминания"""
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


def calculate_next_fact() -> datetime:
    """Рассчитать время следующей отправки факта"""
    now = datetime.now(TIMEZONE)
    
    # Время отправки сегодня
    fact_time = now.replace(
        hour=FACTS_TIME['hour'],
        minute=FACTS_TIME['minute'],
        second=0,
        microsecond=0
    )
    
    if now < fact_time:
        return fact_time
    
    # Если время уже прошло, планируем на завтра
    return fact_time + timedelta(days=1)


async def schedule_daily_fact(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Запланировать отправку следующего факта"""
    config = BotConfig()
    
    # Проверяем, включены ли факты
    if not config.daily_facts_enabled:
        logger.info("Отправка фактов отключена, планирование пропущено")
        # Пробуем снова через 24 часа
        context.application.job_queue.run_once(
            schedule_daily_fact,
            86400  # 24 часа
        )
        return
    
    next_time = calculate_next_fact()
    chat_id = config.chat_id

    if not chat_id:
        logger.warning("Chat ID не установлен, планирование фактов отложено")
        # Пробуем снова через час
        context.application.job_queue.run_once(
            schedule_daily_fact,
            3600
        )
        return

    now = datetime.now(TIMEZONE)
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

            # Планируем следующий факт после отправки текущего
            context.application.job_queue.run_once(
                schedule_daily_fact,
                delay + 60,
                chat_id=chat_id,
                name=f"fact_scheduler_{next_time.strftime('%Y%m%d_%H%M')}"
            )

            logger.info(f"Следующий факт запланирован на {next_time}")
        else:
            logger.info(f"Факт на {next_time} уже запланирован")


async def schedule_next_reminder(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Запланировать следующее напоминание"""
    next_time = calculate_next_reminder()
    config = BotConfig()
    chat_id = config.chat_id

    if not chat_id:
        logger.warning("Chat ID не установлен, планирование отложено")
        # Пробуем снова через час
        context.application.job_queue.run_once(
            schedule_next_reminder,
            3600
        )
        return

    now = datetime.now(TIMEZONE)
    delay = (next_time - now).total_seconds()

    if delay > 0:
        job_name = f"meeting_reminder_{next_time.strftime('%Y%m%d_%H%M')}"
        
        # Проверяем, нет ли уже такой задачи
        existing_jobs = [j for j in get_jobs_from_queue(context.application.job_queue) 
                        if j.name == job_name]
        
        if not existing_jobs:
            context.application.job_queue.run_once(
                send_reminder,
                delay,
                chat_id=chat_id,
                name=job_name
            )

            # Планируем следующее напоминание после отправки текущего
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
    """Очистка старых и дублирующих задач"""
    # ИСПРАВЛЕНО: Используем нашу вспомогательную функцию
    jobs = get_jobs_from_queue(job_queue)
    jobs_by_name = {}
    jobs_to_remove = []
    
    for job in jobs:
        if job.name:
            if job.name in jobs_by_name:
                # Дубликат - удаляем старую задачу
                jobs_to_remove.append(jobs_by_name[job.name])
            jobs_by_name[job.name] = job
    
    # Удаляем просроченные задачи
    now = datetime.now(TIMEZONE)
    for job in jobs:
        if job.next_t and job.next_t < now:
            jobs_to_remove.append(job)
    
    # Удаляем найденные задачи
    for job in jobs_to_remove:
        job.schedule_removal()
    
    if jobs_to_remove:
        logger.info(f"Очищено {len(jobs_to_remove)} старых/дублирующих задач")


def restore_reminders(application: Application) -> None:
    """Восстановить активные напоминания после перезапуска"""
    config = BotConfig()
    now = datetime.now(TIMEZONE)
    
    for job_name, reminder_data in config.active_reminders.items():
        try:
            # Проверяем, актуально ли еще напоминание
            created_at = datetime.fromisoformat(reminder_data["created_at"])
            if (now - created_at).days < 1:  # Напоминание не старше суток
                # Запланируем отправку отмены (имитация)
                application.job_queue.run_once(
                    lambda ctx: logger.info(f"Восстановлено напоминание {job_name}"),
                    1,
                    name=f"restored_{job_name}"
                )
        except Exception as e:
            logger.error(f"Ошибка восстановления напоминания {job_name}: {e}")


def main() -> None:
    """Основная функция запуска бота"""
    if not TOKEN:
        logger.error("❌ Токен бота не найден! Установите переменную окружения TELEGRAM_BOT_TOKEN")
        return
    
    # Проверяем наличие Zoom-ссылки
    if not ZOOM_LINK or ZOOM_LINK == "https://us04web.zoom.us/j/1234567890?pwd=example":
        logger.warning("⚠️  Zoom-ссылка не установлена или используется значение по умолчанию!")
        logger.warning("   Установите переменную окружения ZOOM_MEETING_LINK")
        logger.warning("   Пример: export ZOOM_MEETING_LINK='https://zoom.us/j/your-meeting-id?pwd=your-password'")
    else:
        logger.info(f"✅ Zoom-ссылка загружена (первые 50 символов): {ZOOM_LINK[:50]}...")
    
    logger.info("✅ Все API для фактов бесплатные и не требуют API ключей!")

    try:
        application = Application.builder().token(TOKEN).build()

        # Создаем ConversationHandler для отмены планёрки
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
        application.add_handler(CommandHandler("testfact", test_fact))
        application.add_handler(CommandHandler("togglefacts", toggle_facts))
        application.add_handler(CommandHandler("jobs", list_jobs))
        application.add_handler(CommandHandler("adduser", add_user))
        application.add_handler(CommandHandler("removeuser", remove_user))
        application.add_handler(CommandHandler("users", list_users))
        application.add_handler(CommandHandler("cancelall", cancel_all))

        # Добавляем ConversationHandler
        application.add_handler(conv_handler)

        # Обработчик реакций на факты
        application.add_handler(CallbackQueryHandler(fact_reaction_callback, pattern="^fact_"))

        # Очистка старых задач при запуске
        cleanup_old_jobs(application.job_queue)
        
        # Восстановление напоминаний
        restore_reminders(application)

        # Запуск планировщиков
        application.job_queue.run_once(
            lambda context: schedule_next_reminder(context),
            3
        )
        
        application.job_queue.run_once(
            lambda context: schedule_daily_fact(context),
            5
        )

        logger.info("🤖 Бот запущен и готов к работе!")
        logger.info(f"⏰ Планёрки: {', '.join(['Пн', 'Ср', 'Пт'])} в {MEETING_TIME['hour']:02d}:{MEETING_TIME['minute']:02d}")
        logger.info(f"📊 Факты дня: ежедневно в {FACTS_TIME['hour']:02d}:{FACTS_TIME['minute']:02d}")
        logger.info(f"📚 Категории фактов: {', '.join(FACT_CATEGORIES.keys())}")
        logger.info(f"🌐 Используемые API: Useless Facts, Numbers API, Movie Quotes, Genrenator, GamerPower")
        
        application.run_polling(allowed_updates=Update.ALL_TYPES)

    except Exception as e:
        logger.error(f"❌ Критическая ошибка при запуске бота: {e}")
        raise


if __name__ == "__main__":
    main()
