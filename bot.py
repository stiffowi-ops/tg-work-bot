import os
import json
import random
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple
from functools import wraps
import pytz

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    JobQueue,
    ConversationHandler,
    MessageHandler,
    filters
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
TEAM_DATA_FILE = "team_data.json"

# Время планёрки (9:15 по Москве)
MEETING_TIME = {"hour": 9, "minute": 15}
TIMEZONE = pytz.timezone("Europe/Moscow")

# Дни недели для планёрки (понедельник=0, среда=2, пятница=4)
MEETING_DAYS = [0, 2, 4]

# Время отраслевой встречи (вторник 12:00 по МСК)
INDUSTRY_MEETING_TIME = {"hour": 12, "minute": 0}
INDUSTRY_MEETING_DAY = [1]  # Вторник

# Русские названия месяцев
MONTHS_RU = {
    1: "ЯНВАРЯ", 2: "ФЕВРАЛЯ", 3: "МАРТА", 4: "АПРЕЛЯ",
    5: "МАЯ", 6: "ИЮНЯ", 7: "ИЮЛЯ", 8: "АВГУСТА",
    9: "СЕНТЯБРЯ", 10: "ОКТЯБРЯ", 11: "НОЯБРЯ", 12: "ДЕКАБРЯ"
}

# Русские названия дней недели
WEEKDAYS_RU = ["ПОНЕДЕЛЬНИК", "ВТОРНИК", "СРЕДА", "ЧЕТВЕРГ", "ПЯТНИЦА", "СУББОТА", "ВОСКРЕСЕНЬЕ"]

# Текст для отраслевой встречи
INDUSTRY_MEETING_TEXTS = [
    "🏢 𝗢ТРАСЛЕВАЯ ВСТРЕЧА\n\n🎯 Что делаем:\n• Обсудим итоги за недели\n• Новые тренды и инсайты\n• Обмен опытом с коллегами\n• Запланируем мероприятия на следующую\n\n🕐 Начало: 12:00 по МСК\n📍 Формат: Zoom-конференция\n\n🔗 Всех причастных ждём! {zoom_link} | 👈",
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

# Состояния для ConversationHandler
(
    # Основные состояния
    MAIN_HELP_MENU,
    
    # Состояния для документов
    DOCUMENTS_MENU,
    ADD_FILE_NAME,
    ADD_FILE_DESCRIPTION,
    DELETE_FILE_MENU,
    
    # Состояния для ссылок
    LINKS_MENU,
    
    # Состояния для команды
    TEAM_MENU,
    VIEW_TEAM_MEMBER,
    
    # Состояния для настроек
    SETTINGS_MENU,
    
    # Состояния для управления командой (админы)
    TEAM_MANAGEMENT,
    ADD_MEMBER_START,
    ADD_MEMBER_NAME,
    ADD_MEMBER_POSITION,
    ADD_MEMBER_CITY,
    ADD_MEMBER_YEAR,
    ADD_MEMBER_RESPONSIBILITIES,
    ADD_MEMBER_CONTACT_TOPICS,
    ADD_MEMBER_ABOUT,
    ADD_MEMBER_TELEGRAM,
    ADD_MEMBER_CONFIRM,
    EDIT_MEMBER_MENU,
    EDIT_MEMBER_SELECT,
    EDIT_MEMBER_FIELD,
    EDIT_MEMBER_VALUE,
    DELETE_MEMBER_MENU,
    DELETE_MEMBER_CONFIRM,
    
    # Состояния для отмены встреч
    SELECTING_REASON,
    SELECTING_INDUSTRY_REASON,
    SELECTING_DATE,
    CONFIRM_RESCHEDULE,
) = range(32)

# Настройка логирования
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", 
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ========== КЛАСС КОНФИГА ==========

class BotConfig:
    """Класс для управления конфигурацией бота"""
    
    def __init__(self):
        self.config_file = CONFIG_FILE
        self.help_data_file = HELP_DATA_FILE
        self.team_data_file = TEAM_DATA_FILE
        self.data = self._load_config()
        self.help_data = self._load_help_data()
        self.team_data = self._load_team_data()
    
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
                    if "admins" not in data:
                        data["admins"] = ["Stiff_OWi", "gshabanov"]
                    if "chat_id" not in data:
                        data["chat_id"] = None
                    if "rescheduled_meetings" not in data:
                        data["rescheduled_meetings"] = {}
                    return data
            except Exception as e:
                logger.error(f"Ошибка загрузки конфига: {e}")
        return {
            "chat_id": None,
            "allowed_users": ["Stiff_OWi", "gshabanov"],
            "admins": ["Stiff_OWi", "gshabanov"],
            "active_reminders": {},
            "rescheduled_meetings": {}
        }
    
    def _load_help_data(self) -> Dict[str, Any]:
        """Загрузить данные помощи"""
        default_data = {
            "files": {},  # Пустой словарь - файлы будут добавляться через бота
            "links": {
                "ya_crm": {
                    "name": "🌐 YA CRM",
                    "url": YA_CRM_LINK,
                    "description": "Корпоративная CRM система"
                },
                "wiki": {
                    "name": "📊 WIKI Отрасли",
                    "url": WIKI_LINK,
                    "description": "Презентации и спичи по отраслям"
                },
                "helpy_bot": {
                    "name": "🛠️ Бот Helpy",
                    "url": HELPY_BOT_LINK,
                    "description": "Помощник по внутренним вопросам"
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
    
    def _load_team_data(self) -> Dict[str, Any]:
        """Загрузить данные о команде"""
        default_data = {
            "members": {},  # Словарь для хранения карточек сотрудников
            "last_id": 0    # Счетчик для генерации ID
        }
        
        if os.path.exists(self.team_data_file):
            try:
                with open(self.team_data_file, 'r', encoding='utf-8') as f:
                    loaded_data = json.load(f)
                    # Проверяем наличие обязательных полей
                    if "members" not in loaded_data:
                        loaded_data["members"] = {}
                    if "last_id" not in loaded_data:
                        loaded_data["last_id"] = len(loaded_data["members"])
                    return loaded_data
            except Exception as e:
                logger.error(f"Ошибка загрузки данных команды: {e}")
        
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
    
    def save_team_data(self) -> None:
        """Сохранить данные команды"""
        try:
            with open(self.team_data_file, 'w', encoding='utf-8') as f:
                json.dump(self.team_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Ошибка сохранения данных команды: {e}")
    
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
    
    def is_allowed(self, username: str) -> bool:
        """Проверяет, разрешен ли пользователь"""
        return username in self.allowed_users
    
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
    def active_reminders(self) -> Dict[str, Dict]:
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
    def rescheduled_meetings(self) -> Dict[str, Dict]:
        return self.data.get("rescheduled_meetings", {})
    
    def add_rescheduled_meeting(self, original_job: str, new_time: datetime, meeting_type: str, 
                               rescheduled_by: str, original_message_id: int) -> None:
        """Добавить информацию о перенесенной встрече"""
        meeting_id = f"rescheduled_{int(datetime.now().timestamp())}"
        
        self.data["rescheduled_meetings"][meeting_id] = {
            "original_job": original_job,
            "new_time": new_time.isoformat(),
            "meeting_type": meeting_type,
            "rescheduled_by": rescheduled_by,
            "original_message_id": original_message_id,
            "rescheduled_at": datetime.now(TIMEZONE).isoformat(),
            "status": "scheduled"
        }
        self.save()
    
    def update_rescheduled_meeting_status(self, meeting_id: str, status: str) -> None:
        """Обновить статус перенесенной встречи"""
        if meeting_id in self.data["rescheduled_meetings"]:
            self.data["rescheduled_meetings"][meeting_id]["status"] = status
            self.save()
    
    # Методы для работы с файлами
    def add_file(self, file_id: str, file_name: str, description: str) -> bool:
        """Добавить новый файл"""
        try:
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
    
    # Методы для работы с командой
    def add_team_member(self, member_data: Dict) -> str:
        """Добавить нового члена команды"""
        try:
            # Генерируем ID
            self.team_data["last_id"] += 1
            member_id = str(self.team_data["last_id"])
            
            # Сохраняем дату добавления
            member_data["added_date"] = datetime.now().isoformat()
            member_data["last_updated"] = datetime.now().isoformat()
            
            # Добавляем в данные
            self.team_data["members"][member_id] = member_data
            
            self.save_team_data()
            logger.info(f"Добавлен член команды: {member_data.get('name', 'Без имени')} (ID: {member_id})")
            return member_id
            
        except Exception as e:
            logger.error(f"Ошибка добавления члена команды: {e}")
            return ""
    
    def update_team_member(self, member_id: str, field: str, value: str) -> bool:
        """Обновить поле члена команды"""
        if member_id in self.team_data["members"]:
            self.team_data["members"][member_id][field] = value
            self.team_data["members"][member_id]["last_updated"] = datetime.now().isoformat()
            self.save_team_data()
            logger.info(f"Обновлен член команды {member_id}: {field} = {value}")
            return True
        return False
    
    def delete_team_member(self, member_id: str) -> bool:
        """Удалить члена команды"""
        if member_id in self.team_data["members"]:
            deleted_name = self.team_data["members"][member_id].get("name", "Без имени")
            del self.team_data["members"][member_id]
            self.save_team_data()
            logger.info(f"Удален член команды: {deleted_name} (ID: {member_id})")
            return True
        return False
    
    def get_team_member(self, member_id: str) -> Optional[Dict]:
        """Получить данные члена команды"""
        return self.team_data["members"].get(member_id)
    
    def get_all_team_members(self) -> Dict[str, Dict]:
        """Получить всех членов команды"""
        return self.team_data["members"]

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

def get_industry_meeting_text() -> str:
    """Получаем текст для отраслевой встречи с ссылкой"""
    zoom_link = INDUSTRY_ZOOM_LINK
    
    if zoom_link == DEFAULT_ZOOM_LINK:
        zoom_link_formatted = f'<a href="{zoom_link}">[НЕ НАСТРОЕНА - настройте INDUSTRY_MEETING_LINK]</a>'
    else:
        zoom_link_formatted = f'<a href="{zoom_link}">Присоединиться к Zoom</a>'
    
    text = random.choice(INDUSTRY_MEETING_TEXTS)
    return text.format(zoom_link=zoom_link_formatted)

def get_greeting_by_meeting_day() -> str:
    """Специальные приветствия для дней планёрок"""
    weekday = datetime.now(TIMEZONE).weekday()
    day_names_ru = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
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

def get_available_dates(meeting_type: str, start_from: datetime = None) -> List[datetime]:
    """Получить доступные даты для переноса встречи"""
    if not start_from:
        start_from = datetime.now(TIMEZONE)
    
    available_dates = []
    
    if meeting_type == "planerka":
        # Для планёрки ищем ближайшие дни планёрок (пн, ср, пт)
        days_ahead = 1
        while len(available_dates) < 5:  # Показываем 5 ближайших доступных дат
            check_date = start_from + timedelta(days=days_ahead)
            if check_date.weekday() in MEETING_DAYS:
                # Устанавливаем время планёрки (9:15)
                meeting_time = check_date.replace(
                    hour=MEETING_TIME['hour'],
                    minute=MEETING_TIME['minute'],
                    second=0,
                    microsecond=0
                )
                available_dates.append(meeting_time)
            days_ahead += 1
    
    elif meeting_type == "industry":
        # Для отраслевой встречи ищем ближайшие вторники
        days_ahead = 1
        while len(available_dates) < 5:
            check_date = start_from + timedelta(days=days_ahead)
            if check_date.weekday() in INDUSTRY_MEETING_DAY:
                # Устанавливаем время отраслевой встречи (12:00)
                meeting_time = check_date.replace(
                    hour=INDUSTRY_MEETING_TIME['hour'],
                    minute=INDUSTRY_MEETING_TIME['minute'],
                    second=0,
                    microsecond=0
                )
                available_dates.append(meeting_time)
            days_ahead += 1
    
    return available_dates

def format_date_for_display(date: datetime) -> str:
    """Форматировать дату для отображения"""
    weekday = WEEKDAYS_RU[date.weekday()]
    day = date.day
    month = MONTHS_RU[date.month]
    year = date.year
    
    time_str = date.strftime("%H:%M")
    
    return f"{weekday}, {day} {month} {year} в {time_str}"

def format_date_button(date: datetime) -> str:
    """Форматировать дату для кнопки"""
    return date.strftime("%d.%m.%Y %H:%M")

# ========== КЛАВИАТУРЫ ==========

def create_help_keyboard() -> InlineKeyboardMarkup:
    """Создаем клавиатуру для главного меню помощи"""
    keyboard = [
        [InlineKeyboardButton("📄 Документы", callback_data="help_documents")],
        [InlineKeyboardButton("🔗 Полезные ссылки", callback_data="help_links")],
        [InlineKeyboardButton("👥 О команде", callback_data="help_team")],
        [InlineKeyboardButton("⚙️ Настройки", callback_data="help_settings")]
    ]
    return InlineKeyboardMarkup(keyboard)

def create_documents_keyboard(config: BotConfig, username: str = None) -> InlineKeyboardMarkup:
    """Создаем клавиатуру для документов"""
    keyboard = []
    files = config.help_data.get("files", {})
    
    for file_key, file_data in files.items():
        keyboard.append([
            InlineKeyboardButton(
                f"📄 {file_data.get('name', 'Без названия')[:30]}", 
                callback_data=f"file_{file_key}"
            )
        ])
    
    # Кнопка добавления файла (только для админов)
    if username and config.is_admin(username):
        keyboard.append([InlineKeyboardButton("➕ Добавить файл", callback_data="add_file")])
    
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="help_back")])
    
    return InlineKeyboardMarkup(keyboard)

def create_links_keyboard(config: BotConfig) -> InlineKeyboardMarkup:
    """Создаем клавиатуру для ссылок"""
    keyboard = []
    links = config.help_data.get("links", {})
    
    for link_key, link_data in links.items():
        keyboard.append([
            InlineKeyboardButton(
                link_data.get('name', 'Ссылка'), 
                callback_data=f"link_{link_key}"
            )
        ])
    
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="help_back")])
    
    return InlineKeyboardMarkup(keyboard)

def create_team_keyboard(config: BotConfig, username: str = None) -> InlineKeyboardMarkup:
    """Создаем клавиатуру для команды"""
    keyboard = []
    members = config.get_all_team_members()
    
    if not members:
        keyboard.append([InlineKeyboardButton("👥 Пока нет членов команды", callback_data="no_members")])
    else:
        for member_id, member_data in members.items():
            name = member_data.get('name', 'Без имени')
            # Обрезаем имя если слишком длинное
            display_name = name[:30] + "..." if len(name) > 30 else name
            keyboard.append([
                InlineKeyboardButton(
                    f"👤 {display_name}", 
                    callback_data=f"team_member_{member_id}"
                )
            ])
    
    # Кнопка управления командой (только для админов)
    if username and config.is_admin(username):
        keyboard.append([InlineKeyboardButton("⚙️ Управление командой", callback_data="team_management")])
    
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="help_back")])
    
    return InlineKeyboardMarkup(keyboard)

def create_settings_keyboard(config: BotConfig, username: str = None) -> InlineKeyboardMarkup:
    """Создаем клавиатуру для настроек"""
    keyboard = []
    
    # Кнопки только для админов
    if username and config.is_admin(username):
        keyboard.append([InlineKeyboardButton("🗑️ Удалить файл", callback_data="delete_file_menu")])
    
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="help_back")])
    
    return InlineKeyboardMarkup(keyboard)

def create_delete_file_keyboard(config: BotConfig) -> InlineKeyboardMarkup:
    """Создаем клавиатуру для удаления файлов"""
    keyboard = []
    files = config.help_data.get("files", {})
    
    for file_key, file_data in files.items():
        keyboard.append([
            InlineKeyboardButton(
                f"🗑️ {file_data.get('name', 'Без названия')[:30]}", 
                callback_data=f"delete_file_{file_key}"
            )
        ])
    
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="help_settings")])
    
    return InlineKeyboardMarkup(keyboard)

def create_team_management_keyboard() -> InlineKeyboardMarkup:
    """Создаем клавиатуру для управления командой (админы)"""
    keyboard = [
        [InlineKeyboardButton("➕ Добавить сотрудника", callback_data="team_add_member")],
        [InlineKeyboardButton("✏️ Редактировать карточку", callback_data="team_edit_member")],
        [InlineKeyboardButton("🗑️ Удалить сотрудника", callback_data="team_delete_member")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="help_team")]
    ]
    return InlineKeyboardMarkup(keyboard)

def create_edit_member_keyboard(config: BotConfig) -> InlineKeyboardMarkup:
    """Создаем клавиатуру для выбора сотрудника для редактирования"""
    keyboard = []
    members = config.get_all_team_members()
    
    for member_id, member_data in members.items():
        name = member_data.get('name', 'Без имени')
        display_name = name[:25] + "..." if len(name) > 25 else name
        keyboard.append([
            InlineKeyboardButton(
                f"✏️ {display_name}", 
                callback_data=f"edit_member_select_{member_id}"
            )
        ])
    
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="team_management")])
    
    return InlineKeyboardMarkup(keyboard)

def create_edit_field_keyboard() -> InlineKeyboardMarkup:
    """Создаем клавиатуру для выбора поля для редактирования"""
    keyboard = [
        [InlineKeyboardButton("👤 Имя", callback_data="edit_field_name")],
        [InlineKeyboardButton("💼 Должность", callback_data="edit_field_position")],
        [InlineKeyboardButton("🏙️ Город", callback_data="edit_field_city")],
        [InlineKeyboardButton("📅 Год в компании", callback_data="edit_field_year")],
        [InlineKeyboardButton("🎯 Ответственность", callback_data="edit_field_responsibilities")],
        [InlineKeyboardButton("💬 Вопросы для обращений", callback_data="edit_field_contact_topics")],
        [InlineKeyboardButton("📝 О себе", callback_data="edit_field_about")],
        [InlineKeyboardButton("📱 Telegram", callback_data="edit_field_telegram")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="team_edit_member")]
    ]
    return InlineKeyboardMarkup(keyboard)

def create_delete_member_keyboard(config: BotConfig) -> InlineKeyboardMarkup:
    """Создаем клавиатуру для выбора сотрудника для удаления"""
    keyboard = []
    members = config.get_all_team_members()
    
    for member_id, member_data in members.items():
        name = member_data.get('name', 'Без имени')
        display_name = name[:25] + "..." if len(name) > 25 else name
        keyboard.append([
            InlineKeyboardButton(
                f"🗑️ {display_name}", 
                callback_data=f"delete_member_select_{member_id}"
            )
        ])
    
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="team_management")])
    
    return InlineKeyboardMarkup(keyboard)

def create_confirm_delete_keyboard(member_id: str) -> InlineKeyboardMarkup:
    """Создаем клавиатуру для подтверждения удаления"""
    keyboard = [
        [InlineKeyboardButton("✅ Да, удалить", callback_data=f"delete_confirm_yes_{member_id}")],
        [InlineKeyboardButton("❌ Нет, отмена", callback_data=f"delete_confirm_no_{member_id}")]
    ]
    return InlineKeyboardMarkup(keyboard)

def create_date_selection_keyboard(meeting_type: str, available_dates: List[datetime]) -> InlineKeyboardMarkup:
    """Создать клавиатуру для выбора даты переноса"""
    keyboard = []
    
    for i, date in enumerate(available_dates):
        date_str = format_date_button(date)
        display_date = format_date_for_display(date)
        callback_data = f"reschedule_date_{meeting_type}_{date_str}"
        
        keyboard.append([InlineKeyboardButton(f"📅 {display_date}", callback_data=callback_data)])
    
    keyboard.append([InlineKeyboardButton("↩️ Назад", callback_data=f"cancel_back_{meeting_type}")])
    
    return InlineKeyboardMarkup(keyboard)

def create_confirm_reschedule_keyboard(meeting_type: str, selected_date: datetime, job_name: str) -> InlineKeyboardMarkup:
    """Создать клавиатуру для подтверждения переноса"""
    keyboard = [
        [
            InlineKeyboardButton("✅ Да, перенести", 
                               callback_data=f"confirm_reschedule_{meeting_type}_{selected_date.strftime('%Y%m%d_%H%M')}_{job_name}"),
            InlineKeyboardButton("❌ Нет, отмена", 
                               callback_data=f"cancel_reschedule_{meeting_type}")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

# ========== ФУНКЦИИ ДЛЯ КОМАНДЫ HELP ==========

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик /help - главное меню помощи"""
    keyboard = create_help_keyboard()
    
    await update.message.reply_text(
        "📋 <b>Главное меню помощи</b>\n\n"
        "Выберите раздел:",
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML
    )

async def handle_help_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработка callback от меню помощи"""
    query = update.callback_query
    await query.answer()
    
    config = BotConfig()
    username = query.from_user.username
    
    if query.data == "help_documents":
        keyboard = create_documents_keyboard(config, username)
        await query.edit_message_text(
            "📄 <b>Документы</b>\n\n"
            "Выберите документ:",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
        return DOCUMENTS_MENU
    
    elif query.data == "help_links":
        keyboard = create_links_keyboard(config)
        await query.edit_message_text(
            "🔗 <b>Полезные ссылки</b>\n\n"
            "Выберите ссылку:",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
        return LINKS_MENU
    
    elif query.data == "help_team":
        keyboard = create_team_keyboard(config, username)
        await query.edit_message_text(
            "👥 <b>О команде</b>\n\n"
            "Выберите сотрудника:",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
        return TEAM_MENU
    
    elif query.data == "help_settings":
        if not config.is_admin(username):
            await query.answer("❌ У вас нет прав доступа к настройкам", show_alert=True)
            return MAIN_HELP_MENU
        
        keyboard = create_settings_keyboard(config, username)
        await query.edit_message_text(
            "⚙️ <b>Настройки</b>\n\n"
            "Выберите действие:",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
        return SETTINGS_MENU
    
    elif query.data == "help_back":
        keyboard = create_help_keyboard()
        await query.edit_message_text(
            "📋 <b>Главное меню помощи</b>\n\n"
            "Выберите раздел:",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
        return MAIN_HELP_MENU
    
    elif query.data == "add_file":
        if not config.is_admin(username):
            await query.answer("❌ У вас нет прав для добавления файлов", show_alert=True)
            return DOCUMENTS_MENU
        
        await query.edit_message_text(
            "📄 <b>Добавление файла</b>\n\n"
            "Отправьте мне файл (документ, изображение и т.д.), который хотите добавить.\n\n"
            "После отправки файла я спрошу у вас описание для него.",
            parse_mode=ParseMode.HTML
        )
        return ADD_FILE_NAME
    
    elif query.data == "delete_file_menu":
        if not config.is_admin(username):
            await query.answer("❌ У вас нет прав для удаления файлов", show_alert=True)
            return SETTINGS_MENU
        
        files = config.help_data.get("files", {})
        if not files:
            await query.edit_message_text(
                "🗑️ <b>Удаление файла</b>\n\n"
                "Нет доступных файлов для удаления.",
                parse_mode=ParseMode.HTML
            )
            return SETTINGS_MENU
        
        keyboard = create_delete_file_keyboard(config)
        await query.edit_message_text(
            "🗑️ <b>Удаление файла</b>\n\n"
            "Выберите файл для удаления:",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
        return DELETE_FILE_MENU
    
    elif query.data.startswith("delete_file_"):
        if not config.is_admin(username):
            await query.answer("❌ У вас нет прав для удаления файлов", show_alert=True)
            return DELETE_FILE_MENU
        
        file_key = query.data.replace("delete_file_", "")
        files = config.help_data.get("files", {})
        
        if file_key in files:
            file_name = files[file_key].get("name", "Без названия")
            config.delete_file(file_key)
            
            await query.edit_message_text(
                f"✅ Файл <b>{file_name}</b> успешно удален!",
                parse_mode=ParseMode.HTML
            )
            
            # Возвращаемся в меню настроек
            keyboard = create_settings_keyboard(config, username)
            await query.message.reply_text(
                "⚙️ <b>Настройки</b>\n\n"
                "Выберите действие:",
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML
            )
            return SETTINGS_MENU
        else:
            await query.answer("❌ Файл не найден", show_alert=True)
            return DELETE_FILE_MENU
    
    elif query.data.startswith("file_"):
        file_key = query.data.replace("file_", "")
        files = config.help_data.get("files", {})
        
        if file_key in files:
            file_data = files[file_key]
            file_id = file_data.get("file_id")
            file_name = file_data.get("name", "Без названия")
            description = file_data.get("description", "Без описания")
            
            try:
                await context.bot.send_document(
                    chat_id=query.from_user.id,
                    document=file_id,
                    caption=f"📄 <b>{file_name}</b>\n\n{description}",
                    parse_mode=ParseMode.HTML
                )
                await query.answer(f"📄 Файл '{file_name}' отправлен вам в личные сообщения", show_alert=True)
            except Exception as e:
                logger.error(f"Ошибка отправки файла: {e}")
                await query.answer("❌ Не удалось отправить файл", show_alert=True)
        
        # Возвращаемся в меню документов
        keyboard = create_documents_keyboard(config, username)
        await query.edit_message_text(
            "📄 <b>Документы</b>\n\n"
            "Выберите документ:",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
        return DOCUMENTS_MENU
    
    elif query.data.startswith("link_"):
        link_key = query.data.replace("link_", "")
        links = config.help_data.get("links", {})
        
        if link_key in links:
            link_data = links[link_key]
            link_name = link_data.get("name", "Ссылка")
            link_url = link_data.get("url", "#")
            description = link_data.get("description", "Без описания")
            
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔗 Открыть ссылку", url=link_url)],
                [InlineKeyboardButton("⬅️ Назад", callback_data="help_links")]
            ])
            
            await query.edit_message_text(
                f"🔗 <b>{link_name}</b>\n\n"
                f"{description}\n\n"
                f"Ссылка: {link_url}",
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=False
            )
            return LINKS_MENU
    
    elif query.data.startswith("team_member_"):
        member_id = query.data.replace("team_member_", "")
        member_data = config.get_team_member(member_id)
        
        if member_data:
            # Формируем карточку сотрудника
            card_text = format_team_member_card(member_data)
            
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Назад", callback_data="help_team")]
            ])
            
            await query.edit_message_text(
                card_text,
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=False
            )
            return TEAM_MENU
        else:
            await query.answer("❌ Сотрудник не найден", show_alert=True)
            return TEAM_MENU
    
    elif query.data == "team_management":
        if not config.is_admin(username):
            await query.answer("❌ У вас нет прав для управления командой", show_alert=True)
            return TEAM_MENU
        
        keyboard = create_team_management_keyboard()
        await query.edit_message_text(
            "⚙️ <b>Управление командой</b>\n\n"
            "Выберите действие:",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
        return TEAM_MANAGEMENT
    
    elif query.data == "team_add_member":
        if not config.is_admin(username):
            await query.answer("❌ У вас нет прав для добавления сотрудников", show_alert=True)
            return TEAM_MANAGEMENT
        
        context.user_data["new_member"] = {}
        await query.edit_message_text(
            "👤 <b>Добавление нового сотрудника</b>\n\n"
            "Введите имя и фамилию сотрудника:",
            parse_mode=ParseMode.HTML
        )
        return ADD_MEMBER_NAME
    
    elif query.data == "team_edit_member":
        if not config.is_admin(username):
            await query.answer("❌ У вас нет прав для редактирования сотрудников", show_alert=True)
            return TEAM_MANAGEMENT
        
        members = config.get_all_team_members()
        if not members:
            await query.edit_message_text(
                "✏️ <b>Редактирование карточки</b>\n\n"
                "Нет сотрудников для редактирования.",
                parse_mode=ParseMode.HTML
            )
            return TEAM_MANAGEMENT
        
        keyboard = create_edit_member_keyboard(config)
        await query.edit_message_text(
            "✏️ <b>Редактирование карточки</b>\n\n"
            "Выберите сотрудника для редактирования:",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
        return EDIT_MEMBER_MENU
    
    elif query.data == "team_delete_member":
        if not config.is_admin(username):
            await query.answer("❌ У вас нет прав для удаления сотрудников", show_alert=True)
            return TEAM_MANAGEMENT
        
        members = config.get_all_team_members()
        if not members:
            await query.edit_message_text(
                "🗑️ <b>Удаление сотрудника</b>\n\n"
                "Нет сотрудников для удаления.",
                parse_mode=ParseMode.HTML
            )
            return TEAM_MANAGEMENT
        
        keyboard = create_delete_member_keyboard(config)
        await query.edit_message_text(
            "🗑️ <b>Удаление сотрудника</b>\n\n"
            "Выберите сотрудника для удаления:",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
        return DELETE_MEMBER_MENU
    
    elif query.data.startswith("edit_member_select_"):
        member_id = query.data.replace("edit_member_select_", "")
        context.user_data["edit_member_id"] = member_id
        
        keyboard = create_edit_field_keyboard()
        await query.edit_message_text(
            "✏️ <b>Редактирование карточки</b>\n\n"
            "Выберите поле для редактирования:",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
        return EDIT_MEMBER_FIELD
    
    elif query.data.startswith("delete_member_select_"):
        member_id = query.data.replace("delete_member_select_", "")
        member_data = config.get_team_member(member_id)
        
        if member_data:
            context.user_data["delete_member_id"] = member_id
            member_name = member_data.get("name", "Без имени")
            
            keyboard = create_confirm_delete_keyboard(member_id)
            await query.edit_message_text(
                f"🗑️ <b>Подтверждение удаления</b>\n\n"
                f"Вы уверены, что хотите удалить карточку сотрудника:\n\n"
                f"<b>{member_name}</b>?\n\n"
                f"Это действие нельзя отменить.",
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML
            )
            return DELETE_MEMBER_CONFIRM
        else:
            await query.answer("❌ Сотрудник не найден", show_alert=True)
            return DELETE_MEMBER_MENU
    
    elif query.data.startswith("delete_confirm_yes_"):
        member_id = query.data.replace("delete_confirm_yes_", "")
        
        if config.delete_team_member(member_id):
            await query.edit_message_text(
                f"✅ Карточка сотрудника успешно удалена!",
                parse_mode=ParseMode.HTML
            )
            
            # Возвращаемся в меню управления командой
            keyboard = create_team_management_keyboard()
            await query.message.reply_text(
                "⚙️ <b>Управление командой</b>\n\n"
                "Выберите действие:",
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML
            )
            return TEAM_MANAGEMENT
        else:
            await query.answer("❌ Не удалось удалить карточку", show_alert=True)
            return DELETE_MEMBER_CONFIRM
    
    elif query.data.startswith("delete_confirm_no_"):
        # Возвращаемся в меню удаления
        keyboard = create_delete_member_keyboard(config)
        await query.edit_message_text(
            "🗑️ <b>Удаление сотрудника</b>\n\n"
            "Выберите сотрудника для удаления:",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
        return DELETE_MEMBER_MENU
    
    # Если callback не обработан, возвращаемся в главное меню
    keyboard = create_help_keyboard()
    await query.edit_message_text(
        "📋 <b>Главное меню помощи</b>\n\n"
        "Выберите раздел:",
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML
    )
    return MAIN_HELP_MENU

def format_team_member_card(member_data: Dict) -> str:
    """Форматирует карточку сотрудника"""
    name = member_data.get("name", "Не указано")
    position = member_data.get("position", "Не указано")
    city = member_data.get("city", "Не указано")
    year = member_data.get("year", "Не указано")
    responsibilities = member_data.get("responsibilities", "Не указано")
    contact_topics = member_data.get("contact_topics", "Не указано")
    about = member_data.get("about", "Не указано")
    telegram = member_data.get("telegram", "Не указано")
    
    card = f"👤 <b>{name}</b>\n"
    card += f"💼 {position}\n\n"
    
    card += f"📍 <b>Город:</b> {city}\n"
    card += f"📅 <b>В компании с:</b> {year}\n\n"
    
    card += f"🎯 <b>Сфера ответственности:</b>\n{responsibilities}\n\n"
    
    card += f"💬 <b>По каким вопросам обращаться:</b>\n{contact_topics}\n\n"
    
    card += f"📝 <b>О себе:</b>\n{about}\n\n"
    
    if telegram and telegram != "Не указано":
        if telegram.startswith("@"):
            card += f"📱 <b>Telegram:</b> <a href=\"https://t.me/{telegram[1:]}\">{telegram}</a>"
        else:
            card += f"📱 <b>Telegram:</b> {telegram}"
    
    return card

# ========== ОБРАБОТЧИКИ ДЛЯ ДОБАВЛЕНИЯ СОТРУДНИКА ==========

async def handle_add_member_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработка имени сотрудника"""
    if update.message:
        name = update.message.text.strip()
        if name:
            context.user_data["new_member"]["name"] = name
            
            await update.message.reply_text(
                "💼 Теперь введите должность сотрудника:",
                parse_mode=ParseMode.HTML
            )
            return ADD_MEMBER_POSITION
        else:
            await update.message.reply_text(
                "❌ Имя не может быть пустым. Попробуйте еще раз:",
                parse_mode=ParseMode.HTML
            )
            return ADD_MEMBER_NAME
    
    return ADD_MEMBER_NAME

async def handle_add_member_position(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработка должности сотрудника"""
    if update.message:
        position = update.message.text.strip()
        if position:
            context.user_data["new_member"]["position"] = position
            
            await update.message.reply_text(
                "🏙️ Теперь введите город проживания:",
                parse_mode=ParseMode.HTML
            )
            return ADD_MEMBER_CITY
        else:
            await update.message.reply_text(
                "❌ Должность не может быть пустой. Попробуйте еще раз:",
                parse_mode=ParseMode.HTML
            )
            return ADD_MEMBER_POSITION
    
    return ADD_MEMBER_POSITION

async def handle_add_member_city(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработка города сотрудника"""
    if update.message:
        city = update.message.text.strip()
        if city:
            context.user_data["new_member"]["city"] = city
            
            await update.message.reply_text(
                "📅 Теперь введите год прихода в компанию (например: 2022):",
                parse_mode=ParseMode.HTML
            )
            return ADD_MEMBER_YEAR
        else:
            await update.message.reply_text(
                "❌ Город не может быть пустым. Попробуйте еще раз:",
                parse_mode=ParseMode.HTML
            )
            return ADD_MEMBER_CITY
    
    return ADD_MEMBER_CITY

async def handle_add_member_year(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработка года прихода в компанию"""
    if update.message:
        year = update.message.text.strip()
        if year and year.isdigit() and len(year) == 4:
            context.user_data["new_member"]["year"] = year
            
            await update.message.reply_text(
                "🎯 Теперь введите сферу ответственности (можно несколько пунктов через запятую):",
                parse_mode=ParseMode.HTML
            )
            return ADD_MEMBER_RESPONSIBILITIES
        else:
            await update.message.reply_text(
                "❌ Год должен быть в формате ГГГГ (например: 2022). Попробуйте еще раз:",
                parse_mode=ParseMode.HTML
            )
            return ADD_MEMBER_YEAR
    
    return ADD_MEMBER_YEAR

async def handle_add_member_responsibilities(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработка сферы ответственности"""
    if update.message:
        responsibilities = update.message.text.strip()
        if responsibilities:
            context.user_data["new_member"]["responsibilities"] = responsibilities
            
            await update.message.reply_text(
                "💬 Теперь введите, по каким вопросам можно обращаться (можно несколько пунктов через запятую):",
                parse_mode=ParseMode.HTML
            )
            return ADD_MEMBER_CONTACT_TOPICS
        else:
            await update.message.reply_text(
                "❌ Сфера ответственности не может быть пустой. Попробуйте еще раз:",
                parse_mode=ParseMode.HTML
            )
            return ADD_MEMBER_RESPONSIBILITIES
    
    return ADD_MEMBER_RESPONSIBILITIES

async def handle_add_member_contact_topics(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработка вопросов для обращений"""
    if update.message:
        contact_topics = update.message.text.strip()
        if contact_topics:
            context.user_data["new_member"]["contact_topics"] = contact_topics
            
            await update.message.reply_text(
                "📝 Теперь кратко опишите сотрудника (хобби, интересы, факты):",
                parse_mode=ParseMode.HTML
            )
            return ADD_MEMBER_ABOUT
        else:
            await update.message.reply_text(
                "❌ Вопросы для обращений не могут быть пустыми. Попробуйте еще раз:",
                parse_mode=ParseMode.HTML
            )
            return ADD_MEMBER_CONTACT_TOPICS
    
    return ADD_MEMBER_CONTACT_TOPics

async def handle_add_member_about(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработка информации о себе"""
    if update.message:
        about = update.message.text.strip()
        if about:
            context.user_data["new_member"]["about"] = about
            
            await update.message.reply_text(
                "📱 Теперь введите Telegram username (например: @username или просто username):",
                parse_mode=ParseMode.HTML
            )
            return ADD_MEMBER_TELEGRAM
        else:
            await update.message.reply_text(
                "❌ Информация о себе не может быть пустой. Попробуйте еще раз:",
                parse_mode=ParseMode.HTML
            )
            return ADD_MEMBER_ABOUT
    
    return ADD_MEMBER_ABOUT

async def handle_add_member_telegram(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработка Telegram username"""
    if update.message:
        telegram = update.message.text.strip()
        
        # Добавляем @ если его нет
        if telegram and not telegram.startswith("@"):
            telegram = "@" + telegram
        
        context.user_data["new_member"]["telegram"] = telegram if telegram else "Не указано"
        
        # Показываем превью карточки для подтверждения
        config = BotConfig()
        username = update.effective_user.username
        
        if not config.is_admin(username):
            await update.message.reply_text("❌ У вас нет прав для добавления сотрудников")
            context.user_data.clear()
            return ConversationHandler.END
        
        member_data = context.user_data["new_member"]
        card_preview = format_team_member_card(member_data)
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Да, всё верно", callback_data="add_member_confirm")],
            [InlineKeyboardButton("❌ Нет, отменить", callback_data="add_member_cancel")]
        ])
        
        await update.message.reply_text(
            f"👤 <b>Предпросмотр карточки:</b>\n\n{card_preview}\n\n"
            f"Всё верно?",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=False
        )
        return ADD_MEMBER_CONFIRM
    
    return ADD_MEMBER_TELEGRAM

async def handle_add_member_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Подтверждение добавления сотрудника"""
    query = update.callback_query
    await query.answer()
    
    config = BotConfig()
    username = query.from_user.username
    
    if not config.is_admin(username):
        await query.answer("❌ У вас нет прав для добавления сотрудников", show_alert=True)
        context.user_data.clear()
        return ConversationHandler.END
    
    if query.data == "add_member_confirm":
        member_data = context.user_data.get("new_member", {})
        
        if member_data:
            member_id = config.add_team_member(member_data)
            
            if member_id:
                await query.edit_message_text(
                    f"✅ <b>Сотрудник успешно добавлен!</b>\n\n"
                    f"Имя: {member_data.get('name', 'Не указано')}\n"
                    f"ID карточки: {member_id}",
                    parse_mode=ParseMode.HTML
                )
            else:
                await query.edit_message_text(
                    "❌ Не удалось добавить сотрудника. Попробуйте еще раз.",
                    parse_mode=ParseMode.HTML
                )
        else:
            await query.edit_message_text(
                "❌ Данные сотрудника не найдены. Попробуйте еще раз.",
                parse_mode=ParseMode.HTML
            )
        
        # Очищаем временные данные
        context.user_data.clear()
        
        # Возвращаемся в меню управления командой
        keyboard = create_team_management_keyboard()
        await query.message.reply_text(
            "⚙️ <b>Управление командой</b>\n\n"
            "Выберите действие:",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
        return TEAM_MANAGEMENT
    
    elif query.data == "add_member_cancel":
        context.user_data.clear()
        await query.edit_message_text(
            "❌ Добавление сотрудника отменено.",
            parse_mode=ParseMode.HTML
        )
        
        # Возвращаемся в меню управления командой
        keyboard = create_team_management_keyboard()
        await query.message.reply_text(
            "⚙️ <b>Управление командой</b>\n\n"
            "Выберите действие:",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
        return TEAM_MANAGEMENT
    
    return ADD_MEMBER_CONFIRM

# ========== ОБРАБОТЧИКИ ДЛЯ РЕДАКТИРОВАНИЯ СОТРУДНИКА ==========

async def handle_edit_member_field(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработка выбора поля для редактирования"""
    query = update.callback_query
    await query.answer()
    
    config = BotConfig()
    username = query.from_user.username
    
    if not config.is_admin(username):
        await query.answer("❌ У вас нет прав для редактирования сотрудников", show_alert=True)
        return EDIT_MEMBER_FIELD
    
    field_map = {
        "edit_field_name": ("👤 Имя", "name"),
        "edit_field_position": ("💼 Должность", "position"),
        "edit_field_city": ("🏙️ Город", "city"),
        "edit_field_year": ("📅 Год в компании", "year"),
        "edit_field_responsibilities": ("🎯 Ответственность", "responsibilities"),
        "edit_field_contact_topics": ("💬 Вопросы для обращений", "contact_topics"),
        "edit_field_about": ("📝 О себе", "about"),
        "edit_field_telegram": ("📱 Telegram", "telegram")
    }
    
    if query.data in field_map:
        field_name, field_key = field_map[query.data]
        context.user_data["edit_field_key"] = field_key
        context.user_data["edit_field_name"] = field_name
        
        member_id = context.user_data.get("edit_member_id")
        member_data = config.get_team_member(member_id)
        
        if member_data:
            current_value = member_data.get(field_key, "Не указано")
            
            await query.edit_message_text(
                f"✏️ <b>Редактирование: {field_name}</b>\n\n"
                f"Текущее значение: <i>{current_value}</i>\n\n"
                f"Введите новое значение:",
                parse_mode=ParseMode.HTML
            )
            return EDIT_MEMBER_VALUE
        else:
            await query.answer("❌ Сотрудник не найден", show_alert=True)
            return EDIT_MEMBER_FIELD
    
    return EDIT_MEMBER_FIELD

async def handle_edit_member_value(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработка нового значения поля"""
    if update.message:
        new_value = update.message.text.strip()
        field_key = context.user_data.get("edit_field_key")
        field_name = context.user_data.get("edit_field_name")
        member_id = context.user_data.get("edit_member_id")
        
        config = BotConfig()
        username = update.effective_user.username
        
        if not config.is_admin(username):
            await update.message.reply_text("❌ У вас нет прав для редактирования сотрудников")
            context.user_data.clear()
            return ConversationHandler.END
        
        if field_key and member_id and new_value:
            # Для года проверяем формат
            if field_key == "year":
                if not (new_value.isdigit() and len(new_value) == 4):
                    await update.message.reply_text(
                        "❌ Год должен быть в формате ГГГГ (например: 2022). Попробуйте еще раз:",
                        parse_mode=ParseMode.HTML
                    )
                    return EDIT_MEMBER_VALUE
            
            # Для Telegram добавляем @ если нужно
            if field_key == "telegram" and new_value and not new_value.startswith("@"):
                new_value = "@" + new_value
            
            if config.update_team_member(member_id, field_key, new_value):
                await update.message.reply_text(
                    f"✅ <b>{field_name}</b> успешно обновлено!",
                    parse_mode=ParseMode.HTML
                )
                
                # Показываем обновленную карточку
                member_data = config.get_team_member(member_id)
                if member_data:
                    card_text = format_team_member_card(member_data)
                    
                    keyboard = InlineKeyboardMarkup([
                        [InlineKeyboardButton("✏️ Редактировать еще", callback_data="edit_member_select_" + member_id)],
                        [InlineKeyboardButton("⬅️ В меню управления", callback_data="team_management")]
                    ])
                    
                    await update.message.reply_text(
                        f"👤 <b>Обновленная карточка:</b>\n\n{card_text}",
                        reply_markup=keyboard,
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=False
                    )
                else:
                    # Возвращаемся в меню управления командой
                    keyboard = create_team_management_keyboard()
                    await update.message.reply_text(
                        "⚙️ <b>Управление командой</b>\n\n"
                        "Выберите действие:",
                        reply_markup=keyboard,
                        parse_mode=ParseMode.HTML
                    )
                
                # Очищаем временные данные
                context.user_data.pop("edit_field_key", None)
                context.user_data.pop("edit_field_name", None)
                
                return TEAM_MANAGEMENT
            else:
                await update.message.reply_text(
                    "❌ Не удалось обновить данные. Попробуйте еще раз.",
                    parse_mode=ParseMode.HTML
                )
                return EDIT_MEMBER_VALUE
    
    return EDIT_MEMBER_VALUE

# ========== ОБРАБОТЧИКИ ДЛЯ ФАЙЛОВ ==========

async def handle_file_upload(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработка загрузки файла"""
    config = BotConfig()
    user_id = update.effective_user.id
    username = update.effective_user.username
    
    if not config.is_admin(username):
        await update.message.reply_text("❌ У вас нет прав для добавления файлов")
        return ConversationHandler.END
    
    if update.message.document:
        # Получаем файл
        document = update.message.document
        file_id = document.file_id
        file_name = document.file_name or f"file_{document.file_id[:8]}.bin"
        
        # Сохраняем информацию о файле
        context.user_data["file_id"] = file_id
        context.user_data["file_name"] = file_name
        
        await update.message.reply_text(
            f"📄 Файл <b>{file_name}</b> получен.\n\n"
            "Теперь отправьте описание для этого файла:",
            parse_mode=ParseMode.HTML
        )
        
        return ADD_FILE_DESCRIPTION
    
    return ADD_FILE_NAME

async def handle_file_description(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработка описания файла"""
    config = BotConfig()
    user_id = update.effective_user.id
    username = update.effective_user.username
    
    if not config.is_admin(username):
        await update.message.reply_text("❌ У вас нет прав для добавления файлов")
        return ConversationHandler.END
    
    if "file_id" in context.user_data and "file_name" in context.user_data:
        description = update.message.text
        file_id = context.user_data["file_id"]
        file_name = context.user_data["file_name"]
        
        if config.add_file(file_id, file_name, description):
            await update.message.reply_text(
                f"✅ Файл <b>{file_name}</b> успешно добавлен!\n\n"
                f"Описание: {description}",
                parse_mode=ParseMode.HTML
            )
            
            # Очищаем временные данные
            context.user_data.clear()
            
            # Возвращаемся в меню документов
            keyboard = create_documents_keyboard(config, username)
            await update.message.reply_text(
                "📄 <b>Документы</b>\n\n"
                "Выберите документ:",
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML
            )
            return DOCUMENTS_MENU
        else:
            await update.message.reply_text("❌ Не удалось добавить файл")
    
    return ConversationHandler.END

async def cancel_upload(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отмена загрузки файла или добавления сотрудника"""
    context.user_data.clear()
    await update.message.reply_text("❌ Операция отменена.")
    return ConversationHandler.END

# ========== ФУНКЦИИ ПЛАНЁРОК ==========

async def send_reminder(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправка напоминания о планёрке"""
    config = BotConfig()
    chat_id = config.chat_id

    if not chat_id:
        logger.error("Chat ID не установлен!")
        return

    keyboard = [
        [InlineKeyboardButton("❌ Отменить планёрку", callback_data="cancel_meeting")]
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

async def send_industry_reminder(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправка напоминания об отраслевой встрече"""
    config = BotConfig()
    chat_id = config.chat_id

    if not chat_id:
        logger.error("Chat ID не установлен!")
        return

    keyboard = [
        [InlineKeyboardButton("❌ Отменить встречу", callback_data="cancel_industry")]
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

# ========== ФУНКЦИИ ДЛЯ ОТМЕНЫ ВСТРЕЧ ==========

async def cancel_meeting_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    
    config = BotConfig()
    username = query.from_user.username
    
    # Проверяем, разрешен ли пользователь отменять встречи
    if not config.is_allowed(username):
        await query.answer("❌ У вас нет прав для отмены встреч", show_alert=True)
        return ConversationHandler.END

    context.user_data["original_message_id"] = query.message.message_id
    context.user_data["original_chat_id"] = query.message.chat_id
    context.user_data["meeting_type"] = "planerka"

    keyboard = [
        [InlineKeyboardButton(option, callback_data=f"reason_{i}")]
        for i, option in enumerate(CANCELLATION_OPTIONS)
    ]

    await query.edit_message_text(
        text="📝 Выберите причину отмены планёрки:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

    return SELECTING_REASON

async def cancel_industry_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    
    config = BotConfig()
    username = query.from_user.username
    
    # Проверяем, разрешен ли пользователь отменять встречи
    if not config.is_allowed(username):
        await query.answer("❌ У вас нет прав для отмены встреч", show_alert=True)
        return ConversationHandler.END

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
    
    config = BotConfig()
    username = query.from_user.username
    
    # Проверяем, разрешен ли пользователь отменять встречи
    if not config.is_allowed(username):
        await query.answer("❌ У вас нет прав для отмены встреч", show_alert=True)
        return ConversationHandler.END
    
    try:
        reason_index = int(query.data.split("_")[1])
        reason = CANCELLATION_OPTIONS[reason_index]
        
        context.user_data["selected_reason"] = reason
        context.user_data["reason_index"] = reason_index
        
        # Если выбрана опция "Перенесём на другой день", показываем выбор даты
        if reason_index == 2:  # "Перенесём на другой день"
            meeting_type = context.user_data.get("meeting_type", "planerka")
            
            # Получаем доступные даты для переноса
            available_dates = get_available_dates(meeting_type)
            
            if not available_dates:
                await query.edit_message_text(
                    text="❌ Нет доступных дат для переноса встречи.",
                    parse_mode=ParseMode.HTML
                )
                return ConversationHandler.END
            
            keyboard = create_date_selection_keyboard(meeting_type, available_dates)
            
            await query.edit_message_text(
                text="📅 Выберите дату для переноса встречи:",
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML
            )
            
            return SELECTING_DATE
        
        else:
            # Для других причин - сразу отменяем
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
            return ConversationHandler.END
        
    except Exception as e:
        logger.error(f"Ошибка отмены планёрки: {e}")
        await query.message.reply_text("❌ Произошла ошибка")
    
    return SELECTING_REASON

async def select_industry_reason_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    
    config = BotConfig()
    username = query.from_user.username
    
    # Проверяем, разрешен ли пользователь отменять встречи
    if not config.is_allowed(username):
        await query.answer("❌ У вас нет прав для отмены встреч", show_alert=True)
        return ConversationHandler.END
    
    try:
        reason_index = int(query.data.split("_")[2])
        reason = INDUSTRY_CANCELLATION_OPTIONS[reason_index]
        
        context.user_data["selected_reason"] = reason
        context.user_data["reason_index"] = reason_index
        
        # Если выбрана опция "Переносим на другую дату", показываем выбор даты
        if reason_index == 1:  # "Переносим на другую дату"
            meeting_type = context.user_data.get("meeting_type", "industry")
            
            # Получаем доступные даты для переноса
            available_dates = get_available_dates(meeting_type)
            
            if not available_dates:
                await query.edit_message_text(
                    text="❌ Нет доступных дат для переноса встречи.",
                    parse_mode=ParseMode.HTML
                )
                return ConversationHandler.END
            
            keyboard = create_date_selection_keyboard(meeting_type, available_dates)
            
            await query.edit_message_text(
                text="📅 Выберите дату для переноса отраслевой встречи:",
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML
            )
            
            return SELECTING_DATE
        
        else:
            # Для других причин - сразу отменяем
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
            return ConversationHandler.END
        
    except Exception as e:
        logger.error(f"Ошибка отмены отраслевой встречи: {e}")
        await query.message.reply_text("❌ Произошла ошибка")
    
    return SELECTING_INDUSTRY_REASON

async def select_date_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработка выбора даты для переноса"""
    query = update.callback_query
    await query.answer()
    
    config = BotConfig()
    username = query.from_user.username
    
    # Проверяем, разрешен ли пользователь отменять встречи
    if not config.is_allowed(username):
        await query.answer("❌ У вас нет прав для отмены встреч", show_alert=True)
        return ConversationHandler.END
    
    try:
        # Парсим данные из callback
        parts = query.data.split("_")
        meeting_type = parts[2]
        date_str = parts[3] + "_" + parts[4]  # Дата и время
        
        # Преобразуем строку обратно в datetime
        selected_date = datetime.strptime(date_str, "%d.%m.%Y_%H:%M")
        selected_date = TIMEZONE.localize(selected_date)
        
        # Сохраняем выбранную дату
        context.user_data["selected_date"] = selected_date
        context.user_data["meeting_type"] = meeting_type
        
        # Находим оригинальную задачу
        config = BotConfig()
        original_message_id = context.user_data.get("original_message_id")
        job_name = None
        
        if original_message_id:
            for job in get_jobs_from_queue(context.application.job_queue):
                if job.name in config.active_reminders:
                    reminder_data = config.active_reminders[job.name]
                    if str(reminder_data.get("message_id")) == str(original_message_id):
                        job_name = job.name
                        break
        
        if not job_name:
            await query.edit_message_text(
                text="❌ Не удалось найти запланированную встречу.",
                parse_mode=ParseMode.HTML
            )
            return ConversationHandler.END
        
        # Показываем подтверждение
        formatted_date = format_date_for_display(selected_date)
        
        meeting_type_text = "планёрку" if meeting_type == "planerka" else "отраслевую встречу"
        
        keyboard = create_confirm_reschedule_keyboard(meeting_type, selected_date, job_name)
        
        await query.edit_message_text(
            text=f"📋 <b>Подтверждение переноса</b>\n\n"
                 f"Вы действительно хотите перенести {meeting_type_text} на:\n\n"
                 f"<b>{formatted_date}</b>?\n\n"
                 f"<i>После подтверждения встреча будет запланирована на новое время.</i>",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
        
        return CONFIRM_RESCHEDULE
        
    except Exception as e:
        logger.error(f"Ошибка выбора даты: {e}")
        await query.edit_message_text(
            text="❌ Произошла ошибка при выборе даты.",
            parse_mode=ParseMode.HTML
        )
        return ConversationHandler.END

async def confirm_reschedule_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Подтверждение переноса встречи"""
    query = update.callback_query
    await query.answer()
    
    config = BotConfig()
    username = query.from_user.username
    
    # Проверяем, разрешен ли пользователь отменять встречи
    if not config.is_allowed(username):
        await query.answer("❌ У вас нет прав для отмены встреч", show_alert=True)
        return ConversationHandler.END
    
    try:
        parts = query.data.split("_")
        meeting_type = parts[2]
        date_str = parts[3] + "_" + parts[4]
        job_name = parts[5]
        
        # Преобразуем строку обратно в datetime
        selected_date = datetime.strptime(date_str, "%Y%m%d_%H%M")
        selected_date = TIMEZONE.localize(selected_date)
        
        config = BotConfig()
        username = query.from_user.username or "Пользователь"
        original_message_id = context.user_data.get("original_message_id")
        reason = context.user_data.get("selected_reason", "Перенос на другую дату")
        
        # Удаляем оригинальную задачу
        job_found = False
        for job in get_jobs_from_queue(context.application.job_queue):
            if job.name == job_name:
                job.schedule_removal()
                config.remove_active_reminder(job.name)
                job_found = True
                break
        
        if not job_found:
            await query.edit_message_text(
                text="❌ Не удалось найти запланированную встречу для переноса.",
                parse_mode=ParseMode.HTML
            )
            return ConversationHandler.END
        
        # Создаем новую задачу на выбранную дату
        now = datetime.now(TIMEZONE)
        delay = (selected_date - now).total_seconds()
        
        if delay > 0:
            new_job_name = f"{meeting_type}_rescheduled_{selected_date.strftime('%Y%m%d_%H%M')}"
            
            # Запланировать новую встречу
            if meeting_type == "planerka":
                context.application.job_queue.run_once(
                    send_reminder,
                    delay,
                    chat_id=config.chat_id,
                    name=new_job_name
                )
            else:
                context.application.job_queue.run_once(
                    send_industry_reminder,
                    delay,
                    chat_id=config.chat_id,
                    name=new_job_name
                )
            
            # Сохраняем информацию о перенесенной встрече
            config.add_rescheduled_meeting(
                original_job=job_name,
                new_time=selected_date,
                meeting_type=meeting_type,
                rescheduled_by=username,
                original_message_id=original_message_id
            )
            
            formatted_date = format_date_for_display(selected_date)
            meeting_type_text = "планёрка" if meeting_type == "planerka" else "отраслевая встреча"
            
            await query.edit_message_text(
                text=f"✅ <b>{meeting_type_text.capitalize()} перенесена!</b>\n\n"
                     f"📅 <b>Новая дата:</b> {formatted_date}\n"
                     f"👤 <b>Перенес:</b> @{username}\n"
                     f"📝 <b>Причина:</b> {reason}",
                parse_mode=ParseMode.HTML
            )
            
            logger.info(f"{meeting_type_text.capitalize()} перенесена @{username} на {selected_date}")
            
        else:
            await query.edit_message_text(
                text="❌ Выбранная дата уже прошла. Пожалуйста, выберите другую дату.",
                parse_mode=ParseMode.HTML
            )
            # Возвращаем к выбору даты
            return SELECTING_DATE
        
    except Exception as e:
        logger.error(f"Ошибка переноса встречи: {e}")
        await query.edit_message_text(
            text="❌ Произошла ошибка при переносе встречи.",
            parse_mode=ParseMode.HTML
        )
    
    context.user_data.clear()
    return ConversationHandler.END

async def cancel_reschedule_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отмена переноса встречи"""
    query = update.callback_query
    await query.answer()
    
    config = BotConfig()
    username = query.from_user.username
    
    # Проверяем, разрешен ли пользователь отменять встречи
    if not config.is_allowed(username):
        await query.answer("❌ У вас нет прав для отмены встреч", show_alert=True)
        return ConversationHandler.END
    
    meeting_type = context.user_data.get("meeting_type", "planerka")
    reason_index = context.user_data.get("reason_index", 0)
    
    # Возвращаемся к выбору причины
    if meeting_type == "planerka":
        reason = CANCELLATION_OPTIONS[reason_index]
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
        
        logger.info(f"Планёрка отменена @{query.from_user.username}")
        
    elif meeting_type == "industry":
        reason = INDUSTRY_CANCELLATION_OPTIONS[reason_index]
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
        
        logger.info(f"Отраслевая встреча отменена @{query.from_user.username}")
    
    context.user_data.clear()
    return ConversationHandler.END

async def cancel_back_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Возврат назад из выбора даты"""
    query = update.callback_query
    await query.answer()
    
    config = BotConfig()
    username = query.from_user.username
    
    # Проверяем, разрешен ли пользователь отменять встречи
    if not config.is_allowed(username):
        await query.answer("❌ У вас нет прав для отмены встреч", show_alert=True)
        return ConversationHandler.END
    
    meeting_type = query.data.replace("cancel_back_", "")
    
    if meeting_type == "planerka":
        # Возвращаемся к выбору причины для планёрки
        keyboard = [
            [InlineKeyboardButton(option, callback_data=f"reason_{i}")]
            for i, option in enumerate(CANCELLATION_OPTIONS)
        ]
        
        await query.edit_message_text(
            text="📝 Выберите причину отмены планёрки:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
        return SELECTING_REASON
    
    elif meeting_type == "industry":
        # Возвращаемся к выбору причины для отраслевой встречи
        keyboard = [
            [InlineKeyboardButton(option, callback_data=f"industry_reason_{i}")]
            for i, option in enumerate(INDUSTRY_CANCELLATION_OPTIONS)
        ]
        
        await query.edit_message_text(
            text="📝 Выберите причину отмены отраслевой встречи:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
        return SELECTING_INDUSTRY_REASON
    
    return ConversationHandler.END

async def cancel_conversation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.message:
        await update.message.reply_text("❌ Диалог отменен.")
    elif update.callback_query:
        await update.callback_query.answer("Диалог отменен", show_alert=True)
        await update.callback_query.edit_message_text("❌ Диалог отменен.")
    
    context.user_data.clear()
    return ConversationHandler.END

# ========== ФУНКЦИИ ПЛАНИРОВАНИЯ ==========

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

# ========== ОСНОВНЫЕ КОМАНДЫ ==========

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик /start"""
    await update.message.reply_text(
        "🤖 <b>Бот для планёрок, отраслевых встреч и управления ресурсами!</b>\n\n"
        f"📅 <b>Планёрки:</b>\n"
        f"• Пн, Ср, Пт в 9:30 по МСК\n"
        f"• Возможность отмены и переноса (только для разрешенных пользователей)\n\n"
        f"📅 <b>Отраслевые встречи:</b>\n"
        f"• Вт в 12:00 по МСК\n"
        f"• Обсуждение трендов и инсайтов\n"
        f"• Нетворкинг с коллегами\n\n"
        f"📚 <b>Управление ресурсами:</b>\n"
        f"• Документы и файлы\n"
        f"• Полезные ссылки\n"
        f"• Информация о команде\n"
        f"• Настройки для админов\n\n"
        f"🔧 <b>Основные команды:</b>\n"
        f"/help - главное меню помощи\n"
        f"/info - информация о боте\n"
        f"/setchat - установить чат\n"
        f"/testindustry - тест отраслевой встречи\n"
        f"/testplanerka - тест планёрки\n",
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
        f"• Планёрки (9:30, Пн/Ср/Пт)\n"
        f"• Отраслевые встречи (12:00, Вт)\n\n"
        f"👑 <b>Права на отмену:</b> только разрешенные пользователи",
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
    
    meeting_jobs = len([j for j in all_jobs if j.name and j.name.startswith("meeting_reminder_")])
    industry_jobs = len([j for j in all_jobs if j.name and j.name.startswith("industry_meeting_")])
    rescheduled_jobs = len([j for j in all_jobs if j.name and "rescheduled" in j.name])
    
    now = datetime.now(TIMEZONE)
    weekday = now.weekday()
    day_names = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
    current_day = day_names[weekday]
    
    is_meeting_day = weekday in MEETING_DAYS
    is_industry_day = weekday in INDUSTRY_MEETING_DAY
    
    # Проверяем настройку ссылок
    zoom_status = "✅" if ZOOM_LINK != DEFAULT_ZOOM_LINK else "❌"
    industry_zoom_status = "✅" if INDUSTRY_ZOOM_LINK != DEFAULT_ZOOM_LINK else "❌"
    
    # Статистика ресурсов
    files_count = len(config.help_data.get("files", {}))
    links_count = len(config.help_data.get("links", {}))
    team_count = len(config.get_all_team_members())
    
    # Статистика перенесенных встреч
    rescheduled_count = len(config.rescheduled_meetings)
    active_rescheduled = len([m for m in config.rescheduled_meetings.values() 
                             if m.get("status") == "scheduled"])
    
    # Информация о разрешенных пользователях
    allowed_users = config.allowed_users
    allowed_count = len(allowed_users)
    admins_count = len(config.admins)
    
    await update.message.reply_text(
        f"📊 <b>Информация о боте:</b>\n\n"
        f"{status}\n\n"
        f"⏰ <b>Расписание:</b>\n"
        f"• Планёрки: 9:30 (Пн/Ср/Пт) {'✅ сегодня' if is_meeting_day else '❌ не сегодня'}\n"
        f"• Отраслевые: 12:00 (Вт) {'✅ сегодня' if is_industry_day else '❌ не сегодня'}\n\n"
        f"🔗 <b>Настройка ссылок:</b>\n"
        f"• Планёрки: {zoom_status}\n"
        f"• Отраслевые: {industry_zoom_status}\n\n"
        f"📋 <b>Активные задачи:</b>\n"
        f"• Планёрки: {meeting_jobs}\n"
        f"• Отраслевые: {industry_jobs}\n"
        f"• Перенесенные: {rescheduled_jobs}\n\n"
        f"🔄 <b>Перенесенные встречи:</b>\n"
        f"• Всего: {rescheduled_count}\n"
        f"• Активные: {active_rescheduled}\n\n"
        f"👥 <b>Права доступа:</b>\n"
        f"• Разрешенных пользователей: {allowed_count}\n"
        f"• Администраторов: {admins_count}\n\n"
        f"📚 <b>Ресурсы:</b>\n"
        f"• Файлов: {files_count}\n"
        f"• Ссылок: {links_count}\n"
        f"• Сотрудников в базе: {team_count}\n\n"
        f"📅 <b>Сегодня:</b> {current_day}, {now.day} {MONTHS_RU[now.month]} {now.year}\n\n"
        f"ℹ️ Используйте /help для доступа ко всем ресурсам",
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
        if "meeting_reminder" in job_name:
            icon = "🤝"
            type_text = "Планёрка"
        elif "industry_meeting" in job_name:
            icon = "🏢"
            type_text = "Отраслевая"
        elif "rescheduled" in job_name:
            icon = "🔄"
            type_text = "Перенесенная"
        else:
            icon = "🔧"
            type_text = "Другая"
        
        message += f"{icon} {next_time.strftime('%d.%m.%Y %H:%M')} - {type_text} ({job_name[:25]})\n"
    
    await update.message.reply_text(message, parse_mode=ParseMode.HTML)

@restricted
async def test_industry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Тестовая отправка отраслевой встречи"""
    config = BotConfig()
    if not config.chat_id:
        await update.message.reply_text("❌ Сначала установите чат командой /setchat")
        return

    await update.message.reply_text("⏳ <b>Отправляю тестовое уведомление об отраслевой встрече...</b>", parse_mode=ParseMode.HTML)
    await send_industry_reminder(context)

@restricted
async def test_planerka(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Тестовая отправка планёрки"""
    config = BotConfig()
    if not config.chat_id:
        await update.message.reply_text("❌ Сначала установите чат командой /setchat")
        return

    await update.message.reply_text("⏳ <b>Отправляю тестовое уведомление о планёрке...</b>", parse_mode=ParseMode.HTML)
    await send_reminder(context)

def main() -> None:
    if not TOKEN:
        logger.error("❌ Токен бота не найден!")
        return
    
    try:
        application = Application.builder().token(TOKEN).build()

        # ConversationHandler для помощи (главный)
        help_conv_handler = ConversationHandler(
            entry_points=[
                CommandHandler("help", help_command),
            ],
            states={
                # Основные состояния
                MAIN_HELP_MENU: [
                    CallbackQueryHandler(handle_help_callback, pattern="^help_"),
                    CallbackQueryHandler(handle_help_callback, pattern="^file_"),
                    CallbackQueryHandler(handle_help_callback, pattern="^link_"),
                    CallbackQueryHandler(handle_help_callback, pattern="^team_"),
                    CallbackQueryHandler(handle_help_callback, pattern="^add_"),
                    CallbackQueryHandler(handle_help_callback, pattern="^delete_"),
                    CallbackQueryHandler(handle_help_callback, pattern="^edit_"),
                    CallbackQueryHandler(handle_add_member_confirm, pattern="^add_member_"),
                    CallbackQueryHandler(handle_help_callback, pattern="^no_members$"),
                ],
                
                # Документы
                DOCUMENTS_MENU: [
                    CallbackQueryHandler(handle_help_callback, pattern="^file_|^add_file$|^help_back$"),
                ],
                ADD_FILE_NAME: [
                    MessageHandler(filters.Document.ALL, handle_file_upload),
                    CommandHandler("cancel", cancel_upload),
                ],
                ADD_FILE_DESCRIPTION: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_file_description),
                    CommandHandler("cancel", cancel_upload),
                ],
                DELETE_FILE_MENU: [
                    CallbackQueryHandler(handle_help_callback, pattern="^delete_file_|^help_settings$"),
                ],
                
                # Ссылки
                LINKS_MENU: [
                    CallbackQueryHandler(handle_help_callback, pattern="^link_|^help_back$"),
                ],
                
                # Команда
                TEAM_MENU: [
                    CallbackQueryHandler(handle_help_callback, pattern="^team_member_|^team_management$|^help_back$"),
                ],
                VIEW_TEAM_MEMBER: [
                    CallbackQueryHandler(handle_help_callback, pattern="^help_back$"),
                ],
                
                # Настройки
                SETTINGS_MENU: [
                    CallbackQueryHandler(handle_help_callback, pattern="^delete_file_menu$|^help_back$"),
                ],
                
                # Управление командой (админы)
                TEAM_MANAGEMENT: [
                    CallbackQueryHandler(handle_help_callback, pattern="^team_add_member$|^team_edit_member$|^team_delete_member$|^help_team$"),
                ],
                
                # Добавление сотрудника
                ADD_MEMBER_NAME: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_add_member_name),
                    CommandHandler("cancel", cancel_upload),
                ],
                ADD_MEMBER_POSITION: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_add_member_position),
                    CommandHandler("cancel", cancel_upload),
                ],
                ADD_MEMBER_CITY: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_add_member_city),
                    CommandHandler("cancel", cancel_upload),
                ],
                ADD_MEMBER_YEAR: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_add_member_year),
                    CommandHandler("cancel", cancel_upload),
                ],
                ADD_MEMBER_RESPONSIBILITIES: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_add_member_responsibilities),
                    CommandHandler("cancel", cancel_upload),
                ],
                ADD_MEMBER_CONTACT_TOPICS: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_add_member_contact_topics),
                    CommandHandler("cancel", cancel_upload),
                ],
                ADD_MEMBER_ABOUT: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_add_member_about),
                    CommandHandler("cancel", cancel_upload),
                ],
                ADD_MEMBER_TELEGRAM: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_add_member_telegram),
                    CommandHandler("cancel", cancel_upload),
                ],
                ADD_MEMBER_CONFIRM: [
                    CallbackQueryHandler(handle_add_member_confirm, pattern="^add_member_"),
                ],
                
                # Редактирование сотрудника
                EDIT_MEMBER_MENU: [
                    CallbackQueryHandler(handle_help_callback, pattern="^edit_member_select_|^team_management$"),
                ],
                EDIT_MEMBER_SELECT: [
                    CallbackQueryHandler(handle_edit_member_field, pattern="^edit_field_"),
                    CallbackQueryHandler(handle_help_callback, pattern="^team_edit_member$"),
                ],
                EDIT_MEMBER_FIELD: [
                    CallbackQueryHandler(handle_edit_member_field, pattern="^edit_field_"),
                    CallbackQueryHandler(handle_help_callback, pattern="^team_edit_member$"),
                ],
                EDIT_MEMBER_VALUE: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_edit_member_value),
                    CommandHandler("cancel", cancel_upload),
                ],
                
                # Удаление сотрудника
                DELETE_MEMBER_MENU: [
                    CallbackQueryHandler(handle_help_callback, pattern="^delete_member_select_|^team_management$"),
                ],
                DELETE_MEMBER_CONFIRM: [
                    CallbackQueryHandler(handle_help_callback, pattern="^delete_confirm_"),
                ],
            },
            fallbacks=[
                CommandHandler("cancel", cancel_upload),
            ],
        )

        # ConversationHandler для отмены и переноса встреч
        cancel_conv_handler = ConversationHandler(
            entry_points=[
                CallbackQueryHandler(cancel_meeting_callback, pattern="^cancel_meeting$"),
                CallbackQueryHandler(cancel_industry_callback, pattern="^cancel_industry$")
            ],
            states={
                SELECTING_REASON: [
                    CallbackQueryHandler(select_reason_callback, pattern="^reason_[0-9]+$"),
                    CallbackQueryHandler(cancel_back_callback, pattern="^cancel_back_planerka$"),
                ],
                SELECTING_INDUSTRY_REASON: [
                    CallbackQueryHandler(select_industry_reason_callback, pattern="^industry_reason_[0-9]+$"),
                    CallbackQueryHandler(cancel_back_callback, pattern="^cancel_back_industry$"),
                ],
                SELECTING_DATE: [
                    CallbackQueryHandler(select_date_callback, pattern="^reschedule_date_"),
                    CallbackQueryHandler(cancel_back_callback, pattern="^cancel_back_"),
                ],
                CONFIRM_RESCHEDULE: [
                    CallbackQueryHandler(confirm_reschedule_callback, pattern="^confirm_reschedule_"),
                    CallbackQueryHandler(cancel_reschedule_callback, pattern="^cancel_reschedule_"),
                ],
            },
            fallbacks=[
                CommandHandler("cancel", cancel_conversation),
            ],
        )

        # Основные обработчики
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("setchat", set_chat))
        application.add_handler(CommandHandler("info", show_info))
        application.add_handler(CommandHandler("testindustry", test_industry))
        application.add_handler(CommandHandler("testplanerka", test_planerka))
        application.add_handler(CommandHandler("jobs", list_jobs))
        
        # Добавляем ConversationHandler для помощи
        application.add_handler(help_conv_handler)
        
        # Добавляем ConversationHandler для отмены встреч
        application.add_handler(cancel_conv_handler)

        # Запуск планировщиков
        application.job_queue.run_once(
            lambda ctx: asyncio.create_task(schedule_next_reminder(ctx)),
            5
        )
        
        application.job_queue.run_once(
            lambda ctx: asyncio.create_task(schedule_next_industry_reminder(ctx)),
            7
        )

        # Логирование при запуске
        now = datetime.now(TIMEZONE)
        config = BotConfig()
        logger.info("🤖 Бот запущен и готов к работе!")
        logger.info(f"📅 Планёрки: Пн/Ср/Пт в 9:30 по МСК")
        logger.info(f"🏢 Отраслевые встречи: Вт в 12:00 по МСК")
        logger.info(f"🔄 Система отмены и переноса встреч активирована")
        logger.info(f"🔒 Отменять встречи могут только разрешенные пользователи: {', '.join(config.allowed_users)}")
        logger.info(f"📚 Система помощи с полным управлением ресурсами активирована")
        logger.info(f"👥 Модуль 'О команде' с админ-управлением готов")
        logger.info(f"🔗 Ссылка для планёрок: {'Настроена' if ZOOM_LINK != DEFAULT_ZOOM_LINK else 'НЕ настроена'}")
        logger.info(f"🔗 Ссылка для отраслевых: {'Настроена' if INDUSTRY_ZOOM_LINK != DEFAULT_ZOOM_LINK else 'НЕ настроена'}")
        logger.info(f"🗓️ Сегодня: {now.strftime('%d.%m.%Y')}")
        
        application.run_polling(allowed_updates=Update.ALL_TYPES)

    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        raise


if __name__ == "__main__":
    main()
