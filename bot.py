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
CONFIG_FILE = "bot_config.json"
DATA_FILE = "bot_data.json"

# Время планёрки (9:30 по Москве)
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

# Текст для отраслевой встречи
INDUSTRY_MEETING_TEXTS = [
    "🏢 𝗢ТРАСЛЕВАЯ ВСТРЕЧА\n\n🎯 Что делаем:\n• Обсудим итоги за неделю\n• Новые тренды и инсайты\n• Обмен опытом с коллегами\n• Запланируем мероприятия на следующую\n\n🕐 Начало: 12:00 по МСК\n📍 Формат: Zoom-конференция\n\n🔗 Всех причастных ждём! {zoom_link} | 👈",
    "🏢 𝗢ТРАСЛЕВАЯ ВСТРЕЧА\n\n📊 Сегодня на повестке:\n• Анализ недельных результатов\n• Выявление ключевых трендов\n• Коллективный разбор кейсов\n• Планирование совместных активностей\n\n🕐 Старт: 12:00 (МСК)\n🎥 Онлайн в Zoom\n\n🔗 Присоединяйтесь: {zoom_link} ← переход",
    "🏢 𝗢ТРАСЛЕВАЯ ВСТРЕЧА\n\n✨ В программе:\n• Итоги рабочей недели\n• Прогнозы и инсайты\n• Нетворкинг с экспертами\n• Дорожная карта на неделю\n\n⏰ Время: 12:00 по Москве\n💻 Платформа: Zoom\n\n🔗 Подключайтесь: {zoom_link} | 👈"
]

# Категории по умолчанию
DEFAULT_CATEGORIES = {
    "📄 Документы": [
        {"name": "📋 Инструкции", "type": "category"},
        {"name": "📊 Отчеты", "type": "category"},
        {"name": "📝 Шаблоны", "type": "category"},
    ],
    "🔗 Полезные ссылки": [
        {"name": "🌐 YA CRM", "type": "link", "url": "https://crm.example.com"},
        {"name": "📊 WIKI Отрасли", "type": "link", "url": "https://wiki.example.com"},
        {"name": "🛠️ Бот Helpy", "type": "link", "url": "https://t.me/helpy_bot"},
    ]
}

# Состояния для ConversationHandler
(
    MAIN_MENU,
    VIEW_CATEGORY,
    VIEW_ITEM,
    ADMIN_MENU,
    ADD_FILE,
    DELETE_FILE,
    EDIT_CATEGORIES,
    ADD_CATEGORY,
    DELETE_CATEGORY,
    ADD_LINK,
    EDIT_LINK,
    DELETE_LINK,
    ADD_FILE_TO_CATEGORY,
    CONFIRM_DELETE_FILE,
    CONFIRM_DELETE_LINK,
    CONFIRM_DELETE_CATEGORY
) = range(16)

# Опции отмены встреч
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

SELECTING_REASON = 16
SELECTING_INDUSTRY_REASON = 17

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

class BotData:
    """Класс для управления данными (документы, ссылки, категории)"""
    
    def __init__(self):
        self.data = self._load_data()
    
    def _load_data(self) -> Dict[str, Any]:
        if os.path.exists(DATA_FILE):
            try:
                with open(DATA_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # Проверяем наличие основных полей
                    if "categories" not in data:
                        data["categories"] = DEFAULT_CATEGORIES
                    if "files" not in data:
                        data["files"] = {}
                    if "file_counter" not in data:
                        data["file_counter"] = 1
                    return data
            except Exception as e:
                logger.error(f"Ошибка загрузки данных: {e}")
                return {
                    "categories": DEFAULT_CATEGORIES,
                    "files": {},
                    "file_counter": 1
                }
        return {
            "categories": DEFAULT_CATEGORIES,
            "files": {},
            "file_counter": 1
        }
    
    def save(self) -> None:
        try:
            with open(DATA_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Ошибка сохранения данных: {e}")
    
    @property
    def categories(self) -> Dict[str, List[Dict]]:
        return self.data.get("categories", {})
    
    @property
    def files(self) -> Dict[str, Dict]:
        return self.data.get("files", {})
    
    def get_next_file_id(self) -> str:
        file_id = str(self.data["file_counter"])
        self.data["file_counter"] += 1
        self.save()
        return file_id
    
    def add_file(self, file_info: Dict) -> str:
        file_id = self.get_next_file_id()
        self.data["files"][file_id] = file_info
        self.save()
        return file_id
    
    def delete_file(self, file_id: str) -> bool:
        if file_id in self.data["files"]:
            del self.data["files"][file_id]
            self.save()
            return True
        return False
    
    def get_file(self, file_id: str) -> Optional[Dict]:
        return self.data["files"].get(file_id)
    
    def add_category(self, category_name: str) -> bool:
        if category_name not in self.data["categories"]:
            self.data["categories"][category_name] = []
            self.save()
            return True
        return False
    
    def delete_category(self, category_name: str) -> bool:
        if category_name in self.data["categories"]:
            del self.data["categories"][category_name]
            self.save()
            return True
        return False
    
    def add_item_to_category(self, category_name: str, item: Dict) -> bool:
        if category_name in self.data["categories"]:
            self.data["categories"][category_name].append(item)
            self.save()
            return True
        return False
    
    def delete_item_from_category(self, category_name: str, item_index: int) -> bool:
        if (category_name in self.data["categories"] and 
            0 <= item_index < len(self.data["categories"][category_name])):
            del self.data["categories"][category_name][item_index]
            self.save()
            return True
        return False
    
    def update_item_in_category(self, category_name: str, item_index: int, new_item: Dict) -> bool:
        if (category_name in self.data["categories"] and 
            0 <= item_index < len(self.data["categories"][category_name])):
            self.data["categories"][category_name][item_index] = new_item
            self.save()
            return True
        return False

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

# ========== КЛАВИАТУРЫ И МЕНЮ ==========

def create_main_menu_keyboard() -> InlineKeyboardMarkup:
    """Создаем главное меню"""
    keyboard = [
        [InlineKeyboardButton("📄 Документы", callback_data="menu_documents")],
        [InlineKeyboardButton("🔗 Полезные ссылки", callback_data="menu_links")],
        [InlineKeyboardButton("⚙️ Настройки (админы)", callback_data="menu_admin")]
    ]
    return InlineKeyboardMarkup(keyboard)

def create_category_keyboard(category_name: str, bot_data: BotData) -> InlineKeyboardMarkup:
    """Создаем клавиатуру для категории"""
    keyboard = []
    items = bot_data.categories.get(category_name, [])
    
    for i, item in enumerate(items):
        if item["type"] == "category":
            button_text = f"📁 {item['name']}"
        elif item["type"] == "link":
            button_text = f"🔗 {item['name']}"
        elif item["type"] == "file":
            button_text = f"📄 {item['name']}"
        else:
            button_text = item.get("name", "Неизвестно")
        
        keyboard.append([InlineKeyboardButton(button_text, callback_data=f"item_{category_name}_{i}")])
    
    # Кнопка возврата в главное меню
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")])
    
    return InlineKeyboardMarkup(keyboard)

def create_admin_menu_keyboard() -> InlineKeyboardMarkup:
    """Создаем меню админа"""
    keyboard = [
        [InlineKeyboardButton("➕ Добавить файл", callback_data="admin_add_file")],
        [InlineKeyboardButton("🗑️ Удалить файл", callback_data="admin_delete_file")],
        [InlineKeyboardButton("📁 Редактировать категории", callback_data="admin_edit_categories")],
        [InlineKeyboardButton("🔗 Добавить ссылку", callback_data="admin_add_link")],
        [InlineKeyboardButton("✏️ Редактировать ссылку", callback_data="admin_edit_link")],
        [InlineKeyboardButton("❌ Удалить ссылку", callback_data="admin_delete_link")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]
    ]
    return InlineKeyboardMarkup(keyboard)

def create_categories_keyboard(bot_data: BotData, action: str = "view") -> InlineKeyboardMarkup:
    """Создаем клавиатуру со всеми категориями"""
    keyboard = []
    categories = list(bot_data.categories.keys())
    
    for category in categories:
        if action == "delete_category":
            button_text = f"🗑️ {category}"
            callback_data = f"delete_cat_{category}"
        elif action == "add_file":
            button_text = f"📄 {category}"
            callback_data = f"add_file_to_{category}"
        else:
            button_text = category
            callback_data = f"category_{category}"
        
        keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
    
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_admin")])
    
    return InlineKeyboardMarkup(keyboard)

def create_files_keyboard(bot_data: BotData) -> InlineKeyboardMarkup:
    """Создаем клавиатуру с файлами для удаления"""
    keyboard = []
    files = bot_data.files
    
    for file_id, file_info in files.items():
        button_text = f"🗑️ {file_info.get('name', 'Без названия')}"
        keyboard.append([InlineKeyboardButton(button_text, callback_data=f"delete_file_{file_id}")])
    
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_admin")])
    
    return InlineKeyboardMarkup(keyboard)

def create_links_keyboard(bot_data: BotData, action: str = "edit") -> InlineKeyboardMarkup:
    """Создаем клавиатуру со ссылками для редактирования/удаления"""
    keyboard = []
    
    for category_name, items in bot_data.categories.items():
        for i, item in enumerate(items):
            if item["type"] == "link":
                if action == "edit":
                    button_text = f"✏️ {item['name']}"
                    callback_data = f"edit_link_{category_name}_{i}"
                else:  # delete
                    button_text = f"❌ {item['name']}"
                    callback_data = f"delete_link_{category_name}_{i}"
                
                keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
    
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_admin")])
    
    return InlineKeyboardMarkup(keyboard)

# ========== ОСНОВНЫЕ КОМАНДЫ ==========

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик /start"""
    await update.message.reply_text(
        "🤖 <b>Бот для планёрок, отраслевых встреч и управления ресурсами!</b>\n\n"
        f"📅 <b>Планёрки:</b>\n"
        f"• Пн, Ср, Пт в 9:30 по МСК\n"
        f"• Возможность отмены\n\n"
        f"📅 <b>Отраслевые встречи:</b>\n"
        f"• Вт в 12:00 по МСК\n"
        f"• Обсуждение трендов и инсайтов\n"
        f"• Нетворкинг с коллегами\n\n"
        f"📚 <b>Управление ресурсами:</b>\n"
        f"• Документы и файлы\n"
        f"• Полезные ссылки\n"
        f"• Категории\n\n"
        f"🔧 <b>Основные команды:</b>\n"
        f"/help - главное меню\n"
        f"/info - информация о боте\n"
        f"/setchat - установить чат\n"
        f"/testindustry - тест отраслевой встречи\n",
        parse_mode=ParseMode.HTML
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Главное меню /help"""
    keyboard = create_main_menu_keyboard()
    
    if update.message:
        await update.message.reply_text(
            "📋 <b>Главное меню</b>\n\n"
            "Выберите раздел:",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
    elif update.callback_query:
        await update.callback_query.edit_message_text(
            "📋 <b>Главное меню</b>\n\n"
            "Выберите раздел:",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
    
    return MAIN_MENU

async def handle_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработка главного меню"""
    query = update.callback_query
    await query.answer()
    
    bot_data = BotData()
    
    if query.data == "menu_documents":
        keyboard = create_categories_keyboard(bot_data)
        await query.edit_message_text(
            "📁 <b>Документы</b>\n\n"
            "Выберите категорию:",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
        return VIEW_CATEGORY
        
    elif query.data == "menu_links":
        # Показываем категорию "Полезные ссылки"
        if "🔗 Полезные ссылки" in bot_data.categories:
            keyboard = create_category_keyboard("🔗 Полезные ссылки", bot_data)
            await query.edit_message_text(
                "🔗 <b>Полезные ссылки</b>\n\n"
                "Выберите ссылку:",
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML
            )
            return VIEW_ITEM
    
    elif query.data == "menu_admin":
        # Проверяем права
        config = BotConfig()
        username = query.from_user.username
        
        if username not in config.allowed_users:
            await query.answer("❌ У вас нет прав доступа к настройкам", show_alert=True)
            return MAIN_MENU
        
        keyboard = create_admin_menu_keyboard()
        await query.edit_message_text(
            "⚙️ <b>Панель администратора</b>\n\n"
            "Выберите действие:",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
        return ADMIN_MENU
    
    return MAIN_MENU

async def handle_category(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработка выбора категории"""
    query = update.callback_query
    await query.answer()
    
    bot_data = BotData()
    
    if query.data.startswith("category_"):
        category_name = query.data.replace("category_", "")
        
        if category_name in bot_data.categories:
            keyboard = create_category_keyboard(category_name, bot_data)
            await query.edit_message_text(
                f"📁 <b>{category_name}</b>\n\n"
                "Выберите элемент:",
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML
            )
            return VIEW_ITEM
    
    elif query.data == "back_to_main":
        return await help_command(update, context)
    
    return VIEW_CATEGORY

async def handle_item(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработка выбора элемента (файл, ссылка, подкатегория)"""
    query = update.callback_query
    await query.answer()
    
    bot_data = BotData()
    
    if query.data.startswith("item_"):
        # Формат: item_категория_индекс
        parts = query.data.split("_")
        if len(parts) >= 3:
            category_name = "_".join(parts[1:-1])  # Восстанавливаем название категории с подчеркиваниями
            item_index = int(parts[-1])
            
            # Исправляем название категории (заменяем подчеркивания на пробелы)
            category_name = category_name.replace("_", " ")
            
            items = bot_data.categories.get(category_name, [])
            
            if 0 <= item_index < len(items):
                item = items[item_index]
                
                if item["type"] == "link":
                    # Отправляем ссылку
                    await query.message.reply_text(
                        f"🔗 <b>{item['name']}</b>\n\n"
                        f"Ссылка: {item['url']}\n\n"
                        f"<a href=\"{item['url']}\">Перейти по ссылке</a>",
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=False
                    )
                    
                elif item["type"] == "file":
                    # Отправляем файл
                    file_id = item.get("file_id")
                    if file_id:
                        file_info = bot_data.get_file(file_id)
                        if file_info:
                            try:
                                with open(file_info["path"], 'rb') as file:
                                    await query.message.reply_document(
                                        document=InputFile(file, filename=file_info["name"]),
                                        caption=f"📄 <b>{file_info['name']}</b>\n\n{file_info.get('description', '')}",
                                        parse_mode=ParseMode.HTML
                                    )
                            except Exception as e:
                                logger.error(f"Ошибка отправки файла: {e}")
                                await query.message.reply_text(
                                    "❌ Не удалось отправить файл. Возможно, файл был удален."
                                )
                    
                elif item["type"] == "category":
                    # Открываем подкатегорию
                    keyboard = create_category_keyboard(item["name"], bot_data)
                    await query.edit_message_text(
                        f"📁 <b>{item['name']}</b>\n\n"
                        "Выберите элемент:",
                        reply_markup=keyboard,
                        parse_mode=ParseMode.HTML
                    )
                    return VIEW_ITEM
    
    elif query.data == "back_to_main":
        return await help_command(update, context)
    
    return VIEW_ITEM

# ========== АДМИНСКИЕ ФУНКЦИИ ==========

async def handle_admin_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработка меню админа"""
    query = update.callback_query
    await query.answer()
    
    bot_data = BotData()
    
    if query.data == "admin_add_file":
        keyboard = create_categories_keyboard(bot_data, "add_file")
        await query.edit_message_text(
            "📄 <b>Добавление файла</b>\n\n"
            "Выберите категорию для добавления файла:",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
        return ADD_FILE_TO_CATEGORY
        
    elif query.data == "admin_delete_file":
        keyboard = create_files_keyboard(bot_data)
        await query.edit_message_text(
            "🗑️ <b>Удаление файла</b>\n\n"
            "Выберите файл для удаления:",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
        return CONFIRM_DELETE_FILE
        
    elif query.data == "admin_edit_categories":
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Добавить категорию", callback_data="add_category")],
            [InlineKeyboardButton("🗑️ Удалить категорию", callback_data="delete_category_menu")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_admin")]
        ])
        await query.edit_message_text(
            "📁 <b>Редактирование категорий</b>\n\n"
            "Выберите действие:",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
        return EDIT_CATEGORIES
        
    elif query.data == "admin_add_link":
        keyboard = create_categories_keyboard(bot_data)
        await query.edit_message_text(
            "🔗 <b>Добавление ссылки</b>\n\n"
            "Выберите категорию для добавления ссылки:",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
        context.user_data["action"] = "add_link"
        return ADD_LINK
        
    elif query.data == "admin_edit_link":
        keyboard = create_links_keyboard(bot_data, "edit")
        await query.edit_message_text(
            "✏️ <b>Редактирование ссылки</b>\n\n"
            "Выберите ссылку для редактирования:",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
        return EDIT_LINK
        
    elif query.data == "admin_delete_link":
        keyboard = create_links_keyboard(bot_data, "delete")
        await query.edit_message_text(
            "❌ <b>Удаление ссылки</b>\n\n"
            "Выберите ссылку для удаления:",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
        return CONFIRM_DELETE_LINK
        
    elif query.data == "back_to_admin":
        keyboard = create_admin_menu_keyboard()
        await query.edit_message_text(
            "⚙️ <b>Панель администратора</b>\n\n"
            "Выберите действие:",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
        return ADMIN_MENU
    
    elif query.data == "back_to_main":
        return await help_command(update, context)
    
    return ADMIN_MENU

async def add_file_to_category(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Выбор категории для добавления файла"""
    query = update.callback_query
    await query.answer()
    
    if query.data.startswith("add_file_to_"):
        category_name = query.data.replace("add_file_to_", "")
        category_name = category_name.replace("_", " ")
        
        context.user_data["add_file_category"] = category_name
        await query.edit_message_text(
            f"📄 <b>Добавление файла в категорию: {category_name}</b>\n\n"
            "Отправьте мне файл (документ, изображение, архив и т.д.).\n\n"
            "После отправки файла, напишите описание для него.",
            parse_mode=ParseMode.HTML
        )
        return ADD_FILE
    
    elif query.data == "back_to_admin":
        return await handle_admin_menu(update, context)
    
    return ADD_FILE_TO_CATEGORY

async def add_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработка добавления файла"""
    bot_data = BotData()
    
    if update.message and update.message.document:
        # Получаем файл
        document = update.message.document
        file = await document.get_file()
        
        # Сохраняем файл локально
        file_name = document.file_name or f"file_{document.file_id[:8]}.bin"
        file_path = f"files/{file_name}"
        
        # Создаем папку files если её нет
        os.makedirs("files", exist_ok=True)
        
        await file.download_to_drive(file_path)
        
        # Сохраняем информацию о файле
        context.user_data["file_info"] = {
            "name": file_name,
            "path": file_path,
            "file_id": document.file_id,
            "mime_type": document.mime_type,
            "file_size": document.file_size
        }
        
        await update.message.reply_text(
            f"📄 Файл <b>{file_name}</b> получен.\n\n"
            "Теперь отправьте описание для этого файла:",
            parse_mode=ParseMode.HTML
        )
        return ADD_FILE
    
    elif update.message and update.message.text:
        # Получаем описание
        if "file_info" in context.user_data:
            file_info = context.user_data["file_info"]
            file_info["description"] = update.message.text
            
            # Сохраняем файл в базе данных
            file_id = bot_data.add_file(file_info)
            
            # Добавляем файл в категорию
            category_name = context.user_data.get("add_file_category")
            if category_name:
                bot_data.add_item_to_category(category_name, {
                    "name": file_info["name"],
                    "type": "file",
                    "file_id": file_id
                })
            
            # Очищаем временные данные
            context.user_data.pop("file_info", None)
            context.user_data.pop("add_file_category", None)
            
            await update.message.reply_text(
                f"✅ Файл <b>{file_info['name']}</b> успешно добавлен в категорию <b>{category_name}</b>!",
                parse_mode=ParseMode.HTML
            )
            
            # Возвращаемся в меню админа
            keyboard = create_admin_menu_keyboard()
            await update.message.reply_text(
                "⚙️ <b>Панель администратора</b>\n\n"
                "Выберите действие:",
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML
            )
            return ADMIN_MENU
    
    return ADD_FILE

async def confirm_delete_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Подтверждение удаления файла"""
    query = update.callback_query
    await query.answer()
    
    bot_data = BotData()
    
    if query.data.startswith("delete_file_"):
        file_id = query.data.replace("delete_file_", "")
        file_info = bot_data.get_file(file_id)
        
        if file_info:
            context.user_data["delete_file_id"] = file_id
            context.user_data["delete_file_info"] = file_info
            
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Да, удалить", callback_data="confirm_delete_file_yes")],
                [InlineKeyboardButton("❌ Нет, отмена", callback_data="confirm_delete_file_no")]
            ])
            
            await query.edit_message_text(
                f"🗑️ <b>Подтверждение удаления</b>\n\n"
                f"Вы уверены, что хотите удалить файл <b>{file_info['name']}</b>?\n\n"
                f"<i>Файл также будет удален из всех категорий.</i>",
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML
            )
            return CONFIRM_DELETE_FILE
    
    elif query.data == "confirm_delete_file_yes":
        file_id = context.user_data.get("delete_file_id")
        file_info = context.user_data.get("delete_file_info")
        
        if file_id and file_info:
            # Удаляем файл из файловой системы
            try:
                if os.path.exists(file_info["path"]):
                    os.remove(file_info["path"])
            except Exception as e:
                logger.error(f"Ошибка удаления файла: {e}")
            
            # Удаляем файл из всех категорий
            for category_name, items in bot_data.categories.items():
                items_to_remove = []
                for i, item in enumerate(items):
                    if item.get("type") == "file" and item.get("file_id") == file_id:
                        items_to_remove.append(i)
                
                # Удаляем в обратном порядке
                for i in sorted(items_to_remove, reverse=True):
                    bot_data.delete_item_from_category(category_name, i)
            
            # Удаляем файл из базы данных
            bot_data.delete_file(file_id)
            
            # Очищаем временные данные
            context.user_data.pop("delete_file_id", None)
            context.user_data.pop("delete_file_info", None)
            
            await query.edit_message_text(
                f"✅ Файл <b>{file_info['name']}</b> успешно удален!",
                parse_mode=ParseMode.HTML
            )
            
            # Возвращаемся в меню админа
            keyboard = create_admin_menu_keyboard()
            await query.message.reply_text(
                "⚙️ <b>Панель администратора</b>\n\n"
                "Выберите действие:",
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML
            )
            return ADMIN_MENU
    
    elif query.data == "confirm_delete_file_no" or query.data == "back_to_admin":
        return await handle_admin_menu(update, context)
    
    return CONFIRM_DELETE_FILE

async def handle_edit_categories(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработка редактирования категорий"""
    query = update.callback_query
    await query.answer()
    
    bot_data = BotData()
    
    if query.data == "add_category":
        await query.edit_message_text(
            "➕ <b>Добавление категории</b>\n\n"
            "Отправьте название новой категории:",
            parse_mode=ParseMode.HTML
        )
        return ADD_CATEGORY
        
    elif query.data == "delete_category_menu":
        keyboard = create_categories_keyboard(bot_data, "delete_category")
        await query.edit_message_text(
            "🗑️ <b>Удаление категории</b>\n\n"
            "Выберите категорию для удаления:",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
        return CONFIRM_DELETE_CATEGORY
        
    elif query.data == "back_to_admin":
        return await handle_admin_menu(update, context)
    
    return EDIT_CATEGORIES

async def add_category(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Добавление новой категории"""
    if update.message and update.message.text:
        category_name = update.message.text.strip()
        bot_data = BotData()
        
        if bot_data.add_category(category_name):
            await update.message.reply_text(
                f"✅ Категория <b>{category_name}</b> успешно добавлена!",
                parse_mode=ParseMode.HTML
            )
        else:
            await update.message.reply_text(
                f"❌ Категория <b>{category_name}</b> уже существует!",
                parse_mode=ParseMode.HTML
            )
        
        # Возвращаемся в меню редактирования категорий
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Добавить категорию", callback_data="add_category")],
            [InlineKeyboardButton("🗑️ Удалить категорию", callback_data="delete_category_menu")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_admin")]
        ])
        await update.message.reply_text(
            "📁 <b>Редактирование категорий</b>\n\n"
            "Выберите действие:",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
        return EDIT_CATEGORIES
    
    return ADD_CATEGORY

async def confirm_delete_category(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Подтверждение удаления категории"""
    query = update.callback_query
    await query.answer()
    
    bot_data = BotData()
    
    if query.data.startswith("delete_cat_"):
        category_name = query.data.replace("delete_cat_", "")
        category_name = category_name.replace("_", " ")
        
        context.user_data["delete_category_name"] = category_name
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Да, удалить", callback_data="confirm_delete_cat_yes")],
            [InlineKeyboardButton("❌ Нет, отмена", callback_data="confirm_delete_cat_no")]
        ])
        
        await query.edit_message_text(
            f"🗑️ <b>Подтверждение удаления категории</b>\n\n"
            f"Вы уверены, что хотите удалить категорию <b>{category_name}</b>?\n\n"
            f"<i>Все элементы внутри категории также будут удалены.</i>",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
        return CONFIRM_DELETE_CATEGORY
    
    elif query.data == "confirm_delete_cat_yes":
        category_name = context.user_data.get("delete_category_name")
        
        if category_name and bot_data.delete_category(category_name):
            await query.edit_message_text(
                f"✅ Категория <b>{category_name}</b> успешно удалена!",
                parse_mode=ParseMode.HTML
            )
        else:
            await query.edit_message_text(
                "❌ Не удалось удалить категорию.",
                parse_mode=ParseMode.HTML
            )
        
        # Очищаем временные данные
        context.user_data.pop("delete_category_name", None)
        
        # Возвращаемся в меню редактирования категорий
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Добавить категорию", callback_data="add_category")],
            [InlineKeyboardButton("🗑️ Удалить категорию", callback_data="delete_category_menu")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_admin")]
        ])
        await query.message.reply_text(
            "📁 <b>Редактирование категорий</b>\n\n"
            "Выберите действие:",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
        return EDIT_CATEGORIES
    
    elif query.data == "confirm_delete_cat_no":
        return await handle_edit_categories(update, context)
    
    return CONFIRM_DELETE_CATEGORY

async def add_link(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Добавление новой ссылки"""
    query = update.callback_query
    await query.answer()
    
    bot_data = BotData()
    
    if query.data.startswith("category_"):
        category_name = query.data.replace("category_", "")
        category_name = category_name.replace("_", " ")
        
        context.user_data["add_link_category"] = category_name
        await query.edit_message_text(
            f"🔗 <b>Добавление ссылки в категорию: {category_name}</b>\n\n"
            "Отправьте название ссылки:",
            parse_mode=ParseMode.HTML
        )
        return ADD_LINK
    
    elif update.message and update.message.text:
        if "add_link_category" in context.user_data:
            if "add_link_name" not in context.user_data:
                # Получаем название ссылки
                context.user_data["add_link_name"] = update.message.text
                await update.message.reply_text(
                    "Теперь отправьте URL ссылки (начинается с http:// или https://):",
                    parse_mode=ParseMode.HTML
                )
                return ADD_LINK
            else:
                # Получаем URL ссылки
                url = update.message.text.strip()
                if url.startswith("http://") or url.startswith("https://"):
                    category_name = context.user_data["add_link_category"]
                    link_name = context.user_data["add_link_name"]
                    
                    # Добавляем ссылку в категорию
                    bot_data.add_item_to_category(category_name, {
                        "name": link_name,
                        "type": "link",
                        "url": url
                    })
                    
                    # Очищаем временные данные
                    context.user_data.pop("add_link_category", None)
                    context.user_data.pop("add_link_name", None)
                    
                    await update.message.reply_text(
                        f"✅ Ссылка <b>{link_name}</b> успешно добавлена в категорию <b>{category_name}</b>!",
                        parse_mode=ParseMode.HTML
                    )
                    
                    # Возвращаемся в меню админа
                    keyboard = create_admin_menu_keyboard()
                    await update.message.reply_text(
                        "⚙️ <b>Панель администратора</b>\n\n"
                        "Выберите действие:",
                        reply_markup=keyboard,
                        parse_mode=ParseMode.HTML
                    )
                    return ADMIN_MENU
                else:
                    await update.message.reply_text(
                        "❌ Неверный URL. URL должен начинаться с http:// или https://\n\n"
                        "Попробуйте еще раз:",
                        parse_mode=ParseMode.HTML
                    )
                    return ADD_LINK
    
    elif query.data == "back_to_admin":
        return await handle_admin_menu(update, context)
    
    return ADD_LINK

async def edit_link(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Редактирование ссылки"""
    query = update.callback_query
    await query.answer()
    
    bot_data = BotData()
    
    if query.data.startswith("edit_link_"):
        # Формат: edit_link_категория_индекс
        parts = query.data.split("_")
        if len(parts) >= 4:
            category_name = "_".join(parts[2:-1])  # Восстанавливаем название категории
            item_index = int(parts[-1])
            
            # Исправляем название категории
            category_name = category_name.replace("_", " ")
            
            items = bot_data.categories.get(category_name, [])
            
            if 0 <= item_index < len(items):
                item = items[item_index]
                if item["type"] == "link":
                    context.user_data["edit_link_category"] = category_name
                    context.user_data["edit_link_index"] = item_index
                    context.user_data["edit_link_old_name"] = item["name"]
                    context.user_data["edit_link_old_url"] = item["url"]
                    
                    await query.edit_message_text(
                        f"✏️ <b>Редактирование ссылки: {item['name']}</b>\n\n"
                        "Отправьте новое название ссылки (или отправьте '-' чтобы оставить текущее):",
                        parse_mode=ParseMode.HTML
                    )
                    return EDIT_LINK
    
    elif update.message and update.message.text:
        if "edit_link_category" in context.user_data:
            if "edit_link_new_name" not in context.user_data:
                # Получаем новое название
                new_name = update.message.text.strip()
                if new_name == "-":
                    new_name = context.user_data["edit_link_old_name"]
                
                context.user_data["edit_link_new_name"] = new_name
                await update.message.reply_text(
                    f"Текущий URL: {context.user_data['edit_link_old_url']}\n\n"
                    "Отправьте новый URL (или отправьте '-' чтобы оставить текущий):",
                    parse_mode=ParseMode.HTML
                )
                return EDIT_LINK
            else:
                # Получаем новый URL
                new_url = update.message.text.strip()
                if new_url == "-":
                    new_url = context.user_data["edit_link_old_url"]
                
                category_name = context.user_data["edit_link_category"]
                item_index = context.user_data["edit_link_index"]
                new_name = context.user_data["edit_link_new_name"]
                
                # Обновляем ссылку
                if bot_data.update_item_in_category(category_name, item_index, {
                    "name": new_name,
                    "type": "link",
                    "url": new_url
                }):
                    await update.message.reply_text(
                        f"✅ Ссылка успешно обновлена!\n\n"
                        f"<b>Новое название:</b> {new_name}\n"
                        f"<b>Новый URL:</b> {new_url}",
                        parse_mode=ParseMode.HTML
                    )
                else:
                    await update.message.reply_text(
                        "❌ Не удалось обновить ссылку.",
                        parse_mode=ParseMode.HTML
                    )
                
                # Очищаем временные данные
                context.user_data.pop("edit_link_category", None)
                context.user_data.pop("edit_link_index", None)
                context.user_data.pop("edit_link_old_name", None)
                context.user_data.pop("edit_link_old_url", None)
                context.user_data.pop("edit_link_new_name", None)
                
                # Возвращаемся в меню админа
                keyboard = create_admin_menu_keyboard()
                await update.message.reply_text(
                    "⚙️ <b>Панель администратора</b>\n\n"
                    "Выберите действие:",
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML
                )
                return ADMIN_MENU
    
    elif query.data == "back_to_admin":
        return await handle_admin_menu(update, context)
    
    return EDIT_LINK

async def confirm_delete_link(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Подтверждение удаления ссылки"""
    query = update.callback_query
    await query.answer()
    
    bot_data = BotData()
    
    if query.data.startswith("delete_link_"):
        # Формат: delete_link_категория_индекс
        parts = query.data.split("_")
        if len(parts) >= 4:
            category_name = "_".join(parts[2:-1])  # Восстанавливаем название категории
            item_index = int(parts[-1])
            
            # Исправляем название категории
            category_name = category_name.replace("_", " ")
            
            items = bot_data.categories.get(category_name, [])
            
            if 0 <= item_index < len(items):
                item = items[item_index]
                if item["type"] == "link":
                    context.user_data["delete_link_category"] = category_name
                    context.user_data["delete_link_index"] = item_index
                    context.user_data["delete_link_name"] = item["name"]
                    
                    keyboard = InlineKeyboardMarkup([
                        [InlineKeyboardButton("✅ Да, удалить", callback_data="confirm_delete_link_yes")],
                        [InlineKeyboardButton("❌ Нет, отмена", callback_data="confirm_delete_link_no")]
                    ])
                    
                    await query.edit_message_text(
                        f"❌ <b>Подтверждение удаления ссылки</b>\n\n"
                        f"Вы уверены, что хотите удалить ссылку <b>{item['name']}</b>?",
                        reply_markup=keyboard,
                        parse_mode=ParseMode.HTML
                    )
                    return CONFIRM_DELETE_LINK
    
    elif query.data == "confirm_delete_link_yes":
        category_name = context.user_data.get("delete_link_category")
        item_index = context.user_data.get("delete_link_index")
        link_name = context.user_data.get("delete_link_name")
        
        if category_name is not None and item_index is not None:
            if bot_data.delete_item_from_category(category_name, item_index):
                await query.edit_message_text(
                    f"✅ Ссылка <b>{link_name}</b> успешно удалена!",
                    parse_mode=ParseMode.HTML
                )
            else:
                await query.edit_message_text(
                    "❌ Не удалось удалить ссылку.",
                    parse_mode=ParseMode.HTML
                )
        
        # Очищаем временные данные
        context.user_data.pop("delete_link_category", None)
        context.user_data.pop("delete_link_index", None)
        context.user_data.pop("delete_link_name", None)
        
        # Возвращаемся в меню админа
        keyboard = create_admin_menu_keyboard()
        await query.message.reply_text(
            "⚙️ <b>Панель администратора</b>\n\n"
            "Выберите действие:",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
        return ADMIN_MENU
    
    elif query.data == "confirm_delete_link_no" or query.data == "back_to_admin":
        return await handle_admin_menu(update, context)
    
    return CONFIRM_DELETE_LINK

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

# ========== ФУНКЦИИ ДЛЯ ОТМЕНЫ ВСТРЕЧ ==========

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

# ========== ОСНОВНЫЕ КОМАНДЫ (продолжение) ==========

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
        f"• Отраслевые встречи (12:00, Вт)\n",
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
    
    now = datetime.now(TIMEZONE)
    weekday = now.weekday()
    day_names = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Саббота", "Воскресенье"]
    current_day = day_names[weekday]
    
    is_meeting_day = weekday in MEETING_DAYS
    is_industry_day = weekday in INDUSTRY_MEETING_DAY
    
    # Проверяем настройку ссылок
    zoom_status = "✅" if ZOOM_LINK != DEFAULT_ZOOM_LINK else "❌"
    industry_zoom_status = "✅" if INDUSTRY_ZOOM_LINK != DEFAULT_ZOOM_LINK else "❌"
    
    # Статистика ресурсов
    bot_data = BotData()
    total_files = len(bot_data.files)
    total_categories = len(bot_data.categories)
    
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
        f"• Отраслевые: {industry_jobs}\n\n"
        f"📚 <b>Ресурсы:</b>\n"
        f"• Файлов: {total_files}\n"
        f"• Категорий: {total_categories}\n\n"
        f"📅 <b>Сегодня:</b> {current_day}, {now.day} {MONTHS_RU[now.month]} {now.year}\n\n"
        f"ℹ️ Используйте /help для доступа к документам и ссылкам",
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
        elif "industry_meeting" in job_name:
            icon = "🏢"
        else:
            icon = "🔧"
        
        message += f"{icon} {next_time.strftime('%d.%m.%Y %H:%M')} - {job_name[:30]}\n"
    
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

def main() -> None:
    if not TOKEN:
        logger.error("❌ Токен бота не найден!")
        return
    
    try:
        application = Application.builder().token(TOKEN).build()

        # Создаем папку для файлов если её нет
        os.makedirs("files", exist_ok=True)

        # ConversationHandler для главного меню
        main_conv_handler = ConversationHandler(
            entry_points=[CommandHandler("help", help_command)],
            states={
                MAIN_MENU: [
                    CallbackQueryHandler(handle_main_menu, pattern="^menu_"),
                    CallbackQueryHandler(help_command, pattern="^back_to_main$"),
                ],
                VIEW_CATEGORY: [
                    CallbackQueryHandler(handle_category, pattern="^(category_|back_to_main)"),
                ],
                VIEW_ITEM: [
                    CallbackQueryHandler(handle_item, pattern="^(item_|back_to_main)"),
                ],
                ADMIN_MENU: [
                    CallbackQueryHandler(handle_admin_menu, pattern="^(admin_|back_to_admin|back_to_main)"),
                ],
                ADD_FILE_TO_CATEGORY: [
                    CallbackQueryHandler(add_file_to_category, pattern="^(add_file_to_|back_to_admin)"),
                ],
                ADD_FILE: [
                    MessageHandler(filters.Document.ALL | filters.TEXT & ~filters.COMMAND, add_file),
                    CallbackQueryHandler(handle_admin_menu, pattern="^back_to_admin$"),
                ],
                CONFIRM_DELETE_FILE: [
                    CallbackQueryHandler(confirm_delete_file, pattern="^(delete_file_|confirm_delete_file_|back_to_admin)"),
                ],
                EDIT_CATEGORIES: [
                    CallbackQueryHandler(handle_edit_categories, pattern="^(add_category|delete_category_menu|back_to_admin)"),
                ],
                ADD_CATEGORY: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, add_category),
                ],
                CONFIRM_DELETE_CATEGORY: [
                    CallbackQueryHandler(confirm_delete_category, pattern="^(delete_cat_|confirm_delete_cat_|back_to_admin)"),
                ],
                ADD_LINK: [
                    CallbackQueryHandler(add_link, pattern="^(category_|back_to_admin)"),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, add_link),
                ],
                EDIT_LINK: [
                    CallbackQueryHandler(edit_link, pattern="^(edit_link_|back_to_admin)"),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, edit_link),
                ],
                CONFIRM_DELETE_LINK: [
                    CallbackQueryHandler(confirm_delete_link, pattern="^(delete_link_|confirm_delete_link_|back_to_admin)"),
                ],
            },
            fallbacks=[
                CommandHandler("help", help_command),
                CommandHandler("cancel", help_command),
            ],
        )

        # ConversationHandler для отмены встреч
        cancel_conv_handler = ConversationHandler(
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

        # Основные обработчики
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("setchat", set_chat))
        application.add_handler(CommandHandler("info", show_info))
        application.add_handler(CommandHandler("testindustry", test_industry))
        application.add_handler(CommandHandler("jobs", list_jobs))
        
        # Добавляем ConversationHandler для главного меню
        application.add_handler(main_conv_handler)
        
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
        logger.info("🤖 Бот запущен и готов к работе!")
        logger.info(f"📅 Планёрки: Пн/Ср/Пт в 9:30 по МСК")
        logger.info(f"🏢 Отраслевые встречи: Вт в 12:00 по МСК")
        logger.info(f"📚 Система документов и ссылок активирована")
        logger.info(f"🔗 Ссылка для планёрок: {'Настроена' if ZOOM_LINK != DEFAULT_ZOOM_LINK else 'НЕ настроена'}")
        logger.info(f"🔗 Ссылка для отраслевых: {'Настроена' if INDUSTRY_ZOOM_LINK != DEFAULT_ZOOM_LINK else 'НЕ настроена'}")
        logger.info(f"🗓️ Сегодня: {now.strftime('%d.%m.%Y')}")
        
        application.run_polling(allowed_updates=Update.ALL_TYPES)

    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        raise


if __name__ == "__main__":
    main()
