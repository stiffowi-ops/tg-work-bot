import os
import json
import random
import logging
import asyncio
import html
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple, TypedDict
from functools import wraps
import pytz
from urllib.parse import quote
import re
import time
import aiohttp

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

# Константы для системы помощи
YA_CRM_LINK = os.getenv("YA_CRM_LINK", "https://crm.example.com")
WIKI_LINK = os.getenv("WIKI_LINK", "https://wiki.example.com")
HELPY_BOT_LINK = os.getenv("HELPY_BOT_LINK", "https://t.me/helpy_bot")

CONFIG_FILE = "bot_config.json"
HELP_DATA_FILE = "help_data.json"
USER_DATA_FILE = "user_data.json"

# Время планёрки (9:15 по Москве)
MEETING_TIME = {"hour": 9, "minute": 15}
TIMEZONE = pytz.timezone("Europe/Moscow")

# Дни недели для планёрки (понедельник=0, среда=2, пятница=4)
MEETING_DAYS = [0, 2, 4]

# Время отраслевой встречи (вторник 12:00 по МСК)
INDUSTRY_MEETING_TIME = {"hour": 12, "minute": 0}
INDUSTRY_MEETING_DAY = [1]  # Вторник

# Текст для отраслевой встречи
INDUSTRY_MEETING_TEXTS = [
    "🏢 𝗢ТРАСЛЕВАЯ ВСТРЕЧА\n\n🎯 Что делаем:\n• Обсудим итоги за неделю\n• Новые тренды и инсайты\n• Обмен опытом с коллегами\n• Запланируем мероприятия на следующую\n\n🕐 Начало: 12:00 по МСК\n📍 Формат: Zoom-конференция\n\n🔗 Всех причастных ждём! {zoom_link} | 👈",
    "🏢 𝗢ТРАСЛЕВАЯ ВСТРЕЧА\n\n📊 Сегодня на повестке:\n• Анализ недельных результатов\n• Выявление ключевых трендов\n• Коллективный разбор кейсов\n• Планирование активностей\n\n🕐 Старт: 12:00 (МСК)\n🎥 Онлайн в Zoom\n\n🔗 Присоединяйтесь: {zoom_link} ← переход",
    "🏢 𝗢ТРАСЛЕВАЯ ВСТРЕЧА\n\n✨ На повестке дня:\n• Итоги рабочей недели\n• Прогнозы и инсайты\n•Планы на неделю\n\n⏰ Время: 12:00 по Москве\n💻 Платформа: Zoom\n\n🔗 Подключайтесь: {zoom_link} | 👈"
]

# Meme API (оставлено для совместимости, но не используется)
MEME_API_URL = "https://meme-api.com/gimme"
REQUEST_TIMEOUT = 10

# ========== ТИПЫ ДАННЫХ ==========
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

# Состояния для системы помощи
ADDING_FILE_NAME, ADDING_FILE_DESCRIPTION = range(5, 7)

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

# ========== ФУНКЦИИ ДЛЯ ПЛАНЁРКИ ==========

def get_meeting_text() -> str:
    """Получаем текст для планёрки с ссылкой"""
    zoom_link = ZOOM_LINK
    
    if zoom_link == DEFAULT_ZOOM_LINK:
        zoom_link_formatted = f'<a href="{zoom_link}">[НЕ НАСТРОЕНА - настройте ZOOM_MEETING_LINK]</a>'
    else:
        zoom_link_formatted = f'<a href="{zoom_link}">Присоединиться к Zoom</a>'
    
    return (
        f"<b>⚠️ СТОЙ! КУДА! ВСТРЕЧАЧ! ⚠️</b>\n\n"
        f"🤖 Робот-напоминалка активирована!\n"
        f"Собираемся на планёрку\n\n"
        f"<b>🕘 Время:</b> 9:15 по МСК\n"
        f"<b>📍 Ссылка:</b> {zoom_link_formatted}"
    )

def get_industry_meeting_text() -> str:
    """Получаем текст для отраслевой встречи с ссылкой"""
    zoom_link = INDUSTRY_ZOOM_LINK
    
    if zoom_link == DEFAULT_ZOOM_LINK:
        zoom_link_formatted = f'<a href="{zoom_link}">[НЕ НАСТРОЕНА - настройте INDUSTRY_MEETING_LINK]</a>'
    else:
        zoom_link_formatted = f'<a href="{zoom_link}">Присоединиться к Zoom</a>'
    
    text = random.choice(INDUSTRY_MEETING_TEXTS)
    return text.format(zoom_link=zoom_link_formatted)

def create_cancel_keyboard(options: List[str], cancel_type: str = "regular") -> InlineKeyboardMarkup:
    """Создает клавиатуру для отмены встречи"""
    keyboard = []
    for i, option in enumerate(options):
        keyboard.append([InlineKeyboardButton(
            option, 
            callback_data=f"cancel_{cancel_type}_{i}"
        )])
    
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel_cancel")])
    return InlineKeyboardMarkup(keyboard)

def create_confirm_keyboard(cancel_type: str, reason_index: int, date: Optional[str] = None) -> InlineKeyboardMarkup:
    """Создает клавиатуру подтверждения отмены"""
    keyboard = []
    
    if date:
        callback_data = f"confirm_cancel_{cancel_type}_{reason_index}_{date}"
    else:
        callback_data = f"confirm_cancel_{cancel_type}_{reason_index}"
    
    keyboard.append([InlineKeyboardButton("✅ Подтвердить отмену", callback_data=callback_data)])
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel_cancel")])
    
    return InlineKeyboardMarkup(keyboard)

def create_date_keyboard(cancel_type: str, reason_index: int) -> InlineKeyboardMarkup:
    """Создает клавиатуру для выбора даты переноса"""
    today = datetime.now(TIMEZONE)
    keyboard = []
    
    # Следующие 7 дней
    for i in range(1, 8):
        future_date = today + timedelta(days=i)
        if future_date.weekday() in MEETING_DAYS:  # Только дни планёрок
            date_str = future_date.strftime("%d.%m.%Y")
            weekday = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"][future_date.weekday()]
            button_text = f"{date_str} ({weekday})"
            callback_data = f"select_date_{cancel_type}_{reason_index}_{date_str}"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
    
    keyboard.append([InlineKeyboardButton("❌ Не переносить, просто отменить", callback_data=f"no_date_{cancel_type}_{reason_index}")])
    keyboard.append([InlineKeyboardButton("↩️ Назад", callback_data=f"back_reason_{cancel_type}")])
    
    return InlineKeyboardMarkup(keyboard)

# ========== ОСНОВНЫЕ ФУНКЦИИ ПЛАНЁРКИ ==========

async def send_meeting_reminder(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправка напоминания о планёрке"""
    try:
        config = BotConfig()
        chat_id = config.chat_id
        
        if not chat_id:
            logger.error("Chat ID не установлен для отправки планёрки!")
            await schedule_next_meeting(context)
            return
        
        message = await context.bot.send_message(
            chat_id=chat_id,
            text=get_meeting_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🗑️ Отменить планёрку", callback_data="start_cancel_regular")
            ]])
        )
        
        # Сохраняем ID сообщения для возможного удаления
        config.add_reminder(message.message_id, chat_id)
        
        logger.info(f"✅ Напоминание о планёрке отправлено в чат {chat_id}")
        await schedule_next_meeting(context)
        
    except Exception as e:
        logger.error(f"❌ Ошибка отправки напоминания о планёрке: {e}")
        await schedule_next_meeting(context)

async def send_industry_meeting_reminder(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправка напоминания об отраслевой встрече"""
    try:
        config = BotConfig()
        chat_id = config.chat_id
        
        if not chat_id:
            logger.error("Chat ID не установлен для отправки отраслевой встречи!")
            await schedule_next_industry_meeting(context)
            return
        
        message = await context.bot.send_message(
            chat_id=chat_id,
            text=get_industry_meeting_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🗑️ Отменить встречу", callback_data="start_cancel_industry")
            ]])
        )
        
        logger.info(f"✅ Напоминание об отраслевой встрече отправлено в чат {chat_id}")
        await schedule_next_industry_meeting(context)
        
    except Exception as e:
        logger.error(f"❌ Ошибка отправки напоминания об отраслевой встрече: {e}")
        await schedule_next_industry_meeting(context)

# ========== ФУНКЦИИ ПЛАНИРОВАНИЯ ==========

def calculate_next_meeting_time() -> datetime:
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

async def schedule_next_meeting(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Запланировать следующую планёрку"""
    try:
        next_time = calculate_next_meeting_time()
        config = BotConfig()
        chat_id = config.chat_id

        if not chat_id:
            logger.warning("Chat ID не установлен, планирование планёрок отложено")
            context.application.job_queue.run_once(
                lambda ctx: asyncio.create_task(schedule_next_meeting(ctx)),
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
                    send_meeting_reminder,
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
                lambda ctx: asyncio.create_task(schedule_next_meeting(ctx)),
                60
            )
            
    except Exception as e:
        logger.error(f"Ошибка планирования планёрки: {e}")
        context.application.job_queue.run_once(
            lambda ctx: asyncio.create_task(schedule_next_meeting(ctx)),
            300
        )

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
            logger.warning(f"Время отраслевой встречи уже прошло ({next_time}), планируем на следующий день")
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

# ========== ОБРАБОТЧИКИ ОТМЕНЫ ВСТРЕЧ ==========

async def start_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Начало процесса отмены встречи"""
    query = update.callback_query
    await query.answer()
    
    cancel_type = query.data.replace("start_cancel_", "")
    
    if cancel_type == "regular":
        options = CANCELLATION_OPTIONS
        title = "🗑️ <b>ОТМЕНА ПЛАНЁРКИ</b>\n\nВыберите причину отмены:"
    else:  # industry
        options = INDUSTRY_CANCELLATION_OPTIONS
        title = "🗑️ <b>ОТМЕНА ОТРАСЛЕВОЙ ВСТРЕЧИ</b>\n\nВыберите причину отмены:"
    
    await query.edit_message_text(
        text=title,
        reply_markup=create_cancel_keyboard(options, cancel_type),
        parse_mode=ParseMode.HTML
    )
    
    context.user_data['cancel_type'] = cancel_type
    
    return SELECTING_REASON

async def select_cancel_reason(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Выбор причины отмены"""
    query = update.callback_query
    await query.answer()
    
    parts = query.data.split("_")
    cancel_type = parts[1]
    reason_index = int(parts[2])
    
    if cancel_type == "regular":
        options = CANCELLATION_OPTIONS
    else:
        options = INDUSTRY_CANCELLATION_OPTIONS
    
    reason = options[reason_index]
    context.user_data['reason_index'] = reason_index
    
    if "перен" in reason.lower():
        # Если выбрана причина с переносом, предлагаем выбрать дату
        await query.edit_message_text(
            text=f"🗓️ <b>ВЫБЕРИТЕ ДАТУ ПЕРЕНОСА:</b>\n\nПричина: <i>{reason}</i>",
            reply_markup=create_date_keyboard(cancel_type, reason_index),
            parse_mode=ParseMode.HTML
        )
        return SELECTING_DATE
    else:
        # Если причина без переноса, сразу подтверждение
        await query.edit_message_text(
            text=f"⚠️ <b>ПОДТВЕРЖДЕНИЕ ОТМЕНЫ</b>\n\n<b>Причина:</b> {reason}",
            reply_markup=create_confirm_keyboard(cancel_type, reason_index),
            parse_mode=ParseMode.HTML
        )
        return CONFIRMING_DATE

async def select_date(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Выбор даты переноса"""
    query = update.callback_query
    await query.answer()
    
    parts = query.data.split("_")
    cancel_type = parts[2]
    reason_index = int(parts[3])
    date_str = parts[4]
    
    if cancel_type == "regular":
        options = CANCELLATION_OPTIONS
    else:
        options = INDUSTRY_CANCELLATION_OPTIONS
    
    reason = options[reason_index]
    context.user_data['selected_date'] = date_str
    
    await query.edit_message_text(
        text=f"⚠️ <b>ПОДТВЕРЖДЕНИЕ ОТМЕНЫ С ПЕРЕНОСОМ</b>\n\n"
             f"<b>Причина:</b> {reason}\n"
             f"<b>Перенос на:</b> {date_str}",
        reply_markup=create_confirm_keyboard(cancel_type, reason_index, date_str),
        parse_mode=ParseMode.HTML
    )
    
    return CONFIRMING_DATE

async def no_date(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отмена без переноса"""
    query = update.callback_query
    await query.answer()
    
    parts = query.data.split("_")
    cancel_type = parts[2]
    reason_index = int(parts[3])
    
    if cancel_type == "regular":
        options = CANCELLATION_OPTIONS
    else:
        options = INDUSTRY_CANCELLATION_OPTIONS
    
    reason = options[reason_index]
    
    await query.edit_message_text(
        text=f"⚠️ <b>ПОДТВЕРЖДЕНИЕ ОТМЕНЫ</b>\n\n<b>Причина:</b> {reason}",
        reply_markup=create_confirm_keyboard(cancel_type, reason_index),
        parse_mode=ParseMode.HTML
    )
    
    return CONFIRMING_DATE

async def confirm_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Подтверждение отмены встречи"""
    query = update.callback_query
    await query.answer()
    
    parts = query.data.split("_")
    cancel_type = parts[2]
    reason_index = int(parts[3])
    
    if cancel_type == "regular":
        options = CANCELLATION_OPTIONS
        meeting_name = "планёрки"
    else:
        options = INDUSTRY_CANCELLATION_OPTIONS
        meeting_name = "отраслевой встречи"
    
    reason = options[reason_index]
    
    if len(parts) > 4:
        # Есть дата переноса
        date_str = parts[4]
        message_text = (f"✅ <b>ОТМЕНА ПОДТВЕРЖДЕНА</b>\n\n"
                       f"<b>{meeting_name.upper()} НЕ БУДЕТ</b>\n"
                       f"<b>Причина:</b> {reason}\n"
                       f"<b>Перенос на:</b> {date_str}")
    else:
        # Без переноса
        message_text = (f"✅ <b>ОТМЕНА ПОДТВЕРЖДЕНА</b>\n\n"
                       f"<b>{meeting_name.upper()} НЕ БУДЕТ</b>\n"
                       f"<b>Причина:</b> {reason}")
    
    await query.edit_message_text(
        text=message_text,
        parse_mode=ParseMode.HTML
    )
    
    # Удаляем все активные напоминания об этой встрече
    config = BotConfig()
    
    if cancel_type == "regular":
        # Удаляем напоминание о планёрке
        reminders = config.active_reminders
        for msg_id, reminder_data in list(reminders.items()):
            try:
                await context.bot.delete_message(
                    chat_id=reminder_data["chat_id"],
                    message_id=msg_id
                )
                config.remove_reminder(msg_id)
            except:
                pass
        
        # Отменяем запланированную планёрку
        job_queue = context.application.job_queue
        jobs = get_jobs_from_queue(job_queue)
        
        for job in jobs:
            if job.name and "meeting_reminder" in job.name:
                job.schedule_removal()
                logger.info(f"Отменена запланированная планёрка: {job.name}")
    
    # Сбрасываем состояние
    context.user_data.clear()
    
    return ConversationHandler.END

async def back_to_reason(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Возврат к выбору причины"""
    query = update.callback_query
    await query.answer()
    
    parts = query.data.split("_")
    cancel_type = parts[2]
    
    if cancel_type == "regular":
        options = CANCELLATION_OPTIONS
        title = "🗑️ <b>ОТМЕНА ПЛАНЁРКИ</b>\n\nВыберите причину отмены:"
    else:
        options = INDUSTRY_CANCELLATION_OPTIONS
        title = "🗑️ <b>ОТМЕНА ОТРАСЛЕВОЙ ВСТРЕЧИ</b>\n\nВыберите причину отмены:"
    
    await query.edit_message_text(
        text=title,
        reply_markup=create_cancel_keyboard(options, cancel_type),
        parse_mode=ParseMode.HTML
    )
    
    return SELECTING_REASON

async def cancel_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отмена процесса отмены встречи"""
    query = update.callback_query
    await query.answer()
    
    await query.edit_message_text(
        text="❌ <b>ОТМЕНА ОТМЕНЫ</b>\n\nПроцесс отмены встречи прерван.",
        parse_mode=ParseMode.HTML
    )
    
    # Сбрасываем состояние
    context.user_data.clear()
    
    return ConversationHandler.END

async def cancel_conversation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отмена разговора"""
    await update.message.reply_text(
        "❌ Процесс отмены встречи прерван.",
        parse_mode=ParseMode.HTML
    )
    
    context.user_data.clear()
    return ConversationHandler.END

# ========== КЛАСС КОНФИГА ==========

class BotConfig:
    """Класс для управления конфигурацией бота"""
    
    def __init__(self):
        self.data_file = USER_DATA_FILE
        self.help_data_file = HELP_DATA_FILE
        self.data = self._load_data()
        self.help_data = self._load_help_data()
    
    def _load_data(self) -> Dict[str, Any]:
        """Загрузить основные данные"""
        if os.path.exists(self.data_file):
            try:
                with open(self.data_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Ошибка загрузки данных: {e}")
        
        return {
            "chat_id": None,
            "admins": ["Stiff_OWi", "gshabanov"],
            "allowed_users": ["Stiff_OWi", "gshabanov"],
            "active_reminders": {},
            "pending_files": {}
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
            with open(self.data_file, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Ошибка сохранения данных: {e}")
    
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
    
    def add_reminder(self, message_id: int, chat_id: int) -> None:
        self.data["active_reminders"][str(message_id)] = {
            "message_id": message_id,
            "chat_id": chat_id,
            "created_at": datetime.now().isoformat()
        }
        self.save()
    
    def remove_reminder(self, message_id: int) -> bool:
        if str(message_id) in self.data["active_reminders"]:
            del self.data["active_reminders"][str(message_id)]
            self.save()
            return True
        return False

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

# ========== ОСНОВНЫЕ КОМАНДЫ ==========

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик /start"""
    await update.message.reply_text(
        "🤖 <b>Бот-напоминалка для планёрок и встреч</b>\n\n"
        "Автоматически отправляет напоминания о:\n"
        "• Ежедневных планёрках (Пн, Ср, Пт в 9:15)\n"
        "• Отраслевых встречах (Вт в 12:00)\n\n"
        "Основные команды:\n"
        "/help - Центр помощи с файлами и ссылками\n"
        "/setchat - Установить чат для рассылки напоминаний\n"
        "/testmeeting - Тест напоминания о планёрке\n"
        "/testindustry - Тест напоминания об отраслевой встрече\n"
        "/status - Статус бота и запланированные события",
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
        f"• Напоминания о планёрках (Пн, Ср, Пт в 9:15)\n"
        f"• Напоминания об отраслевых встречах (Вт в 12:00)\n\n"
        f"ID чата: {update.effective_chat.id}",
        parse_mode=ParseMode.HTML
    )
    
    logger.info(f"Установлен чат {update.effective_chat.id}")
    
    # Планируем ближайшие события
    await schedule_next_meeting(context)
    await schedule_next_industry_meeting(context)

async def test_meeting(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Тестовая отправка напоминания о планёрке"""
    config = BotConfig()
    if not config.chat_id:
        await update.message.reply_text("❌ Сначала установите чат командой /setchat")
        return
    
    await update.message.reply_text("⏳ <b>Отправляю тестовое напоминание о планёрке...</b>", parse_mode=ParseMode.HTML)
    await send_meeting_reminder(context)

async def test_industry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Тестовая отправка напоминания об отраслевой встрече"""
    config = BotConfig()
    if not config.chat_id:
        await update.message.reply_text("❌ Сначала установите чат командой /setchat")
        return
    
    await update.message.reply_text("⏳ <b>Отправляю тестовое напоминание об отраслевой встрече...</b>", parse_mode=ParseMode.HTML)
    await send_industry_meeting_reminder(context)

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показать статус бота и запланированные события"""
    config = BotConfig()
    
    try:
        next_meeting = calculate_next_meeting_time()
        next_industry = calculate_next_industry_meeting_time()
        
        status_text = (
            f"📊 <b>СТАТУС БОТА</b>\n\n"
            f"<b>Чат:</b> {'✅ Установлен' if config.chat_id else '❌ Не установлен'}\n"
            f"<b>ID чата:</b> {config.chat_id or 'Не задан'}\n\n"
            f"<b>📅 Ближайшие события:</b>\n"
            f"• Следующая планёрка: {next_meeting.strftime('%d.%m.%Y %H:%M')}\n"
            f"• След. отраслевая встреча: {next_industry.strftime('%d.%m.%Y %H:%M')}\n\n"
            f"<b>👤 Разрешенные пользователи:</b>\n"
        )
        
        for user in config.allowed_users:
            status_text += f"  • @{user}\n"
        
        await update.message.reply_text(status_text, parse_mode=ParseMode.HTML)
        
    except Exception as e:
        logger.error(f"Ошибка получения статуса: {e}")
        await update.message.reply_text("❌ Ошибка получения статуса", parse_mode=ParseMode.HTML)

# ========== ОБРАБОТЧИКИ КНОПОК СИСТЕМЫ ПОМОЩИ ==========

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик всех callback-кнопок"""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    user = query.from_user.username
    user_id = query.from_user.id
    
    config = BotConfig()
    
    # ========== ОБРАБОТКА СИСТЕМЫ ПОМОЩИ ==========
    
    if data == "help_back":
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
    
    # ========== ОБРАБОТКА ОТМЕНЫ ВСТРЕЧ ==========
    
    elif data.startswith("start_cancel_"):
        await start_cancel(update, context)
    
    elif data.startswith("cancel_"):
        # Проверяем, что это callback от отмены встречи, а не от системы помощи
        if data.startswith("cancel_regular_") or data.startswith("cancel_industry_"):
            await select_cancel_reason(update, context)
        elif data == "cancel_cancel":
            await cancel_cancel(update, context)
    
    elif data.startswith("select_date_"):
        await select_date(update, context)
    
    elif data.startswith("no_date_"):
        await no_date(update, context)
    
    elif data.startswith("confirm_cancel_"):
        await confirm_cancel(update, context)
    
    elif data.startswith("back_reason_"):
        await back_to_reason(update, context)

# ========== ОБРАБОТЧИКИ ДЛЯ ДОБАВЛЕНИЯ ФАЙЛОВ ==========

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик получения документа для добавления"""
    user = update.message.from_user.username
    user_id = update.message.from_user.id
    
    config = BotConfig()
    
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
    user = update.message.from_user.username
    user_id = update.message.from_user.id
    
    config = BotConfig()
    
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
    user = update.message.from_user.username
    user_id = update.message.from_user.id
    
    config = BotConfig()
    
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
        
        # Инициализация конфига
        config = BotConfig()
        
        # Создаем ConversationHandler для отмены встреч
        cancel_conv_handler = ConversationHandler(
            entry_points=[
                CallbackQueryHandler(start_cancel, pattern="^start_cancel_(regular|industry)$")
            ],
            states={
                SELECTING_REASON: [
                    CallbackQueryHandler(select_cancel_reason, pattern="^cancel_(regular|industry)_\d+$"),
                    CallbackQueryHandler(back_to_reason, pattern="^back_reason_(regular|industry)$")
                ],
                SELECTING_DATE: [
                    CallbackQueryHandler(select_date, pattern="^select_date_"),
                    CallbackQueryHandler(no_date, pattern="^no_date_"),
                    CallbackQueryHandler(back_to_reason, pattern="^back_reason_")
                ],
                CONFIRMING_DATE: [
                    CallbackQueryHandler(confirm_cancel, pattern="^confirm_cancel_"),
                    CallbackQueryHandler(cancel_cancel, pattern="^cancel_cancel$")
                ]
            },
            fallbacks=[
                CommandHandler("cancel", cancel_conversation),
                CallbackQueryHandler(cancel_cancel, pattern="^cancel_cancel$")
            ]
        )
        
        # Основные обработчики команд
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("help", help_command))
        application.add_handler(CommandHandler("setchat", restricted(set_chat)))
        application.add_handler(CommandHandler("testmeeting", restricted(test_meeting)))
        application.add_handler(CommandHandler("testindustry", restricted(test_industry)))
        application.add_handler(CommandHandler("status", restricted(status)))
        application.add_handler(CommandHandler("cancel", cancel_command))
        
        # Обработчик callback-кнопок (включая помощь и отмену встреч)
        application.add_handler(CallbackQueryHandler(handle_callback))
        
        # Добавляем ConversationHandler
        application.add_handler(cancel_conv_handler)
        
        # Обработчики для добавления файлов
        application.add_handler(MessageHandler(
            filters.Document.ALL & filters.ChatType.PRIVATE,
            handle_document
        ))
        
        application.add_handler(MessageHandler(
            filters.TEXT & filters.ChatType.PRIVATE & ~filters.COMMAND,
            handle_text
        ))
        
        # Запускаем планировщики с задержкой
        application.job_queue.run_once(
            lambda ctx: asyncio.create_task(schedule_next_meeting(ctx)),
            3
        )
        application.job_queue.run_once(
            lambda ctx: asyncio.create_task(schedule_next_industry_meeting(ctx)),
            5
        )
        
        # Логирование при запуске
        logger.info("🤖 Бот запущен и готов к работе!")
        logger.info(f"📅 Планёрки: Пн, Ср, Пт в 9:15 по МСК")
        logger.info(f"🏢 Отраслевые встречи: Вт в 12:00 по МСК")
        logger.info(f"🔗 Zoom ссылка: {'Настроена' if ZOOM_LINK != DEFAULT_ZOOM_LINK else 'НЕ настроена!'}")
        logger.info(f"🏢 Oтраслевая Zoom: {'Настроена' if INDUSTRY_ZOOM_LINK != DEFAULT_ZOOM_LINK else 'НЕ настроена!'}")
        logger.info(f"📚 Система помощи активна")
        logger.info(f"📁 Файлов в базе помощи: {len(config.help_data['files'])}")
        logger.info(f"🔗 Ссылок в базе помощи: {len(config.help_data['links'])}")
        logger.info(f"👑 Админы: {', '.join(config.admins)}")
        
        application.run_polling(allowed_updates=Update.ALL_TYPES)
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        raise

if __name__ == "__main__":
    main()
