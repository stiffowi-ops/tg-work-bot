import os
import logging
import json
import random
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from functools import wraps
import pytz

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
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

# Время планёрки (9:15 по Москве)
MEETING_TIME = {"hour": 9, "minute": 15}
TIMEZONE = pytz.timezone("Europe/Moscow")

# Дни недели для планёрки (понедельник=0, среда=2, пятница=4)
MEETING_DAYS = [0, 2, 4]

# Варианты отмены планёрки
CANCELLATION_OPTIONS = [
    "Все вопросы решены, планёрка не нужна",
    "Ключевые участники отсутствуют",
    "Перенесём на другой день",
]

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
    """Специальные приветствия для дней планёрок с ссылкой на Zoom"""
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
        
        # Форматируем ссылку на Zoom для Markdown
        zoom_link_formatted = f"[Ссылка на Zoom]({ZOOM_LINK})"
        
        # Варианты упоминания Zoom
        zoom_notes = [
            f"\n\n🎥 *{zoom_link_formatted}* - подключение через календарное приглашение в почте",
            f"\n\n👨‍💻 *{zoom_link_formatted}* - присоединяйтесь через календарь в почте",
            f"\n\n💻 *{zoom_link_formatted}* - детали в календарном приглашении",
            f"\n\n🔗 *{zoom_link_formatted}* - подключение как обычно через календарь",
            f"\n\n📅 *{zoom_link_formatted}* - ссылка также в календарном приглашении",
            f"\n\n✉️ *{zoom_link_formatted}* - проверьте календарное приглашение для пароля",
        ]
        
        zoom_note = random.choice(zoom_notes)
        
        greetings = {
            0: [
                f"🚀 **{day_names[0]}** - старт новой недели!\n\n📋 *Планёрка в 9:15 по МСК*.\nДавайте обсудим планы на неделю! 🌟{zoom_note}",
                f"🌞 Доброе утро! Сегодня **{day_names[0]}**!\n\n🤝 *Планёрка в 9:15 по МСК*.\nНачинаем неделю продуктивно! 💪{zoom_note}",
                f"⚡ **{day_names[0]}**, время действовать!\n\n🎯 *Утренняя планёрка в 9:15 по МСК*.\nГотовьте вопросы и обновления! 📊{zoom_note}"
            ],
            2: [
                f"⚡ **{day_names[2]}** - середина недели!\n\n📋 *Планёрка в 9:15 по МСК*.\nВремя для корректировок и обновлений! 🔄{zoom_note}",
                f"🌞 **{day_names[2]}**, доброе утро!\n\n🤝 *Планёрка в 9:15 по МСК*.\nКак продвигаются задачи? 📈{zoom_note}",
                f"💪 **{day_names[2]}** - день прорыва!\n\n🎯 *Планёрка в 9:15 по МСК*.\nДелитесь прогрессом! 🚀{zoom_note}"
            ],
            4: [
                f"🎉 **{day_names[4]}** - завершаем неделю!\n\n📋 *Планёрка в 9:15 по МСК*.\nДавайте подведем итоги недели! 🏆{zoom_note}",
                f"🌞 Пятничное утро! 🎊\n\n🤝 **{day_names[4]}**, *планёрка в 9:15 по МСК*.\nГотовьте отчеты о неделе! 📊{zoom_note}",
                f"✨ **{day_names[4]}** - время подводить итоги!\n\n🎯 *Планёрка в 9:15 по МСК*.\nЧто успели за неделю? 📈{zoom_note}"
            ]
        }
        
        return random.choice(greetings[weekday])
    else:
        # Если почему-то напоминание отправлено не в день планёрки
        zoom_link_formatted = f"[Ссылка на Zoom]({ZOOM_LINK})"
        return f"👋 Доброе утро! Сегодня *{current_day}*.\n\n📋 *Напоминаю о планёрке в 9:15 по МСК*.\nПрисоединяйтесь к звонку! 🤝\n\n🎥 *{zoom_link_formatted}* - подключение через календарное приглашение"


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
                    return data
            except Exception as e:
                logger.error(f"Ошибка загрузки конфига: {e}")
        return {
            "chat_id": None,
            "allowed_users": ["Stiff_OWi", "gshabanov"],
            "active_reminders": {}
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
            parse_mode="Markdown",
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
    
    reason_index = int(query.data.split("_")[1])
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
             "*Ближайшие дни планёрок (Пн/Ср/Пт):*",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
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
                 "*Важно:* выбирайте только дни планёрок (понедельник, среда, пятница)\n\n"
                 "Или отправьте 'отмена' для возврата.",
            parse_mode="Markdown"
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
    selected_date_str = query.data.split("_")[1]
    selected_date = datetime.strptime(selected_date_str, "%Y-%m-%d")
    
    # Сохраняем выбранную дату
    context.user_data["selected_date"] = selected_date_str
    context.user_data["selected_date_display"] = selected_date.strftime("%d.%m.%Y")
    
    # Переходим к подтверждению
    return await confirm_cancellation(update, context)


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
        return await show_confirmation(update, context)
        
    except ValueError as e:
        await update.message.reply_text(
            "❌ Неверный формат даты! Используйте ДД.ММ.ГГГГ\n"
            "Например: 15.12.2024\n\n"
            "Попробуйте снова или отправьте 'отмена':"
        )
        return CONFIRMING_DATE


async def show_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Показать подтверждение отмены"""
    reason = context.user_data.get("selected_reason", "")
    selected_date = context.user_data.get("selected_date_display", "")
    
    message = f"📋 *Подтверждение отмены планёрки:*\n\n"
    
    if "Перенесём" in reason:
        message += f"❌ *Отмена сегодняшней планёрки*\n"
        message += f"📅 *Перенос на {selected_date}*\n\n"
        message += "*Подтвердить отмену?*"
    else:
        message += f"❌ *Отмена планёрки*\n"
        message += f"📝 *Причина:* {reason}\n\n"
        message += "*Подтвердить отмену?*"
    
    keyboard = [
        [
            InlineKeyboardButton("✅ Да, отменить", callback_data="confirm_cancel"),
            InlineKeyboardButton("❌ Нет, вернуться", callback_data="back_to_reasons")
        ]
    ]
    
    if update.callback_query:
        await update.callback_query.edit_message_text(
            text=message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
    else:
        await update.message.reply_text(
            text=message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
    
    return CONFIRMING_DATE


async def confirm_cancellation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Показать подтверждение отмены после выбора причины/даты"""
    return await show_confirmation(update, context)


async def execute_cancellation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Выполнить отмену планёрки"""
    query = update.callback_query
    await query.answer()
    
    config = BotConfig()
    reason = context.user_data.get("selected_reason", "")
    reason_index = context.user_data.get("reason_index", -1)
    username = query.from_user.username
    
    # Формируем финальное сообщение
    if reason_index == 2:  # "Перенесём на другой день"
        selected_date = context.user_data.get("selected_date_display", "")
        final_message = f"❌ @{username} отменил сегодняшнюю планёрку\n\n📅 *Перенос на {selected_date}*"
    else:
        final_message = f"❌ @{username} отменил планёрку\n\n📝 *Причина:* {reason}"
    
    # Удаляем задание из планировщика
    original_message_id = context.user_data.get("original_message_id")
    job_name_to_remove = None
    
    for job_name, reminder_data in config.active_reminders.items():
        if reminder_data.get("message_id") == original_message_id:
            # Ищем задание в планировщике
            for job in context.application.job_queue.jobs():
                if job.name == job_name:
                    job.schedule_removal()
                    job_name_to_remove = job_name
                    break
    
    # Удаляем из конфига
    if job_name_to_remove:
        config.remove_active_reminder(job_name_to_remove)
    
    # Отправляем финальное сообщение
    await query.edit_message_text(
        text=final_message,
        parse_mode="Markdown"
    )
    
    logger.info(f"Планёрка отменена @{username} — {reason}")
    
    # Очищаем user_data
    context.user_data.clear()
    
    return ConversationHandler.END


async def cancel_conversation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отмена диалога"""
    await update.message.reply_text("❌ Диалог отменен.")
    context.user_data.clear()
    return ConversationHandler.END


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик команды /start"""
    await update.message.reply_text(
        "🤖 *Бот для напоминаний о планёрке активен!*\n\n"
        f"📅 *Напоминания отправляются:*\n"
        f"• Понедельник\n• Среда\n• Пятница\n"
        f"⏰ *Время:* {MEETING_TIME['hour']:02d}:{MEETING_TIME['minute']:02d} по МСК\n\n"
        "🔧 *Доступные команды:*\n"
        "/info - информация о боте\n"
        "/jobs - список запланированных задач\n"
        "/test - тестовое напоминание (через 5 сек)\n"
        "/testnow - мгновенное тестовое напоминание\n\n"
        "👮‍♂️ *Команды для администраторов:*\n"
        "/setchat - установить чат для уведомлений\n"
        "/adduser @username - добавить пользователя\n"
        "/removeuser @username - удалить пользователя\n"
        "/users - список пользователей\n"
        "/cancelall - отменить все напоминания",
        parse_mode="Markdown"
    )


@restricted
async def set_chat(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Установка чата для уведомлений"""
    chat_id = update.effective_chat.id
    chat_title = update.effective_chat.title or "личный чат"

    config = BotConfig()
    config.chat_id = chat_id

    await update.message.reply_text(
        f"✅ *Чат установлен:* {chat_title}\n"
        f"*Chat ID:* {chat_id}\n\n"
        "Напоминания будут отправляться в этот чат.",
        parse_mode="Markdown"
    )

    logger.info(f"Установлен чат {chat_title} ({chat_id})")


@restricted
async def show_info(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показать информацию о боте"""
    config = BotConfig()
    chat_id = config.chat_id

    if chat_id:
        status = f"✅ *Чат установлен* (ID: {chat_id})"
    else:
        status = "❌ *Чат не установлен*. Используйте /setchat"

    # Подсчет запланированных задач
    job_count = len([j for j in context.application.job_queue.jobs() 
                    if j.name and j.name.startswith("meeting_reminder_")])
    
    # Следующее напоминание
    next_job = None
    for job in context.application.job_queue.jobs():
        if job.name and job.name.startswith("meeting_reminder_"):
            if not next_job or job.next_t < next_job.next_t:
                next_job = job
    
    next_time = next_job.next_t.astimezone(TIMEZONE) if next_job else "не запланировано"
    
    # Ближайшие дни планёрок
    today = datetime.now(TIMEZONE)
    upcoming_meetings = []
    for i in range(1, 8):
        next_day = today + timedelta(days=i)
        if next_day.weekday() in MEETING_DAYS:
            upcoming_meetings.append(next_day.strftime("%d.%m.%Y"))

    # Показываем текущую Zoom-ссылку (без полного URL)
    zoom_info = f"\n🎥 *Zoom-ссылка:* {'установлена' if ZOOM_LINK and ZOOM_LINK != 'https://us04web.zoom.us/j/1234567890?pwd=example' else 'не установлена (используйте переменную ZOOM_MEETING_LINK)'}"

    await update.message.reply_text(
        f"📊 *Информация о боте:*\n\n"
        f"{status}\n"
        f"📅 *Дни планёрок:* понедельник, среда, пятница\n"
        f"⏰ *Время:* {MEETING_TIME['hour']:02d}:{MEETING_TIME['minute']:02d} по МСК\n"
        f"👥 *Разрешённые пользователи:* {len(config.allowed_users)}\n"
        f"📋 *Активные напоминания:* {len(config.active_reminders)}\n"
        f"⏳ *Запланировано задач:* {job_count}\n"
        f"➡️ *Следующее напоминание:* {next_time}\n"
        f"📈 *Ближайшие планёрки:* {', '.join(upcoming_meetings[:3]) if upcoming_meetings else 'нет'}"
        f"{zoom_info}\n\n"
        f"Используйте /users для списка пользователей\n"
        f"Используйте /jobs для списка задач",
        parse_mode="Markdown"
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
    else:
        day_type = "не день планёрки ⚠️"
    
    await update.message.reply_text(
        f"⏳ *Тестовое напоминание будет отправлено через 5 секунд...*\n\n"
        f"📅 *Сегодня:* {current_day} ({day_type})\n"
        f"⏰ *Время:* {MEETING_TIME['hour']:02d}:{MEETING_TIME['minute']:02d} по МСК\n"
        f"🎥 *Zoom-ссылка:* {'установлена ✅' if ZOOM_LINK and ZOOM_LINK != 'https://us04web.zoom.us/j/1234567890?pwd=example' else 'не установлена ⚠️'}",
        parse_mode="Markdown"
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
    
    await update.message.reply_text(
        f"🚀 *Отправляю тестовое напоминание прямо сейчас...*\n\n"
        f"📅 *Сегодня:* {current_day}\n"
        f"⏰ *Время:* {MEETING_TIME['hour']:02d}:{MEETING_TIME['minute']:02d} по МСК",
        parse_mode="Markdown"
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
    jobs = context.application.job_queue.jobs()
    
    if not jobs:
        await update.message.reply_text("📭 *Нет запланированных задач.*", parse_mode="Markdown")
        return
    
    meeting_jobs = [j for j in jobs if j.name and j.name.startswith("meeting_reminder_")]
    other_jobs = [j for j in jobs if j not in meeting_jobs]
    
    message = "📋 *Запланированные задачи:*\n\n"
    
    if meeting_jobs:
        message += "🔔 *Напоминания о планёрках:*\n"
        for job in sorted(meeting_jobs, key=lambda j: j.next_t):
            next_time = job.next_t.astimezone(TIMEZONE)
            message += f"  • {next_time.strftime('%d.%m.%Y %H:%M')} ({job.name})\n"
    
    if other_jobs:
        message += "\n🔧 *Другие задачи:*\n"
        for job in other_jobs:
            next_time = job.next_t.astimezone(TIMEZONE)
            job_name = job.name or "Без имени"
            message += f"  • {next_time.strftime('%d.%m.%Y %H:%M')} ({job_name})\n"
    
    await update.message.reply_text(message, parse_mode="Markdown")


@restricted
async def add_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Добавить пользователя в список разрешенных"""
    if not context.args:
        await update.message.reply_text("❌ *Используйте:* /adduser @username", parse_mode="Markdown")
        return
    
    username = context.args[0].lstrip('@')
    config = BotConfig()
    
    if config.add_allowed_user(username):
        await update.message.reply_text(f"✅ *Пользователь @{username} добавлен*", parse_mode="Markdown")
        logger.info(f"Добавлен пользователь @{username}")
    else:
        await update.message.reply_text(f"ℹ️ *Пользователь @{username} уже есть в списке*", parse_mode="Markdown")


@restricted
async def remove_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Удалить пользователя из списка разрешенных"""
    if not context.args:
        await update.message.reply_text("❌ *Используйте:* /removeuser @username", parse_mode="Markdown")
        return
    
    username = context.args[0].lstrip('@')
    config = BotConfig()
    
    if config.remove_allowed_user(username):
        await update.message.reply_text(f"✅ *Пользователь @{username} удален*", parse_mode="Markdown")
        logger.info(f"Удален пользователь @{username}")
    else:
        await update.message.reply_text(f"❌ *Пользователь @{username} не найден*", parse_mode="Markdown")


@restricted
async def list_users(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показать список разрешенных пользователей"""
    config = BotConfig()
    users = config.allowed_users
    
    if not users:
        await update.message.reply_text("📭 *Список пользователей пуст*", parse_mode="Markdown")
        return
    
    message = "👥 *Разрешенные пользователи:*\n\n"
    for i, user in enumerate(users, 1):
        message += f"{i}. @{user}\n"
    
    message += f"\n*Всего:* {len(users)} пользователь(ей)"
    await update.message.reply_text(message, parse_mode="Markdown")


@restricted
async def cancel_all(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отменить все запланированные напоминания"""
    jobs = context.application.job_queue.jobs()
    canceled_count = 0
    
    for job in jobs[:]:  # Копируем список для безопасного удаления
        if job.name and job.name.startswith("meeting_reminder_"):
            job.schedule_removal()
            canceled_count += 1
    
    # Очищаем активные напоминания в конфиге
    config = BotConfig()
    config.clear_active_reminders()
    
    await update.message.reply_text(
        f"✅ *Отменено {canceled_count} напоминаний(я)*\n"
        f"Очищено {len(config.active_reminders)} активных напоминаний в конфиге",
        parse_mode="Markdown"
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
        existing_jobs = [j for j in context.application.job_queue.jobs() 
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
    jobs_by_name = {}
    jobs_to_remove = []
    
    for job in job_queue.jobs():
        if job.name:
            if job.name in jobs_by_name:
                # Дубликат - удаляем старую задачу
                jobs_to_remove.append(jobs_by_name[job.name])
            jobs_by_name[job.name] = job
    
    # Удаляем просроченные задачи
    now = datetime.now(TIMEZONE)
    for job in job_queue.jobs():
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
    else:
        logger.info(f"✅ Zoom-ссылка загружена (первые 50 символов): {ZOOM_LINK[:50]}...")

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
                    CallbackQueryHandler(cancel_meeting_callback, pattern="^back_to_reasons$"),
                ],
            },
            fallbacks=[
                CommandHandler("cancel", cancel_conversation),
                CallbackQueryHandler(cancel_conversation, pattern="^cancel$"),
            ],
        )

        # Обработчики команд
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("setchat", set_chat))
        application.add_handler(CommandHandler("info", show_info))
        application.add_handler(CommandHandler("test", test_reminder))
        application.add_handler(CommandHandler("testnow", test_now))
        application.add_handler(CommandHandler("jobs", list_jobs))
        application.add_handler(CommandHandler("adduser", add_user))
        application.add_handler(CommandHandler("removeuser", remove_user))
        application.add_handler(CommandHandler("users", list_users))
        application.add_handler(CommandHandler("cancelall", cancel_all))

        # Добавляем ConversationHandler
        application.add_handler(conv_handler)

        # Очистка старых задач при запуске
        cleanup_old_jobs(application.job_queue)
        
        # Восстановление напоминаний
        restore_reminders(application)

        # Запуск планировщика
        application.job_queue.run_once(
            lambda context: schedule_next_reminder(context),
            3
        )

        logger.info("🤖 Бот запущен и готов к работе!")
        logger.info(f"⏰ Планёрки: {', '.join(['Пн', 'Ср', 'Пт'])} в {MEETING_TIME['hour']:02d}:{MEETING_TIME['minute']:02d}")
        
        application.run_polling(allowed_updates=Update.ALL_TYPES)

    except Exception as e:
        logger.error(f"❌ Критическая ошибка при запуске бота: {e}")
        raise


if __name__ == "__main__":
    main()
