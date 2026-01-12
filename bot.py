import os
import logging
import json
from datetime import datetime, timedelta
from typing import Optional, Dict
import pytz

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    JobQueue,
    MessageHandler,
    filters
)

# Настройка логирования
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# Конфигурация
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")  # Токен бота из переменных окружения
CONFIG_FILE = "bot_config.json"  # Файл для хранения настроек

ALLOWED_USERS = ["Stiff_OWi", "gshabanov"]  # Разрешенные пользователи для отмены

# Время планёрки (9:15 по Москве)
MEETING_TIME = {"hour": 9, "minute": 15}
TIMEZONE = pytz.timezone("Europe/Moscow")

# Дни недели для планёрки (понедельник=0, среда=2, пятница=4)
MEETING_DAYS = [0, 2, 4]

# Варианты отмены планёрки (можно редактировать)
CANCELLATION_OPTIONS = [
    "Перенесём на другой день. Дата такая-то",
    "Причину сообщу позже",
    "Все вопросы решены, планёрка не нужна",
    "Ключевые участники отсутствуют",
    "Экстренная ситуация, подробности в ЛС",
]

# Глобальная переменная для хранения активного напоминания
active_reminder: Optional[Dict] = None

def load_config() -> Dict:
    """Загрузка конфигурации из файла"""
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Ошибка загрузки конфига: {e}")
    return {"chat_id": None}

def save_config(config: Dict) -> None:
    """Сохранение конфигурации в файл"""
    try:
        with open(CONFIG_FILE, 'w') as f:
            json.dump(config, f)
    except Exception as e:
        logger.error(f"Ошибка сохранения конфига: {e}")

async def send_reminder(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправка напоминания о планёрке"""
    global active_reminder
    
    config = load_config()
    chat_id = config.get("chat_id")
    
    if not chat_id:
        logger.error("Chat ID не установлен!")
        return
    
    # Создаем клавиатуру с кнопкой отмены
    keyboard = [
        [InlineKeyboardButton("Отменить планёрку", callback_data="cancel_meeting")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Текст напоминания
    message_text = (
        "👋 Коллеги, доброе утро!\n\n"
        "📋 Напоминаю о ежедневной планёрке в 9:15 по МСК.\n"
        "Присоединяйтесь к звонку!"
    )
    
    try:
        message = await context.bot.send_message(
            chat_id=chat_id,
            text=message_text,
            reply_markup=reply_markup
        )
        
        # Сохраняем информацию о напоминании
        active_reminder = {
            "message_id": message.message_id,
            "chat_id": chat_id,
            "job": context.job
        }
        
        logger.info(f"Отправлено напоминание о планёрке в чат {chat_id}")
        
    except Exception as e:
        logger.error(f"Ошибка при отправке напоминания: {e}")

async def cancel_meeting_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработка нажатия на кнопку отмены планёрки"""
    query = update.callback_query
    
    # Проверяем, разрешен ли пользователь
    username = query.from_user.username
    if username not in ALLOWED_USERS:
        await query.answer("❌ У вас нет прав для отмены планёрки", show_alert=True)
        return
    
    await query.answer()
    
    # Создаем варианты отмены
    keyboard = []
    for i, option in enumerate(CANCELLATION_OPTIONS):
        keyboard.append([InlineKeyboardButton(option, callback_data=f"reason_{i}")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Редактируем сообщение
    await query.edit_message_text(
        text="📝 Выберите причину отмены планёрки:",
        reply_markup=reply_markup
    )

async def reason_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработка выбора причины отмены"""
    global active_reminder
    
    query = update.callback_query
    reason_index = int(query.data.split("_")[1])
    reason = CANCELLATION_OPTIONS[reason_index]
    username = query.from_user.username
    
    # Отменяем задачу напоминания
    if active_reminder and "job" in active_reminder:
        active_reminder["job"].schedule_removal()
        active_reminder = None
    
    # Обновляем сообщение
    await query.edit_message_text(
        text=f"❌ @{username} отменил планёрку\n\nПричина: {reason}",
        reply_markup=None
    )
    
    logger.info(f"Планёрка отменена пользователем @{username}. Причина: {reason}")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /start для проверки работы бота"""
    await update.message.reply_text(
        "Бот для напоминаний о планёрке активен!\n"
        f"Напоминания отправляются по понедельникам, средам и пятницам в {MEETING_TIME['hour']:02d}:{MEETING_TIME['minute']:02d} по МСК.\n\n"
        "Для установки чата используйте команду /setchat"
    )

async def set_chat(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Установка текущего чата для отправки уведомлений"""
    if update.effective_user.username not in ALLOWED_USERS:
        await update.message.reply_text("❌ У вас нет прав для этой команды")
        return
    
    chat_id = update.effective_chat.id
    chat_title = update.effective_chat.title or "личный чат"
    
    config = load_config()
    config["chat_id"] = chat_id
    save_config(config)
    
    await update.message.reply_text(
        f"✅ Чат установлен: {chat_title}\n"
        f"Chat ID: {chat_id}\n\n"
        f"Напоминания будут отправляться в этот чат."
    )
    
    logger.info(f"Установлен чат: {chat_title} (ID: {chat_id})")

async def show_info(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показать информацию о текущих настройках"""
    config = load_config()
    chat_id = config.get("chat_id")
    
    if update.effective_user.username not in ALLOWED_USERS:
        await update.message.reply_text("❌ У вас нет прав для этой команды")
        return
    
    if chat_id:
        status = f"✅ Чат установлен (ID: {chat_id})"
    else:
        status = "❌ Чат не установлен. Используйте /setchat"
    
    await update.message.reply_text(
        f"Информация о боте:\n\n"
        f"{status}\n"
        f"Дни планёрок: понедельник, среда, пятница\n"
        f"Время: {MEETING_TIME['hour']:02d}:{MEETING_TIME['minute']:02d} по МСК\n"
        f"Разрешённые пользователи: {', '.join(ALLOWED_USERS)}"
    )

async def test_reminder(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Тестовая отправка напоминания"""
    if update.effective_user.username not in ALLOWED_USERS:
        await update.message.reply_text("❌ У вас нет прав для этой команды")
        return
    
    config = load_config()
    if not config.get("chat_id"):
        await update.message.reply_text("❌ Сначала установите чат командой /setchat")
        return
    
    # Отправляем тестовое напоминание через 5 секунд
    context.application.job_queue.run_once(send_reminder, 5, chat_id=config["chat_id"])
    await update.message.reply_text("✅ Тестовое напоминание будет отправлено через 5 секунд...")

def calculate_next_reminder() -> datetime:
    """Вычисляет время следующего напоминания"""
    now = datetime.now(TIMEZONE)
    
    # Проверяем текущий день
    current_weekday = now.weekday()
    
    # Если сегодня день планёрки и время еще не прошло
    if current_weekday in MEETING_DAYS:
        reminder_time = now.replace(
            hour=MEETING_TIME['hour'],
            minute=MEETING_TIME['minute'],
            second=0,
            microsecond=0
        )
        
        if now < reminder_time:
            return reminder_time
    
    # Ищем следующий день планёрки
    days_ahead = 1
    while True:
        next_day = now + timedelta(days=days_ahead)
        if next_day.weekday() in MEETING_DAYS:
            reminder_time = next_day.replace(
                hour=MEETING_TIME['hour'],
                minute=MEETING_TIME['minute'],
                second=0,
                microsecond=0
            )
            return reminder_time
        days_ahead += 1

async def schedule_next_reminder(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Планирует следующее напоминание"""
    next_time = calculate_next_reminder()
    config = load_config()
    chat_id = config.get("chat_id")
    
    if not chat_id:
        logger.warning("Chat ID не установлен, планирование отложено")
        return
    
    # Рассчитываем интервал до следующего напоминания
    now = datetime.now(TIMEZONE)
    delay = (next_time - now).total_seconds()
    
    if delay > 0:
        job = context.application.job_queue.run_once(
            send_reminder,
            delay,
            chat_id=chat_id,
            name=f"meeting_reminder_{next_time.strftime('%Y%m%d_%H%M')}"
        )
        
        # После отправки напоминания планируем следующее
        context.application.job_queue.run_once(
            schedule_next_reminder,
            delay + 60,  # Через минуту после отправки напоминания
            chat_id=chat_id
        )
        
        logger.info(f"Следующее напоминание запланировано на {next_time} (через {delay/3600:.1f} часов)")

def main() -> None:
    """Запуск бота"""
    # Проверка токена
    if not TOKEN:
        logger.error("Токен бота не найден! Установите переменную окружения TELEGRAM_BOT_TOKEN")
        return
    
    # Создаем приложение
    application = Application.builder().token(TOKEN).build()
    
    # Обработчики команд
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("setchat", set_chat))
    application.add_handler(CommandHandler("info", show_info))
    application.add_handler(CommandHandler("test", test_reminder))
    
    # Обработчики callback-кнопок
    application.add_handler(CallbackQueryHandler(cancel_meeting_callback, pattern="^cancel_meeting$"))
    application.add_handler(CallbackQueryHandler(reason_callback, pattern="^reason_"))
    
    # Запускаем планировщик при старте
    application.job_queue.run_once(
        lambda context: schedule_next_reminder(context),
        2,
        chat_id=None
    )
    
    # Запуск бота
    logger.info("Бот запущен...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
