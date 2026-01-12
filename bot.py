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

# Варианты отмены планёрки
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

    keyboard = [
        [InlineKeyboardButton("Отменить планёрку", callback_data="cancel_meeting")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

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

        active_reminder = {
            "message_id": message.message_id,
            "chat_id": chat_id,
            "job": getattr(context, "job", None)
        }

        logger.info(f"Отправлено напоминание в чат {chat_id}")

    except Exception as e:
        logger.error(f"Ошибка при отправке напоминания: {e}")


async def cancel_meeting_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query

    username = query.from_user.username
    if username not in ALLOWED_USERS:
        await query.answer("❌ У вас нет прав для отмены планёрки", show_alert=True)
        return

    await query.answer()

    keyboard = [
        [InlineKeyboardButton(option, callback_data=f"reason_{i}")]
        for i, option in enumerate(CANCELLATION_OPTIONS)
    ]

    await query.edit_message_text(
        text="📝 Выберите причину отмены планёрки:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def reason_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global active_reminder

    query = update.callback_query
    reason_index = int(query.data.split("_")[1])
    reason = CANCELLATION_OPTIONS[reason_index]
    username = query.from_user.username

    if active_reminder and "job" in active_reminder and active_reminder["job"]:
        active_reminder["job"].schedule_removal()
        active_reminder = None

    await query.edit_message_text(
        text=f"❌ @{username} отменил планёрку\n\nПричина: {reason}"
    )

    logger.info(f"Планёрка отменена @{username} — {reason}")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Бот для напоминаний о планёрке активен!\n"
        f"Напоминания отправляются по понедельникам, средам и пятницам в "
        f"{MEETING_TIME['hour']:02d}:{MEETING_TIME['minute']:02d} по МСК.\n\n"
        "Для установки чата используйте команду /setchat"
    )


async def set_chat(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
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
        "Напоминания будут отправляться в этот чат."
    )

    logger.info(f"Установлен чат {chat_title} ({chat_id})")


async def show_info(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.username not in ALLOWED_USERS:
        await update.message.reply_text("❌ У вас нет прав для этой команды")
        return

    config = load_config()
    chat_id = config.get("chat_id")

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
    if update.effective_user.username not in ALLOWED_USERS:
        await update.message.reply_text("❌ У вас нет прав для этой команды")
        return

    config = load_config()
    if not config.get("chat_id"):
        await update.message.reply_text("❌ Сначала установите чат командой /setchat")
        return

    context.application.job_queue.run_once(send_reminder, 5, chat_id=config["chat_id"])

    await update.message.reply_text("⏳ Тестовое напоминание будет отправлено через 5 секунд...")


async def test_now(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Мгновенная отправка уведомления"""
    if update.effective_user.username not in ALLOWED_USERS:
        await update.message.reply_text("❌ У вас нет прав для этой команды")
        return

    config = load_config()
    if not config.get("chat_id"):
        await update.message.reply_text("❌ Сначала установите чат командой /setchat")
        return

    await update.message.reply_text("🚀 Отправляю тестовое напоминание прямо сейчас...")

    class DummyJob:
        def __init__(self):
            self.name = "manual_test_job"

    context.job = DummyJob()

    await send_reminder(context)


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
    config = load_config()
    chat_id = config.get("chat_id")

    if not chat_id:
        logger.warning("Chat ID не установлен, планирование отложено")
        return

    now = datetime.now(TIMEZONE)
    delay = (next_time - now).total_seconds()

    if delay > 0:
        context.application.job_queue.run_once(
            send_reminder,
            delay,
            chat_id=chat_id,
            name=f"meeting_reminder_{next_time.strftime('%Y%m%d_%H%M')}"
        )

        context.application.job_queue.run_once(
            schedule_next_reminder,
            delay + 60,
            chat_id=chat_id
        )

        logger.info(f"Следующее напоминание запланировано на {next_time}")


def main() -> None:
    if not TOKEN:
        logger.error("Токен бота не найден!")
        return

    application = Application.builder().token(TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("setchat", set_chat))
    application.add_handler(CommandHandler("info", show_info))
    application.add_handler(CommandHandler("test", test_reminder))
    application.add_handler(CommandHandler("testnow", test_now))

    application.add_handler(CallbackQueryHandler(cancel_meeting_callback, pattern="^cancel_meeting$"))
    application.add_handler(CallbackQueryHandler(reason_callback, pattern="^reason_"))

    application.job_queue.run_once(
        lambda context: schedule_next_reminder(context),
        2
    )

    logger.info("Бот запущен...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
