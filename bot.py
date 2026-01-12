import logging
import json
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from config import (
    BOT_TOKEN,
    CHAT_ID,
    ADMIN_IDS,
    IS_CONFIGURED,
    REMINDER_TIMES,
    REMINDER_DAYS,
    REMINDER_TEXT,
    CANCELLATION_REASONS,
    save_settings,
    settings,
)
from utils import update_chat_settings, get_chat_admins

# Настройка логирования
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# Хранилище отменённых планёрок
cancelled_meetings = {}

# ================== Клавиатуры ==================

def get_reminder_keyboard():
    keyboard = [
        [InlineKeyboardButton("❌ Отменить планёрку", callback_data="cancel_meeting")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_cancellation_reasons_keyboard():
    keyboard = []
    for reason in CANCELLATION_REASONS:
        if reason.startswith("Перенесём на другой день"):
            tomorrow = (datetime.now() + timedelta(days=1)).strftime("%d.%m.%Y")
            reason_with_date = reason + tomorrow
            keyboard.append([InlineKeyboardButton(reason_with_date, callback_data=f"cancel_reason:0:{tomorrow}")])
        else:
            idx = CANCELLATION_REASONS.index(reason)
            keyboard.append([InlineKeyboardButton(reason, callback_data=f"cancel_reason:{idx}")])
    
    keyboard.append([InlineKeyboardButton("↩️ Назад", callback_data="back_to_cancel")])
    return InlineKeyboardMarkup(keyboard)

# ================== Обработчики команд ==================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    user_id = update.effective_user.id
    
    if update.message.chat.type == "private":
        if IS_CONFIGURED and CHAT_ID:
            chat_info = await context.bot.get_chat(CHAT_ID)
            await update.message.reply_text(
                f"Бот уже настроен для чата:\n"
                f"📋 Название: {chat_info.title}\n"
                f"🆔 Chat ID: {CHAT_ID}\n"
                f"👑 Админов: {len(ADMIN_IDS)}\n"
                f"⏰ Напоминания: ПН, СР, ПТ в 9:15 по МСК\n\n"
                f"Планёрка в 9:30 по МСК."
            )
        else:
            await update.message.reply_text(
                "Привет! Я бот для напоминаний о планёрках.\n\n"
                "Напоминания: ПН, СР, ПТ в 9:15 по МСК\n"
                "Планёрка: в 9:30 по МСК\n\n"
                "1. Добавьте меня в группу\n"
                "2. Дайте права администратора\n"
                "3. Отправьте в группе команду /setup\n\n"
                "Я автоматически определю чат и список администраторов."
            )

async def setup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Настройка бота в группе"""
    chat = update.effective_chat
    
    if chat.type not in ["group", "supergroup"]:
        await update.message.reply_text("Эту команду нужно использовать в группе!")
        return
    
    # Проверяем, отправитель — администратор?
    try:
        member = await chat.get_member(update.effective_user.id)
        if member.status not in ["creator", "administrator"]:
            await update.message.reply_text("Только администраторы могут настраивать бота!")
            return
    except:
        await update.message.reply_text("Не удалось проверить ваши права!")
        return
    
    # Получаем список администраторов
    admin_ids = get_chat_admins(chat.id, context.bot)
    
    if not admin_ids:
        await update.message.reply_text("Не удалось получить список администраторов!")
        return
    
    # Сохраняем настройки
    global CHAT_ID, ADMIN_IDS, IS_CONFIGURED
    new_settings = update_chat_settings(chat.id, admin_ids)
    
    # Обновляем глобальные переменные
    CHAT_ID = new_settings["chat_id"]
    ADMIN_IDS = new_settings["admin_ids"]
    IS_CONFIGURED = new_settings["is_configured"]
    
    # Форматируем время для отображения
    reminder_time = REMINDER_TIMES[0]
    utc_time = f"{reminder_time['hour']:02d}:{reminder_time['minute']:02d} UTC"
    
    await update.message.reply_text(
        f"✅ Настройка завершена!\n"
        f"Чат: {chat.title}\n"
        f"ID чата: {chat.id}\n"
        f"Администраторы: {len(admin_ids)} пользователей\n\n"
        f"Напоминания будут отправляться:\n"
        f"📅 Дни: понедельник, среда, пятница\n"
        f"⏰ Время: {utc_time} (9:15 по МСК)\n"
        f"🎯 Планёрка в 9:30 по МСК\n\n"
        f"Используйте /refresh_admins чтобы обновить список администраторов."
    )
    
    logger.info(f"Бот настроен для чата {chat.id} с {len(admin_ids)} администраторами")

async def refresh_admins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обновление списка администраторов"""
    chat = update.effective_chat
    
    if not IS_CONFIGURED or chat.id != CHAT_ID:
        await update.message.reply_text("Бот ещё не настроен для этого чата!")
        return
    
    # Проверяем права
    try:
        member = await chat.get_member(update.effective_user.id)
        if member.status not in ["creator", "administrator"]:
            await update.message.reply_text("Только администраторы могут обновлять список!")
            return
    except:
        await update.message.reply_text("Не удалось проверить ваши права!")
        return
    
    # Обновляем список
    admin_ids = get_chat_admins(chat.id, context.bot)
    new_settings = update_chat_settings(chat.id, admin_ids)
    
    global ADMIN_IDS
    ADMIN_IDS = new_settings["admin_ids"]
    
    await update.message.reply_text(
        f"✅ Список администраторов обновлён!\n"
        f"Теперь администраторов: {len(admin_ids)}"
    )

# ================== Напоминания ==================

async def send_reminder(context: ContextTypes.DEFAULT_TYPE):
    """Отправка напоминания в чат"""
    if not IS_CONFIGURED or not CHAT_ID:
        logger.warning("Бот не настроен, пропускаем напоминание")
        return
    
    try:
        keyboard = get_reminder_keyboard()
        message = await context.bot.send_message(
            chat_id=CHAT_ID,
            text=REMINDER_TEXT,
            reply_markup=keyboard,
        )
        context.job.data = message.message_id
        logger.info(f"Напоминание отправлено в чат {CHAT_ID} (9:15 МСК)")
    except Exception as e:
        logger.error(f"Ошибка отправки напоминания: {e}")

# ================== Обработчики кнопок ==================

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка нажатий на кнопок"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    chat_id = query.message.chat_id
    
    # Проверка, что это настроенный чат
    if not IS_CONFIGURED or chat_id != CHAT_ID:
        await query.edit_message_text("Бот не настроен для этого чата!")
        return
    
    # Проверка прав администратора
    if user_id not in ADMIN_IDS:
        await query.edit_message_text("У вас нет прав для отмены планёрки.")
        return
    
    data = query.data
    
    if data == "cancel_meeting":
        keyboard = get_cancellation_reasons_keyboard()
        await query.edit_message_text(
            text="Выберите причину отмены планёрки:",
            reply_markup=keyboard
        )
    
    elif data == "back_to_cancel":
        original_text = REMINDER_TEXT + "\n\n(Планёрка ещё не отменена)"
        await query.edit_message_text(
            text=original_text,
            reply_markup=get_reminder_keyboard()
        )
    
    elif data.startswith("cancel_reason"):
        parts = data.split(":")
        reason_idx = int(parts[1])
        
        if len(parts) > 2 and parts[2]:
            date = parts[2]
            reason_text = CANCELLATION_REASONS[reason_idx] + date
        else:
            reason_text = CANCELLATION_REASONS[reason_idx]
        
        cancelled_text = f"❌ Планёрка ОТМЕНЕНА\nПричина: {reason_text}"
        await query.edit_message_text(
            text=cancelled_text,
            reply_markup=None
        )
        
        cancelled_meetings[f"{chat_id}_{query.message.message_id}"] = {
            "date": datetime.now().isoformat(),
            "reason": reason_text,
            "cancelled_by": user_id,
        }
        
        logger.info(f"Планёрка отменена пользователем {user_id}. Причина: {reason_text}")

# ================== Планировщик ==================

def setup_jobs(application):
    """Настройка регулярных напоминаний"""
    if not IS_CONFIGURED:
        logger.warning("Бот не настроен, планировщик не запущен")
        return
    
    job_queue = application.job_queue
    
    for time_config in REMINDER_TIMES:
        for day in REMINDER_DAYS:
            job_queue.run_daily(
                send_reminder,
                time=datetime.time(hour=time_config["hour"], minute=time_config["minute"]),
                days=(day,),
                data={"day": day, "time": time_config},
                name=f"reminder_{day}_{time_config['hour']}:{time_config['minute']}",
            )
    
    logger.info(f"Запланировано {len(REMINDER_TIMES) * len(REMINDER_DAYS)} напоминаний для чата {CHAT_ID}")
    logger.info(f"Расписание: ПН, СР, ПТ в {time_config['hour']:02d}:{time_config['minute']:02d} UTC (9:15 МСК)")

# ================== Запуск бота ==================

def main():
    """Запуск бота"""
    if not BOT_TOKEN:
        logger.error("Токен бота не найден! Проверьте файл .env")
        return
    
    # Создаём приложение
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Обработчики команд
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("setup", setup))
    application.add_handler(CommandHandler("refresh_admins", refresh_admins))
    
    # Обработчик кнопок
    application.add_handler(CallbackQueryHandler(button_callback))
    
    # Настраиваем расписание (после запуска бота)
    application.job_queue.run_once(
        callback=lambda ctx: setup_jobs(application),
        when=5  # через 5 секунд после запуска
    )
    
    # Запускаем бота
    logger.info("Бот запущен...")
    logger.info(f"Напоминания: ПН, СР, ПТ в 6:15 UTC (9:15 МСК)")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
