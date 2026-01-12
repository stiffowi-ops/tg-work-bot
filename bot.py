import logging
import asyncio
from datetime import datetime, time, timedelta
from typing import Dict, List, Optional
import pytz

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    JobQueue,
)

# ==================== КОНФИГУРАЦИЯ ====================

# Токен бота (замените на свой)
BOT_TOKEN = "ВАШ_ТОКЕН_БОТА"

# ID чата для отправки напоминаний (замените на ID вашего чата)
CHAT_ID = "ВАШ_CHAT_ID"

# Пользователи с правами администратора (могут отменять планёрки)
ADMIN_USERS = ["@Stiff_OWi", "@gshabanov"]

# Время планёрки (по Москве)
MEETING_TIME = time(hour=9, minute=15)

# Дни недели для планёрок (понедельник=0, среда=2, пятница=4)
MEETING_DAYS = [0, 2, 4]  # Пн, Ср, Пт

# Текст напоминания
REMINDER_TEXT = """👋 Коллеги, доброе утро!

📅 Напоминаю о ежедневной планёрке в 9:15 по МСК.

Пожалуйста, подготовьтесь к обсуждению:
1. Что сделали вчера
2. Планы на сегодня
3. Есть ли блокеры

Жду всех в канале для созвонов!"""

# Варианты отмены планёрки (редактируемые)
CANCEL_OPTIONS = [
    "Перенесём на другой день. Дата такая-то",
    "Причину сообщу позже",
    "Технические проблемы с подключением",
    "Многие участники отсутствуют",
    "Срочные задачи с дедлайном",
    "Выходной день/праздник"
]

# ==================== ЛОГГИРОВАНИЕ ====================

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================

def is_admin(username: str) -> bool:
    """Проверяет, является ли пользователь администратором"""
    return username in ADMIN_USERS

def get_next_meeting_time() -> Optional[datetime]:
    """Возвращает следующее время планёрки"""
    moscow_tz = pytz.timezone('Europe/Moscow')
    now = datetime.now(moscow_tz)
    
    # Проверяем текущий день и время
    current_weekday = now.weekday()
    current_time = now.time()
    
    # Ищем следующий день с планёркой
    for days_ahead in range(8):  # Ищем на неделю вперёд
        check_date = now + timedelta(days=days_ahead)
        if check_date.weekday() in MEETING_DAYS:
            # Если это сегодня и время ещё не наступило
            if days_ahead == 0 and current_time < MEETING_TIME:
                meeting_datetime = datetime.combine(check_date.date(), MEETING_TIME)
            else:
                if days_ahead == 0:  # Сегодня, но время уже прошло
                    continue
                meeting_datetime = datetime.combine(check_date.date(), MEETING_TIME)
            
            return moscow_tz.localize(meeting_datetime)
    
    return None

# ==================== ОСНОВНОЙ ФУНКЦИОНАЛ ====================

async def send_reminder(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправляет напоминание о планёрке"""
    try:
        # Создаем клавиатуру для отмены
        keyboard = [
            [InlineKeyboardButton("✅ Планёрка состоится", callback_data="meeting_on")],
            [InlineKeyboardButton("❌ Отменить планёрку", callback_data="cancel_meeting")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await context.bot.send_message(
            chat_id=CHAT_ID,
            text=REMINDER_TEXT,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
        logger.info(f"Напоминание отправлено в чат {CHAT_ID}")
    except Exception as e:
        logger.error(f"Ошибка при отправке напоминания: {e}")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /start"""
    await update.message.reply_text(
        "🤖 Бот для напоминаний о планёрках запущен!\n"
        "Я буду отправлять напоминания в Пн, Ср, Пт в 9:15 по МСК.\n\n"
        f"Администраторы: {', '.join(ADMIN_USERS)}\n"
        f"Следующая планёрка: {get_next_meeting_time()}"
    )

async def setup_jobs(application: Application) -> None:
    """Настраивает регулярные задачи"""
    job_queue = application.job_queue
    
    if job_queue:
        # Добавляем задачу на нужные дни
        moscow_tz = pytz.timezone('Europe/Moscow')
        
        for day in MEETING_DAYS:
            # Создаем время для задачи (9:15 по Москве)
            job_time = time(hour=9, minute=15, tzinfo=moscow_tz)
            
            # Добавляем задачу на конкретные дни
            job_queue.run_daily(
                send_reminder,
                time=job_time,
                days=tuple(MEETING_DAYS),
                name=f"meeting_reminder_{day}"
            )
        
        logger.info(f"Задачи настроены на дни: {MEETING_DAYS} в {MEETING_TIME}")

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик нажатий на кнопки"""
    query = update.callback_query
    await query.answer()
    
    user = query.from_user
    user_mention = f"@{user.username}" if user.username else user.first_name
    
    # Проверяем, является ли пользователь администратором
    if not is_admin(f"@{user.username}" if user.username else ""):
        await query.edit_message_text(
            text=f"⚠️ {user_mention}, у вас нет прав для отмены планёрки.\n"
                 f"Только администраторы ({', '.join(ADMIN_USERS)}) могут это делать.",
            reply_markup=None
        )
        return
    
    # Обработка разных callback_data
    if query.data == "meeting_on":
        await query.edit_message_text(
            text=f"✅ {user_mention} подтвердил, что планёрка состоится!\n\n{REMINDER_TEXT}",
            parse_mode='HTML'
        )
        
    elif query.data == "cancel_meeting":
        # Показываем варианты отмены
        keyboard = []
        for i, option in enumerate(CANCEL_OPTIONS):
            keyboard.append([InlineKeyboardButton(
                f"• {option}", 
                callback_data=f"cancel_reason_{i}"
            )])
        
        keyboard.append([InlineKeyboardButton("↩️ Назад", callback_data="back_to_main")])
        
        await query.edit_message_text(
            text=f"📝 {user_mention}, выберите причину отмены:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    elif query.data == "back_to_main":
        # Возвращаемся к основному меню
        keyboard = [
            [InlineKeyboardButton("✅ Планёрка состоится", callback_data="meeting_on")],
            [InlineKeyboardButton("❌ Отменить планёрку", callback_data="cancel_meeting")]
        ]
        
        await query.edit_message_text(
            text=REMINDER_TEXT,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
    
    elif query.data.startswith("cancel_reason_"):
        # Обработка выбора причины отмены
        reason_index = int(query.data.split("_")[2])
        reason = CANCEL_OPTIONS[reason_index]
        
        # Обновляем сообщение с информацией об отмене
        await query.edit_message_text(
            text=f"🚫 **ПЛАНЁРКА ОТМЕНЕНА**\n\n"
                 f"👤 Отменил: {user_mention}\n"
                 f"📝 Причина: {reason}\n"
                 f"🕐 Время: {datetime.now(pytz.timezone('Europe/Moscow')).strftime('%d.%m.%Y %H:%M')}\n\n"
                 f"Следующая планёрка: {get_next_meeting_time()}",
            parse_mode='HTML',
            reply_markup=None
        )
        
        logger.info(f"Планёрка отменена пользователем {user_mention}, причина: {reason}")

async def admin_commands(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показывает команды для администраторов"""
    user = update.effective_user
    if is_admin(f"@{user.username}" if user.username else ""):
        await update.message.reply_text(
            "👑 **Команды администратора:**\n\n"
            "/next - Показать следующую планёрку\n"
            "/test - Тестовое напоминание\n"
            "/options - Показать текущие варианты отмены\n"
            "/add_option [текст] - Добавить вариант отмены\n"
            "/remove_option [номер] - Удалить вариант отмены\n"
            "/admins - Показать список администраторов",
            parse_mode='HTML'
        )
    else:
        await update.message.reply_text("У вас нет прав администратора.")

async def test_reminder(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Тестовая отправка напоминания (только для админов)"""
    user = update.effective_user
    if is_admin(f"@{user.username}" if user.username else ""):
        await send_reminder(context)
        await update.message.reply_text("✅ Тестовое напоминание отправлено!")
    else:
        await update.message.reply_text("У вас нет прав для этой команды.")

async def show_next_meeting(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показывает следующую планёрку"""
    next_meeting = get_next_meeting_time()
    if next_meeting:
        await update.message.reply_text(
            f"📅 Следующая планёрка:\n"
            f"Дата: {next_meeting.strftime('%d.%m.%Y')}\n"
            f"День недели: {['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс'][next_meeting.weekday()]}\n"
            f"Время: {next_meeting.strftime('%H:%M')} по МСК"
        )
    else:
        await update.message.reply_text("Не удалось определить следующую планёрку.")

async def show_options(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показывает текущие варианты отмены"""
    options_text = "📋 **Текущие варианты отмены:**\n\n"
    for i, option in enumerate(CANCEL_OPTIONS, 1):
        options_text += f"{i}. {option}\n"
    
    await update.message.reply_text(options_text, parse_mode='HTML')

async def add_option(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Добавляет новый вариант отмены"""
    user = update.effective_user
    if not is_admin(f"@{user.username}" if user.username else ""):
        await update.message.reply_text("У вас нет прав для этой команды.")
        return
    
    if not context.args:
        await update.message.reply_text("Использование: /add_option [текст варианта]")
        return
    
    new_option = " ".join(context.args)
    CANCEL_OPTIONS.append(new_option)
    
    await update.message.reply_text(f"✅ Добавлен новый вариант: {new_option}")
    logger.info(f"Пользователь {user.username} добавил вариант: {new_option}")

async def remove_option(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Удаляет вариант отмены"""
    user = update.effective_user
    if not is_admin(f"@{user.username}" if user.username else ""):
        await update.message.reply_text("У вас нет прав для этой команды.")
        return
    
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Использование: /remove_option [номер]")
        return
    
    index = int(context.args[0]) - 1
    if 0 <= index < len(CANCEL_OPTIONS):
        removed = CANCEL_OPTIONS.pop(index)
        await update.message.reply_text(f"✅ Удалён вариант: {removed}")
        logger.info(f"Пользователь {user.username} удалил вариант: {removed}")
    else:
        await update.message.reply_text("❌ Неверный номер варианта")

async def show_admins(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показывает список администраторов"""
    admins_text = "👑 **Администраторы бота:**\n\n"
    for admin in ADMIN_USERS:
        admins_text += f"• {admin}\n"
    
    await update.message.reply_text(admins_text, parse_mode='HTML')

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик ошибок"""
    logger.error(f"Ошибка: {context.error}", exc_info=context.error)

# ==================== ОСНОВНАЯ ФУНКЦИЯ ====================

def main() -> None:
    """Запуск бота"""
    # Создаем приложение
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Добавляем обработчики команд
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", admin_commands))
    application.add_handler(CommandHandler("admin", admin_commands))
    application.add_handler(CommandHandler("test", test_reminder))
    application.add_handler(CommandHandler("next", show_next_meeting))
    application.add_handler(CommandHandler("options", show_options))
    application.add_handler(CommandHandler("add_option", add_option))
    application.add_handler(CommandHandler("remove_option", remove_option))
    application.add_handler(CommandHandler("admins", show_admins))
    
    # Добавляем обработчик кнопок
    application.add_handler(CallbackQueryHandler(button_handler))
    
    # Добавляем обработчик ошибок
    application.add_error_handler(error_handler)
    
    # Настраиваем задачи
    application.job_queue.scheduler.configure(timezone=pytz.timezone('Europe/Moscow'))
    
    # Запускаем бота
    print("🤖 Бот запускается...")
    print(f"⏰ Напоминания будут отправляться в: {MEETING_TIME} по МСК")
    print(f"📅 Дни: {MEETING_DAYS}")
    print(f"👑 Администраторы: {ADMIN_USERS}")
    
    # Настраиваем и запускаем
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
