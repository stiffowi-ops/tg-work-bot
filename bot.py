import logging
import os
import json
import random
import requests
from datetime import datetime, timedelta, time
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

# Время планёрки (9:15 по Москве)
MEETING_TIME = {"hour": 9, "minute": 15}
# Время отправки факта (10:00 по Москве)
FACT_TIME = {"hour": 10, "minute": 0}
TIMEZONE = pytz.timezone("Europe/Moscow")

# Дни недели для планёрки (понедельник=0, среда=2, пятница=4)
MEETING_DAYS = [0, 2, 4]

# Категории для фактов
CATEGORIES = {
    'музыка': '🎵 Музыка',
    'кино': '🎬 Кино', 
    'технологии': '💻 Технологии',
    'игры': '🎮 Игры'
}

# Смайлики для реакций
REACTIONS = {
    'like': '👍',
    'dislike': '👎',
    'poop': '💩',
    'fire': '🔥',
    'laugh': '😂',
    'mind_blown': '🤯'
}

# Файл для хранения последней отправленной категории
LAST_CATEGORY_FILE = "last_category.json"

# База фактов (запасной вариант, если API не работает)
BACKUP_FACTS = {
    'музыка': [
        "Битлз первоначально назывались The Quarrymen.",
        "У Моцарта была кошка, которая любила слушать его игру на фортепиано.",
        "Гитара Fender Stratocaster была изобретена в 1954 году.",
        "Виниловые пластинки переживают ренессанс - продажи растут каждый год.",
        "Первая коммерческая запись была сделана в 1888 году.",
    ],
    'кино': [
        "В фильме 'Матрица' все сцены с зеленым оттенком были отсняты на пленку с зеленым фильтром.",
        "Актеру Джону Хёрту на съемках 'Чужого' было действительно плохо, когда пришелец вырывался из груди.",
        "Самый кассовый фильм в истории - 'Аватар' Джеймса Кэмерона.",
        "Для съемок 'Властелина колец' было создано более 48 000 предметов реквизита.",
        "Фильм 'Паразиты' - первый неанглоязычный фильм, получивший Оскар за лучший фильм.",
    ],
    'технологии': [
        "Первый компьютерный вирус был создан в 1983 году.",
        "Средний человек проверяет свой телефон 150 раз в день.",
        "Пароль '123456' до сих пор остается одним из самых популярных в мире.",
        "Первая веб-камера была создана для отслеживания кофеварки в Кембридже.",
        "ИИ уже обыгрывает людей в сложных играх вроде Go и покера.",
    ],
    'игры': [
        "Первая коммерческая видеоигра - Computer Space (1971).",
        "Марио изначально назывался 'Прыгающий человек' (Jumpman).",
        "Самый продаваемый игровой персонаж - Пикачу.",
        "Minecraft - самая продаваемая игра в истории.",
        "Первая игра с трехмерной графикой была выпущена в 1980 году.",
    ]
}

# Варианты отмены планёрки
CANCELLATION_OPTIONS = [
    "Все вопросы решены, планёрка не нужна",
    "Ключевые участники отсутствуют",
    "Перенесём на другой день",
]

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


# ========== ФУНКЦИИ ДЛЯ ФАКТОВ ==========

def get_last_category() -> Optional[str]:
    """Получить последнюю отправленную категорию"""
    try:
        if os.path.exists(LAST_CATEGORY_FILE):
            with open(LAST_CATEGORY_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get('last_category')
    except Exception as e:
        logger.error(f"Ошибка чтения last_category: {e}")
    return None

def save_last_category(category: str):
    """Сохранить последнюю отправленную категорию"""
    try:
        with open(LAST_CATEGORY_FILE, 'w', encoding='utf-8') as f:
            json.dump({'last_category': category}, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Ошибка сохранения last_category: {e}")

def get_random_category() -> str:
    """Получить случайную категорию, исключая последнюю"""
    last_category = get_last_category()
    available_categories = [cat for cat in CATEGORIES.keys() if cat != last_category]
    
    if not available_categories:
        available_categories = list(CATEGORIES.keys())
    
    return random.choice(available_categories)

def get_fact_from_api(category: str) -> Optional[str]:
    """Получить факт из API (заглушка - можно подключить реальное API)"""
    try:
        # Здесь можно подключить реальное API, например:
        # response = requests.get(f"https://api.example.com/facts/{category}")
        # return response.json()['fact']
        
        # Пока используем локальную базу
        facts = BACKUP_FACTS.get(category, [])
        if facts:
            return random.choice(facts)
        
        return None
    except Exception as e:
        logger.error(f"Ошибка получения факта из API: {e}")
        return None

def create_fact_message(category: str, fact: str) -> str:
    """Создать сообщение с фактом"""
    category_emoji = CATEGORIES[category]
    return f"{category_emoji}\n\n📚 <b>Факт дня:</b>\n\n{fact}\n\n#факт #{category}"

def create_reactions_keyboard(message_id: int) -> InlineKeyboardMarkup:
    """Создать клавиатуру с реакциями"""
    keyboard = []
    row = []
    
    for i, (reaction_id, emoji) in enumerate(REACTIONS.items()):
        row.append(InlineKeyboardButton(
            f"{emoji} 0",  # Начинаем с 0 реакций
            callback_data=f"fact_react_{reaction_id}_{message_id}"
        ))
        
        # Размещаем по 3 кнопки в строке
        if (i + 1) % 3 == 0:
            keyboard.append(row)
            row = []
    
    if row:  # Добавляем последнюю строку, если она не полная
        keyboard.append(row)
    
    # Кнопка для получения нового факта
    keyboard.append([
        InlineKeyboardButton("🎲 Новый факт", callback_data="new_fact_random"),
        InlineKeyboardButton("📊 Статистика", callback_data="facts_stats")
    ])
    
    return InlineKeyboardMarkup(keyboard)

async def send_daily_fact(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправка ежедневного факта"""
    config = BotConfig()
    chat_id = config.chat_id

    if not chat_id:
        logger.warning("Chat ID не установлен для отправки факта")
        return

    # Выбираем категорию (исключая вчерашнюю)
    category = get_random_category()
    
    # Получаем факт
    fact_text = get_fact_from_api(category)
    
    if not fact_text:
        fact_text = random.choice(BACKUP_FACTS.get(category, ["Интересный факт скоро появится!"]))
    
    # Создаем сообщение
    message = create_fact_message(category, fact_text)
    
    try:
        # Отправляем сообщение с кнопками реакций
        sent_message = await context.bot.send_message(
            chat_id=chat_id,
            text=message,
            parse_mode=ParseMode.HTML,
            reply_markup=create_reactions_keyboard(0)  # 0 - временный ID
        )
        
        # Обновляем клавиатуру с реальным ID сообщения
        await sent_message.edit_reply_markup(
            reply_markup=create_reactions_keyboard(sent_message.message_id)
        )
        
        # Сохраняем категорию
        save_last_category(category)
        
        logger.info(f"Отправлен факт категории '{category}' в чат {chat_id}")
        
    except Exception as e:
        logger.error(f"Ошибка отправки факта: {e}")

async def handle_reaction(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик реакций на факты"""
    query = update.callback_query
    await query.answer()
    
    # Парсим callback_data: fact_react_{reaction}_{message_id}
    parts = query.data.split('_')
    if len(parts) != 4:
        return
    
    reaction_type = parts[2]
    message_id = int(parts[3])
    
    # Здесь можно добавить логику подсчета реакций
    # Пока просто показываем всплывающее уведомление
    emoji = REACTIONS.get(reaction_type, '👍')
    await query.answer(f"Вы поставили {emoji} этому факту!", show_alert=False)

async def send_new_fact(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправить новый случайный факт по запросу"""
    query = update.callback_query
    await query.answer()
    
    config = BotConfig()
    chat_id = config.chat_id or query.message.chat_id
    
    # Выбираем случайную категорию
    category = random.choice(list(CATEGORIES.keys()))
    fact_text = get_fact_from_api(category)
    
    if not fact_text:
        fact_text = random.choice(BACKUP_FACTS.get(category, ["Интересный факт скоро появится!"]))
    
    # Создаем сообщение
    message = create_fact_message(category, fact_text)
    
    try:
        # Отправляем как новое сообщение или редактируем текущее
        if query.message:
            await query.message.edit_text(
                text=message,
                parse_mode=ParseMode.HTML,
                reply_markup=create_reactions_keyboard(query.message.message_id)
            )
        else:
            sent_message = await context.bot.send_message(
                chat_id=chat_id,
                text=message,
                parse_mode=ParseMode.HTML,
                reply_markup=create_reactions_keyboard(0)
            )
            await sent_message.edit_reply_markup(
                reply_markup=create_reactions_keyboard(sent_message.message_id)
            )
        
    except Exception as e:
        logger.error(f"Ошибка отправки нового факта: {e}")
        await query.answer("❌ Ошибка при отправке факта", show_alert=True)

async def show_facts_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показать статистику по фактам"""
    query = update.callback_query
    await query.answer("📊 Статистика скоро появится!", show_alert=True)

# ========== КОМАНДЫ ДЛЯ ФАКТОВ ==========

async def fact_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда для получения факта по категории"""
    if not context.args:
        # Показываем список категорий
        keyboard = []
        row = []
        
        for i, (category_key, category_name) in enumerate(CATEGORIES.items()):
            row.append(InlineKeyboardButton(
                category_name,
                callback_data=f"fact_category_{category_key}"
            ))
            
            if (i + 1) % 2 == 0:
                keyboard.append(row)
                row = []
        
        if row:
            keyboard.append(row)
        
        keyboard.append([InlineKeyboardButton("🎲 Случайная категория", callback_data="fact_random")])
        
        await update.message.reply_text(
            "📚 <b>Выберите категорию факта:</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return
    
    # Если указана категория
    category_input = context.args[0].lower()
    category_key = None
    
    for key, name in CATEGORIES.items():
        if key in category_input or name.lower() in category_input:
            category_key = key
            break
    
    if not category_key:
        await update.message.reply_text(
            "❌ <b>Категория не найдена!</b>\n\n"
            f"Доступные категории: {', '.join(CATEGORIES.values())}",
            parse_mode=ParseMode.HTML
        )
        return
    
    # Получаем факт
    fact_text = get_fact_from_api(category_key)
    
    if not fact_text:
        fact_text = random.choice(BACKUP_FACTS.get(category_key, ["Интересный факт скоро появится!"]))
    
    # Отправляем факт
    message = create_fact_message(category_key, fact_text)
    sent_message = await update.message.reply_text(
        message,
        parse_mode=ParseMode.HTML,
        reply_markup=create_reactions_keyboard(0)
    )
    
    # Обновляем с реальным ID сообщения
    await sent_message.edit_reply_markup(
        reply_markup=create_reactions_keyboard(sent_message.message_id)
    )

async def fact_category_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик выбора категории из inline-кнопок"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "fact_random":
        category_key = random.choice(list(CATEGORIES.keys()))
    else:
        # fact_category_{category}
        category_key = query.data.split('_')[2]
    
    if category_key not in CATEGORIES:
        await query.answer("❌ Категория не найдена", show_alert=True)
        return
    
    # Получаем факт
    fact_text = get_fact_from_api(category_key)
    
    if not fact_text:
        fact_text = random.choice(BACKUP_FACTS.get(category_key, ["Интересный факт скоро появится!"]))
    
    # Отправляем факт
    message = create_fact_message(category_key, fact_text)
    
    try:
        await query.edit_message_text(
            text=message,
            parse_mode=ParseMode.HTML,
            reply_markup=create_reactions_keyboard(query.message.message_id)
        )
    except Exception as e:
        logger.error(f"Ошибка редактирования сообщения: {e}")
        # Если не получилось отредактировать, отправляем новое
        await query.message.reply_text(
            message,
            parse_mode=ParseMode.HTML,
            reply_markup=create_reactions_keyboard(0)
        )

# ========== ПЛАНИРОВАНИЕ ФАКТОВ ==========

def schedule_daily_fact(application: Application, chat_id: int) -> None:
    """Запланировать ежедневную отправку факта"""
    # Устанавливаем время отправки (10:00 по Москве)
    fact_time = time(hour=FACT_TIME['hour'], minute=FACT_TIME['minute'])
    
    # Создаем job для ежедневной отправки
    application.job_queue.run_daily(
        send_daily_fact,
        time=fact_time,
        days=(0, 1, 2, 3, 4, 5, 6),  # Все дни недели
        chat_id=chat_id,
        name="daily_fact_10am"
    )
    
    logger.info(f"Ежедневная отправка факта запланирована на {FACT_TIME['hour']:02d}:{FACT_TIME['minute']:02d}")

# ========== ОБНОВЛЕННЫЙ КЛАСС BotConfig ==========

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
                    if "fact_reactions" not in data:
                        data["fact_reactions"] = {}
                    if "sent_facts_count" not in data:
                        data["sent_facts_count"] = 0
                    return data
            except Exception as e:
                logger.error(f"Ошибка загрузки конфига: {e}")
        return {
            "chat_id": None,
            "allowed_users": ["Stiff_OWi", "gshabanov"],
            "active_reminders": {},
            "fact_reactions": {},
            "sent_facts_count": 0
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
    
    def increment_fact_count(self) -> None:
        """Увеличить счетчик отправленных фактов"""
        self.data["sent_facts_count"] = self.data.get("sent_facts_count", 0) + 1
        self.save()
    
    def add_reaction(self, message_id: int, reaction_type: str) -> None:
        """Добавить реакцию к факту"""
        if "fact_reactions" not in self.data:
            self.data["fact_reactions"] = {}
        
        if str(message_id) not in self.data["fact_reactions"]:
            self.data["fact_reactions"][str(message_id)] = {}
        
        if reaction_type not in self.data["fact_reactions"][str(message_id)]:
            self.data["fact_reactions"][str(message_id)][reaction_type] = 0
        
        self.data["fact_reactions"][str(message_id)][reaction_type] += 1
        self.save()
    
    # ... остальные методы класса остаются без изменений ...

# ========== ОБНОВЛЕННАЯ ФУНКЦИЯ send_daily_fact ==========

async def send_daily_fact(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправка ежедневного факта"""
    config = BotConfig()
    chat_id = config.chat_id

    if not chat_id:
        logger.warning("Chat ID не установлен для отправки факта")
        return

    # Выбираем категорию (исключая вчерашнюю)
    category = get_random_category()
    
    # Получаем факт
    fact_text = get_fact_from_api(category)
    
    if not fact_text:
        fact_text = random.choice(BACKUP_FACTS.get(category, ["Интересный факт скоро появится!"]))
    
    # Создаем сообщение
    message = create_fact_message(category, fact_text)
    
    try:
        # Отправляем сообщение с кнопками реакций
        sent_message = await context.bot.send_message(
            chat_id=chat_id,
            text=message,
            parse_mode=ParseMode.HTML,
            reply_markup=create_reactions_keyboard(0)  # 0 - временный ID
        )
        
        # Обновляем клавиатуру с реальным ID сообщения
        await sent_message.edit_reply_markup(
            reply_markup=create_reactions_keyboard(sent_message.message_id)
        )
        
        # Сохраняем категорию
        save_last_category(category)
        
        # Увеличиваем счетчик
        config.increment_fact_count()
        
        logger.info(f"Отправлен факт категории '{category}' в чат {chat_id}, всего фактов: {config.data.get('sent_facts_count', 0)}")
        
    except Exception as e:
        logger.error(f"Ошибка отправки факта: {e}")

# ========== ОБНОВЛЕННЫЙ ОБРАБОТЧИК РЕАКЦИЙ ==========

async def handle_reaction(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик реакций на факты"""
    query = update.callback_query
    await query.answer()
    
    # Парсим callback_data: fact_react_{reaction}_{message_id}
    parts = query.data.split('_')
    if len(parts) != 4:
        return
    
    reaction_type = parts[2]
    message_id = int(parts[3])
    
    # Добавляем реакцию в статистику
    config = BotConfig()
    config.add_reaction(message_id, reaction_type)
    
    # Получаем текущие реакции для этого сообщения
    message_reactions = config.data.get("fact_reactions", {}).get(str(message_id), {})
    
    # Обновляем кнопки
    keyboard = query.message.reply_markup.inline_keyboard
    new_keyboard = []
    
    for row in keyboard:
        new_row = []
        for button in row:
            # Проверяем, является ли это кнопкой реакции
            btn_data = button.callback_data
            if btn_data and btn_data.startswith('fact_react_'):
                btn_parts = btn_data.split('_')
                if len(btn_parts) == 4:
                    btn_reaction = btn_parts[2]
                    btn_msg_id = btn_parts[3]
                    
                    if int(btn_msg_id) == message_id:
                        # Обновляем счетчик
                        count = message_reactions.get(btn_reaction, 0)
                        emoji = REACTIONS.get(btn_reaction, '👍')
                        new_text = f"{emoji} {count}"
                        
                        # Создаем новую кнопку
                        new_row.append(InlineKeyboardButton(
                            new_text,
                            callback_data=btn_data
                        ))
                        continue
            
            # Для остальных кнопок оставляем как есть
            new_row.append(button)
        
        new_keyboard.append(new_row)
    
    # Применяем обновленную клавиатуру
    try:
        await query.edit_message_reply_markup(
            reply_markup=InlineKeyboardMarkup(new_keyboard)
        )
    except Exception as e:
        logger.error(f"Ошибка обновления клавиатуры: {e}")
    
    # Показываем уведомление
    emoji = REACTIONS.get(reaction_type, '👍')
    await query.answer(f"Вы поставили {emoji} этому факту!", show_alert=False)

# ========== ОБНОВЛЕННАЯ ФУНКЦИЯ main ==========

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
        application.add_handler(CommandHandler("jobs", list_jobs))
        application.add_handler(CommandHandler("adduser", add_user))
        application.add_handler(CommandHandler("removeuser", remove_user))
        application.add_handler(CommandHandler("users", list_users))
        application.add_handler(CommandHandler("cancelall", cancel_all))
        
        # Новые команды для фактов
        application.add_handler(CommandHandler("fact", fact_command))
        application.add_handler(CommandHandler("факт", fact_command))

        # Обработчики callback-ов для фактов
        application.add_handler(CallbackQueryHandler(fact_category_callback, pattern="^fact_category_"))
        application.add_handler(CallbackQueryHandler(fact_category_callback, pattern="^fact_random$"))
        application.add_handler(CallbackQueryHandler(handle_reaction, pattern="^fact_react_"))
        application.add_handler(CallbackQueryHandler(send_new_fact, pattern="^new_fact_random$"))
        application.add_handler(CallbackQueryHandler(show_facts_stats, pattern="^facts_stats$"))

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
        
        # Планирование ежедневного факта после установки чата
        config = BotConfig()
        if config.chat_id:
            schedule_daily_fact(application, config.chat_id)
            logger.info(f"✅ Ежедневные факты запланированы на 10:00 по МСК")
        else:
            logger.warning("⚠️  Chat ID не установлен, факты не запланированы")

        logger.info("🤖 Бот запущен и готов к работе!")
        logger.info(f"⏰ Планёрки: {', '.join(['Пн', 'Ср', 'Пт'])} в {MEETING_TIME['hour']:02d}:{MEETING_TIME['minute']:02d}")
        logger.info(f"📚 Факты: ежедневно в {FACT_TIME['hour']:02d}:{FACT_TIME['minute']:02d}")
        logger.info(f"🎲 Категории: {', '.join(CATEGORIES.values())}")
        
        application.run_polling(allowed_updates=Update.ALL_TYPES)

    except Exception as e:
        logger.error(f"❌ Критическая ошибка при запуске бота: {e}")
        raise


# ========== ОБНОВЛЕННАЯ КОМАНДА start ==========

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик команды /start"""
    await update.message.reply_text(
        "🤖 <b>Бот для напоминаний о планёрке и интересных фактов!</b>\n\n"
        f"📅 <b>Напоминания отправляются:</b>\n"
        f"• Понедельник\n• Среда\n• Пятница\n"
        f"⏰ <b>Время:</b> {MEETING_TIME['hour']:02d}:{MEETING_TIME['minute']:02d} по МСК\n\n"
        f"📚 <b>Интересные факты:</b>\n"
        f"• Каждый день в {FACT_TIME['hour']:02d}:{FACT_TIME['minute']:02d} по МСК\n"
        f"• Категории: {', '.join(CATEGORIES.values())}\n\n"
        "🔧 <b>Доступные команды:</b>\n"
        "/fact или /факт - получить интересный факт\n"
        "/info - информация о боте\n"
        "/jobs - список запланированных задач\n"
        "/test - тестовое напоминание (через 5 сек)\n"
        "/testnow - мгновенное тестовое напоминание\n\n"
        "👮‍♂️ <b>Команды для администраторов:</b>\n"
        "/setchat - установить чат для уведомлений\n"
        "/adduser @username - добавить пользователя\n"
        "/removeuser @username - удалить пользователя\n"
        "/users - список пользователей\n"
        "/cancelall - отменить все напоминания",
        parse_mode=ParseMode.HTML
    )


# ========== ОБНОВЛЕННАЯ КОМАНДА info ==========

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
                     if j.name and "daily_fact" in j.name])
    
    # Следующее напоминание о планёрке
    next_meeting_job = None
    next_fact_job = None
    
    for job in all_jobs:
        if job.name and job.name.startswith("meeting_reminder_"):
            if not next_meeting_job or job.next_t < next_meeting_job.next_t:
                next_meeting_job = job
        elif job.name and "daily_fact" in job.name:
            if not next_fact_job or job.next_t < next_fact_job.next_t:
                next_fact_job = job
    
    next_meeting_time = next_meeting_job.next_t.astimezone(TIMEZONE) if next_meeting_job else "не запланировано"
    next_fact_time = next_fact_job.next_t.astimezone(TIMEZONE) if next_fact_job else "не запланировано"
    
    # Статистика фактов
    sent_facts_count = config.data.get("sent_facts_count", 0)
    
    # Показываем текущую Zoom-ссылка (без полного URL)
    zoom_info = f"\n🎥 <b>Zoom-ссылка:</b> {'установлена ✅' if ZOOM_LINK and ZOOM_LINK != 'https://us04web.zoom.us/j/1234567890?pwd=example' else 'не установлена ⚠️'}"
    
    # Статистика реакций
    total_reactions = 0
    reaction_stats = config.data.get("fact_reactions", {})
    for msg_reactions in reaction_stats.values():
        total_reactions += sum(msg_reactions.values())

    await update.message.reply_text(
        f"📊 <b>Информация о боте:</b>\n\n"
        f"{status}\n\n"
        f"📅 <b>Планёрки:</b>\n"
        f"• Дни: понедельник, среда, пятница\n"
        f"• Время: {MEETING_TIME['hour']:02d}:{MEETING_TIME['minute']:02d} по МСК\n"
        f"• Следующая: {next_meeting_time}\n\n"
        f"📚 <b>Факты:</b>\n"
        f"• Время: {FACT_TIME['hour']:02d}:{FACT_TIME['minute']:02d} по МСК\n"
        f"• Категории: {', '.join(CATEGORIES.values())}\n"
        f"• Отправлено: {sent_facts_count} фактов\n"
        f"• Реакций: {total_reactions}\n"
        f"• Следующий: {next_fact_time}\n\n"
        f"👥 <b>Пользователи:</b>\n"
        f"• Разрешённые: {len(config.allowed_users)}\n"
        f"• Активные напоминания: {len(config.active_reminders)}\n"
        f"• Запланировано задач: {meeting_job_count + fact_job_count}\n"
        f"{zoom_info}\n\n"
        f"<b>Используйте:</b>\n"
        f"/users - список пользователей\n"
        f"/jobs - список задач\n"
        f"/fact - получить факт",
        parse_mode=ParseMode.HTML
    )


if __name__ == "__main__":
    main()
