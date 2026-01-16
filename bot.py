import os
import json
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
    ConversationHandler
)

# ========== КОНСТАНТЫ ==========
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# Приватные ссылки из переменных окружения
YA_CRM_LINK = os.getenv("YA_CRM_LINK", "https://crm.example.com")
WIKI_LINK = os.getenv("WIKI_LINK", "https://wiki.example.com")
HELPY_BOT_LINK = os.getenv("HELPY_BOT_LINK", "https://t.me/helpy_bot")

# Файлы бота
HELP_DATA_FILE = "help_data.json"
USER_DATA_FILE = "user_data.json"

# Настройка логирования
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", 
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Состояния для диалогов
ADDING_FILE_NAME, ADDING_FILE_DESCRIPTION = range(2)

# ========== КЛАСС ДЛЯ ХРАНЕНИЯ ДАННЫХ ==========

class HelpSystem:
    """Класс для управления базой данных помощи"""
    
    def __init__(self):
        self.data_file = HELP_DATA_FILE
        self.user_data_file = USER_DATA_FILE
        self.data = self._load_data()
        self.user_data = self._load_user_data()
    
    def _load_data(self) -> Dict[str, Any]:
        """Загрузить данные помощи"""
        default_data = {
            "files": {
                "speech_main": {
                    "name": "Спич main",
                    "description": "Основной спич для команды",
                    "file_id": None,
                    "category": "documents",
                    "added_date": None
                },
                "speech_events": {
                    "name": "Спич мероприятия",
                    "description": "Спич для мероприятий и встреч",
                    "file_id": None,
                    "category": "documents",
                    "added_date": None
                }
            },
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
        
        if os.path.exists(self.data_file):
            try:
                with open(self.data_file, 'r', encoding='utf-8') as f:
                    loaded_data = json.load(f)
                    
                    # Обновляем ссылки из переменных окружения
                    if "links" in loaded_data:
                        loaded_data["links"]["ya_crm"]["url"] = YA_CRM_LINK
                        loaded_data["links"]["wiki"]["url"] = WIKI_LINK
                        loaded_data["links"]["helpy_bot"]["url"] = HELPY_BOT_LINK
                    
                    return loaded_data
            except Exception as e:
                logger.error(f"Ошибка загрузки данных: {e}")
        
        return default_data
    
    def _load_user_data(self) -> Dict[str, Any]:
        """Загрузить пользовательские данные"""
        if os.path.exists(self.user_data_file):
            try:
                with open(self.user_data_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Ошибка загрузки пользовательских данных: {e}")
        
        return {
            "admins": ["Stiff_OWi", "gshabanov"],  # Список админов
            "pending_file": {}  # Временные данные для добавления файлов
        }
    
    def save_data(self) -> None:
        """Сохранить данные помощи"""
        try:
            with open(self.data_file, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Ошибка сохранения данных: {e}")
    
    def save_user_data(self) -> None:
        """Сохранить пользовательские данные"""
        try:
            with open(self.user_data_file, 'w', encoding='utf-8') as f:
                json.dump(self.user_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Ошибка сохранения пользовательских данных: {e}")
    
    def get_main_menu(self) -> List[List[InlineKeyboardButton]]:
        """Получить главное меню"""
        keyboard = []
        
        for cat_id, cat_data in self.data["categories"].items():
            keyboard.append([
                InlineKeyboardButton(cat_data["name"], callback_data=f"cat_{cat_id}")
            ])
        
        # Кнопка настроек (только для админов)
        keyboard.append([
            InlineKeyboardButton("⚙️ Настройки", callback_data="settings")
        ])
        
        return InlineKeyboardMarkup(keyboard)
    
    def get_category_menu(self, category_id: str) -> InlineKeyboardMarkup:
        """Получить меню категории"""
        keyboard = []
        
        if category_id == "documents":
            # Показываем файлы
            for file_id, file_data in self.data["files"].items():
                if file_data["category"] == category_id:
                    keyboard.append([
                        InlineKeyboardButton(
                            f"📋 {file_data['name']}",
                            callback_data=f"file_{file_id}"
                        )
                    ])
        
        elif category_id == "links":
            # Показываем ссылки
            for link_id, link_data in self.data["links"].items():
                keyboard.append([
                    InlineKeyboardButton(
                        f"🔗 {link_data['name']}",
                        callback_data=f"link_{link_id}"
                    )
                ])
        
        # Кнопка назад
        keyboard.append([
            InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")
        ])
        
        return InlineKeyboardMarkup(keyboard)
    
    def get_settings_menu(self) -> InlineKeyboardMarkup:
        """Получить меню настроек (для админов)"""
        keyboard = [
            [InlineKeyboardButton("➕ Добавить файл", callback_data="add_file")],
            [InlineKeyboardButton("🗑️ Удалить файл", callback_data="delete_file")],
            [InlineKeyboardButton("📊 Статистика", callback_data="stats")],
            [InlineKeyboardButton("🔙 Главное меню", callback_data="back_to_main")]
        ]
        
        return InlineKeyboardMarkup(keyboard)
    
    def get_delete_files_menu(self) -> InlineKeyboardMarkup:
        """Получить меню для удаления файлов"""
        keyboard = []
        
        for file_id, file_data in self.data["files"].items():
            keyboard.append([
                InlineKeyboardButton(
                    f"🗑️ {file_data['name']}",
                    callback_data=f"delete_{file_id}"
                )
            ])
        
        keyboard.append([
            InlineKeyboardButton("🔙 Назад", callback_data="settings")
        ])
        
        return InlineKeyboardMarkup(keyboard)
    
    def add_file(self, file_id: str, file_name: str, description: str) -> bool:
        """Добавить новый файл"""
        try:
            file_key = file_name.lower().replace(' ', '_').replace('(', '').replace(')', '')
            
            self.data["files"][file_key] = {
                "name": file_name,
                "description": description,
                "file_id": file_id,
                "category": "documents",
                "added_date": datetime.now().isoformat()
            }
            
            self.save_data()
            logger.info(f"Файл добавлен: {file_name} (ID: {file_key})")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка добавления файла: {e}")
            return False
    
    def delete_file(self, file_id: str) -> bool:
        """Удалить файл"""
        if file_id in self.data["files"]:
            deleted_name = self.data["files"][file_id]["name"]
            del self.data["files"][file_id]
            self.save_data()
            logger.info(f"Файл удален: {deleted_name} (ID: {file_id})")
            return True
        return False
    
    def is_admin(self, username: str) -> bool:
        """Проверить, является ли пользователь админом"""
        return username in self.user_data["admins"]

# ========== ИНИЦИАЛИЗАЦИЯ СИСТЕМЫ ПОМОЩИ ==========

help_system = HelpSystem()

# ========== КОМАНДЫ ПОЛЬЗОВАТЕЛЕЙ ==========

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик команды /help"""
    text = (
        "📚 *ЦЕНТР ПОМОЩИ СОТРУДНИКАМ*\n\n"
        "Здесь вы найдете все необходимые материалы для работы:\n\n"
        "• 📄 *Документы* – корпоративные спичи и шаблоны\n"
        "• 🔗 *Полезные ссылки* – внутренние ресурсы и системы\n\n"
        "Выберите категорию:"
    )
    
    await update.message.reply_text(
        text=text,
        reply_markup=help_system.get_main_menu(),
        parse_mode=ParseMode.MARKDOWN
    )

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик команды /start"""
    await update.message.reply_text(
        "👋 *Добро пожаловать в бот-помощник!*\n\n"
        "Используйте /help для доступа ко всем рабочим материалам.",
        parse_mode=ParseMode.MARKDOWN
    )

# ========== ОБРАБОТЧИКИ КНОПОК ==========

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик всех callback-кнопок"""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    user = query.from_user.username
    
    # Главное меню
    if data == "back_to_main":
        text = (
            "📚 *ЦЕНТР ПОМОЩИ СОТРУДНИКАМ*\n\n"
            "Выберите категорию:"
        )
        await query.edit_message_text(
            text=text,
            reply_markup=help_system.get_main_menu(),
            parse_mode=ParseMode.MARKDOWN
        )
    
    # Категории
    elif data.startswith("cat_"):
        category_id = data.replace("cat_", "")
        category = help_system.data["categories"][category_id]
        
        text = f"*{category['name']}*\n\n{category['description']}\n\nВыберите нужный материал:"
        
        await query.edit_message_text(
            text=text,
            reply_markup=help_system.get_category_menu(category_id),
            parse_mode=ParseMode.MARKDOWN
        )
    
    # Файлы
    elif data.startswith("file_"):
        file_id = data.replace("file_", "")
        file_data = help_system.data["files"].get(file_id)
        
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
                keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=f"cat_{file_data['category']}")]]
                await query.edit_message_reply_markup(
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                
            except Exception as e:
                logger.error(f"Ошибка отправки файла: {e}")
                await query.edit_message_text(
                    text="❌ Ошибка при отправке файла. Возможно, файл был перезагружен.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]])
                )
        else:
            await query.edit_message_text(
                text="❌ Файл не загружен. Обратитесь к администратору.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]])
            )
    
    # Ссылки
    elif data.startswith("link_"):
        link_id = data.replace("link_", "")
        link_data = help_system.data["links"].get(link_id)
        
        if link_data:
            text = (
                f"🔗 *{link_data['name']}*\n\n"
                f"{link_data['description']}\n\n"
                f"*Ссылка:* {link_data['url']}"
            )
            
            keyboard = [
                [InlineKeyboardButton("🌐 Открыть ссылку", url=link_data["url"])],
                [InlineKeyboardButton("🔙 Назад", callback_data="cat_links")]
            ]
            
            await query.edit_message_text(
                text=text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.MARKDOWN
            )
    
    # Настройки
    elif data == "settings":
        if help_system.is_admin(user):
            text = "⚙️ *Панель администратора*\n\nВыберите действие:"
            await query.edit_message_text(
                text=text,
                reply_markup=help_system.get_settings_menu(),
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await query.answer("❌ У вас нет прав доступа!", show_alert=True)
    
    # Добавление файла
    elif data == "add_file":
        if help_system.is_admin(user):
            help_system.user_data["pending_file"] = {"user_id": query.from_user.id}
            help_system.save_user_data()
            
            await query.edit_message_text(
                text="📤 *Добавление нового файла*\n\n"
                     "1. Отправьте мне файл (PDF, Word, Excel и т.д.)\n"
                     "2. Затем укажите название файла\n"
                     "3. Добавьте описание\n\n"
                     "❌ *Отмена:* /cancel",
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await query.answer("❌ У вас нет прав доступа!", show_alert=True)
    
    # Удаление файла
    elif data == "delete_file":
        if help_system.is_admin(user):
            if not help_system.data["files"]:
                await query.edit_message_text(
                    text="📭 *Нет файлов для удаления*\n\n"
                         "База файлов пуста.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="settings")]]),
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                await query.edit_message_text(
                    text="🗑️ *Удаление файла*\n\n"
                         "Выберите файл для удаления:",
                    reply_markup=help_system.get_delete_files_menu(),
                    parse_mode=ParseMode.MARKDOWN
                )
        else:
            await query.answer("❌ У вас нет прав доступа!", show_alert=True)
    
    # Подтверждение удаления файла
    elif data.startswith("delete_"):
        if help_system.is_admin(user):
            file_id = data.replace("delete_", "")
            file_data = help_system.data["files"].get(file_id)
            
            if file_data:
                keyboard = [
                    [
                        InlineKeyboardButton("✅ Да, удалить", callback_data=f"confirm_delete_{file_id}"),
                        InlineKeyboardButton("❌ Нет, отмена", callback_data="delete_file")
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
    
    # Подтвержденное удаление
    elif data.startswith("confirm_delete_"):
        if help_system.is_admin(user):
            file_id = data.replace("confirm_delete_", "")
            
            if help_system.delete_file(file_id):
                await query.edit_message_text(
                    text="✅ *Файл успешно удален!*",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 В настройки", callback_data="settings")]])
                )
            else:
                await query.edit_message_text(
                    text="❌ *Ошибка при удалении файла*",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 В настройки", callback_data="settings")]])
                )
    
    # Статистика
    elif data == "stats":
        if help_system.is_admin(user):
            files_count = len(help_system.data["files"])
            links_count = len(help_system.data["links"])
            
            text = (
                "📊 *Статистика системы*\n\n"
                f"📁 Файлов в базе: *{files_count}*\n"
                f"🔗 Ссылок в базе: *{links_count}*\n"
                f"📂 Категорий: *{len(help_system.data['categories'])}*\n\n"
                "*Доступные файлы:*\n"
            )
            
            for file_id, file_data in help_system.data["files"].items():
                added_date = file_data.get("added_date", "неизвестно")
                if added_date:
                    added_date = added_date[:10]
                text += f"• {file_data['name']} (добавлен: {added_date})\n"
            
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="settings")]]
            
            await query.edit_message_text(
                text=text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.MARKDOWN
            )

# ========== ОБРАБОТЧИКИ ДЛЯ ДОБАВЛЕНИЯ ФАЙЛОВ ==========

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработчик получения документа для добавления"""
    user = update.message.from_user.username
    
    if not help_system.is_admin(user):
        await update.message.reply_text("❌ У вас нет прав для добавления файлов.")
        return ConversationHandler.END
    
    pending = help_system.user_data["pending_file"]
    
    if pending.get("user_id") != update.message.from_user.id:
        return ConversationHandler.END
    
    # Сохраняем информацию о файле
    document = update.message.document
    pending["file_id"] = document.file_id
    pending["file_name"] = document.file_name or "Без названия"
    help_system.save_user_data()
    
    await update.message.reply_text(
        f"📁 *Файл получен:* {pending['file_name']}\n\n"
        f"Теперь введите *название файла* для отображения в меню:\n\n"
        f"❌ *Отмена:* /cancel",
        parse_mode=ParseMode.MARKDOWN
    )
    
    return ADDING_FILE_NAME

async def handle_file_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработчик ввода названия файла"""
    pending = help_system.user_data["pending_file"]
    
    if pending.get("user_id") != update.message.from_user.id:
        return ConversationHandler.END
    
    pending["display_name"] = update.message.text
    help_system.save_user_data()
    
    await update.message.reply_text(
        f"✅ *Название сохранено:* {pending['display_name']}\n\n"
        f"Теперь введите *описание файла*:\n\n"
        f"❌ *Отмена:* /cancel",
        parse_mode=ParseMode.MARKDOWN
    )
    
    return ADDING_FILE_DESCRIPTION

async def handle_file_description(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработчик ввода описания файла"""
    pending = help_system.user_data["pending_file"]
    
    if pending.get("user_id") != update.message.from_user.id:
        return ConversationHandler.END
    
    description = update.message.text
    
    # Добавляем файл в систему
    success = help_system.add_file(
        file_id=pending["file_id"],
        file_name=pending["display_name"],
        description=description
    )
    
    if success:
        await update.message.reply_text(
            f"✅ *Файл успешно добавлен!*\n\n"
            f"📁 *Название:* {pending['display_name']}\n"
            f"📝 *Описание:* {description}\n\n"
            f"Файл теперь доступен в разделе 📄 Документы.",
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await update.message.reply_text(
            "❌ *Ошибка при добавлении файла*\n\n"
            "Попробуйте еще раз или обратитесь к разработчику.",
            parse_mode=ParseMode.MARKDOWN
        )
    
    # Очищаем временные данные
    help_system.user_data["pending_file"] = {}
    help_system.save_user_data()
    
    return ConversationHandler.END

async def cancel_add_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отмена добавления файла"""
    user = update.message.from_user.username
    
    if help_system.is_admin(user):
        help_system.user_data["pending_file"] = {}
        help_system.save_user_data()
        
        await update.message.reply_text(
            "❌ *Добавление файла отменено.*",
            parse_mode=ParseMode.MARKDOWN
        )
    
    return ConversationHandler.END

# ========== ГЛАВНАЯ ФУНКЦИЯ ==========

def main() -> None:
    """Запуск бота"""
    if not TOKEN:
        logger.error("❌ Токен бота не найден! Установите TELEGRAM_BOT_TOKEN")
        return
    
    try:
        # Создаем приложение
        application = Application.builder().token(TOKEN).build()
        
        # Команды пользователей - ТОЛЬКО ЛАТИНСКИЕ БУКВЫ!
        application.add_handler(CommandHandler("start", start_command))
        application.add_handler(CommandHandler("help", help_command))
        
        # Обработчик callback-кнопок
        application.add_handler(CallbackQueryHandler(handle_callback))
        
        # ConversationHandler для добавления файлов
        conv_handler = ConversationHandler(
            entry_points=[CallbackQueryHandler(handle_callback, pattern="^add_file$")],
            states={
                ADDING_FILE_NAME: [
                    MessageHandler(filters.Document.ALL, handle_document),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_file_name)
                ],
                ADDING_FILE_DESCRIPTION: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_file_description)
                ],
            },
            fallbacks=[CommandHandler("cancel", cancel_add_file)],
        )
        
        application.add_handler(conv_handler)
        
        # Логирование при запуске
        logger.info("🤖 Бот помощи запущен!")
        logger.info(f"📁 Файлов в базе: {len(help_system.data['files'])}")
        logger.info(f"🔗 Ссылок в базе: {len(help_system.data['links'])}")
        logger.info(f"👑 Админы: {', '.join(help_system.user_data['admins'])}")
        
        # Запуск бота
        application.run_polling(allowed_updates=Update.ALL_TYPES)
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        raise

if __name__ == "__main__":
    main()
