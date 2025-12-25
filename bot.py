import asyncio
import os
import random
import json
import time
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
    CallbackQueryHandler,
)

# ------------------ НАСТРОЙКИ ------------------
load_dotenv(dotenv_path=Path(__file__).with_name(".env"), override=True)
BOT_TOKEN = os.getenv("BOT_TOKEN")

# ------------------ ВИСЕЛИЦА: СОСТОЯНИЕ ------------------
active_games: dict[int, dict] = {}  # chat_id -> game_data
user_scores: dict[int, int] = {}  # user_id -> wins
_last_guess_time: dict[str, float] = {}  # "chat_id_user_id" -> timestamp
_current_turn: dict[int, int] = {}  # chat_id -> current player index
_update_locks: dict[int, asyncio.Lock] = {}  # chat_id -> lock для предотвращения гонок

# Стадии виселицы для визуализации
hangman_stages = [
    """
    
    
    
    
    
=======
    """,
    """
      |
      |
      |
      |
      |
      |
=======
    """,
    """
      _______
      |
      |
      |
      |
      |
      |
=======
    """,
    """
      _______
      |     |
      |     O
      |
      |
      |
      |
=======
    """,
    """
      _______
      |     |
      |     O
      |     |
      |
      |
      |
=======
    """,
    """
      _______
      |     |
      |     O
      |    /|\\
      |
      |
      |
=======
    """,
    """
      _______
      |     |
      |     O
      |    /|\\
      |    / \\
      |
      |
=======
    """
]

# Большой словарь русских слов по категориям
russian_word_categories = {
    "технологии": [
        "КОМПЬЮТЕР", "ПРОГРАММА", "СЕРВЕР", "БРАУЗЕР", "ПРИЛОЖЕНИЕ",
        "ИНТЕРНЕТ", "СОЦИАЛЬНЫЙ", "ТЕХНОЛОГИЯ", "ИННОВАЦИЯ", "РАЗРАБОТКА",
        "АЛГОРИТМ", "БАЗАДАННЫХ", "ФРЕЙМВОРК", "ИНТЕРФЕЙС", "ПРОГРАММИСТ",
        "ОПЕРАЦИОНКА", "МОНИТОР", "КЛАВИАТУРА", "ПРОЦЕССОР", "ОПЕРАТИВКА",
        "ЖЕСТКИЙДИСК", "ВИДЕОКАРТА", "МАТЕРИНКА", "БЛОКПИТАНИЯ", "КОЛОНКИ",
        "МИКРОФОН", "ВЕБКАМЕРА", "СКАНЕР", "ПРИНТЕР", "МАРШРУТИЗАТОР"
    ],
    
    "животные": [
        "СЛОН", "ТИГР", "ЛЕВ", "ВОЛК", "МЕДВЕДЬ", "ЗАЯЦ", "ЛИСА", "ЕНОТ",
        "БЕЛКА", "ЕЖ", "КРОЛИК", "ХОМЯК", "СОБАКА", "КОШКА", "КОРОВА",
        "ЛОШАДЬ", "ОВЦА", "КОЗА", "СВИНЬЯ", "КУРИЦА", "УТКА", "ГУСЬ",
        "ПЕТУХ", "ИНДЮК", "ВОРОБЕЙ", "СОРОКА", "ВОРОН", "СОВА", "ОРЁл",
        "ЯСТРЕБ", "КРОКОДИЛ", "АЛЛИГАТОР", "ЧЕРЕПАХА", "ЯЩЕРИЦА", "ЗМЕЯ"
    ],
    
    "города": [
        "МОСКВА", "ПИТЕР", "НОВОСИБИРСК", "ЕКАТЕРИНБУРГ", "НИЖНИЙНОВГОРОД",
        "КАЗАНЬ", "ЧЕЛЯБИНСК", "ОМСК", "САМАРА", "РОСТОВ", "УФА", "КРАСНОЯРСК",
        "ПЕРМЬ", "ВОРОНЕЖ", "ВОЛГОГРАД", "КРАСНОДАР", "САРАТОВ", "ТЮМЕНЬ",
        "ТОЛЬЯТТИ", "ИЖЕВСК", "БАРНАУЛ", "УЛЬЯНОВСК", "ИРКУТСК", "ХАБАРОВСК",
        "ЯРОСЛАВЛЬ", "ВЛАДИВОСТОК", "СЕВАСТОПОЛЬ", "СИМФЕРОПОЛЬ", "МУРМАНСК",
        "АРХАНГЕЛЬСК", "КАЛИНИНГРАД", "СМОЛЕНСК", "ТВЕРЬ", "ТУЛА", "РЯЗАНЬ"
    ],
    
    "еда": [
        "ПИЦЦА", "СУШИ", "ПАСТА", "БУРГЕР", "ТАКО", "САЛАТ", "СУП", "СТЕЙК",
        "КАРРИ", "СЭНДВИЧ", "ХЛЕБ", "СЫР", "МАСЛО", "МОЛОКО", "КОФЕ", "ЧАЙ",
        "СОК", "ВОДА", "ЛИМОНАД", "КОКТЕЙЛЬ", "ПИВО", "ВИНО", "ВИСКИ", "ВОДКА",
        "ШОКОЛАД", "ПЕЧЕНЬЕ", "ТОРТ", "ПИРОГ", "МОРОЖЕНОЕ", "БЛИНЫ", "ВАФЛИ",
        "ОМЛЕТ", "СПАГЕТТИ", "РАВИОЛИ", "ПЕЛЬМЕНИ", "ВАРЕНИКИ", "БОРЩ", "ЩИ"
    ],
    
    "спорт": [
        "ФУТБОЛ", "ХОККЕЙ", "БАСКЕТБОЛ", "ВОЛЕЙБОЛ", "ТЕННИС", "БЕЙСБОЛ",
        "БОКС", "БОРЬБА", "ПЛАВАНИЕ", "ГОЛЬФ", "КРИКЕТ", "РЕГБИ", "БАДМИНТОН",
        "НАСТОЛЬНЫЙТЕННИС", "ГАНДБОЛ", "ВОДНОЕПОЛО", "ЛЫЖИ", "СНОУБОРД",
        "КОНЬКИ", "СЕРФИНГ", "СКЕЙТБОРД", "ЛЕГКАЯАТЛЕТИКА", "МАРАФОН",
        "ТРИАТЛОН", "ГИМНАСТИКА", "ДЗЮДО", "КАРАТЕ", "ТХЭКВОНДО", "ФЕХТОВАНИЕ",
        "СТРЕЛЬБА", "СТРЕЛЬБАИЗЛУКА", "ВЕЛОСПОРТ", "МОТОСПОРТ", "АВТОСПОРТ"
    ],
    
    "профессии": [
        "ВРАЧ", "УЧИТЕЛЬ", "ИНЖЕНЕР", "ПРОГРАММИСТ", "ДИЗАЙНЕР",
        "МЕНЕДЖЕР", "ДИРЕКТОР", "БУХГАЛТЕР", "ЮРИСТ", "ЖУРНАЛИСТ",
        "РЕПОРТЕР", "ФОТОГРАФ", "ХУДОЖНИК", "МУЗЫКАНТ", "ПЕВЕЦ",
        "АКТЕР", "ПИСАТЕЛЬ", "ПОЭТ", "УЧЕНЫЙ", "ИССЛЕДОВАТЕЛЬ", "АНАЛИТИК",
        "ВОДИТЕЛЬ", "ПИЛОТ", "КАПИТАН", "ШЕФПОВАР", "ПОВАР", "ОФИЦИАНТ",
        "МЕДСЕСТРА", "СТОМАТОЛГ", "ПСИХОЛОГ", "АРХИТЕКТОР", "СТРОИТЕЛЬ",
        "ФЕРМЕР", "ПОЛИЦЕЙСКИЙ", "ПОЖАРНЫЙ", "СПАСАТЕЛЬ", "КОСМОНАВТ"
    ],
    
    "природа": [
        "ГОРА", "ЛЕС", "РЕКА", "ОКЕАН", "ОЗЕРО", "ВОДОПАД",
        "ВУЛКАН", "КАНЬОН", "ПУСТЫНЯ", "ОСТРОВ", "ПЛЯЖ", "СКАЛА",
        "ДОЛИНА", "ЛУГ", "ДЖУНГЛИ", "ПЕЩЕРА", "ЛЕДНИК", "РОДНИК",
        "ВЕСНА", "ЛЕТО", "ОСЕНЬ", "ЗИМА", "ПОГОДА", "КЛИМАТ",
        "СОЛНЦЕ", "ЛУНА", "ЗВЕЗДА", "ПЛАНЕТА", "КОМЕТА", "ГАЛАКТИКА",
        "РАДУГА", "ГРОЗА", "МОЛНИЯ", "ГРОМ", "ВЕТЕР", "УРАГАН", "ТОРНАДО"
    ]
}

# Эмодзи для категорий
category_emojis = {
    "технологии": "💻",
    "животные": "🐾",
    "города": "🏙️",
    "еда": "🍕",
    "спорт": "⚽",
    "профессии": "👨‍⚕️",
    "природа": "🌿"
}

# Файлы для сохранения статистики
SCORES_FILE = Path(__file__).with_name("hangman_scores.json")
GAMES_FILE = Path(__file__).with_name("hangman_games.json")

# ------------------ УТИЛИТЫ СОХРАНЕНИЯ/ЗАГРУЗКИ ------------------
def load_scores():
    """Загружает статистику из файла"""
    global user_scores
    if SCORES_FILE.exists():
        try:
            with SCORES_FILE.open("r", encoding="utf-8") as f:
                data = json.load(f)
                # Конвертируем ключи из строк в int
                user_scores = {int(k): v for k, v in data.items()}
        except Exception as e:
            print(f"Failed to load scores: {e}")
            user_scores = {}

def save_scores():
    """Сохраняет статистику в файл"""
    try:
        with SCORES_FILE.open("w", encoding="utf-8") as f:
            json.dump(user_scores, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Failed to save scores: {e}")

def load_games_history():
    """Загружает историю игр"""
    if GAMES_FILE.exists():
        try:
            with GAMES_FILE.open("r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return []
    return []

def save_game_history(game_data):
    """Сохраняет завершенную игру в историю"""
    try:
        history = load_games_history()
        history.append(game_data)
        # Ограничиваем историю последними 100 играми
        if len(history) > 100:
            history = history[-100:]
        with GAMES_FILE.open("w", encoding="utf-8") as f:
            json.dump(history, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Failed to save game history: {e}")

# ------------------ ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ------------------
def get_attempts_left(game: dict) -> int:
    """Вычисляет оставшиеся попытки на основе wrong_letters."""
    wrong_count = len(game.get("wrong_letters", set()))
    return max(0, 6 - wrong_count)  # Защита от отрицательных значений

async def is_user_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Проверяет, является ли пользователь админом/владельцем чата."""
    chat = update.effective_chat
    user = update.effective_user
    
    if not chat or not user:
        return False
    
    # В личных сообщениях считаем пользователя админом
    if chat.type == "private":
        return True
    
    try:
        member = await context.bot.get_chat_member(chat.id, user.id)
        # Проверяем статусы
        return member.status in ["creator", "administrator"]
    except Exception as e:
        print(f"Ошибка проверки прав админа: {e}")
        # В случае ошибки разрешаем запуск для отладки
        return True

async def is_chat_admin(bot, chat_id: int, user_id: int) -> bool:
    """Проверка прав администратора по chat_id и user_id."""
    try:
        member = await bot.get_chat_member(chat_id, user_id)
        return member.status in ["creator", "administrator"]
    except Exception as e:
        print(f"Ошибка проверки прав админа (chat): {e}")
        return False

def join_game(chat_id: int, user_id: int, user_name: str) -> bool:
    """Игрок присоединяется к активной игре."""
    if chat_id in active_games:
        game = active_games[chat_id]
        # Проверяем лимит игроков (макс 10)
        if len(game["players"]) >= 10:
            return False
        if user_id not in game["players"]:
            game["players"][user_id] = {
                "name": user_name,
                "correct_guesses": 0,
                "wrong_guesses": 0,
                "joined_at": time.time(),
                "active": True,
                "eliminated": False,  # Новое поле: выбыл ли игрок
            }
            return True
    return False

def leave_game(chat_id: int, user_id: int) -> bool:
    """Игрок покидает игру."""
    if chat_id in active_games and user_id in active_games[chat_id]["players"]:
        del active_games[chat_id]["players"][user_id]
        return True
    return False

def eliminate_player(chat_id: int, user_id: int) -> bool:
    """Игрок выбывает из игры за неправильную попытку угадать слово."""
    if chat_id in active_games and user_id in active_games[chat_id]["players"]:
        active_games[chat_id]["players"][user_id]["eliminated"] = True
        active_games[chat_id]["players"][user_id]["active"] = False
        return True
    return False

def get_current_player(chat_id: int) -> tuple[int, str] | None:
    """Получает текущего игрока, чья очередь ходить."""
    if chat_id not in active_games:
        return None
    
    game = active_games[chat_id]
    
    # Получаем только активных невыбывших игроков
    active_players = [
        pid for pid, data in game["players"].items() 
        if data.get("active", True) and not data.get("eliminated", False)
    ]
    
    if not active_players:
        return None
    
    # Если нет текущего хода, устанавливаем первого игрока
    if chat_id not in _current_turn:
        _current_turn[chat_id] = 0
    
    # Проверяем, что индекс в пределах диапазона
    if _current_turn[chat_id] >= len(active_players):
        _current_turn[chat_id] = _current_turn[chat_id] % len(active_players)
    
    # Находим активного игрока по индексу
    turn_index = _current_turn[chat_id] % len(active_players)
    players_list = list(active_players)
    
    # Безопасное получение игрока
    if turn_index < len(players_list):
        player_id = players_list[turn_index]
        player_data = game["players"][player_id]
        
        # Двойная проверка, что игрок активен и не выбыл
        if player_data.get("active", True) and not player_data.get("eliminated", False):
            player_name = player_data["name"]
            return player_id, player_name
    
    # Если что-то пошло не так, возвращаем первого активного игрока
    if active_players:
        player_id = active_players[0]
        player_data = game["players"][player_id]
        player_name = player_data["name"]
        return player_id, player_name
    
    return None

def next_turn(chat_id: int) -> tuple[int, str] | None:
    """Передает ход следующему игроку. Если игрок один, возвращает его же."""
    if chat_id not in active_games:
        return None
    
    if chat_id not in _current_turn:
        _current_turn[chat_id] = 0
    else:
        _current_turn[chat_id] += 1
    
    # Получаем следующего игрока
    next_player = get_current_player(chat_id)
    
    # Если следующий игрок не найден (все выбыли или проблемы с индексами)
    # Сбрасываем счетчик и пробуем снова
    if not next_player:
        # Сбрасываем счетчик и ищем первого активного игрока
        _current_turn[chat_id] = 0
        
        # Получаем список активных игроков
        game = active_games[chat_id]
        active_players = [
            pid for pid, data in game["players"].items() 
            if data.get("active", True) and not data.get("eliminated", False)
        ]
        
        if active_players:
            # Берем первого активного игрока
            player_id = active_players[0]
            player_name = game["players"][player_id]["name"]
            return player_id, player_name
    
    return next_player

def get_active_players_count(chat_id: int) -> int:
    """Возвращает количество активных невыбывших игроков."""
    if chat_id not in active_games:
        return 0
    
    game = active_games[chat_id]
    return len([
        pid for pid, data in game["players"].items() 
        if data.get("active", True) and not data.get("eliminated", False)
    ])

# ------------------ ОТОБРАЖЕНИЕ ИГРЫ ------------------
async def update_game_display(context: ContextTypes.DEFAULT_TYPE, chat_id: int, force_update: bool = False) -> None:
    """Обновить основное сообщение с состоянием игры."""
    # Создаем или получаем блокировку для этого чата
    if chat_id not in _update_locks:
        _update_locks[chat_id] = asyncio.Lock()
    
    # Используем блокировку для предотвращения гонок
    async with _update_locks[chat_id]:
        if chat_id not in active_games:
            print(f"DEBUG update_game_display: Нет активной игры для chat_id {chat_id}")
            return
        
        game = active_games[chat_id]
        
        # Если игра еще не начата (слово не выбрано)
        if not game.get("word"):
            print(f"DEBUG update_game_display: Слово не выбрано для chat_id {chat_id}")
            return
        
        word = game["word"]
        wrong_count = len(game.get("wrong_letters", set()))
        attempts_left = get_attempts_left(game)
        
        print(f"DEBUG update_game_display: chat_id={chat_id}, wrong_count={wrong_count}, attempts_left={attempts_left}")

        # Формируем отображение слова
        display_word = ""
        for letter in word:
            if letter in game.get("guessed_letters", set()) or not letter.isalpha():
                display_word += letter + " "
            else:
                display_word += "_ "
        
        # Формируем список ТОЛЬКО активных невыбывших игроков
        active_players = {
            pid: data for pid, data in game.get("players", {}).items() 
            if data.get("active", True) and not data.get("eliminated", False)
        }

        # Формируем список выбывших игроков
        eliminated_players = {
            pid: data for pid, data in game.get("players", {}).items() 
            if data.get("eliminated", False)
        }

        players_text = ""
        if active_players:
            # Сортируем по количеству правильных ответов
            sorted_players = sorted(
                active_players.items(), key=lambda x: x[1].get("correct_guesses", 0), reverse=True
            )

            for i, (player_id, player_data) in enumerate(sorted_players, 1):
                medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "👤"
                players_text += (
                    f"{medal} {player_data.get('name', 'Unknown')}: "
                    f"✅{player_data.get('correct_guesses', 0)} ❌{player_data.get('wrong_guesses', 0)}\n"
                )
        else:
            players_text = "❌ Нет активных игроков\n💡 Все игроки выбыли или покинули игру"

        # Список выбывших игроков
        eliminated_text = ""
        if eliminated_players:
            eliminated_text = "💀 *Выбывшие игроки:*\n"
            for player_id, player_data in eliminated_players.items():
                eliminated_text += f"☠️ {player_data.get('name', 'Unknown')}\n"

        # Текущая стадия виселицы - вычисляем из wrong_letters
        stage_index = min(wrong_count, len(hangman_stages) - 1)
        
        hangman_display = hangman_stages[stage_index]

        # Получаем эмодзи для категории
        category_emoji = category_emojis.get(game.get('category', ''), '🎯')

        # Формируем список неправильных букв для отображения
        wrong_letters_text = ', '.join(sorted(game.get('wrong_letters', []))) if game.get('wrong_letters') else 'пока нет'
        
        # Определяем, чья очередь ходить
        current_player_info = get_current_player(chat_id)
        turn_text = ""
        if current_player_info:
            player_id, player_name = current_player_info
            turn_text = f"🎮 *Сейчас ходит:* {player_name}\n\n"

        message_text = f"""
🎮 *ВИСЕЛИЦА* | {category_emoji} Категория: {game.get('category', '').upper()}
👑 Запустил: {game.get('started_by_name', 'Unknown')}

{turn_text}{hangman_display}

📖 Слово: `{display_word.strip()}`
📏 Длина слова: {len(word)} букв

❌ Неправильные попытки ({wrong_count}/6): {wrong_letters_text}

❤️ Осталось попыток: {attempts_left}
👥 Активных игроков: {len(active_players)}

*Активные игроки ({len(active_players)}):*
{players_text}

{eliminated_text}
💡 *Как играть:*
• Пишите ОДНУ букву в чат
• Или попробуйте угадать слово целиком (выбываете при ошибке)
• Ждите своей очереди
• Бот сам подскажет, чей ход

📝 *Команды:*
/join - присоединиться к игре
/leave - выйти из игры
/hint - получить подсказку (1 за игру)
/skip - пропустить ход (если игрок не отвечает 30 сек)
        """.strip()

        # Кнопки
        buttons = [
            [
                InlineKeyboardButton("🎮 Присоединиться", callback_data="hangman_join"),
                InlineKeyboardButton("👋 Выйти", callback_data="hangman_leave"),
            ],
            [
                InlineKeyboardButton("💡 Подсказка", callback_data="hangman_hint"),
                InlineKeyboardButton("⏭️ Пропустить ход", callback_data="hangman_skip"),
            ]
        ]

        # Кнопку остановки показываем только админу, который запустил игру
        is_admin = await is_chat_admin(context.bot, chat_id, game.get("started_by", 0))
        if is_admin:
            buttons.append([InlineKeyboardButton("🛑 Остановить игру", callback_data="admin_stop_game")])

        markup = InlineKeyboardMarkup(buttons)

        try:
            message_id = game.get("message_id")
            if message_id:
                # Просто редактируем существующее сообщение
                try:
                    await context.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=message_id,
                        text=message_text,
                        parse_mode=ParseMode.MARKDOWN,
                        reply_markup=markup,
                    )
                    print(f"DEBUG: Успешно обновлено сообщение с ID {message_id}")
                except Exception as edit_error:
                    print(f"Ошибка редактирования сообщения: {edit_error}")
                    # Если не удалось отредактировать (например, сообщение удалено), 
                    # пробуем отправить новое
                    try:
                        msg = await context.bot.send_message(
                            chat_id=chat_id,
                            text=message_text,
                            parse_mode=ParseMode.MARKDOWN,
                            reply_markup=markup,
                        )
                        active_games[chat_id]["message_id"] = msg.message_id
                        print(f"DEBUG: Создано новое сообщение с ID {msg.message_id}")
                    except Exception as send_error:
                        print(f"Ошибка отправки нового сообщения: {send_error}")
        except Exception as e:
            print(f"Error updating hangman display: {e}")

async def show_category_selection(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    """Показать инлайн-меню выбора категории для виселицы."""
    if chat_id not in active_games:
        return
    
    game = active_games[chat_id]
    admin_name = game.get("started_by_name", "Unknown")

    buttons = []
    for category in russian_word_categories.keys():
        emoji = category_emojis.get(category, '🎯')
        buttons.append(
            [InlineKeyboardButton(f"{emoji} {category.capitalize()}", 
              callback_data=f"hangman_category_{category}")]
        )

    # Добавляем случайную категорию
    buttons.append(
        [InlineKeyboardButton("🎲 Случайная категория", callback_data="hangman_category_random")]
    )

    markup = InlineKeyboardMarkup(buttons)

    try:
        msg = await context.bot.send_message(
            chat_id,
            text=(
                f"👑 *Администратор {admin_name} запускает игру 'Виселица'!*\n\n"
                "📖 *Правила:*\n"
                "• Бот загадывает слово\n"
                "• Игроки присоединяются командой /join\n"
                "• Игроки пишут буквы в ОБЩИЙ чат по очереди\n"
                "• Можно угадать слово целиком (выбываешь при ошибке)\n"
                "• У команды 6 попыток на ошибки\n"
                "• Побеждает тот, кто угадает слово!\n"
                "• Можно получить 1 подсказку за игру\n\n"
                "🎯 *Выберите категорию слов:*"
            ),
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=markup,
        )
        active_games[chat_id]["message_id"] = msg.message_id
        print(f"DEBUG: Создано сообщение с категориями, ID: {msg.message_id}")
    except Exception as e:
        print(f"Ошибка отправки сообщения с категориями: {e}")

# ------------------ ЛОГИКА ИГРЫ ------------------
async def process_word_guess(
    context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int, guessed_word: str
) -> bool:
    """Обработка попытки угадать слово целиком."""
    if chat_id not in active_games:
        return False

    game = active_games[chat_id]
    word = game.get("word", "")

    # Если игрок не зарегистрирован в игре
    if user_id not in game.get("players", {}):
        return False

    player = game["players"][user_id]
    player_name = player.get("name", "Unknown")

    # Нормализуем слово (верхний регистр, Ё -> Е)
    guessed_word = guessed_word.upper().replace('Ё', 'Е')
    
    if guessed_word == word:
        # Игрок угадал слово!
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"🎉🎉🎉 *ПОБЕДА!* 🎉🎉🎉\n\n{player_name} угадал(а) слово: *{word}*!",
            parse_mode=ParseMode.MARKDOWN,
        )
        
        # Завершаем игру победой
        await end_game_win(context, chat_id, user_id)
        return True
    else:
        # Игрок не угадал слово - выбывает
        eliminate_player(chat_id, user_id)
        
        # Отправляем сообщение о выбывании
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                f"💀 *{player_name} выбывает из игры!*\n\n"
                f"Названное слово: *{guessed_word}*\n"
                f"Правильное слово: *{word}*\n\n"
                "Игрок выбывает за неправильную попытку угадать слово целиком!"
            ),
            parse_mode=ParseMode.MARKDOWN,
        )
        
        # Проверяем, остались ли активные игроки
        active_players_count = get_active_players_count(chat_id)
        if active_players_count == 0:
            await context.bot.send_message(
                chat_id=chat_id,
                text="💀 Все игроки выбыли! Игра окончена.",
            )
            await end_game_lose(context, chat_id)
            return False
        
        # Передаем ход следующему активному игроку
        next_player = next_turn(chat_id)
        if next_player:
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"🎮 Теперь ходит: {next_player[1]}",
            )
        
        # ОБНОВЛЯЕМ главное сообщение с небольшой задержкой
        await asyncio.sleep(0.5)
        await update_game_display(context, chat_id)
        return False

async def process_guess(
    context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int, guess: str
) -> None:
    """Обработка хода игрока в общем чате."""
    if chat_id not in active_games:
        print(f"DEBUG: Нет активной игры для chat_id {chat_id}")
        return

    game = active_games[chat_id]
    word = game.get("word", "")

    # Если игрок не зарегистрирован в игре
    if user_id not in game.get("players", {}):
        print(f"DEBUG: Игрок {user_id} не зарегистрирован в игре")
        return

    # Проверяем, чья очередь ходить
    current_player = get_current_player(chat_id)
    if not current_player:
        print(f"DEBUG: Нет текущего игрока для chat_id {chat_id}")
        return
        
    if current_player[0] != user_id:
        print(f"DEBUG: Не очередь игрока {user_id}. Сейчас ходит: {current_player[0]}")
        return  # Не очередь этого игрока

    player = game["players"][user_id]
    player_name = player.get("name", "Unknown")

    # Проверяем скорость хода (защита от флуда)
    user_key = f"{chat_id}_{user_id}"
    last_time = _last_guess_time.get(user_key)
    now_time = time.time()
    if last_time and now_time - last_time < 1:  # 1 секунда между ходами
        return
    _last_guess_time[user_key] = now_time

    # Нормализуем букву (Ё -> Е)
    if guess == 'Ё':
        guess = 'Е'
    
    # Проверяем, не угадывали ли эту букву уже
    guessed_letters = game.get("guessed_letters", set())
    wrong_letters = game.get("wrong_letters", set())
    
    if guess in guessed_letters:
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"❌ {player_name}, буква '{guess}' уже была угадана! Попробуйте другую букву.",
        )
        # Ход НЕ пропускается при повторной букве
        return
    
    if guess in wrong_letters:
        # ОШИБКА! Буква уже была ошибочной
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"❌ {player_name}, буква '{guess}' уже была ошибочной! Попробуйте другую букву.",
        )
        # Счётчик НЕ меняем, ход НЕ пропускается
        return
    
    if guess in word:
        # Правильная буква
        if "guessed_letters" not in game:
            game["guessed_letters"] = set()
        game["guessed_letters"].add(guess)
        player["correct_guesses"] = player.get("correct_guesses", 0) + 1
        
        print(f"DEBUG: Правильная буква '{guess}'. guessed_letters теперь: {game.get('guessed_letters', set())}")
        
        # Отправляем сообщение о правильной букве (БЕЗ счетчика)
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"✅ {player_name}, буква '{guess}' есть в слове! {player_name} ходит ещё раз.",
        )
        
        # ОБНОВЛЯЕМ главное сообщение с виселицей
        await asyncio.sleep(0.3)
        await update_game_display(context, chat_id)

        # Проверяем, угадано ли слово полностью
        if all(letter in game.get("guessed_letters", set()) for letter in word if letter.isalpha()):
            await end_game_win(context, chat_id, user_id)
            return

    else:
        # Неправильная буква - ДОБАВЛЯЕМ в wrong_letters
        if "wrong_letters" not in game:
            game["wrong_letters"] = set()
        game["wrong_letters"].add(guess)
        player["wrong_guesses"] = player.get("wrong_guesses", 0) + 1
        
        # Вычисляем текущее количество попыток
        wrong_count = len(game["wrong_letters"])
        attempts_left = get_attempts_left(game)
        print(f"DEBUG: Неправильная буква '{guess}'. wrong_count теперь: {wrong_count}, attempts_left теперь: {attempts_left}")
        print(f"DEBUG: wrong_letters теперь: {game.get('wrong_letters', set())}")
        
        # Отправляем сообщение о неправильной букве
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"❌ {player_name}, буквы '{guess}' нет в слове.",
        )
        
        # СРАЗУ обновляем отображение игры ПЕРЕД проверкой поражения
        await asyncio.sleep(0.3)
        await update_game_display(context, chat_id, force_update=True)
        
        # Проверяем поражение (используем вычисленное значение)
        if attempts_left <= 0:
            await asyncio.sleep(0.3)
            await end_game_lose(context, chat_id)
            return
        else:
            # Передаем ход следующему игроку
            next_player = next_turn(chat_id)
            if next_player:
                next_player_name = next_player[1]
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"🎮 Теперь ходит: {next_player_name}",
                )
                # Обновляем отображение после смены игрока
                await asyncio.sleep(0.5)
                await update_game_display(context, chat_id, force_update=True)
            else:
                # Если next_player вернул None (например, активных игроков нет)
                # Назначаем нового текущего игрока принудительно
                current_player = get_current_player(chat_id)
                if current_player:
                    current_player_name = current_player[1]
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"🎮 Снова ходит: {current_player_name}",
                    )
                    # Обновляем отображение
                    await asyncio.sleep(0.5)
                    await update_game_display(context, chat_id, force_update=True)
                else:
                    # Если вообще нет активных игроков - завершаем игру
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text="💀 Нет активных игроков! Игра окончена.",
                    )
                    await end_game_lose(context, chat_id)
                    return

async def give_hint(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int) -> bool:
    """Дать подсказку игроку (открыть одну букву)."""
    if chat_id not in active_games:
        return False
    
    game = active_games[chat_id]
    word = game.get("word", "")
    
    # Проверяем, что игрок еще не использовал подсказку
    if game.get("hint_used"):
        return False
    
    # Находим неотгаданные буквы
    guessed_letters = game.get("guessed_letters", set())
    unguessed = [letter for letter in word if letter.isalpha() and letter not in guessed_letters]
    if not unguessed:
        return False
    
    # Выбираем случайную букву для подсказки
    hint_letter = random.choice(unguessed)
    if "guessed_letters" not in game:
        game["guessed_letters"] = set()
    game["guessed_letters"].add(hint_letter)
    game["hint_used"] = True
    
    # Даем бонус игроку, который запросил подсказку
    if user_id in game.get("players", {}):
        player = game["players"][user_id]
        player["correct_guesses"] = player.get("correct_guesses", 0) + 1
    
    await context.bot.send_message(
        chat_id=chat_id,
        text=f"💡 Подсказка: в слове есть буква '{hint_letter}'!",
    )
    
    # ОБНОВЛЯЕМ главное сообщение с задержкой
    await asyncio.sleep(0.3)
    await update_game_display(context, chat_id)
    
    # Проверяем, не выиграли ли мы после подсказки
    if all(letter in game.get("guessed_letters", set()) for letter in word if letter.isalpha()):
        await end_game_win(context, chat_id, user_id)
        return True
    
    return True

async def skip_turn(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int) -> bool:
    """Пропустить ход текущего игрока."""
    if chat_id not in active_games:
        return False
    
    game = active_games[chat_id]
    
    # Только админ или текущий игрок может пропустить ход
    current_player = get_current_player(chat_id)
    if not current_player:
        return False
    
    current_player_id = current_player[0]
    
    # Проверяем права
    is_admin = await is_chat_admin(context.bot, chat_id, user_id)
    if not is_admin and user_id != current_player_id:
        return False
    
    # Пропускаем ход
    next_player = next_turn(chat_id)
    if next_player:
        player_name = game["players"][current_player_id].get("name", "Unknown")
        next_player_name = next_player[1]
        
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"⏭️ Ход игрока {player_name} пропущен!\n🎮 Теперь ходит: {next_player_name}",
        )
        
        # ОБНОВЛЯЕМ главное сообщение с задержкой
        await asyncio.sleep(0.3)
        await update_game_display(context, chat_id)
        return True
    
    return False

async def end_game_win(context: ContextTypes.DEFAULT_TYPE, chat_id: int, winner_id: int) -> None:
    """Завершение игры победой."""
    if chat_id not in active_games:
        return
    
    game = active_games[chat_id]
    word = game.get("word", "")
    winner_name = game.get("players", {}).get(winner_id, {}).get("name", "Игрок")
    
    # Сохраняем данные игры для истории
    game_data = {
        "chat_id": chat_id,
        "word": word,
        "category": game.get("category", ""),
        "winner_id": winner_id,
        "winner_name": winner_name,
        "players_count": len(game.get("players", {})),
        "timestamp": datetime.now().isoformat(),
        "result": "win"
    }
    
    # Обновляем счет ТОЛЬКО для активных игроков
    active_players = {
        pid: data for pid, data in game.get("players", {}).items() 
        if data.get("active", True) and not data.get("eliminated", False)
    }

    for player_id in active_players:
        user_scores[player_id] = user_scores.get(player_id, 0) + 1  # Все активные игроки получают очко

    # Бонус победителю
    user_scores[winner_id] = user_scores.get(winner_id, 0) + 2  # +2 очка за победу
    
    # Сохраняем статистику
    save_scores()
    save_game_history(game_data)

    # Формируем итоговую таблицу ТОЛЬКО активных игроков
    players_sorted = sorted(
        active_players.items(), key=lambda x: x[1].get("correct_guesses", 0), reverse=True
    )

    leaderboard = "🏆 *Результаты:*\n"
    for i, (player_id, player_data) in enumerate(players_sorted, 1):
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "👤"
        leaderboard += (
            f"{medal} {player_data.get('name', 'Unknown')}: "
            f"✅{player_data.get('correct_guesses', 0)} ❌{player_data.get('wrong_guesses', 0)}\n"
        )

    message_text = f"""
🎉 *ПОБЕДА!*

👑 Победитель: *{winner_name}*

📖 Загаданное слово: *{word}*

{leaderboard}

🎯 Для новой игры используйте /newgame
    """.strip()

    try:
        message_id = game.get("message_id")
        if message_id:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=message_text,
                parse_mode=ParseMode.MARKDOWN,
            )
    except Exception as e:
        print(f"Error editing message on win: {e}")

    # Очищаем таймауты и удаляем игру
    if chat_id in _last_guess_time:
        keys_to_remove = [k for k in _last_guess_time.keys() if k.startswith(f"{chat_id}_")]
        for key in keys_to_remove:
            del _last_guess_time[key]
    
    if chat_id in _current_turn:
        del _current_turn[chat_id]
    
    if chat_id in _update_locks:
        del _update_locks[chat_id]
    
    if chat_id in active_games:
        del active_games[chat_id]

async def end_game_lose(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    """Завершение игры поражением (закончились попытки)."""
    if chat_id not in active_games:
        return
    
    game = active_games[chat_id]
    word = game.get("word", "")
    wrong_count = len(game.get("wrong_letters", set()))
    
    # Сохраняем данные игры для истории
    game_data = {
        "chat_id": chat_id,
        "word": word,
        "category": game.get("category", ""),
        "players_count": len(game.get("players", {})),
        "wrong_attempts": wrong_count,
        "timestamp": datetime.now().isoformat(),
        "result": "lose"
    }
    save_game_history(game_data)

    # Формируем итоговую таблицу всех игроков
    players_sorted = sorted(
        game.get("players", {}).items(), key=lambda x: x[1].get("correct_guesses", 0), reverse=True
    )

    leaderboard = "📊 *Результаты:*\n"
    for i, (player_id, player_data) in enumerate(players_sorted, 1):
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "👤"
        status = "☠️" if player_data.get("eliminated", False) else "✅"
        leaderboard += (
            f"{medal} {status} {player_data.get('name', 'Unknown')}: "
            f"✅{player_data.get('correct_guesses', 0)} ❌{player_data.get('wrong_guesses', 0)}\n"
        )

    message_text = f"""
💀 *ИГРА ОКОНЧЕНА*

📖 Загаданное слово было: *{word}*
❌ Неправильных попыток: {wrong_count} из 6

{leaderboard}

🎯 Для новой игры используйте /newgame
    """.strip()

    try:
        message_id = game.get("message_id")
        if message_id:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=message_text,
                parse_mode=ParseMode.MARKDOWN,
            )
    except Exception as e:
        print(f"Error editing message on lose: {e}")

    # Очищаем таймауты и удаляем игру
    if chat_id in _last_guess_time:
        keys_to_remove = [k for k in _last_guess_time.keys() if k.startswith(f"{chat_id}_")]
        for key in keys_to_remove:
            del _last_guess_time[key]
    
    if chat_id in _current_turn:
        del _current_turn[chat_id]
    
    if chat_id in _update_locks:
        del _update_locks[chat_id]
    
    if chat_id in active_games:
        del active_games[chat_id]

# ------------------ КОМАНДЫ БОТА ------------------
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Приветственное сообщение."""
    text = """
🎮 *Добро пожаловать в бот "Виселица"!*

🤖 Я помогу вам весело провести время с друзьями в групповом чате.

🎯 *Как начать играть:*
1. Добавьте меня в групповой чат
2. Администратор использует команду /newgame
3. Выбирает категорию слов
4. Все присоединяются командой /join
5. Пишут буквы прямо в чат по очереди
6. Можно рискнуть и угадать слово целиком!

📚 *Команды:*
/newgame - начать новую игру (админы)
/join - присоединиться к игре
/leave - выйти из игры
/hint - получить подсказку (1 за игру)
/skip - пропустить ход (если игрок не отвечает)
/stop - остановить игру (админы)
/stats - статистика игроков
/history - история игр
/rules - правила игры

✨ Удачи в игре! Начните с /newgame в групповом чате!
    """.strip()
    
    await update.effective_message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

async def newgame_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начало новой игры (только для групп и только админы)."""
    chat = update.effective_chat
    message = update.effective_message
    user = update.effective_user

    if not chat:
        await message.reply_text("❌ Чат не найден!")
        return
    
    # Проверяем тип чата
    if chat.type == "private":
        await message.reply_text("❌ Эта игра только для групповых чатов! Добавьте меня в группу и используйте там /newgame")
        return
    
    if chat.type not in ("group", "supergroup"):
        await message.reply_text("❌ Эта игра только для групповых чатов!")
        return

    # Проверяем права администратора
    is_admin = await is_user_admin(update, context)
    if not is_admin:
        await message.reply_text(
            "❌ Только администраторы могут запускать игру!\n"
            "👑 Обратитесь к администратору чата."
        )
        return

    chat_id = chat.id

    if chat_id in active_games:
        await message.reply_text("🎮 Игра уже идет! Дождитесь окончания.")
        return

    started_by_name = f"{user.first_name} {(user.last_name or '')}".strip()

    active_games[chat_id] = {
        "word": "",
        "guessed_letters": set(),
        "wrong_letters": set(),
        "category": "",
        "players": {},
        "message_id": None,
        "started_by": user.id,
        "started_by_name": started_by_name,
        "start_time": time.time(),
        "hint_used": False,
    }
    
    print(f"DEBUG newgame: Создана новая игра для chat_id {chat_id}")

    await show_category_selection(context, chat_id)

async def join_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для присоединения к игре."""
    chat = update.effective_chat
    message = update.effective_message

    if not chat:
        return
    chat_id = chat.id

    if chat_id not in active_games:
        await message.reply_text("❌ Сейчас нет активной игры! Сначала запустите /newgame")
        return

    user = update.effective_user
    user_id = user.id
    user_name = f"{user.first_name} {(user.last_name or '')}".strip()

    if join_game(chat_id, user_id, user_name):
        await message.reply_text(
            f"🎮 {user_name} присоединился к игре!",
            reply_to_message_id=message.message_id,
        )
        
        # Если это первый игрок, назначаем его текущим
        if len(active_games[chat_id]["players"]) == 1:
            _current_turn[chat_id] = 0
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"🎮 Первым ходит: {user_name}",
            )
        
        await update_game_display(context, chat_id)
    else:
        if len(active_games[chat_id]["players"]) >= 10:
            await message.reply_text(
                "❌ В игре уже максимальное количество игроков (10)!",
                reply_to_message_id=message.message_id,
            )
        else:
            await message.reply_text(
                f"❌ {user_name}, вы уже в игре!",
                reply_to_message_id=message.message_id,
            )

async def leave_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для выхода из игры."""
    chat = update.effective_chat
    message = update.effective_message

    if not chat:
        return
    chat_id = chat.id

    if chat_id not in active_games:
        await message.reply_text("❌ Сейчас нет активной игры!")
        return

    user = update.effective_user
    user_id = user.id
    user_name = f"{user.first_name} {(user.last_name or '')}".strip()

    if leave_game(chat_id, user_id):
        # Если ушел текущий игрок, передаем ход следующему
        current_player = get_current_player(chat_id)
        if current_player and current_player[0] == user_id:
            next_player = next_turn(chat_id)
            if next_player:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"👋 {user_name} вышел из игры.\n🎮 Теперь ходит: {next_player[1]}",
                )
        
        await message.reply_text(
            f"👋 {user_name} вышел из игры.",
            reply_to_message_id=message.message_id,
        )
        await update_game_display(context, chat_id)
    else:
        await message.reply_text(
            f"❌ {user_name}, вы не в игре!",
            reply_to_message_id=message.message_id,
        )

async def hint_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Запросить подсказку."""
    chat = update.effective_chat
    message = update.effective_message

    if not chat:
        return
    chat_id = chat.id

    if chat_id not in active_games:
        await message.reply_text("❌ Сейчас нет активной игры!")
        return

    user = update.effective_user
    user_id = user.id

    success = await give_hint(context, chat_id, user_id)
    if success:
        await message.reply_text(
            "💡 Подсказка получена!",
            reply_to_message_id=message.message_id,
        )
    else:
        await message.reply_text(
            "❌ Подсказка уже была использована или нет доступных букв!",
            reply_to_message_id=message.message_id,
        )

async def skip_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Пропустить ход текущего игрока."""
    chat = update.effective_chat
    message = update.effective_message

    if not chat:
        return
    chat_id = chat.id

    if chat_id not in active_games:
        await message.reply_text("❌ Сейчас нет активной игры!")
        return

    user = update.effective_user
    user_id = user.id

    success = await skip_turn(context, chat_id, user_id)
    if success:
        await message.reply_text(
            "⏭️ Ход пропущен!",
            reply_to_message_id=message.message_id,
        )
    else:
        await message.reply_text(
            "❌ Не удалось пропустить ход! Только администратор или текущий игрок могут пропускать ход.",
            reply_to_message_id=message.message_id,
        )

async def stop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда остановки игры (только для админов)."""
    chat = update.effective_chat
    message = update.effective_message
    if not chat:
        return
    chat_id = chat.id

    if chat_id not in active_games:
        await message.reply_text("❌ Активной игры нет.")
        return

    if not await is_user_admin(update, context):
        await message.reply_text("❌ Только администраторы могут останавливать игру!")
        return

    # Очищаем состояние
    if chat_id in _last_guess_time:
        keys_to_remove = [k for k in _last_guess_time.keys() if k.startswith(f"{chat_id}_")]
        for key in keys_to_remove:
            del _last_guess_time[key]
    
    if chat_id in _current_turn:
        del _current_turn[chat_id]
    
    if chat_id in _update_locks:
        del _update_locks[chat_id]
    
    if chat_id in active_games:
        del active_games[chat_id]
    
    await message.reply_text(f"🛑 Игра остановлена администратором {update.effective_user.first_name}.")

async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает статистику игроков по виселице."""
    message = update.effective_message
    
    if not user_scores:
        await message.reply_text("📊 Статистика пока пуста. Сыграйте в игру!")
        return

    # Топ игроков
    top_players = sorted(user_scores.items(), key=lambda x: x[1], reverse=True)[:10]

    stats_text = "🏆 *Топ игроков виселицы:*\n\n"
    for i, (player_id, score) in enumerate(top_players, 1):
        try:
            member = await context.bot.get_chat_member(update.effective_chat.id, player_id)
            name = member.user.first_name
            if member.user.username:
                name = f"@{member.user.username}"
            stats_text += f"{i}. {name}: {score} побед\n"
        except Exception:
            stats_text += f"{i}. Игрок {player_id}: {score} побед\n"
    
    stats_text += f"\nВсего игроков: {len(user_scores)}"
    
    await message.reply_text(stats_text, parse_mode=ParseMode.MARKDOWN)

async def history_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает последние 5 игр."""
    history = load_games_history()
    
    if not history:
        await update.effective_message.reply_text("📜 История игр пока пуста.")
        return
    
    # Берем последние 5 игр
    recent_games = history[-5:][::-1]  # переворачиваем, чтобы новые были первыми
    
    history_text = "📜 *Последние игры:*\n\n"
    for i, game in enumerate(recent_games, 1):
        result = "🎉 ПОБЕДА" if game["result"] == "win" else "💀 ПОРАЖЕНИЕ"
        winner = f"\n👑 Победитель: {game.get('winner_name', 'Неизвестно')}" if game["result"] == "win" else ""
        history_text += (
            f"{i}. Слово: *{game['word']}*\n"
            f"   Категория: {game['category']}\n"
            f"   Игроков: {game['players_count']}\n"
            f"   Результат: {result}{winner}\n"
            f"   Время: {game['timestamp'][:16]}\n\n"
        )
    
    await update.effective_message.reply_text(history_text, parse_mode=ParseMode.MARKDOWN)

async def rules_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает правила игры 'Виселица'."""
    rules_text = """
🎮 *Правила игры "Виселица":*

📖 *Цель игры:* угадать загаданное слово

👥 *Как играть:*
1. Администратор запускает игру командой /newgame
2. Игроки присоединяются командой /join
3. Бот загадывает слово из выбранной категории
4. Игроки пишут буквы в ОБЩИЙ чат по очереди
5. Можно рискнуть и угадать слово целиком!

⚡ *Особенности:*
• У команды 6 попыток на ошибки
• Игроки ходят по очереди
• После правильной буквы ход остается у того же игрока
• После ошибки ход передается следующему
• Если игрок угадывает слово целиком - ПОБЕДА!
• Если игрок ошибся в слове целиком - ВЫБЫВАЕТ!
• Можно получить 1 подсказку за игру (/hint)
• Можно пропустить ход (/skip)

🎯 *Команды:*
/newgame - начать игру (только админы)
/join - присоединиться к игре
/leave - выйти из игры
/hint - получить подсказку
/skip - пропустить ход
/stop - остановить игру (только админы)
/stats - статистика игроков
/history - история игр
/rules - правила игры
    """.strip()

    await update.effective_message.reply_text(rules_text, parse_mode=ParseMode.MARKDOWN)

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает список команд."""
    help_text = """
🤖 *Бот для игры в Виселицу*

🎮 *Основные команды:*
/newgame - начать новую игру (админы)
/join - присоединиться к игре
/leave - выйти из игры
/hint - получить подсказку (1 за игру)
/skip - пропустить ход
/stop - остановить игру (админы)

📊 *Информация:*
/stats - статистика игроков
/history - история последних игр
/rules - правила игры
/help - эта справка

💡 *Как играть:*
1. Админ запускает игру /newgame
2. Выбирает категорию слов
3. Все присоединяются /join
4. Пишут буквы в чат по очереди
5. Рискуйте - угадывайте слово целиком!

✨ Удачи в игре! 🎯
    """.strip()
    
    await update.effective_message.reply_text(help_text, parse_mode=ParseMode.MARKDOWN)

async def debug_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отладочная команда для проверки прав."""
    chat = update.effective_chat
    user = update.effective_user
    
    is_admin = await is_user_admin(update, context)
    
    debug_text = f"""
🔧 *Отладочная информация:*
    
👤 Пользователь: {user.first_name} (ID: {user.id})
💬 Чат: {chat.title if chat.title else chat.type} (ID: {chat.id})
👑 Админ: {'✅ ДА' if is_admin else '❌ НЕТ'}
🎮 Активных игр: {len(active_games)}
📊 Игроков в статистике: {len(user_scores)}

📋 Активные игры: {list(active_games.keys()) if active_games else 'Нет'}
    """.strip()
    
    await update.effective_message.reply_text(debug_text, parse_mode=ParseMode.MARKDOWN)

# ------------------ ОБРАБОТЧИКИ CALLBACK ------------------
async def handle_hangman_category_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка выбора категории виселицы через inline-кнопки."""
    query = update.callback_query
    await query.answer()
    chat_id = query.message.chat.id

    if chat_id not in active_games:
        await query.edit_message_text("❌ Игра уже завершена.")
        return

    # Проверяем, что callback от администратора
    user_id = query.from_user.id
    is_admin = await is_chat_admin(context.bot, chat_id, user_id)
    if not is_admin and user_id != active_games[chat_id]["started_by"]:
        await query.answer("❌ Только администратор может выбирать категорию!", show_alert=True)
        return

    category = query.data.replace("hangman_category_", "")
    if category == "random":
        category = random.choice(list(russian_word_categories.keys()))

    # Обновляем игру с выбранной категорией
    word = random.choice(russian_word_categories[category])
    game = active_games[chat_id]
    game["word"] = word.upper()
    game["category"] = category

    # Получаем эмодзи для категории
    category_emoji = category_emojis.get(category, '🎯')

    # Обновляем сообщение
    try:
        await query.edit_message_text(
            text=(
                f"🎮 *Категория выбрана: {category_emoji} {category.upper()}*\n\n"
                f"📖 Слово загадано: {len(word)} букв\n\n"
                "💡 *Как играть:*\n"
                "1. Присоединяйтесь командой /join\n"
                "2. Пишите буквы в чат по очереди\n"
                "3. Или угадайте слово целиком (риск!)\n"
                "4. Бот покажет, чей ход\n\n"
                f"👑 Игру запустил: {game['started_by_name']}"
            ),
            parse_mode=ParseMode.MARKDOWN,
        )
    except Exception as e:
        print(f"Error editing category selection message: {e}")

    # Показываем текущее состояние игры
    await asyncio.sleep(0.5)
    await update_game_display(context, chat_id)

async def handle_hangman_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка инлайн -кнопок игры (join/leave/stop/hint/skip)."""
    query = update.callback_query
    await query.answer()
    chat_id = query.message.chat.id

    if chat_id not in active_games:
        await query.answer("❌ Игра уже завершена!", show_alert=True)
        return

    data = query.data
    user = query.from_user
    user_id = user.id
    user_name = f"{user.first_name} {(user.last_name or '')}".strip()

    if data == "admin_stop_game":
        # Проверяем права
        is_admin = await is_chat_admin(context.bot, chat_id, user_id)
        if not is_admin and user_id != active_games[chat_id]["started_by"]:
            await query.answer("❌ Только администратор может остановить игру!", show_alert=True)
            return

        if chat_id in active_games:
            # Очищаем состояние
            if chat_id in _last_guess_time:
                keys_to_remove = [k for k in _last_guess_time.keys() if k.startswith(f"{chat_id}_")]
                for key in keys_to_remove:
                    del _last_guess_time[key]
            
            if chat_id in _current_turn:
                del _current_turn[chat_id]
            
            if chat_id in _update_locks:
                del _update_locks[chat_id]
            
            del active_games[chat_id]
            try:
                await query.edit_message_text(
                    text="🛑 Игра остановлена администратором.",
                    reply_markup=None,
                )
            except Exception as e:
                print(f"Error editing stop-game message: {e}")
        return

    elif data == "hangman_join":
        if join_game(chat_id, user_id, user_name):
            await query.answer("🎮 Вы присоединились к игре!")
            await context.bot.send_message(chat_id=chat_id, text=f"🎮 {user_name} присоединился к игре!")
            
            # Если это первый игрок, назначаем его текущим
            if len(active_games[chat_id]["players"]) == 1:
                _current_turn[chat_id] = 0
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"🎮 Первым ходит: {user_name}",
                )
        else:
            await query.answer("❌ Вы уже в игре или достигнут лимит игроков!")

    elif data == "hangman_leave":
        if leave_game(chat_id, user_id):
            await query.answer("👋 Вы вышли из игры")
            
            # Если ушел текущий игрок, передаем ход следующему
            current_player = get_current_player(chat_id)
            if current_player and current_player[0] == user_id:
                next_player = next_turn(chat_id)
                if next_player:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"👋 {user_name} вышел из игры.\n🎮 Теперь ходит: {next_player[1]}",
                    )
            
            await context.bot.send_message(chat_id=chat_id, text=f"👋 {user_name} вышел из игры.")
        else:
            await query.answer("❌ Вы не в играх!")

    elif data == "hangman_hint":
        success = await give_hint(context, chat_id, user_id)
        if success:
            await query.answer("💡 Подсказка получена!")
        else:
            await query.answer("❌ Подсказка уже использована или нет доступных букв!", show_alert=True)

    elif data == "hangman_skip":
        success = await skip_turn(context, chat_id, user_id)
        if success:
            await query.answer("⏭️ Ход пропущен!")
        else:
            await query.answer("❌ Не удалось пропустить ход!", show_alert=True)

    await asyncio.sleep(0.3)
    await update_game_display(context, chat_id)

# ------------------ ОБРАБОТКА СООБЩЕНИЙ В ЧАТЕ ------------------
async def handle_chat_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка сообщений в общем чате для игры 'Виселица'."""
    message = update.effective_message
    chat = update.effective_chat
    user = update.effective_user
    
    if not chat or chat.type not in ("group", "supergroup"):
        return
    
    chat_id = chat.id
    
    # Проверяем, есть ли активная игра в этом чате
    if chat_id not in active_games:
        return
    
    text = (message.text or "").strip().upper()
    
    # Проверяем, что пользователь участвует в игре
    if user.id not in active_games[chat_id].get("players", {}):
        return
    
    # Проверяем, не выбыл ли игрок
    player_data = active_games[chat_id]["players"][user.id]
    if player_data.get("eliminated", False):
        return
    
    # Проверяем, чья очередь ходить
    current_player = get_current_player(chat_id)
    if not current_player or current_player[0] != user.id:
        return  # Не очередь этого игрока
    
    # Если сообщение - одна буква
    if len(text) == 1 and text.isalpha():
        await process_guess(context, chat_id, user.id, text)
    
    # Если сообщение - слово целиком (длина >= 2 букв)
    elif len(text) >= 2:
        # Проверяем, что это похоже на слово (только буквы)
        if all(c.isalpha() or c.isspace() for c in text):
            await process_word_guess(context, chat_id, user.id, text)

# ------------------ ОБРАБОТКА ОШИБОК ------------------
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик ошибок для предотвращения падения бота."""
    try:
        raise context.error
    except Exception as e:
        print(f"Exception while handling an update: {e}")
        print(f"Update: {update}")
        print(f"Context: {context}")
        
        # Логируем ошибку, но не падаем
        try:
            if update and update.effective_chat:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text="⚠️ Произошла ошибка при обработке команды. Пожалуйста, попробуйте еще раз."
                )
        except:
            pass

# ------------------ MAIN ------------------
def main():
    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN не задан в .env")

    # Загружаем статистику
    load_scores()
    print(f"🤖 Загружено {len(user_scores)} игроков в статистике")

    # Создаем приложение с обработчиком ошибок
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    
    # Добавляем обработчик ошибок
    app.add_error_handler(error_handler)

    # Команды
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("newgame", newgame_cmd))
    app.add_handler(CommandHandler("join", join_cmd))
    app.add_handler(CommandHandler("leave", leave_cmd))
    app.add_handler(CommandHandler("hint", hint_cmd))
    app.add_handler(CommandHandler("skip", skip_cmd))
    app.add_handler(CommandHandler("stop", stop_cmd))
    app.add_handler(CommandHandler("stats", stats_cmd))
    app.add_handler(CommandHandler("history", history_cmd))
    app.add_handler(CommandHandler("rules", rules_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("debug", debug_cmd))

    # Обработка сообщений для виселицы в общем чате
    app.add_handler(
        MessageHandler(
            filters.ChatType.GROUPS & filters.TEXT & ~filters.COMMAND,
            handle_chat_message,
        )
    )

    # Callback-обработчики
    app.add_handler(CallbackQueryHandler(handle_hangman_category_selection, pattern=r"^hangman_category_"))
    app.add_handler(CallbackQueryHandler(handle_hangman_buttons, pattern=r"^(hangman_join|hangman_leave|admin_stop_game|hangman_hint|hangman_skip)$"))

    print("🤖 Бот запущен! Ожидание сообщений...")
    print("📝 Используйте /debug для проверки прав")
    app.run_polling()

if __name__ == "__main__":
    main()
