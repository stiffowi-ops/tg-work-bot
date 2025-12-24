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

# Стадии виселицы для визуализации
hangman_stages = [
    """
    
       
       
       
       
       
    """,
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
        "ПЕТУХ", "ИНДЮК", "ВОРОБЕЙ", "СОРОКА", "ВОРОН", "СОВА", "ОРЁЛ",
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
        "МЕДСЕСТРА", "СТОМАТОЛОГ", "ПСИХОЛОГ", "АРХИТЕКТОР", "СТРОИТЕЛЬ",
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

# ------------------ ОЧИСТКА ТАЙМАУТОВ ------------------
def cleanup_timeouts(chat_id: int):
    """Очищает таймауты для завершенной игры"""
    keys_to_remove = [k for k in _last_guess_time.keys() if k.startswith(f"{chat_id}_")]
    for key in keys_to_remove:
        del _last_guess_time[key]

# ------------------ ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ------------------
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
        # Проверяем лимит игроков (макс 20)
        if len(game["players"]) >= 20:
            return False
        if user_id not in game["players"]:
            game["players"][user_id] = {
                "name": user_name,
                "correct_guesses": 0,
                "wrong_guesses": 0,
                "joined_at": time.time(),
                "active": True,
            }
            return True
    return False

def leave_game(chat_id: int, user_id: int) -> bool:
    """Игрок покидает игру."""
    if chat_id in active_games and user_id in active_games[chat_id]["players"]:
        del active_games[chat_id]["players"][user_id]
        return True
    return False

# ------------------ ОТОБРАЖЕНИЕ ИГРЫ ------------------
async def update_game_display(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    """Обновить основное сообщение с состоянием игры."""
    if chat_id not in active_games:
        return
    
    game = active_games[chat_id]
    
    # Если игра еще не начата (слово не выбрано)
    if not game["word"]:
        return
    
    word = game["word"]

    # Формируем отображение слова
    display_word = ""
    for letter in word:
        if letter in game["guessed_letters"] or not letter.isalpha():
            display_word += letter + " "
        else:
            display_word += "_ "

    # Формируем список ТОЛЬКО активных игроков
    active_players = {pid: data for pid, data in game["players"].items() if data.get("active", True)}

    players_text = ""
    if active_players:
        # Сортируем по количеству правильных ответов
        sorted_players = sorted(
            active_players.items(), key=lambda x: x[1]["correct_guesses"], reverse=True
        )

        for i, (player_id, player_data) in enumerate(sorted_players, 1):
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "👤"
            players_text += (
                f"{medal} {player_data['name']}: "
                f"✅{player_data['correct_guesses']} ❌{player_data['wrong_guesses']}\n"
            )
    else:
        players_text = "❌ Нет активных игроков\n💡 Используйте /join чтобы присоединиться"

    # Текущая стадия виселицы - ОЧЕНЬ ВАЖНО: считаем количество неправильных букв
    # Каждая неправильная буква = 1 попытка
    wrong_count = len(game["wrong_letters"])
    
    # Убедимся, что wrong_count не превышает доступные стадии
    if wrong_count >= len(hangman_stages):
        wrong_count = len(hangman_stages) - 1
    
    hangman_display = hangman_stages[wrong_count]

    # Получаем эмодзи для категории
    category_emoji = category_emojis.get(game['category'], '🎯')

    message_text = f"""
🎮 *ВИСЕЛИЦА* | {category_emoji} Категория: {game['category'].upper()}
👑 Запустил: {game['started_by_name']}

{hangman_display}

📖 Слово: `{display_word.strip()}`

❌ Неправильные буквы ({wrong_count}/6): {', '.join(sorted(game['wrong_letters'])) or 'пока нет'}

❤️ Осталось попыток: {game['attempts_left']}

👥 *Активные игроки ({len(active_players)}):*
{players_text}

💡 *Команды:*
/join - присоединиться к игре
/leave - выйти из игры
/hint - получить подсказку (1 за игру)
    """.strip()

    # Кнопки
    buttons = [
        [
            InlineKeyboardButton("🎮 Присоединиться", callback_data="hangman_join"),
            InlineKeyboardButton("👋 Выйти", callback_data="hangman_leave"),
        ],
        [
            InlineKeyboardButton("💡 Подсказка", callback_data="hangman_hint"),
        ]
    ]

    # Кнопку остановки показываем только админу, который запустил игру
    is_admin = await is_chat_admin(context.bot, chat_id, game["started_by"])
    if is_admin:
        buttons.append([InlineKeyboardButton("🛑 Остановить игру", callback_data="admin_stop_game")])

    markup = InlineKeyboardMarkup(buttons)

    try:
        if game.get("message_id"):
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=game["message_id"],
                text=message_text,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=markup,
            )
    except Exception as e:
        print(f"Error updating hangman display: {e}")

async def show_category_selection(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    """Показать инлайн-меню выбора категории для виселицы."""
    if chat_id not in active_games:
        return
    
    game = active_games[chat_id]
    admin_name = game["started_by_name"]

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
                "• Игроки пишут буквы в ЛС боту\n"
                "• У команды 6 попыток\n"
                "• Победит тот, кто угадает слово!\n"
                "• Можно получить 1 подсказку за игру\n\n"
                "🎯 *Выберите категорию слов:*"
            ),
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=markup,
        )
        active_games[chat_id]["message_id"] = msg.message_id
    except Exception as e:
        print(f"Ошибка отправки сообщения с категориями: {e}")

# ------------------ ЛОГИКА ИГРЫ ------------------
async def process_guess(
    context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int, guess: str
) -> None:
    """Обработка хода игрока."""
    if chat_id not in active_games:
        return

    game = active_games[chat_id]
    word = game["word"]

    # Если игрок не зарегистрирован в игре — игнорируем
    if user_id not in game["players"]:
        return

    player = game["players"][user_id]

    # Проверяем скорость хода (защита от флуда)
    user_key = f"{chat_id}_{user_id}"
    last_time = _last_guess_time.get(user_key)
    now_time = time.time()
    if last_time and now_time - last_time < 1:  # 1 секунда между ходами
        await context.bot.send_message(
            chat_id=user_id, 
            text="⏳ Подождите 1 секунду перед следующим ходом!"
        )
        return
    _last_guess_time[user_key] = now_time

    # Нормализуем букву (Ё -> Е)
    if guess == 'Ё':
        guess = 'Е'
    
    # Проверяем, не угадывали ли эту букву уже
    if guess in game["guessed_letters"]:
        await context.bot.send_message(
            chat_id=user_id, 
            text=f"❌ Буква '{guess}' уже была угадана!"
        )
        return
    
    if guess in game["wrong_letters"]:
        await context.bot.send_message(
            chat_id=user_id, 
            text=f"❌ Буква '{guess}' уже была ошибочной!"
        )
        return
    
    # ДЕБАГ: выводим текущее состояние
    print(f"DEBUG: Попытка буквы '{guess}' в слове '{word}'")
    print(f"DEBUG: Правильные буквы: {game['guessed_letters']}")
    print(f"DEBUG: Неправильные буквы: {game['wrong_letters']}")
    print(f"DEBUG: Осталось попыток до: {game['attempts_left']}")
    
    if guess in word:
        # Правильная буква
        game["guessed_letters"].add(guess)
        player["correct_guesses"] += 1

        # Формируем текущее состояние слова для ЛС
        display_word = ""
        for letter in word:
            if letter in game["guessed_letters"] or not letter.isalpha():
                display_word += letter + " "
            else:
                display_word += "_ "
        
        await context.bot.send_message(
            chat_id=user_id, 
            text=f"✅ Буква '{guess}' есть в слове!\n\n📖 Текущее слово: `{display_word.strip()}`"
        )

        # Проверяем, угадано ли слово полностью
        if all(letter in game["guessed_letters"] for letter in word if letter.isalpha()):
            await end_game_win(context, chat_id, user_id)
            return

    else:
        # Неправильная буква
        game["wrong_letters"].add(guess)
        game["attempts_left"] -= 1
        player["wrong_guesses"] += 1
        
        # Формируем текущее состояние слова для ЛС
        display_word = ""
        for letter in word:
            if letter in game["guessed_letters"] or not letter.isalpha():
                display_word += letter + " "
            else:
                display_word += "_ "

        await context.bot.send_message(
            chat_id=user_id,
            text=(
                f"❌ Буквы '{guess}' нет в слове.\n"
                f"❤️ Осталось попыток: {game['attempts_left']}\n\n"
                f"📖 Текущее слово: `{display_word.strip()}`"
            ),
        )

        # Проверяем поражение
        if game["attempts_left"] <= 0:
            await end_game_lose(context, chat_id)
            return

    # Обновляем отображение игры
    await update_game_display(context, chat_id)

async def give_hint(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int) -> bool:
    """Дать подсказку игроку (открыть одну букву)."""
    if chat_id not in active_games:
        return False
    
    game = active_games[chat_id]
    word = game["word"]
    
    # Проверяем, что игрок еще не использовал подсказку
    if game.get("hint_used"):
        return False
    
    # Находим неотгаданные буквы
    unguessed = [letter for letter in word if letter.isalpha() and letter not in game["guessed_letters"]]
    if not unguessed:
        return False
    
    # Выбираем случайную букву для подсказки
    hint_letter = random.choice(unguessed)
    game["guessed_letters"].add(hint_letter)
    game["hint_used"] = True
    
    # Даем бонус игроку, который запросил подсказку
    if user_id in game["players"]:
        game["players"][user_id]["correct_guesses"] += 1
    
    # Формируем текущее состояние слова
    display_word = ""
    for letter in word:
        if letter in game["guessed_letters"] or not letter.isalpha():
            display_word += letter + " "
        else:
            display_word += "_ "
    
    await context.bot.send_message(
        chat_id=user_id,
        text=f"💡 Подсказка: в слове есть буква '{hint_letter}'!\n\n📖 Текущее слово: `{display_word.strip()}`"
    )
    
    # Проверяем, не выиграли ли мы после подсказки
    if all(letter in game["guessed_letters"] for letter in word if letter.isalpha()):
        await end_game_win(context, chat_id, user_id)
        return True
    
    await update_game_display(context, chat_id)
    return True

async def end_game_win(context: ContextTypes.DEFAULT_TYPE, chat_id: int, winner_id: int) -> None:
    """Завершение игры победой."""
    if chat_id not in active_games:
        return
    
    game = active_games[chat_id]
    word = game["word"]
    winner_name = game["players"].get(winner_id, {}).get("name", "Игрок")
    
    # Сохраняем данные игры для истории
    game_data = {
        "chat_id": chat_id,
        "word": word,
        "category": game["category"],
        "winner_id": winner_id,
        "winner_name": winner_name,
        "players_count": len(game["players"]),
        "timestamp": datetime.now().isoformat(),
        "result": "win"
    }
    
    # Обновляем счет ТОЛЬКО для активных игроков
    active_players = {pid: data for pid, data in game["players"].items() if data.get("active", True)}

    for player_id in active_players:
        user_scores[player_id] = user_scores.get(player_id, 0) + 1  # Все активные игроки получают очко

    # Бонус победителю
    user_scores[winner_id] = user_scores.get(winner_id, 0) + 2  # +2 очка за победу
    
    # Сохраняем статистику
    save_scores()
    save_game_history(game_data)

    # Формируем итоговую таблицу ТОЛЬКО активных игроков
    players_sorted = sorted(
        active_players.items(), key=lambda x: x[1]["correct_guesses"], reverse=True
    )

    leaderboard = "🏆 *Результаты:*\n"
    for i, (player_id, player_data) in enumerate(players_sorted, 1):
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "👤"
        leaderboard += (
            f"{medal} {player_data['name']}: "
            f"✅{player_data['correct_guesses']} ❌{player_data['wrong_guesses']}\n"
        )

    message_text = f"""
🎉 *ПОБЕДА!*

👑 Победитель: *{winner_name}*

📖 Загаданное слово: *{word}*

{leaderboard}

🎯 Для новой игры используйте /newgame
    """.strip()

    try:
        if game.get("message_id"):
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=game["message_id"],
                text=message_text,
                parse_mode=ParseMode.MARKDOWN,
            )
    except Exception as e:
        print(f"Error editing message on win: {e}")

    # Очищаем таймауты и удаляем игру
    cleanup_timeouts(chat_id)
    del active_games[chat_id]

async def end_game_lose(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    """Завершение игры поражением (закончились попытки)."""
    if chat_id not in active_games:
        return
    
    game = active_games[chat_id]
    word = game["word"]
    
    # Сохраняем данные игры для истории
    game_data = {
        "chat_id": chat_id,
        "word": word,
        "category": game["category"],
        "players_count": len(game["players"]),
        "timestamp": datetime.now().isoformat(),
        "result": "lose"
    }
    save_game_history(game_data)

    # Формируем итоговую таблицу ТОЛЬКО активных игроков
    active_players = {pid: data for pid, data in game["players"].items() if data.get("active", True)}
    players_sorted = sorted(
        active_players.items(), key=lambda x: x[1]["correct_guesses"], reverse=True
    )

    leaderboard = "📊 *Результаты:*\n"
    for i, (player_id, player_data) in enumerate(players_sorted, 1):
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "👤"
        leaderboard += (
            f"{medal} {player_data['name']}: "
            f"✅{player_data['correct_guesses']} ❌{player_data['wrong_guesses']}\n"
        )

    message_text = f"""
💀 *ИГРА ОКОНЧЕНА*

📖 Загаданное слово было: *{word}*

{leaderboard}

🎯 Для новой игры используйте /newgame
    """.strip()

    try:
        if game.get("message_id"):
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=game["message_id"],
                text=message_text,
                parse_mode=ParseMode.MARKDOWN,
            )
    except Exception as e:
        print(f"Error editing message on lose: {e}")

    # Очищаем таймауты и удаляем игру
    cleanup_timeouts(chat_id)
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
5. Пишут буквы мне в личные сообщения

📚 *Команды:*
/newgame - начать новую игру (админы)
/join - присоединиться к игре
/leave - выйти из игры
/hint - получить подсказку (1 за игру)
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
        "attempts_left": 6,
        "category": "",
        "players": {},
        "message_id": None,
        "started_by": user.id,
        "started_by_name": started_by_name,
        "start_time": time.time(),
        "hint_used": False,
    }

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
        await update_game_display(context, chat_id)
    else:
        if len(active_games[chat_id]["players"]) >= 20:
            await message.reply_text(
                "❌ В игре уже максимальное количество игроков (20)!",
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
            "💡 Вы получили подсказку! Проверьте ЛС с ботом.",
            reply_to_message_id=message.message_id,
        )
    else:
        await message.reply_text(
            "❌ Подсказка уже была использована или нет доступных букв!",
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

    del active_games[chat_id]
    cleanup_timeouts(chat_id)
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

📖 *Цель игры:* угадать загаданное слово по буквам

👥 *Как играть:*
1. Администратор запускает игру командой /newgame
2. Игроки присоединяются командой /join
3. Бот загадывает слово из выбранной категории
4. Игроки пишут буквы боту в ЛИЧНЫЕ СООБЩЕНИЯ
5. Бот показывает прогресс в общем чате

⚡ *Особенности:*
• У команды 6 попыток на ошибки
• Все видят прогресс в реальном времени
• Побеждает игрок, угадавший последнюю букву
• Можно играть одновременно всем составом!
• Можно получить 1 подсказку за игру (/hint)

🎯 *Команды:*
/newgame - начать игру (только админы)
/join - присоединиться к игре
/leave - выйти из игры
/hint - получить подсказку
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
4. Пишут буквы боту в ЛС
5. Угадывают слово!

✨ Удачи в игре! 🎯
    """.strip()
    
    await update.effective_message.reply_text(help_text, parse_mode=ParseMode.MARKDOWN)

async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает текущее состояние игры в ЛС."""
    user = update.effective_user
    user_id = user.id
    
    # Ищем активную игру для этого пользователя
    active_chat_id = None
    for chat_id, game in active_games.items():
        if user_id in game.get("players", {}):
            active_chat_id = chat_id
            break
    
    if active_chat_id is None:
        await update.effective_message.reply_text(
            "🤔 У вас нет активных игр. "
            "Присоединитесь к игре в групповом чате командой /join!"
        )
        return
    
    game = active_games[active_chat_id]
    player = game["players"].get(user_id)
    
    if not player:
        await update.effective_message.reply_text("❌ Вы не найдены в игре!")
        return
    
    # Формируем отображение слова
    display_word = ""
    if game["word"]:
        for letter in game["word"]:
            if letter in game["guessed_letters"] or not letter.isalpha():
                display_word += letter + " "
            else:
                display_word += "_ "
    
    status_text = f"""
📊 *Ваш статус в игре:*

📖 Категория: {game['category'].upper()}
📖 Слово: `{display_word.strip() if game['word'] else 'Загадывается...'}`
📏 Длина слова: {len(game['word']) if game['word'] else '?'} букв

✅ Ваши правильные буквы: {player['correct_guesses']}
❌ Ваши ошибки: {player['wrong_guesses']}

❌ Неправильные буквы команды: {', '.join(sorted(game['wrong_letters'])) or 'пока нет'}
❤️ Осталось попыток: {game['attempts_left']}

💡 *Совет:* 
• Пишите буквы боту в ЛС
• Можно использовать /hint для подсказки
• Следите за прогрессом в групповом чате
    """.strip()
    
    await update.effective_message.reply_text(status_text, parse_mode=ParseMode.MARKDOWN)

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
                "2. Напишите боту в ЛС\n"
                "3. Отправляйте по одной букве\n"
                "4. Следите за прогрессом в чате\n\n"
                f"👑 Игру запустил: {game['started_by_name']}"
            ),
            parse_mode=ParseMode.MARKDOWN,
        )
    except Exception as e:
        print(f"Error editing category selection message: {e}")

    # Показываем текущее состояние игры
    await update_game_display(context, chat_id)

async def handle_hangman_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка инлайн-кнопок игры (join/leave/stop/hint)."""
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
            del active_games[chat_id]
            cleanup_timeouts(chat_id)
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
        else:
            await query.answer("❌ Вы уже в игре или достигнут лимит игроков!")

    elif data == "hangman_leave":
        if leave_game(chat_id, user_id):
            await query.answer("👋 Вы вышли из игры")
            await context.bot.send_message(chat_id=chat_id, text=f"👋 {user_name} вышел из игры.")
        else:
            await query.answer("❌ Вы не в играх!")

    elif data == "hangman_hint":
        success = await give_hint(context, chat_id, user_id)
        if success:
            await query.answer("💡 Подсказка отправлена в ЛС!")
        else:
            await query.answer("❌ Подсказка уже использована или нет доступных букв!", show_alert=True)

    await update_game_display(context, chat_id)

# ------------------ ОБРАБОТКА ЛС С БУКВАМИ ------------------
async def handle_private_guess(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка букв, присланных в ЛС боту для игры 'Виселица'."""
    message = update.effective_message
    user = update.effective_user
    user_id = user.id
    guess = (message.text or "").strip().upper()

    # Проверяем, что это буква
    if len(guess) != 1 or not guess.isalpha():
        await context.bot.send_message(chat_id=user_id, text="❌ Пожалуйста, отправьте ОДНУ букву!")
        return

    # Ищем активную игру для этого пользователя
    active_chat_id = None
    for chat_id, game in active_games.items():
        if user_id in game.get("players", {}):
            active_chat_id = chat_id
            break

    # Если пользователь не в игре, но есть активная игра - присоединяем автоматически
    if active_chat_id is None and active_games:
        active_chat_id = list(active_games.keys())[0]
        user_name = f"{user.first_name} {(user.last_name or '')}".strip()

        if join_game(active_chat_id, user_id, user_name):
            # Формируем текущее состояние слова для приветствия
            game = active_games[active_chat_id]
            display_word = ""
            if game["word"]:
                for letter in game["word"]:
                    if letter in game["guessed_letters"] or not letter.isalpha():
                        display_word += letter + " "
                    else:
                        display_word += "_ "
            
            welcome_text = (
                "🎮 Вы автоматически присоединились к игре!\n\n"
                f"📖 Категория: {game['category'].upper() if game['category'] else 'Выбирается...'}\n"
                f"📖 Слово: `{display_word.strip() if game['word'] else 'Загадывается...'}`\n"
                f"❤️ Осталось попыток: {game['attempts_left']}\n\n"
                "💡 Теперь можете отправлять буквы."
            )
            
            await context.bot.send_message(
                chat_id=user_id,
                text=welcome_text,
            )
            # Уведомляем в группе
            await context.bot.send_message(
                chat_id=active_chat_id,
                text=f"🎮 {user_name} присоединился к игре (первый ход в ЛС)!",
            )
            await update_game_display(context, active_chat_id)
        else:
            await context.bot.send_message(
                chat_id=user_id,
                text="❌ Не удалось присоединиться к игре. Возможно, достигнут лимит игроков.",
            )
            return

    if active_chat_id is None:
        await context.bot.send_message(
            chat_id=user_id,
            text=(
                "🤔 У вас нет активных игр. "
                "Присоединитесь к игре в групповом чате командой /join!"
            ),
        )
        return

    game = active_games.get(active_chat_id)
    if not game:
        await context.bot.send_message(chat_id=user_id, text="❌ Игра уже завершена.")
        return

    await process_guess(context, active_chat_id, user_id, guess)

# ------------------ MAIN ------------------
def main():
    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN не задан в .env")

    # Загружаем статистику
    load_scores()
    print(f"🤖 Загружено {len(user_scores)} игроков в статистике")

    # Создаем приложение
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # Команды
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("newgame", newgame_cmd))
    app.add_handler(CommandHandler("join", join_cmd))
    app.add_handler(CommandHandler("leave", leave_cmd))
    app.add_handler(CommandHandler("hint", hint_cmd))
    app.add_handler(CommandHandler("stop", stop_cmd))
    app.add_handler(CommandHandler("stats", stats_cmd))
    app.add_handler(CommandHandler("history", history_cmd))
    app.add_handler(CommandHandler("rules", rules_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("debug", debug_cmd))

    # Обработка букв для виселицы в ЛС
    app.add_handler(
        MessageHandler(
            filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND,
            handle_private_guess,
        )
    )

    # Callback-обработчики
    app.add_handler(CallbackQueryHandler(handle_hangman_category_selection, pattern=r"^hangman_category_"))
    app.add_handler(CallbackQueryHandler(handle_hangman_buttons, pattern=r"^(hangman_join|hangman_leave|admin_stop_game|hangman_hint)$"))

    print("🤖 Бот запущен! Ожидание сообщений...")
    print("📝 Используйте /debug для проверки прав")
    app.run_polling()

if __name__ == "__main__":
    main()
