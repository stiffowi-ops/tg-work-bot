import os
import random
import logging
import requests
import html
import json
from pathlib import Path
from datetime import datetime, time, timedelta
from dateutil import tz
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode, ChatMemberStatus
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ChatMemberHandler,
    ContextTypes,
    filters,
    CallbackQueryHandler,
)
import holidays

# ------------------ НАСТРОЙКИ ------------------

load_dotenv(dotenv_path=Path(__file__).with_name(".env"), override=True)

BOT_TOKEN = os.getenv("BOT_TOKEN")
DAILY_FACT_TIME_STR = os.getenv("DAILY_FACT_TIME", "09:10")  # HH:MM (по Москве) для викторины
STANDUP_REMINDER_TIME_STR = os.getenv("STANDUP_REMINDER_TIME", "09:00")  # HH:MM (по Москве) для напоминания
STANDUP_MEETING_TIME_STR = os.getenv("STANDUP_MEETING_TIME", "09:30")  # Текстовое время самой планёрки
MOVIE_RECOMMEND_TIME_STR = os.getenv("MOVIE_RECOMMEND_TIME", "18:00")  # Время рекомендации фильма (пятница)
WEEKLY_SUMMARY_TIME_STR = os.getenv("WEEKLY_SUMMARY_TIME", "17:00")  # Итоги викторины за неделю (пятница)

TZ_MSK = tz.gettz("Europe/Moscow")

# Список отслеживаемых юзернеймов, задаётся в .env:
# WATCHED_USERNAMES=@user1,@user2
WATCHED_USERNAMES_RAW = os.getenv("WATCHED_USERNAMES", "")
WATCHED_USERNAMES = {
    u.lstrip("@").lower()
    for u in WATCHED_USERNAMES_RAW.replace(";", ",").split(",")
    if u.strip()
}

KINOPOISK_API_KEY = os.getenv("KINOPOISK_API_KEY")

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("workbot")

_last_holiday_congrats_date = None

# Текущее состояние викторины по чатам
_current_quiz: dict[int, dict] = {}

# Чаты, для которых уже повесили задания
_scheduled_chats: set[int] = set()

# Отслеживаемые участники по чатам: chat_id -> { user_id: mention_html }
_tracked_participants: dict[int, dict[int, str]] = {}

# Праздники РФ
RU_HOLIDAYS = holidays.Russia()

# Рейтинг викторины по неделям:
# структура: { "<chat_id>": { "<year-week>": { "<user_id>": score_int } } }
_weekly_scores: dict[str, dict[str, dict[str, int]]] = {}
SCORES_FILE = Path(__file__).with_name("quiz_scores.json")

# ------------------ ВИСЕЛИЦА: СОСТОЯНИЕ И СЛОВА ------------------

# Состояние игр по чатам
active_games: dict[int, dict] = {}
# Глобальная статистика победителей по виселице
user_scores: dict[int, int] = {}

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

# ------------------ ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ------------------

def parse_hhmm(hhmm: str) -> time:
    h, m = hhmm.split(":")
    return time(hour=int(h), minute=int(m), tzinfo=TZ_MSK)

def now_msk() -> datetime:
    return datetime.now(tz=TZ_MSK)

def get_current_week_id(dt: datetime) -> str:
    """Возвращает идентификатор недели в формате 'YYYY-Www', например '2025-W03'."""
    iso_year, iso_week, _ = dt.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"

def load_weekly_scores() -> None:
    """Загружаем рейтинг из файла, если он есть."""
    global _weekly_scores
    if not SCORES_FILE.exists():
        _weekly_scores = {}
        return
    try:
        with SCORES_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                _weekly_scores = data
            else:
                _weekly_scores = {}
    except Exception as e:
        logger.warning(f"Failed to load weekly scores: {e}")
        _weekly_scores = {}

def save_weekly_scores() -> None:
    """Сохраняем рейтинг в файл."""
    try:
        with SCORES_FILE.open("w", encoding="utf-8") as f:
            json.dump(_weekly_scores, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"Failed to save weekly scores: {e}")

def update_weekly_scores(chat_id: int, participants_ids: set[int], answers: list[dict]) -> None:
    """Обновляет рейтинг за текущую неделю."""
    if not participants_ids:
        return
    now = now_msk()
    week_id = get_current_week_id(now)
    chat_key = str(chat_id)
    chat_weeks = _weekly_scores.setdefault(chat_key, {})
    week_scores = chat_weeks.setdefault(week_id, {})

    # Для быстрого доступа по user_id
    answers_by_uid: dict[int, dict] = {a["uid"]: a for a in answers}

    for uid in participants_ids:
        uid_key = str(uid)
        current_score = week_scores.get(uid_key, 0)
        ans = answers_by_uid.get(uid)
        if ans:
            if ans.get("ok"):  # правильный ответ: +1
                current_score += 1
            else:  # неправильный: 0
                pass
        else:  # не ответил вообще: -1
            current_score -= 1
        week_scores[uid_key] = current_score

    save_weekly_scores()

def get_on_this_day_fact(dt: datetime) -> tuple[str | None, str | None]:
    """Возвращает (текст факта без года, год события) с Wikipedia OnThisDay."""
    url = f"https://ru.wikipedia.org/api/rest_v1/feed/onthisday/events/{dt.month}/{dt.day}"
    headers = {"User-Agent": "tg-work-bot/1.0"}
    try:
        r = requests.get(url, timeout=10, headers=headers)
        r.raise_for_status()
        events = r.json().get("events", [])
        if not events:
            return None, None
        event = random.choice(events)
        year = event.get("year")
        text = event.get("text") or ""

        # На всякий случай убираем html-теги из текста
        for tag in ("<b>", "</b>", "<i>", "</i>", "<br>", "</br>"):
            text = text.replace(tag, "")
        text_without_year = text.replace(str(year), "***").replace(f"в {year}", "в ***")
        return text_without_year, year
    except Exception as e:
        logger.warning(f"Wikipedia fact fetch error: {e}")
        return None, None

def generate_year_options(correct_year: str) -> list[str]:
    """Генерируем 4 варианта ответа: правильный год + 3 рядом."""
    correct_year_int = int(correct_year)
    options = [correct_year_int]
    while len(options) < 4:
        year_var = correct_year_int + random.randint(-50, 50)
        if year_var != correct_year_int and year_var not in options:
            options.append(year_var)
    random.shuffle(options)
    return [str(year) for year in options]

def get_ru_holiday_name(dt: datetime) -> str | None:
    """Название праздника РФ для указанной даты, если есть."""
    try:
        name = RU_HOLIDAYS.get(dt.date())
        if not name:
            return None
        return name if isinstance(name, str) else ", ".join(name)
    except Exception as e:
        logger.warning(f"Holidays check error: {e}")
        return None

# ------------------ УТИЛИТЫ ДОСТУПА И ОТСЛЕЖИВАНИЯ ------------------

async def is_user_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Проверяет, является ли пользователь админом/владельцем чата."""
    chat = update.effective_chat
    user = update.effective_user
    if not chat or not user:
        return False
    try:
        member = await context.bot.get_chat_member(chat.id, user.id)
    except Exception as e:
        logger.warning(f"Failed to get chat member ({chat.id}, {user.id}): {e}")
        return False
    return member.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER)

async def is_chat_admin(bot, chat_id: int, user_id: int) -> bool:
    """Проверка прав администратора по chat_id и user_id (для callback-кнопок)."""
    try:
        member = await bot.get_chat_member(chat_id, user_id)
        return member.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER)
    except Exception as e:
        logger.warning(f"Failed to get chat member ({chat_id}, {user_id}): {e}")
        return False

def remember_tracked_user(chat_id: int, user) -> None:
    """Запоминаем пользователя как 'отслеживаемого'."""
    if not user or user.is_bot:
        return
    username = (user.username or "").lower()
    if not username or username not in WATCHED_USERNAMES:
        return
    chat_users = _tracked_participants.setdefault(chat_id, {})
    chat_users[user.id] = user.mention_html()

# ------------------ ВИСЕЛИЦА: ЛОГИКА ------------------

def join_game(chat_id: int, user_id: int, user_name: str) -> bool:
    """Игрок присоединяется к активной игре."""
    if chat_id in active_games:
        game = active_games[chat_id]
        if user_id not in game["players"]:
            game["players"][user_id] = {
                "name": user_name,
                "correct_guesses": 0,
                "wrong_guesses": 0,
                "joined_at": now_msk().timestamp(),
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

async def show_category_selection(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    """Показать инлайн-меню выбора категории для виселицы."""
    game = active_games[chat_id]
    admin_name = game["started_by_name"]

    buttons = []
    for category in russian_word_categories.keys():
        buttons.append(
            [InlineKeyboardButton(f"🎯 {category.capitalize()}", callback_data=f"hangman_category_{category}")]
        )

    # Добавляем случайную категорию
    buttons.append(
        [InlineKeyboardButton("🎲 Случайная категория", callback_data="hangman_category_random")]
    )

    markup = InlineKeyboardMarkup(buttons)

    msg = await context.bot.send_message(
        chat_id,
        text=(
            f"👑 *Администратор {admin_name} запускает игру 'Виселица'!*

"
            "📖 *Правила:*
"
            "• Бот загадывает слово
"
            "• Игроки пишут буквы в ЛС боту
"
            "• У команды 6 попыток
"
            "• Победит тот, кто угадает слово!

"
            "🎯 *Выберите категорию слов:*"
        ),
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=markup,
    )

    active_games[chat_id]["message_id"] = msg.message_id

async def update_game_display(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    """Обновить основное сообщение с состоянием игры."""
    if chat_id not in active_games:
        return
    game = active_games[chat_id]
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
                f"✅{player_data['correct_guesses']} ❌{player_data['wrong_guesses']}
"
            )
    else:
        players_text = "❌ Нет активных игроков
💡 Используйте /hangman_join чтобы присоединиться"

    # Текущая стадия виселицы
    stage_index = 6 - game["attempts_left"]
    if stage_index < 0:
        stage_index = 0
    if stage_index >= len(hangman_stages):
        stage_index = len(hangman_stages) - 1
    hangman_display = hangman_stages[stage_index]

    message_text = f"""
🎮 *ВИСЕЛИЦА* | Категория: {game['category'].upper()}
👑 Запустил: {game['started_by_name']}

{hangman_display}

📖 Слово: `{display_word.strip()}`

❌ Неправильные буквы: {', '.join(sorted(game['wrong_letters'])) or 'пока нет'}

❤️ Осталось попыток: {game['attempts_left']}

👥 *Активные игроки ({len(active_players)}):*
{players_text}

💡 *Команды:*
/hangman_join - присоединиться к игре
/hangman_leave - выйти из игры
    """.strip()

    # Кнопки
    buttons = [
        [
            InlineKeyboardButton("🎮 Присоединиться", callback_data="hangman_join"),
            InlineKeyboardButton("👋 Выйти", callback_data="hangman_leave"),
        ]
    ]

    # Кнопку остановки показываем только админу, который запустил игру
    if await is_chat_admin(context.bot, chat_id, game["started_by"]):
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
        logger.warning(f"Error updating hangman display for chat {chat_id}: {e}")

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

    if guess in word:
        # Правильная буква
        game["guessed_letters"].add(guess)
        player["correct_guesses"] += 1

        await context.bot.send_message(chat_id=user_id, text=f"✅ Буква '{guess}' есть в слове!")

        # Проверяем, угадано ли слово полностью
        if all(letter in game["guessed_letters"] for letter in word if letter.isalpha()):
            await end_game_win(context, chat_id, user_id)
            return

    else:
        # Неправильная буква
        game["wrong_letters"].add(guess)
        game["attempts_left"] -= 1
        player["wrong_guesses"] += 1

        await context.bot.send_message(
            chat_id=user_id,
            text=f"❌ Буквы '{guess}' нет в слове. Осталось попыток: {game['attempts_left']}",
        )

        # Проверяем поражение
        if game["attempts_left"] <= 0:
            await end_game_lose(context, chat_id)
            return

    # Обновляем отображение игры
    await update_game_display(context, chat_id)

async def end_game_win(context: ContextTypes.DEFAULT_TYPE, chat_id: int, winner_id: int) -> None:
    """Завершение игры победой."""
    if chat_id not in active_games:
        return
    game = active_games[chat_id]
    word = game["word"]
    winner_name = game["players"].get(winner_id, {}).get("name", "Игрок")

    # Обновляем счет ТОЛЬКО для активных игроков
    active_players = {pid: data for pid, data in game["players"].items() if data.get("active", True)}

    for player_id in active_players:
        user_scores[player_id] = user_scores.get(player_id, 0) + 1  # Все активные игроки получают очко

    # Бонус победителю
    user_scores[winner_id] = user_scores.get(winner_id, 0) + 1

    # Формируем итоговую таблицу ТОЛЬКО активных игроков
    players_sorted = sorted(
        active_players.items(), key=lambda x: x[1]["correct_guesses"], reverse=True
    )

    leaderboard = "🏆 *Результаты:*
"
    for i, (player_id, player_data) in enumerate(players_sorted, 1):
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "👤"
        leaderboard += (
            f"{medal} {player_data['name']}: "
            f"✅{player_data['correct_guesses']} ❌{player_data['wrong_guesses']}
"
        )

    message_text = f"""
🎉 *ПОБЕДА!*

👑 Победитель: *{winner_name}*

📖 Загаданное слово: *{word}*

{leaderboard}

🎯 Для новой игры используйте /hangman_start
    """.strip()

    try:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=game["message_id"],
            text=message_text,
            parse_mode=ParseMode.MARKDOWN,
        )
    except Exception as e:
        logger.warning(f"Error editing message on win for chat {chat_id}: {e}")

    del active_games[chat_id]

async def end_game_lose(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    """Завершение игры поражением (закончились попытки)."""
    if chat_id not in active_games:
        return
    game = active_games[chat_id]
    word = game["word"]

    # Формируем итоговую таблицу ТОЛЬКО активных игроков
    active_players = {pid: data for pid, data in game["players"].items() if data.get("active", True)}
    players_sorted = sorted(
        active_players.items(), key=lambda x: x[1]["correct_guesses"], reverse=True
    )

    leaderboard = "📊 *Результаты:*
"
    for i, (player_id, player_data) in enumerate(players_sorted, 1):
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "👤"
        leaderboard += (
            f"{medal} {player_data['name']}: "
            f"✅{player_data['correct_guesses']} ❌{player_data['wrong_guesses']}
"
        )

    message_text = f"""
💀 *ИГРА ОКОНЧЕНА*

📖 Загаданное слово было: *{word}*

{leaderboard}

🎯 Для новой игры используйте /hangman_start
    """.strip()

    try:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=game["message_id"],
            text=message_text,
            parse_mode=ParseMode.MARKDOWN,
        )
    except Exception as e:
        logger.warning(f"Error editing message on lose for chat {chat_id}: {e}")

    del active_games[chat_id]

# ------------------ ВИСЕЛИЦА: ХЕНДЛЕРЫ ------------------

async def hangman_start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Запуск игры 'Виселица' (только для групп и только админы)."""
    chat = update.effective_chat
    message = update.effective_message
    user = update.effective_user

    if not chat or chat.type not in ("group", "supergroup"):
        await message.reply_text("❌ Эта игра только для групповых чатов!")
        return

    if not await is_user_admin(update, context):
        await message.reply_text(
            "❌ Только администраторы могут запускать игру!
"
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
        "start_time": now_msk().timestamp(),
    }

    await show_category_selection(context, chat_id)

async def handle_hangman_category_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка выбора категории виселицы через inline-кнопки."""
    query = update.callback_query
    chat_id = query.message.chat.id

    if chat_id not in active_games:
        await query.answer("❌ Игра уже завершена.", show_alert=True)
        return

    # Проверяем, что callback от администратора
    if not await is_chat_admin(context.bot, chat_id, query.from_user.id):
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

    # Обновляем сообщение
    try:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=query.message.message_id,
            text=(
                f"🎮 *Категория выбрана: {category.upper()}*

"
                "📖 Слово загадано! Игроки, пишите буквы мне в личные сообщения!

"
                "💡 *Как играть:*
"
                "1. Напишите боту в ЛС
"
                "2. Отправьте одну букву
"
                "3. Следите за прогрессом в чате

"
                f"👑 Игру запустил: {game['started_by_name']}"
            ),
            parse_mode=ParseMode.MARKDOWN,
        )
    except Exception as e:
        logger.warning(f"Error editing category selection message: {e}")

    # Показываем текущее состояние игры
    await update_game_display(context, chat_id)

async def hangman_join_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для присоединения к игре."""
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

    if join_game(chat_id, user_id, user_name):
        await message.reply_text(
            f"🎮 {user_name} присоединился к игре!",
            reply_to_message_id=message.message_id,
        )
        await update_game_display(context, chat_id)
    else:
        await message.reply_text(
            f"❌ {user_name}, вы уже в игре!",
            reply_to_message_id=message.message_id,
        )

async def hangman_leave_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

async def handle_private_guess(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка букв, присланных в ЛС боту для игры 'Виселица'."""
    message = update.effective_message
    user = update.effective_user
    user_id = user.id
    guess = (message.text or "").strip().upper()

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
            await context.bot.send_message(
                chat_id=user_id,
                text=(
                    "🎮 Вы автоматически присоединились к игре!

"
                    "💡 Теперь можете отправлять буквы."
                ),
            )
            # Уведомляем в группе
            await context.bot.send_message(
                chat_id=active_chat_id,
                text=f"🎮 {user_name} присоединился к игре (первый ход в ЛС)!",
            )
            await update_game_display(context, active_chat_id)

    if active_chat_id is None:
        await context.bot.send_message(
            chat_id=user_id,
            text=(
                "🤔 У вас нет активных игр. "
                "Присоединитесь к игре в групповом чате командой /hangman_join!"
            ),
        )
        return

    # Проверяем ввод
    if len(guess) != 1 or not guess.isalpha():
        await context.bot.send_message(chat_id=user_id, text="❌ Пожалуйста, отправьте ОДНУ букву!")
        return

    game = active_games.get(active_chat_id)
    if not game:
        await context.bot.send_message(chat_id=user_id, text="❌ Игра уже завершена.")
        return

    # Проверяем, не угадывали ли эту букву уже
    if guess in game["guessed_letters"] or guess in game["wrong_letters"]:
        await context.bot.send_message(chat_id=user_id, text="❌ Эта буква уже была!")
        return

    await process_guess(context, active_chat_id, user_id, guess)

async def handle_hangman_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка инлайн-кнопок игры (join/leave/stop)."""
    query = update.callback_query
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
        if not await is_chat_admin(context.bot, chat_id, user_id):
            await query.answer("❌ Только администратор может остановить игру!", show_alert=True)
            return

        if chat_id in active_games:
            del active_games[chat_id]
            try:
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=query.message.message_id,
                    text="🛑 Игра остановлена администратором.",
                    reply_markup=None,
                )
            except Exception as e:
                logger.warning(f"Error editing stop-game message: {e}")
        return

    if data == "hangman_join":
        if join_game(chat_id, user_id, user_name):
            await query.answer("🎮 Вы присоединились к игре!")
            await context.bot.send_message(chat_id=chat_id, text=f"🎮 {user_name} присоединился к игре!")
        else:
            await query.answer("❌ Вы уже в игре!")

    elif data == "hangman_leave":
        if leave_game(chat_id, user_id):
            await query.answer("👋 Вы вышли из игры")
            await context.bot.send_message(chat_id=chat_id, text=f"👋 {user_name} вышел из игры.")
        else:
            await query.answer("❌ Вы не в игре!")

    await update_game_display(context, chat_id)

async def hangman_stop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
    await message.reply_text(f"🛑 Игра остановлена администратором {update.effective_user.first_name}.")

async def hangman_stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает статистику игроков по виселице."""
    message = update.effective_message
    if not user_scores:
        await message.reply_text("📊 Статистика пока пуста.")
        return

    # Топ игроков
    top_players = sorted(user_scores.items(), key=lambda x: x[1], reverse=True)[:10]

    stats_text = "🏆 *Топ игроков виселицы:*

"
    for i, (player_id, score) in enumerate(top_players, 1):
        try:
            member = await context.bot.get_chat_member(update.effective_chat.id, player_id)
            name = member.user.first_name
            stats_text += f"{i}. {name}: {score} побед
"
        except Exception:
            stats_text += f"{i}. Игрок {player_id}: {score} побед
"

    await message.reply_text(stats_text, parse_mode=ParseMode.MARKDOWN)

async def hangman_rules_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает правила игры 'Виселица'."""
    rules_text = """
🎮 *Правила игры "Виселица":*

📖 *Цель игры:* угадать загаданное слово по буквам

👥 *Как играть:*
1. Администратор запускает игру командой /hangman_start
2. Игроки присоединяются командой /hangman_join
3. Бот загадывает слово из выбранной категории
4. Игроки пишут буквы боту в ЛИЧНЫЕ СООБЩЕНИЯ
5. Бот показывает прогресс в общем чате

⚡ *Особенности:*
• У команды 6 попыток на ошибки
• Все видят прогресс в реальном времени
• Побеждает игрок, угадавший последнюю букву
• Можно играть одновременно всем составом!

🎯 *Команды:*
/hangman_start - начать игру (только админы)
/hangman_join - присоединиться к игре
/hangman_leave - выйти из игры
/hangman_stop - остановить игру (только админы)
/hangman_stats - статистика игроков
/hangman_rules - правила игры
    """.strip()

    await update.effective_message.reply_text(rules_text, parse_mode=ParseMode.MARKDOWN)

async def hangman_admins_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает список администраторов чата."""
    chat = update.effective_chat
    chat_id = chat.id
    try:
        admins = await context.bot.get_chat_administrators(chat_id)
        admin_list = "👑 *Администраторы чата:*

"

        for admin in admins:
            if not admin.user.is_bot:
                status_icon = "👑" if admin.status == "creator" else "⚡"
                admin_list += f"{status_icon} {admin.user.first_name}"
                if admin.user.username:
                    admin_list += f" (@{admin.user.username})"
                admin_list += "\n"

        admin_list += "\n💡 Для запуска игры используйте /hangman_start"

        await update.effective_message.reply_text(admin_list, parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.warning(f"Failed to get admins for chat {chat_id}: {e}")
        await update.effective_message.reply_text("❌ Не удалось получить список администраторов.")

# ------------------ ВИКТОРИНА ------------------

async def quiz_timeout_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Вызывается через 5 минут после отправки викторины — подводит итоги."""
    job = context.job
    chat_id = job.chat_id
    data = job.data or {}
    message_id = data.get("message_id")

    quiz = _current_quiz.get(chat_id)
    if not quiz or quiz.get("message_id") != message_id:
        return

    # Убираем кнопки
    try:
        await context.bot.edit_message_reply_markup(
            chat_id=chat_id, message_id=message_id, reply_markup=None
        )
    except Exception:
        pass

    correct_year = quiz["correct_year"]
    answers = quiz.get("answers", [])
    winner = quiz.get("winner")

    # Все отслеживаемые участники чата
    participants_map = _tracked_participants.get(chat_id, {})  # {uid: mention_html}
    participants_ids = set(participants_map.keys())
    answered_ids = {a["uid"] for a in answers}
    not_answered_ids = participants_ids - answered_ids
    not_answered_mentions = [participants_map[uid] for uid in not_answered_ids]

    # Обновляем рейтинг недели по результатам этой викторины
    update_weekly_scores(chat_id, participants_ids, answers)

    max_listed = 10
    if len(not_answered_mentions) > max_listed:
        listed_not_answered = not_answered_mentions[:max_listed]
        not_answered_suffix = " и другие"
    else:
        listed_not_answered = not_answered_mentions
        not_answered_suffix = ""

    # Если вообще никто не ответил
    if not answers:
        text_lines = [
            "⏰ Время вышло. За 5 минут никто не ответил.",
            f"Правильный ответ: <b>{correct_year}</b>",
        ]
        if listed_not_answered:
            text_lines.append(
                "😴 Из отслеживаемых не ответили: "
                + ", ".join(listed_not_answered)
                + not_answered_suffix
            )
        await context.bot.send_message(
            chat_id=chat_id, text="\n\n".join(text_lines), parse_mode=ParseMode.HTML
        )
        _current_quiz.pop(chat_id, None)
        return

    # Есть хотя бы какие-то ответы
    incorrect_mentions = []
    seen_incorrect = set()
    for a in answers:
        if not a["ok"] and a["uid"] not in seen_incorrect:
            incorrect_mentions.append(a["mention"])
            seen_incorrect.add(a["uid"])

    lines = [f"⏰ Время вышло! Итоги викторины:\nПравильный ответ: <b>{correct_year}</b>"]
    if winner:
        lines.append(f"🥇 Первый правильно ответил(а): {winner['mention']}")
    else:
        lines.append("❌ Никто не ответил правильно.")

    if incorrect_mentions:
        lines.append("🙃 Ответили неверно: " + ", ".join(incorrect_mentions))

    if listed_not_answered:
        lines.append(
            "😴 Из отслеживаемых не ответили: "
            + ", ".join(listed_not_answered)
            + not_answered_suffix
        )

    await context.bot.send_message(
        chat_id=chat_id, text="\n\n".join(lines), parse_mode=ParseMode.HTML
    )
    _current_quiz.pop(chat_id, None)

async def handle_quiz_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка клика по кнопке викторины."""
    query = update.callback_query
    user = query.from_user
    chat_id = query.message.chat_id
    message_id = query.message.message_id
    selected_year = query.data

    # Если юзер отслеживаемый — запомним
    remember_tracked_user(chat_id, user)

    quiz = _current_quiz.get(chat_id)
    if not quiz or quiz.get("message_id") != message_id:
        await query.answer("Викторина уже завершена.", show_alert=False)
        return

    if now_msk() > quiz.get("deadline", now_msk()):
        await query.answer("⏰ Время вышло. Ответы больше не принимаются.", show_alert=True)
        return

    answered_users: set[int] = quiz.setdefault("answered_users", set())
    if user.id in answered_users:
        await query.answer("Вы уже отвечали на эту викторину.", show_alert=False)
        return

    is_correct = selected_year == quiz["correct_year"]
    answered_users.add(user.id)
    quiz["answers"] = quiz.get("answers", [])
    quiz["answers"].append(
        {
            "uid": user.id,
            "mention": user.mention_html(),
            "year": selected_year,
            "ts": now_msk(),
            "ok": is_correct,
        }
    )

    if is_correct and quiz.get("winner") is None:
        quiz["winner"] = {"uid": user.id, "mention": user.mention_html(), "ts": now_msk()}
        await query.answer("✅ Верно! Вы — первый(ая) с правильным ответом.", show_alert=True)
    elif is_correct:
        await query.answer("✅ Верно!", show_alert=False)
    else:
        await query.answer(f"❌ Неверно ({selected_year})", show_alert=False)

async def daily_fact_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Ежедневная викторина (только будни)."""
    global _last_holiday_congrats_date
    chat_id = context.job.chat_id
    today = now_msk()

    logger.info(f"🔄 Daily fact job triggered for chat {chat_id} at {today}")

    # Поздравление с праздником РФ (раз в день)
    holiday_name = get_ru_holiday_name(today)
    if holiday_name and _last_holiday_congrats_date != today.date():
        _last_holiday_congrats_date = today.date()
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"🎉 Сегодня в РФ праздник: *{holiday_name}*!\nС праздником и отличного дня! 🇷🇺",
                parse_mode=ParseMode.MARKDOWN,
            )
            logger.info(f"✅ Holiday message sent to chat {chat_id}")
        except Exception as e:
            logger.error(f"Send holiday message error: {e}")

    # Факт и викторина
    fact_text, correct_year = get_on_this_day_fact(today)
    if fact_text and correct_year:
        year_options = generate_year_options(correct_year)
        keyboard = [[InlineKeyboardButton(year, callback_data=year)] for year in year_options]
        reply_markup = InlineKeyboardMarkup(keyboard)
        quiz_message = await context.bot.send_message(
            chat_id=chat_id,
            text=(
                "📚 Доброе утро. Интересный факт в этот день:\n\n"
                f"{fact_text}\n\n"
                "🔍 В каком году произошло событие?\n⏳ На ответ — 5 минут."
            ),
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML,
        )
        _current_quiz[chat_id] = {
            "message_id": quiz_message.message_id,
            "correct_year": correct_year,
            "answered_users": set(),
            "answers": [],
            "winner": None,
            "deadline": now_msk() + timedelta(minutes=5),
        }
        logger.info(f"✅ Quiz sent to chat {chat_id}")

        try:
            context.application.job_queue.run_once(
                quiz_timeout_job,
                when=timedelta(minutes=5),
                chat_id=chat_id,
                name=f"quiz_timeout_{chat_id}_{quiz_message.message_id}",
                data={"message_id": quiz_message.message_id},
            )
        except Exception as e:
            logger.error(f"Schedule quiz timeout failed: {e}")
    else:
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text="📚 Сегодня тоже отличный день, хотя подходящего факта не нашлось 🙂",
            )
            logger.info(f"✅ No fact message sent to chat {chat_id}")
        except Exception as e:
            logger.error(f"Send fact message error: {e}")

# ------------------ НАПОМИНАНИЕ О ПЛАНЁРКЕ ------------------

async def send_standup_reminder(bot, chat_id: int) -> None:
    """Отправка текста напоминания о планёрке в указанный чат."""
    logger.info(f"🔄 Standup reminder triggered for chat {chat_id} at {now_msk()}")

    text = (
        f"🌅 Доброе утро! Сегодня в {STANDUP_MEETING_TIME_STR} — планёрка нашей команды. "
        "✍️ Подготовь вопросы и хорошее настроение! 🙂🚀"
    )

    try:
        await bot.send_message(chat_id=chat_id, text=text)
        logger.info(f"✅ Standup reminder sent to chat {chat_id}")
    except Exception as e:
        logger.error(f"❌ Send standup reminder error: {e}")

async def standup_reminder_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Напоминание о планёрке (пн/ср/пт)."""
    chat_id = context.job.chat_id
    await send_standup_reminder(context.bot, chat_id)

# ------------------ КИНОПОИСК: РЕКОМЕНДАЦИЯ ФИЛЬМА ------------------

def kino_request(params: dict) -> dict | None:
    """Вспомогательная функция запроса к API Кинопоиска."""
    if not KINOPOISK_API_KEY:
        logger.warning("KINOPOISK_API_KEY is not set")
        return None
    url = "https://api.kinopoisk.dev/v1.4/movie"
    headers = {
        "X-API-KEY": KINOPOISK_API_KEY,
        "Accept": "application/json",
        "User-Agent": "tg-work-bot/1.0",
    }
    try:
        r = requests.get(url, headers=headers, params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.error(f"KinoPoisk request error: {e}")
        return None

def pick_movie_from_docs(docs: list[dict]) -> dict | None:
    if not docs:
        return None
    return random.choice(docs)

def fetch_movie_recommendation() -> dict | None:
    """Пытаемся найти новинку, если не получилось — берём топовый фильм."""
    current_year = now_msk().year

    # 1. Новинки за последний год
    new_params = {
        "page": 1,
        "limit": 20,
        "type": "movie",
        "year": f"{current_year - 1}-{current_year}",
        "rating.kp": "6-10",
        "sortField": "year",
        "sortType": -1,  # по убыванию
    }
    data = kino_request(new_params)
    docs = (data or {}).get("docs") or []
    movie = pick_movie_from_docs(docs)
    if movie:
        return movie

    # 2. Топовый фильм по рейтингу
    top_params = {
        "page": 1,
        "limit": 50,
        "type": "movie",
        "rating.kp": "7-10",
        "votes.kp": "10000-100000000",
        "sortField": "rating.kp",
        "sortType": -1,
    }
    data = kino_request(top_params)
    docs = (data or {}).get("docs") or []
    movie = pick_movie_from_docs(docs)
    return movie

def build_movie_message(movie: dict) -> tuple[str, str | None]:
    """Собирает текст сообщения и постер (url)."""
    title = movie.get("name") or movie.get("alternativeName") or "Фильм"
    rating = (movie.get("rating") or {}).get("kp") or (movie.get("rating") or {}).get("imdb")
    description = movie.get("description") or "Описание отсутствует."
    genres = (
        ", ".join(g.get("name") for g in (movie.get("genres") or []) if g.get("name")) or "—"
    )
    countries = (
        ", ".join(c.get("name") for c in (movie.get("countries") or []) if c.get("name")) or "—"
    )
    kp_id = movie.get("id") or movie.get("kinopoiskId")
    kp_url = f"https://www.kinopoisk.ru/film/{kp_id}/" if kp_id else "https://www.kinopoisk.ru/"

    # Обрезаем слишком длинное описание, чтобы влезло в caption
    if len(description) > 500:
        description = description[:497] + "..."

    # Экранируем HTML
    title_html = html.escape(str(title))
    description_html = html.escape(str(description))
    genres_html = html.escape(genres)
    countries_html = html.escape(countries)
    rating_html = html.escape(str(rating)) if rating is not None else "—"
    kp_url_html = html.escape(kp_url)

    text = (
        f"<b>{title_html}</b>\n\n"
        f"⭐ Оценка: <b>{rating_html}</b> / 10\n\n"
        f"{description_html}\n\n"
        f"🎭 Жанр: {genres_html}\n"
        f"🌍 Страна: {countries_html}\n\n"
        f"🔗 <a href=\"{kp_url_html}\">Смотреть на Кинопоиске</a>\n\n"
        "✨ Всем отличных выходных! Рекомендую разнообразить их этим фильмом 🍿"
    )
    poster = movie.get("poster") or {}
    poster_url = poster.get("url") or poster.get("previewUrl")
    return text, poster_url

async def movie_recommendation_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Пятничная рекомендация фильма (по расписанию)."""
    chat_id = context.job.chat_id
    logger.info(f"🔄 Movie recommendation job triggered for chat {chat_id}")

    movie = fetch_movie_recommendation()
    if not movie:
        await context.bot.send_message(
            chat_id=chat_id,
            text="🎬 Не удалось получить рекомендацию фильма на этот раз. Попробуем в следующую пятницу 🙂",
        )
        return

    text, poster_url = build_movie_message(movie)
    try:
        if poster_url:
            await context.bot.send_photo(
                chat_id=chat_id,
                photo=poster_url,
                caption=text,
                parse_mode=ParseMode.HTML,
            )
        else:
            await context.bot.send_message(
                chat_id=chat_id, text=text, parse_mode=ParseMode.HTML
            )
        logger.info(f"✅ Movie recommendation sent to chat {chat_id}")
    except Exception as e:
        logger.error(f"Send movie recommendation error: {e}")

# ------------------ ИТОГИ НЕДЕЛИ ПО ВИКТОРИНЕ ------------------

async def weekly_quiz_summary_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """По пятницам в 17:00 подводит итоги викторины за текущую неделю."""
    chat_id = context.job.chat_id
    logger.info(f"🔄 Weekly summary job triggered for chat {chat_id}")

    now = now_msk()
    week_id = get_current_week_id(now)
    chat_key = str(chat_id)
    chat_weeks = _weekly_scores.get(chat_key, {})
    week_scores = chat_weeks.get(week_id, {})

    if not week_scores:
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text="📊 На этой неделе по викторине ещё нет результатов — никто не набрал баллов.",
            )
        except Exception as e:
            logger.error(f"Send weekly summary (empty) error: {e}")
        return

    # Находим максимальный счёт
    max_score = max(week_scores.values())
    if max_score is None:
        return

    # Все пользователи с максимальным счётом (вдруг ничья)
    winner_ids = [int(uid_str) for uid_str, score in week_scores.items() if score == max_score]
    participants_map = _tracked_participants.get(chat_id, {})  # {uid: mention_html}
    winner_mentions: list[str] = []
    for uid in winner_ids:
        mention = participants_map.get(uid)
        if not mention:
            # fallback, если пользователя нет в _tracked_participants
            mention = f"<a href=\"tg://user?id={uid}\">участник</a>"
        winner_mentions.append(mention)

    winners_text = ", ".join(winner_mentions)

    # Немного праздника
    if len(winner_ids) == 1:
        text = (
            "🎉 <b>Еженедельный фактобатл окончен!</b>\n\n"
            f"🥇 Абсолютный чемпион недели — {winners_text}\n"
            f"🔥 Итоговый счёт: <b>{max_score}</b>\n\n"
            f"🏆 Ачивка: <b>«Чемпион недели — {winners_text}»</b>\n\n"
            "Поаплодируем чемпиону в чате 👏👏👏"
        )
    else:
        text = (
            "🎉 <b>Еженедельный фактобатл окончен!</b>\n\n"
            f"🥇 У нас несколько чемпионов недели: {winners_text}\n"
            f"🔥 Счёт у каждого: <b>{max_score}</b>\n\n"
            "🏆 Ачивка: <b>«Чемпион недели»</b> достаётся всем перечисленным!\n\n"
            "Можно официально хвастаться в статусе 😎"
        )

    # Небольшой топ-5 в конце (если есть больше одного участника)
    if len(week_scores) > 1:
        sorted_scores = sorted(week_scores.items(), key=lambda kv: kv[1], reverse=True)
        top_lines = []
        for i, (uid_str, score) in enumerate(sorted_scores[:5], start=1):
            uid = int(uid_str)
            mention = participants_map.get(uid) or f"<a href=\"tg://user?id={uid}\">участник</a>"
            top_lines.append(f"{i}. {mention}: <b>{score}</b>")
        text += "\n\n📈 <b>Топ-5 недели:</b>\n" + "\n".join(top_lines)

    try:
        await context.bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML)
        logger.info(f"✅ Weekly summary sent to chat {chat_id}")
    except Exception as e:
        logger.error(f"Send weekly summary error: {e}")

# ------------------ КОМАНДЫ ------------------

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "Привет, команда! 👋
"
        "Я ваш рабочий бот-помощник 🤖

"
        "🕘 Каждое утро по будням напомню о себе маленькой викториной с историческим фактом.
"
        "Обязательно участвуйте — это и полезно, и поднимает настроение! 💡✨

"
        "📣 В нужное время напомню о планёрке, чтобы никто не пропустил общий созвон и был готов к обсуждению дел дня 🧑‍💻📅

"
        "🎮 А ещё у нас есть командная игра «Виселица» — её можно запустить в чате по запросу руководителя.
"
        "Собирайтесь вместе, выбирайте категорию и попробуйте отгадать слово, пока человечек ещё жив! 😄🪢"
    )
    await update.effective_message.reply_text(text)


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(
        "/start — краткая справка\n"
        "/ping — проверка связи\n"
        "/when — показать текущее расписание (МСК)\n"
        "/debug — отладочная информация\n"
        "/test_quiz — прислать тестовую викторину сейчас (только админы)\n"
        "/test_movie — тестовая рекомендация фильма (только админы)\n"
        "/force_standup — принудительно отправить напоминание о планёрке (только админы)\n"
        "/top — топ участников недели по викторине (только админы)\n"
        "/init_jobs — принудительная инициализация джобов (только админы)\n"
        "\n"
        "🎮 Виселица:\n"
        "/hangman_start — начать игру (только админы, в группе)\n"
        "/hangman_join — присоединиться к игре\n"
        "/hangman_leave — выйти из игры\n"
        "/hangman_stop — остановить игру (только админы)\n"
        "/hangman_stats — статистика по виселице\n"
        "/hangman_rules — правила игры"
    )

async def ping_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text("Я здесь 👋")

async def when_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(
        f"🕘 Ежедневная викторина: {DAILY_FACT_TIME_STR} МСК (пн–пт)\n"
        f"📣 Напоминание о планёрке: {STANDUP_REMINDER_TIME_STR} МСК (пн/ср/пт)\n"
        f"🎬 Фильм пятницы: {MOVIE_RECOMMEND_TIME_STR} МСК (пятница)\n"
        f"🏆 Итоги недели по викторине: {WEEKLY_SUMMARY_TIME_STR} МСК (пятница)"
    )

async def debug_schedule_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать текущее расписание и статус jobs."""
    chat_id = update.effective_chat.id
    now = now_msk()

    text = (
        f"🕐 Текущее время сервера: {now}\n"
        f"📅 День недели: {now.weekday()} ({['пн','вт','ср','чт','пт','сб','вс'][now.weekday()]})\n"
        f"💬 Chat ID: {chat_id}\n"
        f"📋 Запланированные чаты: {_scheduled_chats}\n"
        f"⏰ Напоминание о планёрке: {STANDUP_REMINDER_TIME_STR} (пн/ср/пт)\n"
        f"🏢 Время планёрки: {STANDUP_MEETING_TIME_STR}\n"
    )

    jq = context.application.job_queue
    if jq:
        jobs = jq.jobs()
        # все джобы, в имени которых есть id этого чата
        chat_jobs = [j for j in jobs if str(chat_id) in j.name]

        text += f"\n🔧 Активных jobs для этого чата: {len(chat_jobs)}"
        for job in chat_jobs:
            text += f"\n  - {job.name}: next_run={job.next_t}"
    else:
        text += "\n❌ JobQueue не доступен"

    await update.effective_message.reply_text(text)

async def test_quiz_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ручной запуск викторины (только для админа чата)."""
    if not await is_user_admin(update, context):
        await update.effective_message.reply_text("Эта команда доступна только администраторам чата.")
        return

    fact_text, correct_year = get_on_this_day_fact(now_msk())
    if fact_text and correct_year:
        year_options = generate_year_options(correct_year)
        keyboard = [[InlineKeyboardButton(year, callback_data=year)] for year in year_options]
        reply_markup = InlineKeyboardMarkup(keyboard)
        quiz_message = await update.effective_message.reply_text(
            text=(
                "📚 Доброе утро. Интересный факт в этот день:\n\n"
                f"{fact_text}\n\n"
                "🔍 В каком году произошло событие?\n⏳ На ответ — 5 минут."
            ),
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML,
        )
        chat_id = update.effective_chat.id
        _current_quiz[chat_id] = {
            "message_id": quiz_message.message_id,
            "correct_year": correct_year,
            "answered_users": set(),
            "answers": [],
            "winner": None,
            "deadline": now_msk() + timedelta(minutes=5),
        }
        try:
            context.application.job_queue.run_once(
                quiz_timeout_job,
                when=timedelta(minutes=5),
                chat_id=chat_id,
                name=f"quiz_timeout_{chat_id}_{quiz_message.message_id}",
                data={"message_id": quiz_message.message_id},
            )
        except Exception as e:
            logger.error(f"Schedule quiz timeout failed (test): {e}")
    else:
        await update.effective_message.reply_text("Не удалось получить факт для викторины :(")

async def test_movie_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Тестовая команда: показать, какой фильм бот порекомендует в пятницу."""
    if not await is_user_admin(update, context):
        await update.effective_message.reply_text("Эта команда доступна только администраторам чата.")
        return

    if not KINOPOISK_API_KEY:
        await update.effective_message.reply_text(
            "KINOPOISK_API_KEY не задан, не могу получить рекомендацию фильма."
        )
        return

    movie = fetch_movie_recommendation()
    if not movie:
        await update.effective_message.reply_text(
            "🎬 Не удалось получить рекомендацию фильма. Проверь API-ключ или попробуй ещё раз позже."
        )
        return

    text, poster_url = build_movie_message(movie)
    chat_id = update.effective_chat.id
    try:
        if poster_url:
            await context.bot.send_photo(
                chat_id=chat_id,
                photo=poster_url,
                caption=text,
                parse_mode=ParseMode.HTML,
            )
        else:
            await context.bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Send test movie recommendation error: {e}")
        await update.effective_message.reply_text("Что-то пошло не так при отправке фильма :(")

async def force_standup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Принудительно отправить напоминание о планёрке."""
    if not await is_user_admin(update, context):
        await update.effective_message.reply_text("Только для админов")
        return

    chat_id = update.effective_chat.id
    await send_standup_reminder(context.bot, chat_id)
    await update.effective_message.reply_text("✅ Напоминание отправлено")

async def top_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Топ участников недели по викторине. Только для админов."""
    if not await is_user_admin(update, context):
        await update.effective_message.reply_text("Эта команда доступна только администраторам чата.")
        return

    chat = update.effective_chat
    chat_id = chat.id
    now = now_msk()
    week_id = get_current_week_id(now)
    chat_key = str(chat_id)
    chat_weeks = _weekly_scores.get(chat_key, {})
    week_scores = chat_weeks.get(week_id, {})

    if not week_scores:
        await update.effective_message.reply_text("📊 На этой неделе по викторине ещё нет данных для топа.")
        return

    participants_map = _tracked_participants.get(chat_id, {})  # {uid: mention_html}

    # Собираем (uid, score), сортируем по убыванию
    sorted_scores = sorted(week_scores.items(), key=lambda kv: kv[1], reverse=True)

    lines = [
        f"📈 Топ недели по викторине (неделя {week_id}):",
        "Счёт: правильный +1, неправильный 0, не ответил -1\n",
    ]

    # Ограничим топ-20
    for pos, (uid_str, score) in enumerate(sorted_scores[:20], start=1):
        uid = int(uid_str)
        mention = participants_map.get(uid)
        if not mention:
            # Попробуем получить из чата
            try:
                member = await context.bot.get_chat_member(chat_id, uid)
                mention = member.user.mention_html()
            except Exception:
                mention = f"<a href=\"tg://user?id={uid}\">участник</a>"
        lines.append(f"{pos}. {mention}: <b>{score}</b>")

    text = "\n".join(lines)
    await update.effective_message.reply_html(text)

async def init_jobs_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Принудительная инициализация джобов для чата"""
    if not await is_user_admin(update, context):
        await update.effective_message.reply_text("Эта команда доступна только администраторам чата.")
        return
    
    chat_id = update.effective_chat.id
    success = await ensure_jobs_for_chat(context, chat_id)
    if success:
        await update.effective_message.reply_text("✅ Джобы инициализированы для этого чата")
    else:
        await update.effective_message.reply_text("❌ Не удалось инициализировать джобы")

# ------------------ ДРУГИЕ ХЕНДЛЕРЫ ------------------

async def greet_new_members(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Приветствие новых участников + учёт отслеживаемых."""
    chat = update.effective_chat
    chat_title = chat.title or "чате"
    for user in update.effective_message.new_chat_members:
        if not user.is_bot:
            remember_tracked_user(chat.id, user)
            text = f"👋 Добро пожаловать, {user.mention_html()}! Рад(ы) видеть тебя в {chat_title}."
            await update.effective_message.reply_text(text, parse_mode=ParseMode.HTML)

async def handle_my_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Когда бота добавили/изменили права в чате."""
    chat = update.my_chat_member.chat
    new_status = update.my_chat_member.new_chat_member.status
    if new_status in ("member", "administrator"):
        await ensure_jobs_for_chat(context, chat.id)
        logger.info(f"Scheduled jobs for chat {chat.id} (my_chat_member)")

async def ensure_jobs_for_chat(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    """Вешает джобы для конкретного чата.

    Логика устойчивая:
    * проверяем, что нужные задания реально висят в JobQueue;
    * если что‑то пропало (например, JobQueue перезапускался), пересоздаём джобы;
    * если всё на месте — просто выходим.
    """
    jq = context.application.job_queue
    if not jq:
        logger.error("JobQueue missing.")
        return False

    # Набор обязательных задач для этого чата (по именам)
    required_job_names = {
        f"daily_fact_{chat_id}",
        f"standup_reminder_{chat_id}",
        f"movie_friday_{chat_id}",
        f"weekly_quiz_summary_{chat_id}",
    }

    # Смотрим, какие из нужных задач реально есть в очереди
    existing_names = {job.name for job in jq.jobs() if job.name in required_job_names}

    if chat_id in _scheduled_chats and required_job_names.issubset(existing_names):
        # И флаг стоит, и все джобы на месте — ничего делать не нужно
        logger.info(f"Jobs already scheduled for chat {chat_id}")
        return True

    if not required_job_names.issubset(existing_names):
        # Что‑то отсутствует — подчистим возможные старые/битые задачи и создадим заново
        logger.warning(
            "Jobs marker/_scheduled_chats and real JobQueue are out of sync for chat %s. "
            "Recreating jobs…",
            chat_id,
        )
        for name in required_job_names:
            for job in jq.get_jobs_by_name(name):
                job.schedule_removal()

    try:
        logger.info(f"📅 Creating jobs for chat {chat_id}")

        # Ежедневная викторина: ПН–ПТ
        jq.run_daily(
            daily_fact_job,
            time=parse_hhmm(DAILY_FACT_TIME_STR),
            days=(1, 2, 3, 4, 5),          # было (0, 1, 2, 3, 4)
            name=f"daily_fact_{chat_id}",
            chat_id=chat_id,
        )
        logger.info(f"  ✅ Daily fact job: {DAILY_FACT_TIME_STR} (Mon–Fri)")

        # Напоминание о планёрке: ПН/СР/ПТ
        jq.run_daily(
            standup_reminder_job,
            time=parse_hhmm(STANDUP_REMINDER_TIME_STR),
            days=(1, 3, 5),                # было (0, 2, 4)
            name=f"standup_reminder_{chat_id}",
            chat_id=chat_id,
        )
        logger.info(f"  ✅ Standup reminder job: {STANDUP_REMINDER_TIME_STR} (Mon,Wed,Fri)")

        # Рекомендация фильма: ПТ
        jq.run_daily(
            movie_recommendation_job,
            time=parse_hhmm(MOVIE_RECOMMEND_TIME_STR),
            days=(5,),                      # было (4,)
            name=f"movie_friday_{chat_id}",
            chat_id=chat_id,
        )
        logger.info(f"  ✅ Movie recommendation job: {MOVIE_RECOMMEND_TIME_STR} (Fri)")

        # Итоги недели по викторине: ПТ
        jq.run_daily(
            weekly_quiz_summary_job,
            time=parse_hhmm(WEEKLY_SUMMARY_TIME_STR),
            days=(5,),                      # было (4,)
            name=f"weekly_quiz_summary_{chat_id}",
            chat_id=chat_id,
        )
        logger.info(f"  ✅ Weekly summary job: {WEEKLY_SUMMARY_TIME_STR} (Fri)")

        _scheduled_chats.add(chat_id)
        logger.info(f"🎯 All jobs scheduled for chat {chat_id}")
        return True

    except Exception as e:
        logger.error(f"Failed to schedule jobs for chat {chat_id}: {e}")
        return False


async def auto_ensure_jobs_for_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Любое сообщение в группе/супергруппе: убеждаемся, что для этого чата есть джобы."""
    chat = update.effective_chat
    user = update.effective_user
    if not chat:
        return
    if chat.type not in ("group", "supergroup"):
        return
    remember_tracked_user(chat.id, user)
    await ensure_jobs_for_chat(context, chat.id)

# ------------------ MAIN ------------------

def main():
    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN не задан в .env")

    if WATCHED_USERNAMES:
        logger.info(f"Watching usernames: {', '.join(WATCHED_USERNAMES)}")
    else:
        logger.warning("WATCHED_USERNAMES пуст — никто не будет учитываться в статистике викторины.")

    if not KINOPOISK_API_KEY:
        logger.warning("KINOPOISK_API_KEY не задан — рекомендации фильмов работать не будут.")

    # Загружаем рейтинг из файла
    load_weekly_scores()

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # Команды основного бота
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("ping", ping_cmd))
    app.add_handler(CommandHandler("when", when_cmd))
    app.add_handler(CommandHandler("debug", debug_schedule_cmd))
    app.add_handler(CommandHandler("test_quiz", test_quiz_cmd))
    app.add_handler(CommandHandler("test_movie", test_movie_cmd))
    app.add_handler(CommandHandler("force_standup", force_standup_cmd))
    app.add_handler(CommandHandler("top", top_cmd))
    app.add_handler(CommandHandler("init_jobs", init_jobs_cmd))

    # Команды виселицы
    app.add_handler(CommandHandler("hangman_start", hangman_start_cmd))
    app.add_handler(CommandHandler("hangman_join", hangman_join_cmd))
    app.add_handler(CommandHandler("hangman_leave", hangman_leave_cmd))
    app.add_handler(CommandHandler("hangman_stop", hangman_stop_cmd))
    app.add_handler(CommandHandler("hangman_stats", hangman_stats_cmd))
    app.add_handler(CommandHandler("hangman_rules", hangman_rules_cmd))
    app.add_handler(CommandHandler("hangman_admins", hangman_admins_cmd))

    # Обработка букв для виселицы в ЛС (любое текстовое сообщение, не команда)
    app.add_handler(
        MessageHandler(
            filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND,
            handle_private_guess,
        )
    )

    # Автоподвешивание джобов + учёт отслеживаемых по любому сообщению
    app.add_handler(MessageHandler(filters.ALL & ~filters.StatusUpdate.ALL, auto_ensure_jobs_for_chat))

    # Приветствие новых участников (+ добавление отслеживаемых в список)
    app.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, greet_new_members))

    # Обновление статуса бота в чате (добавили/сделали админом и т.п.)
    app.add_handler(ChatMemberHandler(handle_my_chat_member, ChatMemberHandler.MY_CHAT_MEMBER))

    # Ответы на викторину: callback_data — это год (3–4 цифры)
    app.add_handler(CallbackQueryHandler(handle_quiz_answer, pattern=r"^\d{3,4}$"))

    # Callback-и виселицы: выбор категории и игровые кнопки
    app.add_handler(CallbackQueryHandler(handle_hangman_category_selection, pattern=r"^hangman_category_"))
    app.add_handler(CallbackQueryHandler(handle_hangman_buttons, pattern=r"^(hangman_join|hangman_leave|admin_stop_game)$"))

    app.run_polling()

if __name__ == "__main__":
    main()
