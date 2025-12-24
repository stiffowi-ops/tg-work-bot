import os
import json
import time
import random
import asyncio
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

# ================== НАСТРОЙКИ ==================
TURN_TIMEOUT = 30  # секунд на ход
load_dotenv(Path(__file__).with_name(".env"))
BOT_TOKEN = os.getenv("BOT_TOKEN")

# ================== СОСТОЯНИЕ ==================
active_games = {}
_last_guess_time = {}
_current_turn = {}
_turn_tasks = {}

# ================== ВИСЕЛИЦА ==================
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

MAX_MISTAKES = len(hangman_stages) - 1

WORDS = {
    "животные": ["КОТ", "СОБАКА", "ТИГР", "СЛОН", "ЛЕВ"],
    "еда": ["ПИЦЦА", "СУП", "ШАШЛЫК", "БУРГЕР"],
    "технологии": ["КОМПЬЮТЕР", "СЕРВЕР", "ИНТЕРНЕТ"],
}

# ================== УТИЛИТЫ ==================
def get_current_player(chat_id):
    game = active_games.get(chat_id)
    if not game:
        return None

    players = list(game["players"].keys())
    if not players:
        return None

    index = _current_turn.get(chat_id, 0) % len(players)
    return players[index]

def next_turn(chat_id):
    _current_turn[chat_id] = _current_turn.get(chat_id, 0) + 1
    return get_current_player(chat_id)

# ================== ТАЙМЕР ХОДА ==================
async def start_turn_timer(context, chat_id):
    if chat_id in _turn_tasks:
        _turn_tasks[chat_id].cancel()

    async def timer():
        await asyncio.sleep(TURN_TIMEOUT)
        if chat_id not in active_games:
            return

        player_id = get_current_player(chat_id)
        if not player_id:
            return

        name = active_games[chat_id]["players"][player_id]
        next_player = next_turn(chat_id)

        await context.bot.send_message(
            chat_id,
            f"⏰ Время вышло! Ход игрока {name} пропущен."
        )
        await update_game_display(context, chat_id)
        await start_turn_timer(context, chat_id)

    _turn_tasks[chat_id] = asyncio.create_task(timer())

# ================== ОТРИСОВКА ==================
async def update_game_display(context, chat_id):
    game = active_games.get(chat_id)
    if not game:
        return

    wrong = len(game["wrong_letters"])
    stage = hangman_stages[wrong]

    word_view = " ".join(
        c if c in game["guessed_letters"] else "_"
        for c in game["word"]
    )

    current = get_current_player(chat_id)
    name = game["players"].get(current, "?")

    text = f"""
🎮 *ВИСЕЛИЦА*

{stage}

📖 Слово: `{word_view}`
❌ Ошибок: {wrong}/{MAX_MISTAKES}
❤️ Осталось попыток: {MAX_MISTAKES - wrong}

🎮 Сейчас ходит: *{name}*
⏳ Время на ход: {TURN_TIMEOUT} сек
""".strip()

    await context.bot.send_message(
        chat_id,
        text,
        parse_mode=ParseMode.MARKDOWN
    )

# ================== ИГРОВАЯ ЛОГИКА ==================
async def process_guess(context, chat_id, user_id, letter):
    game = active_games[chat_id]

    if user_id != get_current_player(chat_id):
        return

    if letter in game["guessed_letters"] | game["wrong_letters"]:
        return

    if letter in game["word"]:
        game["guessed_letters"].add(letter)
    else:
        game["wrong_letters"].add(letter)

        # АНИМАЦИЯ ВИСЕЛИЦЫ
        for i in range(len(game["wrong_letters"]) - 1, len(game["wrong_letters"]) + 1):
            await update_game_display(context, chat_id)
            await asyncio.sleep(0.5)

    if len(game["wrong_letters"]) >= MAX_MISTAKES:
        await context.bot.send_message(chat_id, f"💀 Игра окончена! Слово: {game['word']}")
        del active_games[chat_id]
        return

    if all(c in game["guessed_letters"] for c in game["word"]):
        await context.bot.send_message(chat_id, f"🎉 Победа! Слово: {game['word']}")
        del active_games[chat_id]
        return

    next_turn(chat_id)
    await update_game_display(context, chat_id)
    await start_turn_timer(context, chat_id)

# ================== ХЕНДЛЕРЫ ==================
async def newgame(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    category = random.choice(list(WORDS.keys()))
    word = random.choice(WORDS[category])

    active_games[chat_id] = {
        "word": word,
        "category": category,
        "players": {},
        "guessed_letters": set(),
        "wrong_letters": set(),
    }
    _current_turn[chat_id] = 0

    await update.effective_message.reply_text(
        f"🎮 Новая игра!\nКатегория: {category}\n/join чтобы войти"
    )

async def join(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user = update.effective_user

    if chat_id not in active_games:
        return

    active_games[chat_id]["players"][user.id] = user.first_name

    if len(active_games[chat_id]["players"]) == 1:
        await update_game_display(context, chat_id)
        await start_turn_timer(context, chat_id)

async def chat_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user = update.effective_user
    text = (update.message.text or "").upper()

    if chat_id not in active_games:
        return
    if user.id not in active_games[chat_id]["players"]:
        return

    if len(text) == 1 and text.isalpha():
        await process_guess(context, chat_id, user.id, text)

# ================== MAIN ==================
def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("newgame", newgame))
    app.add_handler(CommandHandler("join", join))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, chat_message))

    print("🤖 Бот запущен")
    app.run_polling()

if __name__ == "__main__":
    main()
