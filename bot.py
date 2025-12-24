# ===== ВЕРСИЯ С ФИКСОМ ВИСЕЛИЦЫ, БЕЗ УРЕЗАНИЯ ФУНКЦИОНАЛА =====

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
load_dotenv(Path(__file__).with_name(".env"))
BOT_TOKEN = os.getenv("BOT_TOKEN")

MAX_ERRORS = 6  # ✅ ЕДИНСТВЕННЫЙ ЛИМИТ ОШИБОК

# ------------------ СОСТОЯНИЕ ИГРЫ ------------------
active_games = {}
user_scores = {}
_last_guess_time = {}
_current_turn = {}

# ------------------ ВИСЕЛИЦА ------------------
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

# ------------------ КАТЕГОРИИ ------------------
russian_word_categories = {
    "животные": ["КОТ", "СОБАКА", "СЛОН", "ТИГР"],
    "еда": ["ПИЦЦА", "СУП", "БУРГЕР"],
    "города": ["МОСКВА", "ПАРИЖ", "ТОКИО"],
}

category_emojis = {
    "животные": "🐾",
    "еда": "🍕",
    "города": "🏙️",
}

# ------------------ ВСПОМОГАТЕЛЬНЫЕ ------------------
def get_current_player(chat_id):
    if chat_id not in active_games or chat_id not in _current_turn:
        return None

    game = active_games[chat_id]
    players = list(game["players"].keys())
    if not players:
        return None

    idx = _current_turn[chat_id] % len(players)
    pid = players[idx]
    return pid, game["players"][pid]["name"]


def next_turn(chat_id):
    _current_turn[chat_id] += 1
    return get_current_player(chat_id)


# ------------------ ОТОБРАЖЕНИЕ ------------------
async def update_game_display(context, chat_id):
    game = active_games[chat_id]
    word = game["word"]

    display_word = " ".join(
        l if l in game["guessed_letters"] else "_"
        for l in word
    )

    wrong_count = len(game["wrong_letters"])
    attempts_left = MAX_ERRORS - wrong_count

    stage_index = min(wrong_count, len(hangman_stages) - 1)
    hangman = hangman_stages[stage_index]

    wrong_letters = ", ".join(sorted(game["wrong_letters"])) or "нет"

    text = f"""
🎮 *ВИСЕЛИЦА*
{hangman}

📖 Слово: `{display_word}`
❌ Ошибки ({wrong_count}/{MAX_ERRORS}): {wrong_letters}
❤️ Осталось попыток: {attempts_left}
""".strip()

    if game.get("message_id"):
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=game["message_id"],
            text=text,
            parse_mode=ParseMode.MARKDOWN,
        )
    else:
        msg = await context.bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode=ParseMode.MARKDOWN,
        )
        game["message_id"] = msg.message_id


# ------------------ ЛОГИКА ХОДА ------------------
async def process_guess(context, chat_id, user_id, guess):
    game = active_games[chat_id]
    word = game["word"]

    if guess in game["guessed_letters"] or guess in game["wrong_letters"]:
        return

    if guess in word:
        game["guessed_letters"].add(guess)
    else:
        game["wrong_letters"].add(guess)

        # ✅ ФИКС: поражение только по wrong_letters
        if len(game["wrong_letters"]) >= MAX_ERRORS:
            await end_game_lose(context, chat_id)
            return

        next_turn(chat_id)

    await update_game_display(context, chat_id)

    if all(l in game["guessed_letters"] for l in word):
        await end_game_win(context, chat_id, user_id)


# ------------------ КОНЕЦ ИГРЫ ------------------
async def end_game_win(context, chat_id, winner_id):
    word = active_games[chat_id]["word"]
    await context.bot.send_message(
        chat_id,
        f"🎉 Победа!\nСлово: *{word}*",
        parse_mode=ParseMode.MARKDOWN,
    )
    del active_games[chat_id]
    _current_turn.pop(chat_id, None)


async def end_game_lose(context, chat_id):
    word = active_games[chat_id]["word"]
    await context.bot.send_message(
        chat_id,
        f"💀 Поражение!\nСлово было: *{word}*",
        parse_mode=ParseMode.MARKDOWN,
    )
    del active_games[chat_id]
    _current_turn.pop(chat_id, None)


# ------------------ КОМАНДЫ ------------------
async def newgame_cmd(update, context):
    chat_id = update.effective_chat.id
    if chat_id in active_games:
        return

    category = random.choice(list(russian_word_categories))
    word = random.choice(russian_word_categories[category])

    active_games[chat_id] = {
        "word": word,
        "category": category,
        "players": {},
        "guessed_letters": set(),
        "wrong_letters": set(),
        "message_id": None,
    }

    _current_turn[chat_id] = 0
    await update_game_display(context, chat_id)


async def handle_chat(update, context):
    chat = update.effective_chat
    user = update.effective_user
    text = (update.message.text or "").upper()

    if chat.id not in active_games:
        return

    if len(text) == 1 and text.isalpha():
        await process_guess(context, chat.id, user.id, text)


# ------------------ MAIN ------------------
def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("newgame", newgame_cmd))
    app.add_handler(
        MessageHandler(filters.ChatType.GROUPS & filters.TEXT, handle_chat)
    )

    app.run_polling()


if __name__ == "__main__":
    main()
