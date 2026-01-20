import os
import re
import random
import sqlite3
import logging
from datetime import datetime, date, timedelta

import pytz
from dotenv import load_dotenv

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ForceReply,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("standup-bot")

BOT_TOKEN = os.getenv("BOT_TOKEN")
ZOOM_URL = os.getenv("ZOOM_URL")
DB_PATH = os.getenv("DB_PATH", "bot.db")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")
if not ZOOM_URL:
    raise RuntimeError("ZOOM_URL is not set")

MOSCOW_TZ = pytz.timezone("Europe/Moscow")

# ---------------- DB ----------------

def db_init():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS standup_chats (
            chat_id INTEGER PRIMARY KEY,
            added_at TEXT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS standup_state (
            standup_date TEXT PRIMARY KEY,
            canceled INTEGER NOT NULL DEFAULT 0,
            reason TEXT,
            reschedule_date TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS standup_reschedules (
            original_date TEXT PRIMARY KEY,
            new_date TEXT NOT NULL,
            created_at TEXT NOT NULL,
            sent INTEGER NOT NULL DEFAULT 0
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS standup_meta (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)

    con.commit()
    con.close()


def db_get_meta(key: str) -> str | None:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT value FROM standup_meta WHERE key=?", (key,))
    row = cur.fetchone()
    con.close()
    return row[0] if row else None


def db_set_meta(key: str, value: str):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        INSERT INTO standup_meta(key, value)
        VALUES(?, ?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value
    """, (key, value))
    con.commit()
    con.close()


def db_add_chat(chat_id: int):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        INSERT INTO standup_chats(chat_id, added_at)
        VALUES (?, ?)
        ON CONFLICT(chat_id) DO NOTHING
    """, (chat_id, datetime.utcnow().isoformat()))
    con.commit()
    con.close()


def db_remove_chat(chat_id: int):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("DELETE FROM standup_chats WHERE chat_id=?", (chat_id,))
    con.commit()
    con.close()


def db_list_chats() -> list[int]:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT chat_id FROM standup_chats")
    rows = cur.fetchall()
    con.close()
    return [r[0] for r in rows]


def db_get_state(d: date):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        "SELECT canceled, reason, reschedule_date FROM standup_state WHERE standup_date=?",
        (d.isoformat(),),
    )
    row = cur.fetchone()
    con.close()
    if not row:
        return {"canceled": 0, "reason": None, "reschedule_date": None}
    return {"canceled": row[0], "reason": row[1], "reschedule_date": row[2]}


def db_set_canceled(d: date, reason: str, reschedule_date: str | None = None):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        INSERT INTO standup_state (standup_date, canceled, reason, reschedule_date)
        VALUES (?, 1, ?, ?)
        ON CONFLICT(standup_date) DO UPDATE SET
            canceled=1,
            reason=excluded.reason,
            reschedule_date=excluded.reschedule_date
    """, (d.isoformat(), reason, reschedule_date))
    con.commit()
    con.close()


def db_upsert_reschedule(original_d: date, new_d: date):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        INSERT INTO standup_reschedules(original_date, new_date, created_at, sent)
        VALUES (?, ?, ?, 0)
        ON CONFLICT(original_date) DO UPDATE SET
            new_date=excluded.new_date,
            created_at=excluded.created_at,
            sent=0
    """, (original_d.isoformat(), new_d.isoformat(), datetime.utcnow().isoformat()))
    con.commit()
    con.close()


def db_get_due_reschedules(target_day: date) -> list[str]:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        SELECT original_date
        FROM standup_reschedules
        WHERE sent=0 AND new_date = ?
        ORDER BY original_date ASC
    """, (target_day.isoformat(),))
    rows = cur.fetchall()
    con.close()
    return [r[0] for r in rows]


def db_mark_reschedules_sent(original_isos: list[str]):
    if not original_isos:
        return
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.executemany(
        "UPDATE standup_reschedules SET sent=1 WHERE original_date=?",
        [(x,) for x in original_isos],
    )
    con.commit()
    con.close()


# ---------------- TEXT ----------------

DAY_RU = {
    0: "понедельник",
    1: "вторник",
    2: "среда",
    3: "четверг",
    4: "пятница",
    5: "суббота",
    6: "воскресенье",
}

GREETINGS = [
    "Доброе утро, коллеги! ☀️",
    "Всем привет, команда! 👋",
    "Подъём-подъём 😄 Доброе утро!",
    "Коллеги, привет! ✨",
    "Доброе утро! Пусть день будет продуктивным 🚀",
    "Йо! Команда на связи? 😎",
    "Привет-привет! ☕️ Как настроение?",
    "Доброе утро, супергерои задач! 🦸‍♀️🦸‍♂️",
    "Хорошего дня, коллеги! 🌿",
    "Врываемся в день мягко, но уверенно 😄☀️",
]

def today_label_ru(d: date) -> str:
    return DAY_RU.get(d.weekday(), "сегодня")

def build_text(today_d: date, rescheduled_from: list[date] | None):
    greet = random.choice(GREETINGS)
    dow = today_label_ru(today_d)

    extra = ""
    if rescheduled_from:
        items = ", ".join(x.strftime("%d.%m.%y") for x in rescheduled_from)
        extra = f"\n\n📌 <b>Также сегодня пройдёт перенесённая планёрка</b> (перенос(ы) с дат: {items})."

    return (
        f"{greet}\n\n"
        f"Сегодня <b>{dow}</b> 🗓️{extra}\n\n"
        f"Планёрка стартует через <b>15 минут</b> — в <b>09:30 (МСК)</b> ⏰\n\n"
        f'👉 <a href="{ZOOM_URL}">Присоединиться к Zoom</a>\n\n'
        f"Если нужно — можно отменить/перенести ниже 👇"
    )


# ---------------- KEYBOARDS ----------------

def kb_cancel_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Отменить/перенести планёрку 🧩", callback_data="cancel:open")]
    ])

def kb_cancel_options():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("1) Нет срочных тем 💤", callback_data="cancel:reason:no_topics")],
        [InlineKeyboardButton("2) Технические причины 🛠️", callback_data="cancel:reason:tech")],
        [InlineKeyboardButton("3) Перенести на другой день 📆", callback_data="cancel:reason:move")],
        [InlineKeyboardButton("4) Не отменять ✅", callback_data="cancel:close")],
    ])

def next_mon_wed_fri(from_d: date, count=3):
    res = []
    d = from_d + timedelta(days=1)
    while len(res) < count:
        if d.weekday() in (0, 2, 4):
            res.append(d)
        d += timedelta(days=1)
    return res

def kb_reschedule_dates(from_d: date):
    options = next_mon_wed_fri(from_d, count=3)
    rows = []
    for d in options:
        label = f"{DAY_RU.get(d.weekday(), '')[:2].upper()} {d.strftime('%d.%m.%y')}"
        rows.append([InlineKeyboardButton(label, callback_data=f"reschedule:pick:{d.strftime('%d.%m.%y')}")])

    rows.append([InlineKeyboardButton("Ввести дату (ДД.ММ.ГГ) ✍️", callback_data="reschedule:manual")])
    rows.append([InlineKeyboardButton("Назад ↩️", callback_data="cancel:open")])
    return InlineKeyboardMarkup(rows)


# ---------------- ADMIN CHECK ----------------

async def is_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if not update.effective_chat or not update.effective_user:
        return False
    member = await context.bot.get_chat_member(update.effective_chat.id, update.effective_user.id)
    return member.status in ("administrator", "creator")


# ---------------- MANUAL INPUT STATE ----------------
WAITING_DATE_FLAG = "waiting_reschedule_date"
WAITING_PROMPT_MSG_ID = "waiting_prompt_message_id"


# ---------------- CORE SENDERS ----------------

async def send_standup_message(context: ContextTypes.DEFAULT_TYPE, force: bool = False) -> bool:
    today_d = datetime.now(MOSCOW_TZ).date()

    chat_ids = db_list_chats()
    if not chat_ids:
        logger.warning("No chats for notifications. Add via /setchat.")
        return False

    weekday_due = today_d.weekday() in (0, 2, 4)
    state = db_get_state(today_d)
    standard_due = weekday_due and state["canceled"] != 1

    due_orig_isos = db_get_due_reschedules(today_d)
    reschedule_due = len(due_orig_isos) > 0

    if not force and not standard_due and not reschedule_due:
        logger.info("Nothing to send today (%s) under rules", today_d.isoformat())
        return False

    resched_from_dates: list[date] = []
    if reschedule_due:
        for orig_iso in due_orig_isos:
            try:
                resched_from_dates.append(date.fromisoformat(orig_iso))
            except Exception:
                pass

    text = build_text(today_d, resched_from_dates if reschedule_due else None)

    for chat_id in chat_ids:
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=kb_cancel_menu(),
            )
        except Exception as e:
            logger.exception("Cannot send standup to %s: %s", chat_id, e)

    if reschedule_due:
        db_mark_reschedules_sent(due_orig_isos)

    return True


async def check_and_send_915(context: ContextTypes.DEFAULT_TYPE):
    now_msk = datetime.now(MOSCOW_TZ)
    today_iso = now_msk.date().isoformat()

    if not (now_msk.hour == 9 and now_msk.minute == 15):
        return

    last_sent = db_get_meta("last_auto_sent_date")
    if last_sent == today_iso:
        return

    sent = await send_standup_message(context, force=False)

    # фиксируем день, чтобы не повторять попытки
    db_set_meta("last_auto_sent_date", today_iso)

    if sent:
        logger.info("Auto standup sent at 09:15 MSK (%s)", today_iso)
    else:
        logger.info("09:15 MSK reached but nothing to send; marked checked (%s)", today_iso)


# ---------------- COMMANDS ----------------

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.effective_user.first_name if update.effective_user else "коллеги"
    text = (
        f"Привет, {name}! 👋\n\n"
        f"Я бот для уведомлений о планёрке.\n\n"
        f"Команды:\n"
        f"• /setchat — подключить этот чат к рассылке (только админы)\n"
        f"• /unsetchat — отключить этот чат от рассылки (только админы)\n"
        f"• /test915 — проверить логику «как в 09:15» (только админы)\n"
        f"• /force — принудительно отправить сообщение планёрки (только админы)\n"
        f"• /reset — сброс ожидания даты (только админы)\n\n"
        f"Авто-уведомления: ПН/СР/ПТ в 09:15 (МСК) + переносы."
    )
    await update.message.reply_text(text)

async def cmd_setchat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type == "private":
        await update.message.reply_text("Эта команда работает только в групповом чате.")
        return
    if not await is_admin(update, context):
        await update.message.reply_text("Только администраторы могут назначить чат для уведомлений.")
        return

    db_add_chat(update.effective_chat.id)
    await update.message.reply_text("✅ Готово! Этот чат добавлен для уведомлений о планёрке.")

async def cmd_unsetchat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type == "private":
        await update.message.reply_text("Эта команда работает только в групповом чате.")
        return
    if not await is_admin(update, context):
        await update.message.reply_text("Только администраторы могут отключить уведомления.")
        return

    db_remove_chat(update.effective_chat.id)
    await update.message.reply_text("🧹 Этот чат убран из рассылки уведомлений.")

async def cmd_test915(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        await update.message.reply_text("Недостаточно прав.")
        return

    sent = await send_standup_message(context, force=False)
    if sent:
        await update.message.reply_text("✅ Ок, отправил тест «как в 09:15» (по правилам на сегодня).")
    else:
        await update.message.reply_text(
            "ℹ️ По правилам на сегодня уведомление не должно отправляться "
            "(не ПН/СР/ПТ и нет переноса на сегодня). "
            "Для теста используйте /force."
        )

async def cmd_force(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        await update.message.reply_text("Недостаточно прав.")
        return

    chat_ids = db_list_chats()
    if not chat_ids:
        await update.message.reply_text("Сначала подключи чат командой /setchat.")
        return

    await send_standup_message(context, force=True)
    await update.message.reply_text("🚀 Готово! Принудительно отправил сообщение планёрки в подключённые чаты.")

async def cmd_reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        return
    was = bool(context.chat_data.get(WAITING_DATE_FLAG, False))
    context.chat_data[WAITING_DATE_FLAG] = False
    context.chat_data.pop(WAITING_PROMPT_MSG_ID, None)
    if was:
        await update.message.reply_text("✅ Состояние ожидания даты сброшено.")
    else:
        await update.message.reply_text("ℹ️ Режим ожидания даты не был активен.")

# ---------------- CALLBACKS ----------------

async def cb_cancel_open(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query

    if not await is_admin(update, context):
        await query.answer("Только администраторы могут отменять/переносить.", show_alert=True)
        return

    await query.answer()
    await query.edit_message_reply_markup(reply_markup=kb_cancel_options())

async def cb_cancel_close(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query

    if not await is_admin(update, context):
        await query.answer("Только администраторы.", show_alert=True)
        return

    await query.edit_message_reply_markup(reply_markup=None)
    await query.answer("Ок, не отменяем ✅")

async def cb_cancel_reason(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query

    if not await is_admin(update, context):
        await query.answer("Только администраторы.", show_alert=True)
        return

    reason_key = query.data.split(":")[-1]
    today_d = datetime.now(MOSCOW_TZ).date()

    if reason_key == "no_topics":
        reason_text = "Нет срочных тем для обсуждения"
        db_set_canceled(today_d, reason_text)
        await query.edit_message_reply_markup(reply_markup=None)
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"✅ Сегодняшняя планёрка отменена\nПричина: {reason_text}",
        )
        await query.answer("Отменено.")
        return

    if reason_key == "tech":
        reason_text = "Перенесём по техническим причинам"
        db_set_canceled(today_d, reason_text)
        await query.edit_message_reply_markup(reply_markup=None)
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"✅ Сегодняшняя планёрка отменена\nПричина: {reason_text}",
        )
        await query.answer("Ок.")
        return

    if reason_key == "move":
        await query.edit_message_reply_markup(reply_markup=kb_reschedule_dates(today_d))
        await query.answer("Выберите дату переноса 📆")
        return

async def cb_reschedule_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not await is_admin(update, context):
        await query.answer("Только администраторы.", show_alert=True)
        return

    picked = query.data.split(":")[-1]  # DD.MM.YY
    today_d = datetime.now(MOSCOW_TZ).date()

    try:
        dd, mm, yy = picked.split(".")
        new_d = date(int("20" + yy), int(mm), int(dd))
    except Exception:
        await query.answer("Не смог распознать дату.", show_alert=True)
        return

    # (опционально) чтобы не переносили в прошлое:
    if new_d <= today_d:
        await query.answer("Дата переноса должна быть в будущем.", show_alert=True)
        return

    db_set_canceled(today_d, "Перенос на другой день", reschedule_date=picked)
    db_upsert_reschedule(today_d, new_d)

    await query.edit_message_reply_markup(reply_markup=None)
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=(
            "✅ Сегодняшняя планёрка перенесена\n"
            f"Новая дата: {picked} 📌\n"
            "Следите за расписанием или чатом"
        )
    )
    await query.answer("Перенесено.")

async def cb_reschedule_manual(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not await is_admin(update, context):
        await query.answer("❌ Только администраторы.", show_alert=True)
        return

    context.chat_data[WAITING_DATE_FLAG] = True

    await query.answer()
    msg = await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=(
            "📅 <b>Введите дату переноса</b>\n\n"
            "Формат: <b>ДД.ММ.ГГ</b>\n"
            "Пример: <code>22.01.26</code>\n\n"
            "Ответьте на это сообщение датой:"
        ),
        parse_mode=ParseMode.HTML,
        reply_markup=ForceReply(selective=True, input_field_placeholder="Например: 22.01.26"),
    )

    context.chat_data[WAITING_PROMPT_MSG_ID] = msg.message_id
    logger.info("Waiting for date input in chat %s, prompt ID: %s", update.effective_chat.id, msg.message_id)


# ---------------- MANUAL DATE INPUT ----------------

async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка ввода даты вручную + логирование"""
    if not update.message or not update.effective_chat:
        logger.info("on_text: No message or chat in update")
        return

    user_id = update.effective_user.id if update.effective_user else None
    chat_id = update.effective_chat.id
    text = update.message.text or ""

    logger.info("TEXT RECEIVED - Chat: %s, User: %s, Text: %r", chat_id, user_id, text)
    logger.info("WAITING_DATE_FLAG: %s", context.chat_data.get(WAITING_DATE_FLAG, False))

    if not context.chat_data.get(WAITING_DATE_FLAG):
        logger.info("Not waiting for date input, ignoring")
        return

    # Проверяем админа
    if not await is_admin(update, context):
        logger.info("User %s is not admin", user_id)
        context.chat_data[WAITING_DATE_FLAG] = False
        context.chat_data.pop(WAITING_PROMPT_MSG_ID, None)
        await update.message.reply_text("❌ Только администраторы могут переносить планёрку.")
        return

    # Проверяем reply_to_message (важно для privacy mode)
    prompt_id = context.chat_data.get(WAITING_PROMPT_MSG_ID)
    if prompt_id:
        rtm = update.message.reply_to_message
        if not rtm or rtm.message_id != prompt_id:
            logger.info("Message is not a reply to our prompt. prompt_id=%s", prompt_id)
            # подсказка как ответить правильно
            await update.message.reply_text(
                "⚠️ Пожалуйста, ответьте на мое сообщение выше (Reply), чтобы я увидел дату.",
                reply_to_message_id=prompt_id,
            )
            return

    raw = text.strip()
    logger.info("Processing date input: %r", raw)

    if not re.fullmatch(r"\d{2}\.\d{2}\.\d{2}", raw):
        logger.warning("Invalid date format: %r", raw)
        await update.message.reply_text(
            "❌ Неверный формат. Нужно ДД.ММ.ГГ (например 22.01.26).\n\nПопробуйте ещё раз:",
            reply_markup=ForceReply(selective=True, input_field_placeholder="ДД.ММ.ГГ"),
        )
        return

    try:
        dd, mm, yy = raw.split(".")
        new_d = date(int("20" + yy), int(mm), int(dd))
        logger.info("Successfully parsed date: %s", new_d.isoformat())
    except Exception as e:
        logger.error("Date parsing error: %s", e)
        await update.message.reply_text(
            "❌ Не удалось распознать дату. Убедитесь, что она существует.\n\nПопробуйте ещё раз:",
            reply_markup=ForceReply(selective=True, input_field_placeholder="ДД.ММ.ГГ"),
        )
        return

    today_d = datetime.now(MOSCOW_TZ).date()
    if new_d <= today_d:
        await update.message.reply_text(
            "❌ Дата переноса должна быть в будущем.\n\nВыберите другую дату:",
            reply_markup=ForceReply(selective=True, input_field_placeholder="ДД.ММ.ГГ"),
        )
        return

    db_set_canceled(today_d, "Перенос на другой день", reschedule_date=raw)
    db_upsert_reschedule(today_d, new_d)

    context.chat_data[WAITING_DATE_FLAG] = False
    context.chat_data.pop(WAITING_PROMPT_MSG_ID, None)

    logger.info("Rescheduled: %s -> %s", today_d.isoformat(), new_d.isoformat())

    await update.message.reply_text(
        "✅ Сегодняшняя планёрка перенесена\n"
        f"Новая дата: {raw} 📌\n"
        "Следите за расписанием или чатом"
    )


# ---------------- APP ----------------

def main():
    db_init()

    app = Application.builder().token(BOT_TOKEN).build()

    # команды
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("setchat", cmd_setchat))
    app.add_handler(CommandHandler("unsetchat", cmd_unsetchat))
    app.add_handler(CommandHandler("test915", cmd_test915))
    app.add_handler(CommandHandler("force", cmd_force))
    app.add_handler(CommandHandler("reset", cmd_reset))

    # callbacks
    app.add_handler(CallbackQueryHandler(cb_cancel_open, pattern=r"^cancel:open$"))
    app.add_handler(CallbackQueryHandler(cb_cancel_close, pattern=r"^cancel:close$"))
    app.add_handler(CallbackQueryHandler(cb_cancel_reason, pattern=r"^cancel:reason:"))
    app.add_handler(CallbackQueryHandler(cb_reschedule_pick, pattern=r"^reschedule:pick:"))
    app.add_handler(CallbackQueryHandler(cb_reschedule_manual, pattern=r"^reschedule:manual$"))

    # ВАЖНО: обработчик текста должен быть последним среди message handlers
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    # Надёжная отправка 09:15 МСК: проверка каждую минуту
    app.job_queue.run_repeating(check_and_send_915, interval=60, first=10, name="standup_checker")

    logger.info("Bot started. Checking every minute for 09:15 MSK")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
