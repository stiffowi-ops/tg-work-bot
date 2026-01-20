import os
import re
import random
import sqlite3
import logging
import time
from datetime import datetime, date, timedelta

import pytz
from dotenv import load_dotenv

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
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
logger = logging.getLogger("meetings-bot")

BOT_TOKEN = os.getenv("BOT_TOKEN")
ZOOM_URL = os.getenv("ZOOM_URL")  # планёрка
INDUSTRY_ZOOM_URL = os.getenv("INDUSTRY_ZOOM_URL")  # отраслевая
DB_PATH = os.getenv("DB_PATH", "bot.db")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")
if not ZOOM_URL:
    raise RuntimeError("ZOOM_URL is not set")
if not INDUSTRY_ZOOM_URL:
    raise RuntimeError("INDUSTRY_ZOOM_URL is not set")

MOSCOW_TZ = pytz.timezone("Europe/Moscow")

MEETING_STANDUP = "standup"
MEETING_INDUSTRY = "industry"

# ---------------- DB ----------------

def db_init():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS notify_chats (
            chat_id INTEGER PRIMARY KEY,
            added_at TEXT NOT NULL
        )
    """)

    # meeting_type: standup | industry
    cur.execute("""
        CREATE TABLE IF NOT EXISTS meeting_state (
            meeting_type TEXT NOT NULL,
            meeting_date TEXT NOT NULL,
            canceled INTEGER NOT NULL DEFAULT 0,
            reason TEXT,
            reschedule_date TEXT,
            PRIMARY KEY (meeting_type, meeting_date)
        )
    """)

    # reschedule: original_date -> new_date
    cur.execute("""
        CREATE TABLE IF NOT EXISTS meeting_reschedules (
            meeting_type TEXT NOT NULL,
            original_date TEXT NOT NULL,
            new_date TEXT NOT NULL,
            created_at TEXT NOT NULL,
            sent INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (meeting_type, original_date)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)

    con.commit()
    con.close()


def db_get_meta(key: str) -> str | None:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT value FROM meta WHERE key=?", (key,))
    row = cur.fetchone()
    con.close()
    return row[0] if row else None


def db_set_meta(key: str, value: str):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        INSERT INTO meta(key, value)
        VALUES(?, ?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value
    """, (key, value))
    con.commit()
    con.close()


def db_add_chat(chat_id: int):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        INSERT INTO notify_chats(chat_id, added_at)
        VALUES (?, ?)
        ON CONFLICT(chat_id) DO NOTHING
    """, (chat_id, datetime.utcnow().isoformat()))
    con.commit()
    con.close()


def db_remove_chat(chat_id: int):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("DELETE FROM notify_chats WHERE chat_id=?", (chat_id,))
    con.commit()
    con.close()


def db_list_chats() -> list[int]:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT chat_id FROM notify_chats")
    rows = cur.fetchall()
    con.close()
    return [r[0] for r in rows]


def db_get_state(meeting_type: str, d: date):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        "SELECT canceled, reason, reschedule_date FROM meeting_state WHERE meeting_type=? AND meeting_date=?",
        (meeting_type, d.isoformat()),
    )
    row = cur.fetchone()
    con.close()
    if not row:
        return {"canceled": 0, "reason": None, "reschedule_date": None}
    return {"canceled": row[0], "reason": row[1], "reschedule_date": row[2]}


def db_set_canceled(meeting_type: str, d: date, reason: str, reschedule_date: str | None = None):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        INSERT INTO meeting_state (meeting_type, meeting_date, canceled, reason, reschedule_date)
        VALUES (?, ?, 1, ?, ?)
        ON CONFLICT(meeting_type, meeting_date) DO UPDATE SET
            canceled=1,
            reason=excluded.reason,
            reschedule_date=excluded.reschedule_date
    """, (meeting_type, d.isoformat(), reason, reschedule_date))
    con.commit()
    con.close()


def db_upsert_reschedule(meeting_type: str, original_d: date, new_d: date):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        INSERT INTO meeting_reschedules(meeting_type, original_date, new_date, created_at, sent)
        VALUES (?, ?, ?, ?, 0)
        ON CONFLICT(meeting_type, original_date) DO UPDATE SET
            new_date=excluded.new_date,
            created_at=excluded.created_at,
            sent=0
    """, (meeting_type, original_d.isoformat(), new_d.isoformat(), datetime.utcnow().isoformat()))
    con.commit()
    con.close()


def db_get_due_reschedules(meeting_type: str, target_day: date) -> list[str]:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        SELECT original_date
        FROM meeting_reschedules
        WHERE meeting_type=? AND sent=0 AND new_date = ?
        ORDER BY original_date ASC
    """, (meeting_type, target_day.isoformat()))
    rows = cur.fetchall()
    con.close()
    return [r[0] for r in rows]


def db_mark_reschedules_sent(meeting_type: str, original_isos: list[str]):
    if not original_isos:
        return
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.executemany(
        "UPDATE meeting_reschedules SET sent=1 WHERE meeting_type=? AND original_date=?",
        [(meeting_type, x) for x in original_isos],
    )
    con.commit()
    con.close()


# ---------------- TEXT ----------------

DAY_RU_UPPER = {
    0: "ПОНЕДЕЛЬНИК",
    1: "ВТОРНИК",
    2: "СРЕДА",
    3: "ЧЕТВЕРГ",
    4: "ПЯТНИЦА",
    5: "СУББОТА",
    6: "ВОСКРЕСЕНЬЕ",
}

STANDUP_GREETINGS = [
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


def build_standup_text(today_d: date, zoom_url: str) -> str:
    greet = random.choice(STANDUP_GREETINGS)
    dow = DAY_RU_UPPER.get(today_d.weekday(), "СЕГОДНЯ")
    return (
        f"{greet}\n\n"
        f"Сегодня <b>{dow}</b> 🗓️\n\n"
        f"Планёрка стартует через <b>15 минут</b> — в <b>09:30 (МСК)</b> ⏰\n\n"
        f'👉 <a href="{zoom_url}">Присоединиться к Zoom</a>\n\n'
        f"Если нужно — можно отменить/перенести ниже 👇"
    )


def build_industry_text(industry_zoom_url: str) -> str:
    return (
        "Коллеги, привет! ☕️✨\n"
        "На горизонте <b>Отраслевая встреча</b> — стартуем через <b>30 минут</b> 🚀\n\n"
        "⏰ Встречаемся в <b>12:00 (МСК)</b>\n\n"
        f'👉 <a href="{industry_zoom_url}">Присоединиться к Zoom</a>\n\n'
        "Если нужно — можно отменить/перенести ниже 👇"
    )


# ---------------- KEYBOARDS ----------------

def kb_cancel_menu(meeting_type: str):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Отменить/перенести 🧩", callback_data=f"cancel:open:{meeting_type}")]
    ])


def kb_cancel_options(meeting_type: str):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Нет срочных тем 💤", callback_data=f"cancel:reason:{meeting_type}:no_topics")],
        [InlineKeyboardButton("Технические причины 🛠️", callback_data=f"cancel:reason:{meeting_type}:tech")],
        [InlineKeyboardButton("Перенести на другой день 📆", callback_data=f"cancel:reason:{meeting_type}:move")],
        [InlineKeyboardButton("Не отменять ✅", callback_data=f"cancel:close:{meeting_type}")],
    ])


def next_mon_wed_fri(from_d: date, count=3):
    res = []
    d = from_d + timedelta(days=1)
    while len(res) < count:
        if d.weekday() in (0, 2, 4):
            res.append(d)
        d += timedelta(days=1)
    return res


def kb_reschedule_dates(meeting_type: str, from_d: date):
    # По ТЗ: варианты ближайших дней ПН/СР/ПТ или ручной ввод
    options = next_mon_wed_fri(from_d, count=3)
    rows = []
    for d in options:
        label = f"{DAY_RU_UPPER.get(d.weekday(), '')} — {d.strftime('%d.%m.%y')}"
        rows.append([InlineKeyboardButton(label, callback_data=f"reschedule:pick:{meeting_type}:{d.strftime('%d.%m.%y')}")])

    rows.append([InlineKeyboardButton("Ввести дату вручную ✍️", callback_data=f"reschedule:manual:{meeting_type}")])
    rows.append([InlineKeyboardButton("Назад ↩️", callback_data=f"cancel:open:{meeting_type}")])
    return InlineKeyboardMarkup(rows)


def kb_manual_input_controls(meeting_type: str):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Отмена ввода даты ❌", callback_data=f"reschedule:cancel_manual:{meeting_type}")]
    ])


# ---------------- ADMIN CHECK ----------------

async def is_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if not update.effective_chat or not update.effective_user:
        return False
    member = await context.bot.get_chat_member(update.effective_chat.id, update.effective_user.id)
    return member.status in ("administrator", "creator")


# ---------------- MANUAL INPUT STATE ----------------

WAITING_DATE_FLAG = "waiting_reschedule_date"
WAITING_PROMPT_MSG_ID = "waiting_prompt_message_id"
WAITING_USER_ID = "waiting_user_id"
WAITING_SINCE_TS = "waiting_since_ts"
WAITING_MEETING_TYPE = "waiting_meeting_type"


def clear_waiting(context: ContextTypes.DEFAULT_TYPE):
    context.chat_data[WAITING_DATE_FLAG] = False
    context.chat_data.pop(WAITING_PROMPT_MSG_ID, None)
    context.chat_data.pop(WAITING_USER_ID, None)
    context.chat_data.pop(WAITING_SINCE_TS, None)
    context.chat_data.pop(WAITING_MEETING_TYPE, None)


# ---------------- DUE RULES ----------------

def standup_due_on_weekday(d: date) -> bool:
    return d.weekday() in (0, 2, 4)  # ПН/СР/ПТ


def industry_due_on_weekday(d: date) -> bool:
    return d.weekday() == 1  # ВТ


# ---------------- CORE SENDERS ----------------

async def send_meeting_message(meeting_type: str, context: ContextTypes.DEFAULT_TYPE, force: bool = False) -> bool:
    today_d = datetime.now(MOSCOW_TZ).date()

    chat_ids = db_list_chats()
    if not chat_ids:
        logger.warning("No chats for notifications. Add via /setchat.")
        return False

    if meeting_type == MEETING_STANDUP:
        weekday_due = standup_due_on_weekday(today_d)
    elif meeting_type == MEETING_INDUSTRY:
        weekday_due = industry_due_on_weekday(today_d)
    else:
        logger.error("Unknown meeting_type: %s", meeting_type)
        return False

    state = db_get_state(meeting_type, today_d)
    standard_due = weekday_due and state["canceled"] != 1

    due_orig_isos = db_get_due_reschedules(meeting_type, today_d)
    reschedule_due = len(due_orig_isos) > 0

    # --- "железобетон" для отраслевой ---
    # Если сегодня обычный вторник (standard_due=True),
    # и при этом есть переносы, попавшие на сегодня,
    # мы НЕ считаем это отдельным поводом для "второй логики".
    # Мы просто отправляем стандартное уведомление один раз,
    # а переносы помечаем sent=1, чтобы они не "висели".
    if meeting_type == MEETING_INDUSTRY and standard_due and reschedule_due:
        db_mark_reschedules_sent(meeting_type, due_orig_isos)
        due_orig_isos = []
        reschedule_due = False

    if not force and not standard_due and not reschedule_due:
        return False

    if meeting_type == MEETING_STANDUP:
        text = build_standup_text(today_d, ZOOM_URL)
    else:
        text = build_industry_text(INDUSTRY_ZOOM_URL)

    for chat_id in chat_ids:
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=kb_cancel_menu(meeting_type),
            )
        except Exception as e:
            logger.exception("Cannot send %s to %s: %s", meeting_type, chat_id, e)

    # если сработали переносы (и мы отправили по этой причине), помечаем их sent
    if reschedule_due:
        db_mark_reschedules_sent(meeting_type, due_orig_isos)

    return True


async def check_and_send_jobs(context: ContextTypes.DEFAULT_TYPE):
    """
    Надёжно по Москве:
    - Планёрка: 09:15
    - Отраслевая: 11:30
    """
    now_msk = datetime.now(MOSCOW_TZ)
    today_iso = now_msk.date().isoformat()

    # Планёрка 09:15
    if now_msk.hour == 9 and now_msk.minute == 15:
        key = "last_auto_sent_date:standup"
        if db_get_meta(key) != today_iso:
            await send_meeting_message(MEETING_STANDUP, context, force=False)
            db_set_meta(key, today_iso)

    # Отраслевая 11:30
    if now_msk.hour == 11 and now_msk.minute == 30:
        key = "last_auto_sent_date:industry"
        if db_get_meta(key) != today_iso:
            await send_meeting_message(MEETING_INDUSTRY, context, force=False)
            db_set_meta(key, today_iso)


# ---------------- COMMANDS ----------------

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.effective_user.first_name if update.effective_user else "коллеги"
    text = (
        f"Привет, {name}! 👋\n\n"
        "Я бот для уведомлений о встречах.\n\n"
        "Команды:\n"
        "• /setchat — подключить этот чат к уведомлениям (только админы)\n"
        "• /unsetchat — отключить этот чат от уведомлений (только админы)\n"
        "• /test_industry — тестовое уведомление отраслевой (только админы)\n"
        "• /reset — сброс ожидания даты (только админы)\n\n"
        "Авто:\n"
        "• Планёрка — ПН/СР/ПТ 09:15 (МСК)\n"
        "• Отраслевая — ВТ 11:30 (МСК)"
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
    await update.message.reply_text("✅ Готово! Этот чат добавлен в рассылку уведомлений.")


async def cmd_unsetchat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type == "private":
        await update.message.reply_text("Эта команда работает только в групповом чате.")
        return
    if not await is_admin(update, context):
        await update.message.reply_text("Только администраторы могут отключить уведомления.")
        return

    db_remove_chat(update.effective_chat.id)
    await update.message.reply_text("🧹 Этот чат убран из рассылки уведомлений.")


async def cmd_test_industry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # тест отраслевой: отправляет "как в бою", но принудительно
    if not await is_admin(update, context):
        await update.message.reply_text("Недостаточно прав.")
        return
    if not db_list_chats():
        await update.message.reply_text("Сначала подключи чат командой /setchat.")
        return

    await send_meeting_message(MEETING_INDUSTRY, context, force=True)
    await update.message.reply_text("🚀 Отправил тестовое уведомление отраслевой встречи.")


async def cmd_reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        return
    was = bool(context.chat_data.get(WAITING_DATE_FLAG, False))
    clear_waiting(context)
    if was:
        await update.message.reply_text("✅ Состояние ожидания даты сброшено.")
    else:
        await update.message.reply_text("ℹ️ Режим ожидания даты не был активен.")


# ---------------- CALLBACKS ----------------

async def cb_cancel_open(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if not await is_admin(update, context):
        await query.answer("Только администраторы могут отменять/переносить.", show_alert=True)
        return

    _, _, meeting_type = query.data.split(":")
    await query.edit_message_reply_markup(reply_markup=kb_cancel_options(meeting_type))


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

    parts = query.data.split(":")
    # cancel:reason:{meeting_type}:{reason}
    meeting_type = parts[2]
    reason_key = parts[3]

    today_d = datetime.now(MOSCOW_TZ).date()

    if reason_key == "no_topics":
        reason_text = "Нет срочных тем для обсуждения"
        db_set_canceled(meeting_type, today_d, reason_text)
        await query.edit_message_reply_markup(reply_markup=None)

        title = "✅ Сегодняшняя планёрка отменена" if meeting_type == MEETING_STANDUP else "✅ Сегодняшняя отраслевая встреча отменена"
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"{title}\nПричина: {reason_text}",
        )
        await query.answer("Отменено.")
        return

    if reason_key == "tech":
        reason_text = "Перенесём по техническим причинам"
        db_set_canceled(meeting_type, today_d, reason_text)
        await query.edit_message_reply_markup(reply_markup=None)

        title = "✅ Сегодняшняя планёрка отменена" if meeting_type == MEETING_STANDUP else "✅ Сегодняшняя отраслевая встреча отменена"
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"{title}\nПричина: {reason_text}",
        )
        await query.answer("Ок.")
        return

    if reason_key == "move":
        await query.edit_message_reply_markup(reply_markup=kb_reschedule_dates(meeting_type, today_d))
        await query.answer("Выберите дату переноса 📆")
        return


async def cb_reschedule_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not await is_admin(update, context):
        await query.answer("Только администраторы.", show_alert=True)
        return

    # reschedule:pick:{meeting_type}:{DD.MM.YY}
    parts = query.data.split(":")
    meeting_type = parts[2]
    picked = parts[3]

    today_d = datetime.now(MOSCOW_TZ).date()

    try:
        dd, mm, yy = picked.split(".")
        new_d = date(int("20" + yy), int(mm), int(dd))
    except Exception:
        await query.answer("Не смог распознать дату.", show_alert=True)
        return

    if new_d <= today_d:
        await query.answer("Дата переноса должна быть в будущем.", show_alert=True)
        return

    db_set_canceled(meeting_type, today_d, "Перенос на другой день", reschedule_date=picked)
    db_upsert_reschedule(meeting_type, today_d, new_d)

    await query.edit_message_reply_markup(reply_markup=None)

    title = "✅ Сегодняшняя планёрка перенесена" if meeting_type == MEETING_STANDUP else "✅ Сегодняшняя отраслевая встреча перенесена"
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=(
            f"{title}\n"
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

    # reschedule:manual:{meeting_type}
    parts = query.data.split(":")
    meeting_type = parts[2]

    context.chat_data[WAITING_DATE_FLAG] = True
    context.chat_data[WAITING_USER_ID] = update.effective_user.id
    context.chat_data[WAITING_SINCE_TS] = int(time.time())
    context.chat_data[WAITING_MEETING_TYPE] = meeting_type

    await query.answer()

    msg = await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=(
            "📅 <b>Введите дату переноса</b>\n\n"
            "Формат: <b>ДД.ММ.ГГ</b>\n"
            "Пример: <code>22.01.26</code>\n\n"
            "Просто отправьте дату сообщением в чат.\n"
            "Если передумали — нажмите «Отмена ввода даты ❌»."
        ),
        parse_mode=ParseMode.HTML,
        reply_markup=kb_manual_input_controls(meeting_type),
    )
    context.chat_data[WAITING_PROMPT_MSG_ID] = msg.message_id


async def cb_cancel_manual_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not await is_admin(update, context):
        await query.answer("❌ Только администраторы.", show_alert=True)
        return

    clear_waiting(context)
    await query.answer("Ок, отменил ввод даты ✅")
    try:
        await query.edit_message_reply_markup(reply_markup=None)
    except Exception:
        pass

    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="✅ Ввод даты отменён. Если нужно — нажмите «Ввести дату» ещё раз.",
    )


# ---------------- MANUAL TEXT INPUT ----------------

async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat:
        return

    if not context.chat_data.get(WAITING_DATE_FLAG):
        return

    user_id = update.effective_user.id if update.effective_user else None
    text = (update.message.text or "").strip()

    waiting_user = context.chat_data.get(WAITING_USER_ID)
    if waiting_user and user_id != waiting_user:
        return

    since_ts = context.chat_data.get(WAITING_SINCE_TS)
    if since_ts and int(time.time()) - int(since_ts) > 10 * 60:
        clear_waiting(context)
        await update.message.reply_text("⏳ Время ожидания даты истекло. Нажмите «Ввести дату» ещё раз.")
        return

    if not await is_admin(update, context):
        clear_waiting(context)
        await update.message.reply_text("❌ Только администраторы могут переносить встречу.")
        return

    if not re.fullmatch(r"\d{2}\.\d{2}\.\d{2}", text):
        await update.message.reply_text("❌ Неверный формат. Нужно ДД.ММ.ГГ (например 22.01.26).")
        return

    try:
        dd, mm, yy = text.split(".")
        new_d = date(int("20" + yy), int(mm), int(dd))
    except Exception:
        await update.message.reply_text("❌ Не удалось распознать дату. Проверьте корректность.")
        return

    today_d = datetime.now(MOSCOW_TZ).date()
    if new_d <= today_d:
        await update.message.reply_text("❌ Дата переноса должна быть в будущем.")
        return

    meeting_type = context.chat_data.get(WAITING_MEETING_TYPE, MEETING_STANDUP)

    db_set_canceled(meeting_type, today_d, "Перенос на другой день", reschedule_date=text)
    db_upsert_reschedule(meeting_type, today_d, new_d)

    clear_waiting(context)

    title = "✅ Сегодняшняя планёрка перенесена" if meeting_type == MEETING_STANDUP else "✅ Сегодняшняя отраслевая встреча перенесена"
    await update.message.reply_text(
        f"{title}\n"
        f"Новая дата: {text} 📌\n"
        "Следите за расписанием или чатом"
    )


# ---------------- APP ----------------

def main():
    db_init()

    app = Application.builder().token(BOT_TOKEN).build()

    # commands
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("setchat", cmd_setchat))
    app.add_handler(CommandHandler("unsetchat", cmd_unsetchat))
    app.add_handler(CommandHandler("test_industry", cmd_test_industry))
    app.add_handler(CommandHandler("reset", cmd_reset))

    # callbacks
    app.add_handler(CallbackQueryHandler(cb_cancel_open, pattern=r"^cancel:open:(standup|industry)$"))
    app.add_handler(CallbackQueryHandler(cb_cancel_close, pattern=r"^cancel:close:(standup|industry)$"))
    app.add_handler(CallbackQueryHandler(cb_cancel_reason, pattern=r"^cancel:reason:(standup|industry):(no_topics|tech|move)$"))
    app.add_handler(CallbackQueryHandler(cb_reschedule_pick, pattern=r"^reschedule:pick:(standup|industry):\d{2}\.\d{2}\.\d{2}$"))
    app.add_handler(CallbackQueryHandler(cb_reschedule_manual, pattern=r"^reschedule:manual:(standup|industry)$"))
    app.add_handler(CallbackQueryHandler(cb_cancel_manual_input, pattern=r"^reschedule:cancel_manual:(standup|industry)$"))

    # manual date input
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    # schedule checks every minute (MSK)
    app.job_queue.run_repeating(check_and_send_jobs, interval=60, first=10, name="meetings_checker")

    logger.info("Bot started. Standup 09:15 MSK; Industry 11:30 MSK.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
