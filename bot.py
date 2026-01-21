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
from telegram.error import Forbidden
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

YA_CRM_URL = os.getenv("YA_CRM_URL", "")
INDUSTRY_WIKI_URL = os.getenv("INDUSTRY_WIKI_URL", "")
HELPY_BOT_URL = os.getenv("HELPY_BOT_URL", "")

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

    # рассылочные чаты
    cur.execute("""
        CREATE TABLE IF NOT EXISTS notify_chats (
            chat_id INTEGER PRIMARY KEY,
            added_at TEXT NOT NULL
        )
    """)

    # состояния встреч
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

    # переносы встреч
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

    # мета
    cur.execute("""
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)

    # ------- HELP MENU: документы -------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS doc_categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL
        )
    """)

    # добавили description (для новых БД)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS docs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            description TEXT,
            file_id TEXT NOT NULL,
            file_unique_id TEXT,
            mime_type TEXT,
            uploaded_at TEXT NOT NULL,
            FOREIGN KEY(category_id) REFERENCES doc_categories(id) ON DELETE CASCADE
        )
    """)

    # миграция для старых БД: добавить description, если поля нет
    try:
        cur.execute("ALTER TABLE docs ADD COLUMN description TEXT")
    except sqlite3.OperationalError:
        pass

    # ------- HELP MENU: анкеты -------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS profiles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            full_name TEXT NOT NULL,
            year_start INTEGER NOT NULL,
            city TEXT NOT NULL,
            about TEXT NOT NULL,
            topics TEXT NOT NULL,
            tg_link TEXT NOT NULL,
            created_at TEXT NOT NULL
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
    cur.execute("SELECT chat_id FROM notify_chats ORDER BY chat_id ASC")
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

# ---------------- HELP DB: DOCS ----------------

def db_docs_list_categories() -> list[tuple[int, str]]:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT id, title FROM doc_categories ORDER BY title COLLATE NOCASE ASC")
    rows = cur.fetchall()
    con.close()
    return [(r[0], r[1]) for r in rows]

def db_docs_add_category(title: str) -> int:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        "INSERT INTO doc_categories(title, created_at) VALUES (?, ?)",
        (title.strip(), datetime.utcnow().isoformat()),
    )
    con.commit()
    cid = cur.lastrowid
    con.close()
    return cid

def db_docs_delete_category_if_empty(category_id: int) -> bool:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT COUNT(*) FROM docs WHERE category_id=?", (category_id,))
    cnt = cur.fetchone()[0]
    if cnt != 0:
        con.close()
        return False
    cur.execute("DELETE FROM doc_categories WHERE id=?", (category_id,))
    con.commit()
    con.close()
    return True

def db_docs_list_by_category(category_id: int) -> list[tuple[int, str]]:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        "SELECT id, title FROM docs WHERE category_id=? ORDER BY id DESC",
        (category_id,),
    )
    rows = cur.fetchall()
    con.close()
    return [(r[0], r[1]) for r in rows]

def db_docs_get(doc_id: int):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        "SELECT id, category_id, title, description, file_id, mime_type FROM docs WHERE id=?",
        (doc_id,),
    )
    row = cur.fetchone()
    con.close()
    if not row:
        return None
    return {
        "id": row[0],
        "category_id": row[1],
        "title": row[2],
        "description": row[3],
        "file_id": row[4],
        "mime": row[5],
    }

def db_docs_add_doc(category_id: int, title: str, description: str | None, file_id: str, file_unique_id: str | None, mime_type: str | None) -> int:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        INSERT INTO docs(category_id, title, description, file_id, file_unique_id, mime_type, uploaded_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (category_id, title.strip(), (description or "").strip() or None, file_id, file_unique_id, mime_type, datetime.utcnow().isoformat()))
    con.commit()
    did = cur.lastrowid
    con.close()
    return did

def db_docs_delete_doc(doc_id: int) -> bool:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("DELETE FROM docs WHERE id=?", (doc_id,))
    deleted = cur.rowcount > 0
    con.commit()
    con.close()
    return deleted

# ---------------- HELP DB: PROFILES ----------------

def db_profiles_list() -> list[tuple[int, str]]:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT id, full_name FROM profiles ORDER BY full_name COLLATE NOCASE ASC")
    rows = cur.fetchall()
    con.close()
    return [(r[0], r[1]) for r in rows]

def db_profiles_get(pid: int):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        SELECT id, full_name, year_start, city, about, topics, tg_link
        FROM profiles
        WHERE id=?
    """, (pid,))
    row = cur.fetchone()
    con.close()
    if not row:
        return None
    return {
        "id": row[0],
        "full_name": row[1],
        "year_start": row[2],
        "city": row[3],
        "about": row[4],
        "topics": row[5],
        "tg_link": row[6],
    }

def db_profiles_add(full_name: str, year_start: int, city: str, about: str, topics: str, tg_link: str) -> int:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        INSERT INTO profiles(full_name, year_start, city, about, topics, tg_link, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (full_name.strip(), int(year_start), city.strip(), about.strip(), topics.strip(), tg_link.strip(), datetime.utcnow().isoformat()))
    con.commit()
    pid = cur.lastrowid
    con.close()
    return pid

def db_profiles_delete(pid: int) -> bool:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("DELETE FROM profiles WHERE id=?", (pid,))
    ok = cur.rowcount > 0
    con.commit()
    con.close()
    return ok

# ---------------- TEXT (meetings) ----------------

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

# ---------------- KEYBOARDS (meetings) ----------------

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

# ---------------- STATES ----------------
# meeting reschedule manual
WAITING_DATE_FLAG = "waiting_reschedule_date"
WAITING_USER_ID = "waiting_user_id"
WAITING_SINCE_TS = "waiting_since_ts"
WAITING_MEETING_TYPE = "waiting_meeting_type"

# docs add flow (добавили WAITING_DOC_DESC)
WAITING_DOC_UPLOAD = "waiting_doc_upload"
WAITING_DOC_DESC = "waiting_doc_desc"
PENDING_DOC_INFO = "pending_doc_info"
WAITING_NEW_CATEGORY_NAME = "waiting_new_category_name"

# profiles add flow
PROFILE_WIZ_ACTIVE = "profile_wiz_active"
PROFILE_WIZ_STEP = "profile_wiz_step"
PROFILE_WIZ_DATA = "profile_wiz_data"

def clear_waiting_date(context: ContextTypes.DEFAULT_TYPE):
    context.chat_data[WAITING_DATE_FLAG] = False
    context.chat_data.pop(WAITING_USER_ID, None)
    context.chat_data.pop(WAITING_SINCE_TS, None)
    context.chat_data.pop(WAITING_MEETING_TYPE, None)

def clear_docs_flow(context: ContextTypes.DEFAULT_TYPE):
    context.chat_data[WAITING_DOC_UPLOAD] = False
    context.chat_data[WAITING_DOC_DESC] = False
    context.chat_data.pop(PENDING_DOC_INFO, None)
    context.chat_data[WAITING_NEW_CATEGORY_NAME] = False

def clear_profile_wiz(context: ContextTypes.DEFAULT_TYPE):
    context.chat_data[PROFILE_WIZ_ACTIVE] = False
    context.chat_data.pop(PROFILE_WIZ_STEP, None)
    context.chat_data.pop(PROFILE_WIZ_DATA, None)

# ---------------- DUE RULES ----------------

def standup_due_on_weekday(d: date) -> bool:
    return d.weekday() in (0, 2, 4)

def industry_due_on_weekday(d: date) -> bool:
    return d.weekday() == 1

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

    if reschedule_due:
        db_mark_reschedules_sent(meeting_type, due_orig_isos)

    return True


async def check_and_send_jobs(context: ContextTypes.DEFAULT_TYPE):
    now_msk = datetime.now(MOSCOW_TZ)
    today_iso = now_msk.date().isoformat()

    if now_msk.hour == 9 and now_msk.minute == 15:
        key = "last_auto_sent_date:standup"
        if db_get_meta(key) != today_iso:
            await send_meeting_message(MEETING_STANDUP, context, force=False)
            db_set_meta(key, today_iso)

    if now_msk.hour == 11 and now_msk.minute == 30:
        key = "last_auto_sent_date:industry"
        if db_get_meta(key) != today_iso:
            await send_meeting_message(MEETING_INDUSTRY, context, force=False)
            db_set_meta(key, today_iso)

# ---------------- HELP MENUS ----------------

def help_text_main(bot_username: str) -> str:
    return (
        "🤖 <b>Меню «Помогатор Говорун»</b>\n"
        "Тут собраны актуальные материалы для команды:\n"
        "— 📄 Документы\n"
        "— 🔗 Полезные ссылки\n"
        "— 👥 Познакомиться с командой\n\n"
    )

def kb_help_main(include_settings: bool = True):
    rows = [
        [InlineKeyboardButton("📄 Документы", callback_data="help:docs")],
        [InlineKeyboardButton("🔗 Полезные ссылки", callback_data="help:links")],
        [InlineKeyboardButton("👥 Познакомиться с командой", callback_data="help:team")],
    ]
    if include_settings:
        rows.append([InlineKeyboardButton("⚙️ Настройки", callback_data="help:settings")])
    return InlineKeyboardMarkup(rows)

def kb_help_docs_categories(is_admin_user: bool):
    cats = db_docs_list_categories()
    rows = []
    if not cats:
        rows.append([InlineKeyboardButton("— категорий нет —", callback_data="noop")])
    else:
        for cid, title in cats:
            rows.append([InlineKeyboardButton(title, callback_data=f"help:docs:cat:{cid}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="help:main")])
    return InlineKeyboardMarkup(rows)

def kb_help_docs_files(category_id: int):
    items = db_docs_list_by_category(category_id)
    rows = []
    if not items:
        rows.append([InlineKeyboardButton("— файлов нет —", callback_data="noop")])
    else:
        for did, title in items[:40]:
            rows.append([InlineKeyboardButton(title, callback_data=f"help:docs:file:{did}")])
    rows.append([InlineKeyboardButton("⬅️ Назад к категориям", callback_data="help:docs")])
    rows.append([InlineKeyboardButton("🏠 В главное меню", callback_data="help:main")])
    return InlineKeyboardMarkup(rows)

# -------- LINKS (меню + описания) --------

def get_links_catalog() -> dict[str, dict]:
    """
    key -> {title, url, desc}
    """
    catalog = {}

    # добавленная ссылка "Чекко"
    catalog["checko"] = {
        "title": 'Сервис "Чекко" поиск контактов',
        "url": "https://checko.ru/",
        "desc": (
            "Готовишь карточку лида? Отлично! 🚀\n\n"
            "Сервис «Чекко» поможет совершить первый шаг! 🔍\n\n"
            "Поиск ведётся по:\n\n"
            "• Названию компании 🏢\n"
            "• ИНН или ОГРН 📑\n"
            "• Фамилии ИП 👤\n\n"
            "Нашёл контакты? Просто скопируй их и начинай прозвон! 📞✨"
        ),
    }

    if YA_CRM_URL:
        catalog["ya_crm"] = {
            "title": "🌐 YA CRM",
            "url": YA_CRM_URL,
            "desc": "CRM-система для работы с заявками, задачами и клиентскими данными.",
        }
    if INDUSTRY_WIKI_URL:
        catalog["industry_wiki"] = {
            "title": "📊 WIKI Отрасли (презы и спичи)",
            "url": INDUSTRY_WIKI_URL,
            "desc": "Материалы по отрасли: презентации, спичи и полезные справки.",
        }
    if HELPY_BOT_URL:
        catalog["helpy_bot"] = {
            "title": "🛠️ Бот Helpy",
            "url": HELPY_BOT_URL,
            "desc": "Бот поможет с техническими вопросами, связанными с работой.",
        }

    return catalog

def kb_help_links_menu():
    catalog = get_links_catalog()
    rows = []
    if not catalog:
        rows.append([InlineKeyboardButton("— ссылки не настроены —", callback_data="noop")])
    else:
        # сортировка: сверху вниз по длине названия (убывание) — «пирамида»
        items = sorted(catalog.items(), key=lambda kv: len(kv[1]["title"]), reverse=True)
        for key, item in items:
            rows.append([InlineKeyboardButton(item["title"], callback_data=f"help:links:item:{key}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="help:main")])
    return InlineKeyboardMarkup(rows)

def kb_help_link_card(url: str, back_to: str = "help:links"):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔗 Открыть ссылку", url=url)],
        [InlineKeyboardButton("⬅️ Назад", callback_data=back_to)],
    ])

def kb_help_team(is_admin_user: bool):
    people = db_profiles_list()
    rows = []
    if not people:
        rows.append([InlineKeyboardButton("— анкет пока нет —", callback_data="noop")])
    else:
        for pid, name in people[:40]:
            rows.append([InlineKeyboardButton(name, callback_data=f"help:team:person:{pid}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="help:main")])
    return InlineKeyboardMarkup(rows)

def kb_help_profile_card(profile: dict):
    rows = []
    tg = profile["tg_link"].strip()
    if tg:
        if tg.startswith("@"):
            url = f"https://t.me/{tg[1:]}"
        elif tg.startswith("https://t.me/") or tg.startswith("http://t.me/"):
            url = tg
        else:
            if re.fullmatch(r"[A-Za-z0-9_]{4,}", tg):
                url = f"https://t.me/{tg}"
            else:
                url = ""
        if url:
            rows.append([InlineKeyboardButton("🔗 Открыть Telegram", url=url)])
    rows.append([InlineKeyboardButton("⬅️ Назад к списку", callback_data="help:team")])
    rows.append([InlineKeyboardButton("🏠 В главное меню", callback_data="help:main")])
    return InlineKeyboardMarkup(rows)

def kb_help_settings():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Добавить файл", callback_data="help:settings:add_doc")],
        [InlineKeyboardButton("➖ Удалить файл", callback_data="help:settings:del_doc")],
        [InlineKeyboardButton("🗂️ Редактировать категории", callback_data="help:settings:cats")],
        [InlineKeyboardButton("➕ Добавить анкету человека", callback_data="help:settings:add_profile")],
        [InlineKeyboardButton("➖ Удалить анкету человека", callback_data="help:settings:del_profile")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="help:main")],
    ])

def kb_settings_categories():
    cats = db_docs_list_categories()
    rows = [
        [InlineKeyboardButton("➕ Добавить категорию", callback_data="help:settings:cats:add")]
    ]
    if cats:
        rows.append([InlineKeyboardButton("➖ Удалить категорию (только пустую)", callback_data="help:settings:cats:del")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="help:settings")])
    return InlineKeyboardMarkup(rows)

def kb_pick_category_for_new_doc():
    cats = db_docs_list_categories()
    rows = []
    for cid, title in cats:
        rows.append([InlineKeyboardButton(title, callback_data=f"help:settings:add_doc:cat:{cid}")])
    rows.append([InlineKeyboardButton("➕ Создать новую категорию", callback_data="help:settings:add_doc:newcat")])
    rows.append([InlineKeyboardButton("❌ Отмена", callback_data="help:settings:cancel")])
    return InlineKeyboardMarkup(rows)

def kb_pick_doc_to_delete():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        SELECT d.id, c.title, d.title
        FROM docs d
        JOIN doc_categories c ON c.id = d.category_id
        ORDER BY d.id DESC
        LIMIT 30
    """)
    rows_db = cur.fetchall()
    con.close()

    rows = []
    if not rows_db:
        rows.append([InlineKeyboardButton("— файлов нет —", callback_data="noop")])
    else:
        for did, cat_title, doc_title in rows_db:
            rows.append([InlineKeyboardButton(f"{cat_title}: {doc_title}", callback_data=f"help:settings:del_doc:{did}")])

    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="help:settings")])
    return InlineKeyboardMarkup(rows)

def kb_pick_profile_to_delete():
    people = db_profiles_list()
    rows = []
    if not people:
        rows.append([InlineKeyboardButton("— анкет нет —", callback_data="noop")])
    else:
        for pid, name in people[:40]:
            rows.append([InlineKeyboardButton(name, callback_data=f"help:settings:del_profile:{pid}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="help:settings")])
    return InlineKeyboardMarkup(rows)

def kb_cancel_wizard_settings():
    return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="help:settings:cancel")]])

# ---------------- COMMANDS ----------------

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.effective_user.first_name if update.effective_user else "коллеги"
    text = (
        f"Привет, {name}! 👋\n\n"
        "Я бот для уведомлений о встречах и меню /help.\n\n"
        "Команды:\n"
        "• /help — меню «Помогатор» (в ЛС)\n"
        "• /help_admin — меню «Помогатор» с настройками (админы, в группе)\n"
        "• /setchat — подключить чат к уведомлениям (админы)\n"
        "• /unsetchat — отключить уведомления (админы)\n"
        "• /force_standup — принудительно отправить планёрку (админы)\n"
        "• /test_industry — тест отраслевой (админы)\n"
        "• /status — статус (админы)\n"
        "• /reset — сброс ожиданий (админы)\n"
    )
    await update.message.reply_text(text)

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bot_username = (context.bot.username or "blablabird_bot")
    text = help_text_main(bot_username)

    if update.effective_chat and update.effective_chat.type == "private":
        await update.message.reply_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=kb_help_main(include_settings=False),
            disable_web_page_preview=True,
        )
        return

    user_id = update.effective_user.id if update.effective_user else None
    if user_id:
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=text,
                parse_mode=ParseMode.HTML,
                reply_markup=kb_help_main(include_settings=False),
                disable_web_page_preview=True,
            )
            return
        except Forbidden:
            warn_text = (
                "⚠️ Я не могу написать вам в ЛС.\n"
                f"Откройте личку: перейдите к боту @{bot_username} и отправьте /start, "
                "после этого снова нажмите /help в чате."
            )
            msg = await update.message.reply_text(
                warn_text,
                reply_to_message_id=update.message.message_id,
                disable_web_page_preview=True,
            )
            context.job_queue.run_once(
                lambda ctx: ctx.bot.delete_message(chat_id=msg.chat_id, message_id=msg.message_id),
                when=60,
            )
            return
        except Exception as e:
            logger.exception("Failed to DM /help: %s", e)

    await update.message.reply_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=kb_help_main(include_settings=False),
        disable_web_page_preview=True,
        reply_to_message_id=update.message.message_id,
    )

async def cmd_help_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bot_username = (context.bot.username or "blablabird_bot")
    text = help_text_main(bot_username)

    if update.effective_chat and update.effective_chat.type == "private":
        await update.message.reply_text("Эта команда работает в групповом чате для администраторов.")
        return

    if not await is_admin(update, context):
        await update.message.reply_text("❌ Только администраторы могут использовать /help_admin.")
        return

    await update.message.reply_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=kb_help_main(include_settings=True),
        disable_web_page_preview=True,
        reply_to_message_id=update.message.message_id,
    )

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

async def cmd_force_standup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        await update.message.reply_text("Недостаточно прав.")
        return
    if not db_list_chats():
        await update.message.reply_text("Сначала подключи чат командой /setchat.")
        return
    await send_meeting_message(MEETING_STANDUP, context, force=True)
    await update.message.reply_text("🚀 Отправил принудительное уведомление планёрки.")

async def cmd_test_industry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        await update.message.reply_text("Недостаточно прав.")
        return
    if not db_list_chats():
        await update.message.reply_text("Сначала подключи чат командой /setchat.")
        return
    await send_meeting_message(MEETING_INDUSTRY, context, force=True)
    await update.message.reply_text("🚀 Отправил тестовое уведомление отраслевой встречи.")

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        await update.message.reply_text("Только администраторы.")
        return

    now_utc = datetime.utcnow().replace(tzinfo=pytz.UTC)
    now_msk = datetime.now(MOSCOW_TZ)
    today = now_msk.date()

    chats = db_list_chats()
    last_standup = db_get_meta("last_auto_sent_date:standup")
    last_industry = db_get_meta("last_auto_sent_date:industry")

    st_state = db_get_state(MEETING_STANDUP, today)
    in_state = db_get_state(MEETING_INDUSTRY, today)

    st_due_res = db_get_due_reschedules(MEETING_STANDUP, today)
    in_due_res = db_get_due_reschedules(MEETING_INDUSTRY, today)

    def fmt_state(title: str, state: dict, due_res: list[str]) -> str:
        if state["canceled"] == 1:
            reason = state["reason"] or "—"
            rs = state["reschedule_date"]
            if rs:
                return f"• <b>{title}</b>: ❌ отменено/перенесено сегодня\n  Причина: {reason}\n  Новая дата: {rs}"
            return f"• <b>{title}</b>: ❌ отменено сегодня\n  Причина: {reason}"
        else:
            extra = ""
            if due_res:
                extra = f"\n  Переносы на сегодня (sent=0): {', '.join(due_res)}"
            return f"• <b>{title}</b>: ✅ активно{extra}"

    text = (
        "📊 <b>Статус бота</b>\n\n"
        f"🕒 UTC: <code>{now_utc.strftime('%Y-%m-%d %H:%M:%S')}</code>\n"
        f"🕒 МСК: <code>{now_msk.strftime('%Y-%m-%d %H:%M:%S')}</code>\n"
        f"📅 Сегодня (МСК): <b>{DAY_RU_UPPER.get(today.weekday(), '—')}</b> <code>{today.strftime('%d.%m.%y')}</code>\n\n"
        f"💬 Подключённых чатов: <b>{len(chats)}</b>\n\n"
        f"📌 Последняя авто-отправка:\n"
        f"• Планёрка: <code>{last_standup or '—'}</code>\n"
        f"• Отраслевая: <code>{last_industry or '—'}</code>\n\n"
        f"🗂️ Состояние на сегодня:\n"
        f"{fmt_state('Планёрка', st_state, st_due_res)}\n"
        f"{fmt_state('Отраслевая', in_state, in_due_res)}\n"
    )

    await update.message.reply_text(text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)

async def cmd_reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        return
    clear_waiting_date(context)
    clear_docs_flow(context)
    clear_profile_wiz(context)
    await update.message.reply_text("✅ Сбросил состояния ожидания (дата/документы/анкеты).")

# ---------------- CALLBACKS: meetings cancel/reschedule ----------------

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
    meeting_type = parts[2]
    reason_key = parts[3]
    today_d = datetime.now(MOSCOW_TZ).date()

    if reason_key == "no_topics":
        reason_text = "Нет срочных тем для обсуждения"
        db_set_canceled(meeting_type, today_d, reason_text)
        await query.edit_message_reply_markup(reply_markup=None)
        title = "✅ Сегодняшняя планёрка отменена" if meeting_type == MEETING_STANDUP else "✅ Сегодняшняя отраслевая встреча отменена"
        await context.bot.send_message(chat_id=update.effective_chat.id, text=f"{title}\nПричина: {reason_text}")
        await query.answer("Отменено.")
        return

    if reason_key == "tech":
        reason_text = "Перенесём по техническим причинам"
        db_set_canceled(meeting_type, today_d, reason_text)
        await query.edit_message_reply_markup(reply_markup=None)
        title = "✅ Сегодняшняя планёрка отменена" if meeting_type == MEETING_STANDUP else "✅ Сегодняшняя отраслевая встреча отменена"
        await context.bot.send_message(chat_id=update.effective_chat.id, text=f"{title}\nПричина: {reason_text}")
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
        text=f"{title}\nНовая дата: {picked} 📌\nСледите за расписанием или чатом"
    )
    await query.answer("Перенесено.")

async def cb_reschedule_manual(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not await is_admin(update, context):
        await query.answer("❌ Только администраторы.", show_alert=True)
        return

    parts = query.data.split(":")
    meeting_type = parts[2]

    context.chat_data[WAITING_DATE_FLAG] = True
    context.chat_data[WAITING_USER_ID] = update.effective_user.id
    context.chat_data[WAITING_SINCE_TS] = int(time.time())
    context.chat_data[WAITING_MEETING_TYPE] = meeting_type

    await query.answer()
    await context.bot.send_message(
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

async def cb_cancel_manual_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not await is_admin(update, context):
        await query.answer("❌ Только администраторы.", show_alert=True)
        return
    clear_waiting_date(context)
    await query.answer("Ок, отменил ввод даты ✅")
    try:
        await query.edit_message_reply_markup(reply_markup=None)
    except Exception:
        pass
    await context.bot.send_message(chat_id=update.effective_chat.id, text="✅ Ввод даты отменён.")

# ---------------- CALLBACKS: HELP ----------------

async def cb_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    data = q.data
    await q.answer()

    if data == "noop":
        return

    is_private = bool(update.effective_chat and update.effective_chat.type == "private")
    is_adm = (False if is_private else await is_admin(update, context))
    include_settings = (not is_private) and is_adm

    if data == "help:main":
        bot_username = (context.bot.username or "blablabird_bot")
        await q.edit_message_text(
            help_text_main(bot_username),
            parse_mode=ParseMode.HTML,
            reply_markup=kb_help_main(include_settings=include_settings),
            disable_web_page_preview=True,
        )
        return

    if data == "help:docs":
        text = (
            "📄 <b>Документы</b>\n\n"
            "Выберите категорию — внутри будут файлы.\n"
            "Нажмите на файл, чтобы получить его в чат."
        )
        await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb_help_docs_categories(is_adm))
        return

    if data.startswith("help:docs:cat:"):
        cid = int(data.split(":")[-1])
        cats = dict(db_docs_list_categories())
        title = cats.get(cid, "Категория")
        text = f"📄 <b>{title}</b>\n\nВыберите файл:"
        await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb_help_docs_files(cid))
        return

    if data.startswith("help:docs:file:"):
        doc_id = int(data.split(":")[-1])
        doc = db_docs_get(doc_id)
        if not doc:
            await q.edit_message_text("Файл не найден (возможно удалён).", reply_markup=kb_help_main(include_settings=include_settings))
            return
        try:
            caption = f"📄 <b>{doc['title']}</b>"
            if doc.get("description"):
                caption += f"\n\n{doc['description']}"
            await context.bot.send_document(
                chat_id=update.effective_chat.id,
                document=doc["file_id"],
                caption=caption,
                parse_mode=ParseMode.HTML,
            )
        except Exception as e:
            logger.exception("send_document failed: %s", e)
            await context.bot.send_message(chat_id=update.effective_chat.id, text="Не смог отправить файл 😕")
        return

    if data == "help:links":
        text = (
            "🔗 <b>Полезные ссылки</b>\n\n"
            "Выберите ресурс — покажу описание и дам кнопку «Открыть»."
        )
        await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb_help_links_menu(), disable_web_page_preview=True)
        return

    if data.startswith("help:links:item:"):
        key = data.split(":")[-1]
        catalog = get_links_catalog()
        item = catalog.get(key)
        if not item:
            await q.answer("Ссылка не найдена.", show_alert=True)
            return

        # делаем кликабельную ссылку прямо в тексте, плюс кнопка
        url = item["url"]
        title = item["title"]
        desc = item["desc"]

        text = (
            f"<b>{title}</b>\n\n"
            f"{desc}\n\n"
            f'Ссылка: <a href="{url}">{url}</a>'
        )
        await q.edit_message_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=kb_help_link_card(url, back_to="help:links"),
            disable_web_page_preview=True,
        )
        return

    if data == "help:team":
        text = "👥 <b>Познакомиться с командой</b>\n\nВыберите человека:"
        await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb_help_team(is_adm))
        return

    if data.startswith("help:team:person:"):
        pid = int(data.split(":")[-1])
        p = db_profiles_get(pid)
        if not p:
            await q.edit_message_text("Анкета не найдена (возможно удалена).", reply_markup=kb_help_team(is_adm))
            return
        card = (
            f"👤 <b>{p['full_name']}</b>\n"
            f"📅 Работает с: <b>{p['year_start']}</b>\n"
            f"🏙️ Город: <b>{p['city']}</b>\n\n"
            f"📝 <b>Кратко о себе</b>\n{p['about']}\n\n"
            f"❓ <b>По каким вопросам обращаться</b>\n{p['topics']}\n\n"
            f"🔗 TG: {p['tg_link']}"
        )
        await q.edit_message_text(card, parse_mode=ParseMode.HTML, reply_markup=kb_help_profile_card(p), disable_web_page_preview=True)
        return

    if data == "help:settings":
        if is_private:
            await q.answer("⚠️ Настройки доступны только в групповом чате через /help_admin.", show_alert=True)
            return
        if not is_adm:
            await q.answer("⚠️ Кнопка доступна администраторам чата. Обратитесь к ним 🙂", show_alert=True)
            return
        text = (
            "⚙️ <b>Настройки</b>\n\n"
            "Управление документами, категориями и анкетами."
        )
        await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb_help_settings())
        return

    # дальше — настройки (только админы)
    if data.startswith("help:settings:"):
        if is_private:
            await q.answer("⚠️ Настройки доступны только в групповом чате через /help_admin.", show_alert=True)
            return
        if not is_adm:
            await q.answer("⚠️ Доступно администраторам чата.", show_alert=True)
            return

        if data == "help:settings:cancel":
            clear_docs_flow(context)
            clear_profile_wiz(context)
            clear_waiting_date(context)
            await q.edit_message_text("✅ Действие отменено.", reply_markup=kb_help_settings(), parse_mode=ParseMode.HTML)
            return

        if data == "help:settings:cats":
            await q.edit_message_text(
                "🗂️ <b>Категории документов</b>\n\n"
                "• ➕ Добавить категорию — бот попросит название\n"
                "• ➖ Удалить категорию — удаляется только пустая",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_settings_categories(),
            )
            return

        if data == "help:settings:cats:add":
            clear_docs_flow(context)
            context.chat_data[WAITING_NEW_CATEGORY_NAME] = True
            context.chat_data[WAITING_USER_ID] = update.effective_user.id
            context.chat_data[WAITING_SINCE_TS] = int(time.time())
            await q.edit_message_text(
                "➕ <b>Добавление категории</b>\n\n"
                "Отправьте название категории одним сообщением.\n"
                "Пример: <code>Регламенты</code>",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_cancel_wizard_settings(),
            )
            return

        if data == "help:settings:cats:del":
            cats = db_docs_list_categories()
            rows = []
            for cid, title in cats:
                rows.append([InlineKeyboardButton(f"🗑️ {title}", callback_data=f"help:settings:cats:del:{cid}")])
            rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="help:settings:cats")])
            await q.edit_message_text(
                "➖ <b>Удаление категории</b>\n\nУдаляется только пустая категория (без файлов).",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(rows),
            )
            return

        if data.startswith("help:settings:cats:del:"):
            cid = int(data.split(":")[-1])
            ok = db_docs_delete_category_if_empty(cid)
            if ok:
                await q.answer("Удалено ✅")
                await q.edit_message_text("✅ Категория удалена.", reply_markup=kb_settings_categories(), parse_mode=ParseMode.HTML)
            else:
                await q.answer("Нельзя: категория не пустая", show_alert=True)
            return

        if data == "help:settings:add_doc":
            clear_docs_flow(context)
            context.chat_data[WAITING_DOC_UPLOAD] = True
            context.chat_data[WAITING_DOC_DESC] = False
            context.chat_data[WAITING_USER_ID] = update.effective_user.id
            context.chat_data[WAITING_SINCE_TS] = int(time.time())
            await q.edit_message_text(
                "➕ <b>Добавление файла</b>\n\n"
                "1) Отправьте документ (как файл) следующим сообщением.\n"
                "   • Название можно указать в подписи к файлу (caption)\n"
                "2) Затем бот попросит краткое описание\n"
                "3) Потом выберем категорию",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_cancel_wizard_settings(),
            )
            return

        if data == "help:settings:del_doc":
            clear_docs_flow(context)
            await q.edit_message_text(
                "➖ <b>Удаление файла</b>\n\nВыберите файл из последних добавленных:",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_pick_doc_to_delete(),
            )
            return

        if data.startswith("help:settings:del_doc:"):
            did = int(data.split(":")[-1])
            ok = db_docs_delete_doc(did)
            if ok:
                await q.answer("Удалено ✅")
                await q.edit_message_text("✅ Файл удалён.", parse_mode=ParseMode.HTML, reply_markup=kb_help_settings())
            else:
                await q.answer("Не найден", show_alert=True)
            return

        if data.startswith("help:settings:add_doc:cat:"):
            cid = int(data.split(":")[-1])
            pending = context.chat_data.get(PENDING_DOC_INFO)
            if not pending:
                await q.answer("Нет загруженного файла. Начните заново.", show_alert=True)
                return
            db_docs_add_doc(
                cid,
                pending["title"],
                pending.get("description"),
                pending["file_id"],
                pending.get("file_unique_id"),
                pending.get("mime"),
            )
            clear_docs_flow(context)
            await q.edit_message_text("✅ Файл добавлен в документы.", parse_mode=ParseMode.HTML, reply_markup=kb_help_settings())
            return

        if data == "help:settings:add_doc:newcat":
            pending = context.chat_data.get(PENDING_DOC_INFO)
            if not pending:
                await q.answer("Сначала отправьте файл.", show_alert=True)
                return
            context.chat_data[WAITING_NEW_CATEGORY_NAME] = True
            context.chat_data[WAITING_USER_ID] = update.effective_user.id
            context.chat_data[WAITING_SINCE_TS] = int(time.time())
            await q.edit_message_text(
                "➕ <b>Новая категория</b>\n\n"
                "Отправьте название категории одним сообщением.\n"
                "После этого файл будет сохранён в неё.",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_cancel_wizard_settings(),
            )
            return

        if data == "help:settings:add_profile":
            clear_profile_wiz(context)
            context.chat_data[PROFILE_WIZ_ACTIVE] = True
            context.chat_data[PROFILE_WIZ_STEP] = "full_name"
            context.chat_data[PROFILE_WIZ_DATA] = {}
            context.chat_data[WAITING_USER_ID] = update.effective_user.id
            context.chat_data[WAITING_SINCE_TS] = int(time.time())
            await q.edit_message_text(
                "➕ <b>Добавление анкеты</b>\n\n"
                "Шаг 1/6: отправьте <b>Имя и Фамилию</b>.\n"
                "Пример: <code>Иван Петров</code>",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_cancel_wizard_settings(),
            )
            return

        if data == "help:settings:del_profile":
            clear_profile_wiz(context)
            await q.edit_message_text(
                "➖ <b>Удаление анкеты</b>\n\nВыберите человека:",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_pick_profile_to_delete(),
            )
            return

        if data.startswith("help:settings:del_profile:"):
            pid = int(data.split(":")[-1])
            ok = db_profiles_delete(pid)
            if ok:
                await q.answer("Удалено ✅")
                await q.edit_message_text("✅ Анкета удалена.", parse_mode=ParseMode.HTML, reply_markup=kb_help_settings())
            else:
                await q.answer("Не найдено", show_alert=True)
            return

    await q.answer()

# ---------------- HANDLERS: DOCUMENT UPLOAD ----------------

async def on_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat:
        return

    if not context.chat_data.get(WAITING_DOC_UPLOAD):
        return

    user_id = update.effective_user.id if update.effective_user else None
    waiting_user = context.chat_data.get(WAITING_USER_ID)
    if waiting_user and user_id != waiting_user:
        return

    if not await is_admin(update, context):
        clear_docs_flow(context)
        await update.message.reply_text("❌ Только администраторы могут добавлять документы.")
        return

    doc = update.message.document
    if not doc:
        return

    title = (update.message.caption or "").strip() or (doc.file_name or "Документ")
    pending = {
        "file_id": doc.file_id,
        "file_unique_id": doc.file_unique_id,
        "mime": doc.mime_type,
        "title": title[:120],
        "description": None,
    }
    context.chat_data[PENDING_DOC_INFO] = pending
    context.chat_data[WAITING_DOC_UPLOAD] = False

    context.chat_data[WAITING_DOC_DESC] = True

    await update.message.reply_text(
        "✍️ <b>Краткое описание</b>\n\n"
        "Отправьте 1–2 предложения, чтобы коллегам было понятно, что внутри.\n"
        "Если описания не нужно — отправьте <code>-</code>",
        parse_mode=ParseMode.HTML,
        reply_markup=kb_cancel_wizard_settings(),
    )

# ---------------- HANDLERS: TEXT INPUT (dates / categories / profiles / doc desc) ----------------

async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat:
        return

    user_id = update.effective_user.id if update.effective_user else None
    text = (update.message.text or "").strip()

    waiting_user = context.chat_data.get(WAITING_USER_ID)
    if waiting_user and user_id != waiting_user:
        return

    since_ts = context.chat_data.get(WAITING_SINCE_TS)
    if since_ts and int(time.time()) - int(since_ts) > 10 * 60:
        clear_waiting_date(context)
        clear_docs_flow(context)
        clear_profile_wiz(context)
        await update.message.reply_text("⏳ Время ожидания истекло. Начните действие заново через /help_admin.")
        return

    if context.chat_data.get(WAITING_DOC_DESC):
        if not await is_admin(update, context):
            clear_docs_flow(context)
            await update.message.reply_text("❌ Только администраторы могут добавлять документы.")
            return

        pending = context.chat_data.get(PENDING_DOC_INFO)
        if not pending:
            clear_docs_flow(context)
            await update.message.reply_text("❌ Не найден загруженный файл. Начните заново через /help_admin.")
            return

        desc = None if text == "-" else text
        if desc is not None:
            desc = desc.strip()
            if len(desc) < 3:
                await update.message.reply_text("❌ Описание слишком короткое. Напишите чуть подробнее или отправьте <code>-</code>.", parse_mode=ParseMode.HTML)
                return
            desc = desc[:600]

        pending["description"] = desc
        context.chat_data[PENDING_DOC_INFO] = pending
        context.chat_data[WAITING_DOC_DESC] = False

        await update.message.reply_text(
            "✅ Описание сохранено.\n\nТеперь выберите категорию для сохранения:",
            reply_markup=kb_pick_category_for_new_doc(),
        )
        return

    # остальные ветки on_text — без изменений (переносы/категории/анкеты)
    # ... (оставлены как в предыдущей версии, сокращать нельзя — тут полный код ниже)

    if context.chat_data.get(WAITING_DATE_FLAG):
        if not await is_admin(update, context):
            clear_waiting_date(context)
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
        clear_waiting_date(context)

        title = "✅ Сегодняшняя планёрка перенесена" if meeting_type == MEETING_STANDUP else "✅ Сегодняшняя отраслевая встреча перенесена"
        await update.message.reply_text(f"{title}\nНовая дата: {text} 📌\nСледите за расписанием или чатом")
        return

    if context.chat_data.get(WAITING_NEW_CATEGORY_NAME):
        if not await is_admin(update, context):
            clear_docs_flow(context)
            await update.message.reply_text("❌ Только администраторы могут управлять категориями.")
            return

        if len(text) < 2:
            await update.message.reply_text("❌ Слишком коротко. Отправьте нормальное название категории.")
            return

        try:
            cid = db_docs_add_category(text)
        except sqlite3.IntegrityError:
            await update.message.reply_text("❌ Такая категория уже существует. Отправьте другое название.")
            return

        context.chat_data[WAITING_NEW_CATEGORY_NAME] = False

        pending = context.chat_data.get(PENDING_DOC_INFO)
        if pending:
            db_docs_add_doc(
                cid,
                pending["title"],
                pending.get("description"),
                pending["file_id"],
                pending.get("file_unique_id"),
                pending.get("mime"),
            )
            clear_docs_flow(context)
            await update.message.reply_text("✅ Категория создана и файл добавлен.", reply_markup=kb_help_settings())
            return

        clear_docs_flow(context)
        await update.message.reply_text("✅ Категория добавлена.", reply_markup=kb_help_settings())
        return

    if context.chat_data.get(PROFILE_WIZ_ACTIVE):
        if not await is_admin(update, context):
            clear_profile_wiz(context)
            await update.message.reply_text("❌ Только администраторы могут добавлять анкеты.")
            return

        step = context.chat_data.get(PROFILE_WIZ_STEP)
        data = context.chat_data.get(PROFILE_WIZ_DATA) or {}

        if step == "full_name":
            if len(text.split()) < 2:
                await update.message.reply_text("❌ Нужно имя и фамилия. Пример: Иван Петров")
                return
            data["full_name"] = text
            context.chat_data[PROFILE_WIZ_DATA] = data
            context.chat_data[PROFILE_WIZ_STEP] = "year_start"
            await update.message.reply_text("Шаг 2/6: с какого года работает? Пример: 2022", reply_markup=kb_cancel_wizard_settings())
            return

        if step == "year_start":
            if not re.fullmatch(r"\d{4}", text):
                await update.message.reply_text("❌ Введите год 4 цифрами. Пример: 2022")
                return
            year = int(text)
            cur_year = datetime.now(MOSCOW_TZ).year
            if year < 1990 or year > cur_year:
                await update.message.reply_text(f"❌ Год должен быть в диапазоне 1990–{cur_year}.")
                return
            data["year_start"] = year
            context.chat_data[PROFILE_WIZ_DATA] = data
            context.chat_data[PROFILE_WIZ_STEP] = "city"
            await update.message.reply_text("Шаг 3/6: город проживания. Пример: Москва", reply_markup=kb_cancel_wizard_settings())
            return

        if step == "city":
            if len(text) < 2:
                await update.message.reply_text("❌ Укажите город.")
                return
            data["city"] = text
            context.chat_data[PROFILE_WIZ_DATA] = data
            context.chat_data[PROFILE_WIZ_STEP] = "about"
            await update.message.reply_text("Шаг 4/6: кратко о себе (1–3 предложения)", reply_markup=kb_cancel_wizard_settings())
            return

        if step == "about":
            if len(text) < 5:
                await update.message.reply_text("❌ Напишите чуть подробнее 🙂")
                return
            data["about"] = text
            context.chat_data[PROFILE_WIZ_DATA] = data
            context.chat_data[PROFILE_WIZ_STEP] = "topics"
            await update.message.reply_text("Шаг 5/6: по каким вопросам обращаться?", reply_markup=kb_cancel_wizard_settings())
            return

        if step == "topics":
            if len(text) < 3:
                await update.message.reply_text("❌ Укажите темы/вопросы.")
                return
            data["topics"] = text
            context.chat_data[PROFILE_WIZ_DATA] = data
            context.chat_data[PROFILE_WIZ_STEP] = "tg_link"
            await update.message.reply_text("Шаг 6/6: Telegram (@username или https://t.me/username)", reply_markup=kb_cancel_wizard_settings())
            return

        if step == "tg_link":
            tg = text.strip()
            ok = False
            if tg.startswith("@") and re.fullmatch(r"@[A-Za-z0-9_]{4,}", tg):
                ok = True
            if tg.startswith("https://t.me/") or tg.startswith("http://t.me/"):
                ok = True
            if re.fullmatch(r"[A-Za-z0-9_]{4,}", tg):
                ok = True
            if not ok:
                await update.message.reply_text("❌ Не похоже на Telegram. Дайте @username или https://t.me/username")
                return

            data["tg_link"] = tg

            pid = db_profiles_add(
                full_name=data["full_name"],
                year_start=data["year_start"],
                city=data["city"],
                about=data["about"],
                topics=data["topics"],
                tg_link=data["tg_link"],
            )

            clear_profile_wiz(context)
            await update.message.reply_text(f"✅ Анкета добавлена (ID {pid}).\nСмотри /help_admin → Команда", reply_markup=kb_help_settings())
            return

# ---------------- APP ----------------

def main():
    db_init()

    app = Application.builder().token(BOT_TOKEN).build()

    # commands
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("help_admin", cmd_help_admin))
    app.add_handler(CommandHandler("setchat", cmd_setchat))
    app.add_handler(CommandHandler("unsetchat", cmd_unsetchat))
    app.add_handler(CommandHandler("force_standup", cmd_force_standup))
    app.add_handler(CommandHandler("test_industry", cmd_test_industry))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("reset", cmd_reset))

    # callbacks: meetings
    app.add_handler(CallbackQueryHandler(cb_cancel_open, pattern=r"^cancel:open:(standup|industry)$"))
    app.add_handler(CallbackQueryHandler(cb_cancel_close, pattern=r"^cancel:close:(standup|industry)$"))
    app.add_handler(CallbackQueryHandler(cb_cancel_reason, pattern=r"^cancel:reason:(standup|industry):(no_topics|tech|move)$"))
    app.add_handler(CallbackQueryHandler(cb_reschedule_pick, pattern=r"^reschedule:pick:(standup|industry):\d{2}\.\d{2}\.\d{2}$"))
    app.add_handler(CallbackQueryHandler(cb_reschedule_manual, pattern=r"^reschedule:manual:(standup|industry)$"))
    app.add_handler(CallbackQueryHandler(cb_cancel_manual_input, pattern=r"^reschedule:cancel_manual:(standup|industry)$"))

    # callbacks: help
    app.add_handler(CallbackQueryHandler(cb_help, pattern=r"^(help:|noop)"))

    # document upload
    app.add_handler(MessageHandler(filters.Document.ALL, on_document))

    # text input
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    # schedule checker
    app.job_queue.run_repeating(check_and_send_jobs, interval=60, first=10, name="meetings_checker")

    logger.info("Bot started. Standup 09:15 MSK; Industry 11:30 MSK; /help DM-first enabled; /help_admin for admins.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
