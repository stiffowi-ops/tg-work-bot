import os
import re
import random
import sqlite3
import logging
import time
import csv
import io
from pathlib import Path
from datetime import datetime, date, timedelta

import pytz
from dotenv import load_dotenv

from telegram import (
    Update,
    InputMediaPhoto,
    InputMediaVideo,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.constants import ParseMode
from telegram.error import Forbidden, TimedOut, NetworkError
from telegram.helpers import escape
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from telegram.request import HTTPXRequest

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("meetings-bot")

BOT_TOKEN = os.getenv("BOT_TOKEN")
ZOOM_URL = os.getenv("ZOOM_URL")  # планёрка
INDUSTRY_ZOOM_URL = os.getenv("INDUSTRY_ZOOM_URL")  # отраслевая

# ✅ поддержка DATABASE_PATH и DB_PATH
DB_PATH = os.getenv("DATABASE_PATH") or os.getenv("DB_PATH", "bot.db")

STORAGE_DIR = os.getenv("STORAGE_DIR", "storage")

INDUSTRY_WIKI_URL = os.getenv("INDUSTRY_WIKI_URL", "")
STAFF_URL = os.getenv("STAFF_URL", "")
SITE_URL = os.getenv("SITE_URL", "")
LITE_FORM_URL = os.getenv("LITE_FORM_URL", "")
LEAD_CRM_URL = os.getenv("LEAD_CRM_URL", "")
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

# где хранить контекст, из какого чата пользователь открыл /help
HELP_SCOPE_CHAT_ID = "help_scope_chat_id"

# ---------------- DB PATH ENSURE ----------------

def ensure_db_path(db_path: str):
    """
    Создаёт директорию под SQLite файл, если её нет.
    Пишет понятный лог, где именно хранится БД и есть ли права на запись.
    """
    if not db_path:
        raise RuntimeError("DATABASE_PATH/DB_PATH is empty")

    if db_path == ":memory:":
        return

    abs_path = os.path.abspath(db_path) if not os.path.isabs(db_path) else db_path
    db_dir = os.path.dirname(abs_path)

    logger.info("SQLite DB path: %s", abs_path)
    logger.info("SQLite DB dir : %s", db_dir or "(current dir)")

    if db_dir and not os.path.exists(db_dir):
        logger.info("DB dir does not exist -> creating: %s", db_dir)
        os.makedirs(db_dir, exist_ok=True)

    # тест прав на запись
    try:
        if db_dir:
            test_file = os.path.join(db_dir, ".write_test")
            with open(test_file, "w", encoding="utf-8") as f:
                f.write("ok")
            os.remove(test_file)
    except Exception as e:
        logger.exception("No write access to DB directory: %s", e)
        raise


def ensure_storage_dir(base_dir: str):
    """Создаёт директорию для локального хранения файлов (бэкапы из Telegram)."""
    if not base_dir:
        raise RuntimeError("STORAGE_DIR is empty")
    Path(base_dir).mkdir(parents=True, exist_ok=True)
    Path(base_dir, "docs").mkdir(parents=True, exist_ok=True)



async def job_delete_message(context: ContextTypes.DEFAULT_TYPE):
    """Удаляет сообщение, параметры лежат в context.job.data"""
    data = getattr(context.job, "data", None) or {}
    chat_id = data.get("chat_id")
    message_id = data.get("message_id")
    if not chat_id or not message_id:
        return
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        # не критично (нет прав/сообщение уже удалено)
        pass

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

    # rate-limit предложки
    cur.execute("""
        CREATE TABLE IF NOT EXISTS suggest_rate (
            user_id INTEGER PRIMARY KEY,
            last_sent_ts INTEGER NOT NULL
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

    # docs + description
    cur.execute("""
        CREATE TABLE IF NOT EXISTS docs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            description TEXT,
            file_id TEXT NOT NULL,
            file_unique_id TEXT,
            mime_type TEXT,
            local_path TEXT,
            uploaded_at TEXT NOT NULL,
            FOREIGN KEY(category_id) REFERENCES doc_categories(id) ON DELETE CASCADE
        )
    """)

    # миграция для старых БД
    try:
        cur.execute("ALTER TABLE docs ADD COLUMN description TEXT")
    except sqlite3.OperationalError:
        pass

    # миграция для старых БД: local_path (локальный бэкап файла)
    try:
        cur.execute("ALTER TABLE docs ADD COLUMN local_path TEXT")
    except sqlite3.OperationalError:
        pass

    # ------- HELP MENU: анкеты -------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS profiles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            full_name TEXT NOT NULL,
            year_start INTEGER NOT NULL,
            city TEXT NOT NULL,
            birthday TEXT,
            about TEXT NOT NULL,
            topics TEXT NOT NULL,
            tg_link TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)

    # миграция для старых БД: birthday
    try:
        cur.execute("ALTER TABLE profiles ADD COLUMN birthday TEXT")
    except sqlite3.OperationalError:
        pass

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


def db_get_suggest_last_ts(user_id: int) -> int | None:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT last_sent_ts FROM suggest_rate WHERE user_id=?", (int(user_id),))
    row = cur.fetchone()
    con.close()
    return int(row[0]) if row else None

def db_set_suggest_last_ts(user_id: int, ts: int):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        INSERT INTO suggest_rate(user_id, last_sent_ts)
        VALUES(?, ?)
        ON CONFLICT(user_id) DO UPDATE SET last_sent_ts=excluded.last_sent_ts
    """, (int(user_id), int(ts)))
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
        "SELECT id, category_id, title, description, file_id, file_unique_id, mime_type, local_path FROM docs WHERE id=?",
        (doc_id,),
    )
    row = cur.fetchone()
    con.close()
    if not row:
        return None
    return {"id": row[0], "category_id": row[1], "title": row[2], "description": row[3], "file_id": row[4], "file_unique_id": row[5], "mime": row[6], "local_path": row[7]}

def db_docs_add_doc(category_id: int, title: str, description: str | None, file_id: str, file_unique_id: str | None, mime_type: str | None, local_path: str | None) -> int:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        INSERT INTO docs(category_id, title, description, file_id, file_unique_id, mime_type, local_path, uploaded_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (category_id, title.strip(), (description or "").strip() or None, file_id, file_unique_id, mime_type, (local_path or None), datetime.utcnow().isoformat()))
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



def db_docs_get_category_id_by_title(title: str) -> int | None:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT id FROM doc_categories WHERE title=?", (title.strip(),))
    row = cur.fetchone()
    con.close()
    return int(row[0]) if row else None

def db_docs_ensure_category(title: str) -> int:
    cid = db_docs_get_category_id_by_title(title)
    if cid:
        return cid
    return db_docs_add_category(title)

def db_docs_get_by_file_unique_id(file_unique_id: str):
    if not file_unique_id:
        return None
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        "SELECT id, category_id, title, description, file_id, file_unique_id, mime_type, local_path FROM docs WHERE file_unique_id=?",
        (file_unique_id,),
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
        "file_unique_id": row[5],
        "mime": row[6],
        "local_path": row[7],
    }

def db_docs_upsert_by_unique(category_id: int, title: str, description: str | None, file_id: str, file_unique_id: str | None, mime_type: str | None, local_path: str | None) -> int:
    """Upsert документа по file_unique_id (если есть), иначе добавляет новый."""
    if file_unique_id:
        existing = db_docs_get_by_file_unique_id(file_unique_id)
        if existing:
            con = sqlite3.connect(DB_PATH)
            cur = con.cursor()
            cur.execute(
                """UPDATE docs
                   SET category_id=?, title=?, description=?, file_id=?, mime_type=?, local_path=COALESCE(?, local_path)
                   WHERE file_unique_id=?""",
                (category_id, title.strip(), (description or None), file_id, mime_type, local_path, file_unique_id),
            )
            con.commit()
            con.close()
            return int(existing["id"])
    # fallback insert
    return db_docs_add_doc(category_id, title, description, file_id, file_unique_id, mime_type, local_path)

def db_profiles_upsert(full_name: str, year_start: int, city: str, birthday: str | None, about: str, topics: str, tg_link: str) -> int:
    """Upsert анкеты по tg_link (если есть) иначе по full_name."""
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    key = (tg_link or "").strip()
    if key:
        cur.execute("SELECT id FROM profiles WHERE tg_link=?", (key,))
        row = cur.fetchone()
    else:
        cur.execute("SELECT id FROM profiles WHERE full_name=?", (full_name.strip(),))
        row = cur.fetchone()

    if row:
        pid = int(row[0])
        cur.execute(
            """UPDATE profiles
               SET full_name=?, year_start=?, city=?, birthday=?, about=?, topics=?, tg_link=?
               WHERE id=?""",
            (full_name.strip(), int(year_start), city.strip(), birthday, about.strip(), topics.strip(), (tg_link or "").strip(), pid),
        )
        con.commit()
        con.close()
        return pid

    cur.execute(
        """INSERT INTO profiles(full_name, year_start, city, birthday, about, topics, tg_link, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (full_name.strip(), int(year_start), city.strip(), birthday, about.strip(), topics.strip(), (tg_link or "").strip(), datetime.utcnow().isoformat()),
    )
    con.commit()
    pid = cur.lastrowid
    con.close()
    return int(pid)


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
        SELECT id, full_name, year_start, city, birthday, about, topics, tg_link
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
        "birthday": row[4],
        "about": row[5],
        "topics": row[6],
        "tg_link": row[7],
    }

def db_profiles_add(full_name: str, year_start: int, city: str, birthday: str | None, about: str, topics: str, tg_link: str) -> int:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        INSERT INTO profiles(full_name, year_start, city, birthday, about, topics, tg_link, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (full_name.strip(), int(year_start), city.strip(), (birthday or None), about.strip(), topics.strip(), tg_link.strip(), datetime.utcnow().isoformat()))
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

def db_profiles_birthdays(ddmm: str) -> list[dict]:
    """
    Возвращает список профилей, у кого birthday == 'ДД.ММ'
    """
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        SELECT id, full_name, tg_link, birthday
        FROM profiles
        WHERE birthday = ?
        ORDER BY full_name COLLATE NOCASE ASC
    """, (ddmm,))
    rows = cur.fetchall()
    con.close()

    res = []
    for r in rows:
        res.append({
            "id": r[0],
            "full_name": r[1],
            "tg_link": r[2] or "",
            "birthday": r[3],
        })
    return res

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


WELCOME_TEXT = """👋 Привет, {name}! Добро пожаловать в команду! 🎉
Очень рады, что ты с нами 😊
Желаем лёгкого старта, крутых задач, побольше лидов и, конечно, бабосиков 💸🚀

Если что — не стесняйся, всегда поможем 🙌
Познакомиться с коллегами и найти полезности можно через команду /help ✅"""

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

# ---------------- ADMIN CHECK (scoped) ----------------

async def is_admin_in_chat(chat_id: int, user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    try:
        member = await context.bot.get_chat_member(chat_id, user_id)
        return member.status in ("administrator", "creator")
    except Exception:
        return False

def get_scope_chat_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int | None:
    if update.effective_chat and update.effective_chat.type != "private":
        return update.effective_chat.id
    return context.user_data.get(HELP_SCOPE_CHAT_ID)

async def is_admin_scoped(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if not update.effective_user:
        return False
    scope_chat_id = get_scope_chat_id(update, context)
    if not scope_chat_id:
        return False
    return await is_admin_in_chat(scope_chat_id, update.effective_user.id, context)

# ---------------- STATES ----------------
# meeting reschedule manual
WAITING_DATE_FLAG = "waiting_reschedule_date"
WAITING_USER_ID = "waiting_user_id"
WAITING_SINCE_TS = "waiting_since_ts"
WAITING_MEETING_TYPE = "waiting_meeting_type"

# docs add flow
WAITING_DOC_UPLOAD = "waiting_doc_upload"
WAITING_DOC_DESC = "waiting_doc_desc"
PENDING_DOC_INFO = "pending_doc_info"
WAITING_NEW_CATEGORY_NAME = "waiting_new_category_name"

# profiles add flow
PROFILE_WIZ_ACTIVE = "profile_wiz_active"

# csv import flow
WAITING_CSV_IMPORT = "waiting_csv_import"
PROFILE_WIZ_STEP = "profile_wiz_step"
PROFILE_WIZ_DATA = "profile_wiz_data"

# suggest box flow
WAITING_SUGGESTION_TEXT = "waiting_suggestion_text"
SUGGESTION_MODE = "suggestion_mode"  # anon|named

# broadcast flow
BCAST_ACTIVE = "bcast_active"
BCAST_STEP = "bcast_step"  # topic|text|files
BCAST_DATA = "bcast_data"

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



def clear_csv_import(context: ContextTypes.DEFAULT_TYPE):
    context.chat_data[WAITING_CSV_IMPORT] = False
    context.chat_data.pop(WAITING_USER_ID, None)
    context.chat_data.pop(WAITING_SINCE_TS, None)

def clear_profile_wiz(context: ContextTypes.DEFAULT_TYPE):
    context.chat_data[PROFILE_WIZ_ACTIVE] = False
    context.chat_data.pop(PROFILE_WIZ_STEP, None)
    context.chat_data.pop(PROFILE_WIZ_DATA, None)

def clear_suggest_flow(context: ContextTypes.DEFAULT_TYPE):
    context.user_data[WAITING_SUGGESTION_TEXT] = False
    context.user_data.pop(SUGGESTION_MODE, None)

def clear_bcast_flow(context: ContextTypes.DEFAULT_TYPE):
    context.user_data[BCAST_ACTIVE] = False
    context.user_data.pop(BCAST_STEP, None)
    context.user_data.pop(BCAST_DATA, None)

# ---------------- DUE RULES ----------------

def standup_due_on_weekday(d: date) -> bool:
    return d.weekday() in (0, 2, 4)

def industry_due_on_weekday(d: date) -> bool:
    return d.weekday() == 1

# ---------------- BIRTHDAYS ----------------

def normalize_tg_mention(tg_link: str) -> str | None:
    """
    Из tg_link (@username / username / https://t.me/username) делает '@username'
    Возвращает None если не похоже на username.
    """
    tg = (tg_link or "").strip()
    if not tg:
        return None

    # @username
    if tg.startswith("@") and re.fullmatch(r"@[A-Za-z0-9_]{4,}", tg):
        return tg

    # https://t.me/username или http://t.me/username
    m = re.match(r"^https?://t\.me/([A-Za-z0-9_]{4,})/?$", tg)
    if m:
        return "@" + m.group(1)

    # username
    if re.fullmatch(r"[A-Za-z0-9_]{4,}", tg):
        return "@" + tg

    return None


BDAY_TEMPLATE_1 = (
    "🎉 Сегодня день рождения у {NAME}!\n"
    "Поздравляем! 🥳 Желаем здоровья, сил, классных задач и кайфа от работы 💪✨"
)

BDAY_TEMPLATE_2 = (
    "🎈 У нас +1 уровень прокачки у {NAME} 😄\n"
    "С днём рождения! Пусть задачи решаются сами, лиды приходят без прогрева, а кофе всегда горячий ☕️🔥"
)


def pick_bday_text(template_no: int, full_name: str, mention: str | None) -> str:
    """
    template_no: 1 или 2
    Если есть mention -> подставляем @username в {NAME}
    Иначе:
      - в шаблон 1: Имя Фамилия (full_name)
      - в шаблон 2: только Имя (первое слово из full_name)
    """
    if mention:
        name_for_text = mention
    else:
        if template_no == 1:
            name_for_text = full_name
        else:
            name_for_text = (full_name.split()[0] if full_name.strip() else full_name)

    if template_no == 1:
        return BDAY_TEMPLATE_1.format(NAME=name_for_text)
    return BDAY_TEMPLATE_2.format(NAME=name_for_text)


async def send_birthday_congrats(context: ContextTypes.DEFAULT_TYPE) -> bool:
    """
    Шлёт поздравления в notify_chats всем, у кого birthday == сегодня (ДД.ММ).
    Чередует шаблоны по кругу через meta.
    """
    now_msk = datetime.now(MOSCOW_TZ)
    today_ddmm = now_msk.strftime("%d.%m")

    chat_ids = db_list_chats()
    if not chat_ids:
        logger.warning("No chats for notifications. Add via /setchat.")
        return False

    people = db_profiles_birthdays(today_ddmm)

    # какой шаблон следующий (1 или 2)
    next_tpl = db_get_meta("bday_template_next")
    try:
        tpl_no = int(next_tpl) if next_tpl else 1
    except Exception:
        tpl_no = 1
    if tpl_no not in (1, 2):
        tpl_no = 1

    sent_any = False

    for p in people:
        full_name = p["full_name"]
        mention = normalize_tg_mention(p.get("tg_link", ""))

        text = pick_bday_text(tpl_no, full_name, mention)

        # переключаем 1->2->1->2...
        tpl_no = 2 if tpl_no == 1 else 1

        for chat_id in chat_ids:
            try:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    disable_web_page_preview=True,
                )
                sent_any = True
            except Exception as e:
                logger.exception("Cannot send birthday to %s: %s", chat_id, e)

    # сохраняем “следующий шаблон” (какой будет использоваться в следующий раз)
    db_set_meta("bday_template_next", str(tpl_no))

    return sent_any

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

    # 🎂 Автопоздравления в 09:00 МСК
    if now_msk.hour == 9 and now_msk.minute == 0:
        key = "last_auto_sent_date:birthday"
        if db_get_meta(key) != today_iso:
            await send_birthday_congrats(context)
            db_set_meta(key, today_iso)

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
        "🤖 <b>Меню «Помогатор Говорун»</b>\n\n"
        "Тут собраны актуальные материалы для команды:\n"
        "— 📄 Документы\n"
        "— 🔗 Полезные ссылки\n"
        "— 👥 Познакомиться с командой\n\n"
    )

def kb_help_main(is_admin_user: bool):
    rows = [
        [InlineKeyboardButton("📄 Документы", callback_data="help:docs")],
        [InlineKeyboardButton("🔗 Полезные ссылки", callback_data="help:links")],
        [InlineKeyboardButton("👥 Познакомиться с командой", callback_data="help:team")],
        [InlineKeyboardButton("💡 Предложка", callback_data="help:suggest")],
    ]
    if is_admin_user:
        rows.append([InlineKeyboardButton("⚙️ Настройки", callback_data="help:settings")])
    return InlineKeyboardMarkup(rows)


def kb_suggest_modes():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🕵️ Анонимно", callback_data="help:suggest:mode:anon")],
        [InlineKeyboardButton("🙋 Не анонимно", callback_data="help:suggest:mode:named")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="help:main")],
    ])

def kb_suggest_cancel():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("❌ Отмена", callback_data="help:suggest:cancel")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="help:main")],
    ])


def kb_bcast_files_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Отправить", callback_data="help:settings:bcast:send")],
        [InlineKeyboardButton("🗑️ Очистить файлы", callback_data="help:settings:bcast:clear_files")],
        [InlineKeyboardButton("❌ Отмена", callback_data="help:settings:bcast:cancel")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="help:settings")],
    ])

def kb_help_docs_categories():
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

# -------- LINKS (описание) --------

def get_links_catalog() -> dict[str, dict]:
    catalog: dict[str, dict] = {}

    # Чекко
    catalog["checko"] = {
        "title": 'Чекко 🔍',
        "url": "https://checko.ru/",
        "desc": (
            "Поиск контактов и данных компании по названию/ИНН/ОГРН/ФИО ИП. "
            "Удобно для быстрой подготовки перед прозвоном."
        ),
    }

    catalog["linkedin"] = {
        "title": "LinkedIn 🔎",
        "url": "https://www.linkedin.com/feed/",
        "desc": "Ищем ЛПР/контакты и проверяем должности, компанию, активности",
    }

    catalog["yandex_maps"] = {
        "title": "Яндекс Карты 🗺️",
        "url": "https://yandex.ru/maps",
        "desc": "Доп. поиск компании и контактов: филиалы, телефоны, сайт, отзывы, адреса.",
    }

    if STAFF_URL:
        catalog["staff"] = {
            "title": "Стафф 🧑‍🤝‍🧑",
            "url": STAFF_URL,
            "desc": "Находим коллег внутри компании: рабочие контакты",
        }

    if SITE_URL:
        catalog["site"] = {
            "title": "Наш сайт 🌐",
            "url": SITE_URL,
            "desc": "Инфа о продукте: кейсы, клиенты, описание сервиса и ближайшие мероприятия — удобно кидать в диалог.",
        }

    if INDUSTRY_WIKI_URL:
        catalog["industry_wiki"] = {
            "title": "WIKI Отрасли 📊",
            "url": INDUSTRY_WIKI_URL,
            "desc": "Материалы по отрасли: презентации, спичи и полезные справки.",
        }

    if HELPY_BOT_URL:
        catalog["helpy_bot"] = {
            "title": "Бот Helpy 🛠️",
            "url": HELPY_BOT_URL,
            "desc": "Помогает с техническими вопросами, связанными с работой.",
        }

    if LITE_FORM_URL:
        catalog["lite_form"] = {
            "title": "Форма Lite сервиса ✉️",
            "url": LITE_FORM_URL,
            "desc": "Отправляем клиенту описание Lite-версии и контакты техподдержки. Нужна почта клиента.",
        }

    if LEAD_CRM_URL:
        catalog["lead_crm"] = {
            "title": "Заведение лида в CRM 🧾",
            "url": LEAD_CRM_URL,
            "desc": "Создаём лида в CRM при проработке новой компании. <b>ВАЖНО!!! ПРОВЕРЬ ДУБЛИ</b>\nИли используем при задаче на реанимацию от руководителя.",
        }

    return catalog

def kb_help_links_menu():
    catalog = get_links_catalog()
    rows = []
    if not catalog:
        rows.append([InlineKeyboardButton("— ссылки не настроены —", callback_data="noop")])
    else:
        # Сортируем по длине названия (короткие сверху)
        items = sorted(catalog.items(), key=lambda kv: len(kv[1]["title"]))
        pending_row = []

        for key, item in items:
            btn = InlineKeyboardButton(item["title"], callback_data=f"help:links:item:{key}")

            # длинные кнопки — отдельной строкой
            if len(item["title"]) >= 22:
                if pending_row:
                    rows.append(pending_row)
                    pending_row = []
                rows.append([btn])
                continue

            # короткие — по две в ряд
            pending_row.append(btn)
            if len(pending_row) == 2:
                rows.append(pending_row)
                pending_row = []

        if pending_row:
            rows.append(pending_row)

    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="help:main")])
    return InlineKeyboardMarkup(rows)

def kb_help_link_card(url: str):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔗 Открыть ссылку", url=url)],
        [InlineKeyboardButton("⬅️ Назад", callback_data="help:links")],
    ])

def kb_help_team():
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
        [InlineKeyboardButton("📤 Скачать отчёт CSV", callback_data="help:settings:export_csv")],
        [InlineKeyboardButton("📥 Загрузить отчёт CSV", callback_data="help:settings:import_csv")],        [InlineKeyboardButton("📣 Рассылка", callback_data="help:settings:bcast")],

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
        "• /help — меню «Помогатор»\n"
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

    orig_msg = update.message  # to optionally delete /help command in group

    # если команда в личке — просто показываем там
    if update.effective_chat and update.effective_chat.type == "private":
        is_adm = await is_admin_scoped(update, context)
        await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb_help_main(is_admin_user=is_adm), disable_web_page_preview=True)
        return

    # если команда в группе — пытаемся в ЛС, в чат не пишем при успехе
    if update.effective_user:
        context.user_data[HELP_SCOPE_CHAT_ID] = update.effective_chat.id

    user_id = update.effective_user.id if update.effective_user else None
    if user_id:
        try:
            is_adm = await is_admin_scoped(update, context)

            await context.bot.send_message(
                chat_id=user_id,
                text=text,
                parse_mode=ParseMode.HTML,
                reply_markup=kb_help_main(is_admin_user=is_adm),
                disable_web_page_preview=True,
            )
            # успех -> в чат ничего не пишем
            if orig_msg and update.effective_chat and update.effective_chat.type != "private":
                try:
                    await context.bot.delete_message(chat_id=orig_msg.chat_id, message_id=orig_msg.message_id)
                except Exception:
                    pass
            return
        except Forbidden:
            warn_text = (
                "⚠️ Я не могу написать вам в ЛС.\n"
                f"Откройте личку: перейдите к боту @{bot_username} и отправьте /start,\n"
                "после этого снова нажмите /help в чате."
            )
            # пробуем удалить исходное /help, чтобы не засорять чат
            if orig_msg and update.effective_chat and update.effective_chat.type != "private":
                try:
                    await context.bot.delete_message(chat_id=orig_msg.chat_id, message_id=orig_msg.message_id)
                except Exception:
                    pass

            msg = await update.message.reply_text(
                warn_text,
                reply_to_message_id=update.message.message_id,
                disable_web_page_preview=True,
            )
            context.job_queue.run_once(
                job_delete_message,
                when=60,
                data={"chat_id": msg.chat_id, "message_id": msg.message_id},
                name=f"del_help_warn_{msg.chat_id}_{msg.message_id}",
            )
            return
        except Exception as e:
            logger.exception("Failed to DM /help: %s", e)

    # удаляем исходное /help и сообщение бота через 60 секунд (если есть права)
    if update.effective_chat and update.effective_chat.type != "private":
        if orig_msg:
            context.job_queue.run_once(
                job_delete_message,
                when=60,
                data={"chat_id": orig_msg.chat_id, "message_id": orig_msg.message_id},
                name=f"del_help_cmd_{orig_msg.chat_id}_{orig_msg.message_id}",
            )
        if msg:
            context.job_queue.run_once(
                job_delete_message,
                when=60,
                data={"chat_id": msg.chat_id, "message_id": msg.message_id},
                name=f"del_help_fallback_{msg.chat_id}_{msg.message_id}",
            )

    # fallback: отправляем в чат (reply)
    msg = await update.message.reply_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=kb_help_main(is_admin_user=await is_admin_scoped(update, context)),
        disable_web_page_preview=True,
        reply_to_message_id=update.message.message_id,
    )

    # удаляем исходное /help и сообщение бота через 60 секунд (если есть права)
    if update.effective_chat and update.effective_chat.type != "private":
        if orig_msg:
            context.job_queue.run_once(
                job_delete_message,
                when=60,
                data={"chat_id": orig_msg.chat_id, "message_id": orig_msg.message_id},
                name=f"del_help_cmd_{orig_msg.chat_id}_{orig_msg.message_id}",
            )
        if msg:
            context.job_queue.run_once(
                job_delete_message,
                when=60,
                data={"chat_id": msg.chat_id, "message_id": msg.message_id},
                name=f"del_help_fallback_{msg.chat_id}_{msg.message_id}",
            )

async def cmd_setchat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type == "private":
        await update.message.reply_text("Эта команда работает только в групповом чате.")
        return
    if not await is_admin_scoped(update, context):
        await update.message.reply_text("Только администраторы могут назначить чат для уведомлений.")
        return
    db_add_chat(update.effective_chat.id)
    await update.message.reply_text("✅ Готово! Этот чат добавлен в рассылку уведомлений.")

async def cmd_unsetchat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type == "private":
        await update.message.reply_text("Эта команда работает только в групповом чате.")
        return
    if not await is_admin_scoped(update, context):
        await update.message.reply_text("Только администраторы могут отключить уведомления.")
        return
    db_remove_chat(update.effective_chat.id)
    await update.message.reply_text("🧹 Этот чат убран из рассылки уведомлений.")

async def cmd_force_standup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin_scoped(update, context):
        await update.message.reply_text("Недостаточно прав.")
        return
    if not db_list_chats():
        await update.message.reply_text("Сначала подключи чат командой /setchat.")
        return
    await send_meeting_message(MEETING_STANDUP, context, force=True)
    await update.message.reply_text("🚀 Отправил принудительное уведомление планёрки.")

async def cmd_test_industry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin_scoped(update, context):
        await update.message.reply_text("Недостаточно прав.")
        return
    if not db_list_chats():
        await update.message.reply_text("Сначала подключи чат командой /setchat.")
        return
    await send_meeting_message(MEETING_INDUSTRY, context, force=True)
    await update.message.reply_text("🚀 Отправил тестовое уведомление отраслевой встречи.")

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin_scoped(update, context):
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
    if not await is_admin_scoped(update, context):
        return
    clear_waiting_date(context)
    clear_docs_flow(context)
    clear_profile_wiz(context)
    clear_csv_import(context)
    clear_suggest_flow(context)
    clear_bcast_flow(context)
    await update.message.reply_text("✅ Сбросил состояния ожидания (дата/документы/анкеты/CSV/предложка/рассылка).")



# ---------------- CSV BACKUP/RESTORE ----------------

def _csv_bool(v: str | None) -> str:
    return "1" if str(v).strip().lower() in ("1", "true", "yes", "y") else "0"


def export_backup_csv_bytes() -> bytes:
    """
    Собирает CSV-бэкап (категории/документы/анкеты) и возвращает как bytes (UTF-8).
    Используется для кнопки «Скачать отчёт CSV» и команды /export_csv.
    """
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=[
        "kind",
        "category_title",
        "doc_title",
        "doc_description",
        "doc_file_id",
        "doc_file_unique_id",
        "doc_mime_type",
        "doc_local_path",
        "profile_full_name",
        "profile_year_start",
        "profile_city",
        "profile_birthday",
        "profile_about",
        "profile_topics",
        "profile_tg_link",
    ])
    writer.writeheader()

    # categories
    cats = db_docs_list_categories()
    for cid, title in cats:
        writer.writerow({
            "kind": "category",
            "category_title": title,
        })

    # docs
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    # local_path колонка может отсутствовать в старых БД — попробуем мягко
    try:
        cur.execute("""
            SELECT c.title, d.title, d.description, d.file_id, d.file_unique_id, d.mime_type, d.local_path
            FROM docs d
            JOIN doc_categories c ON c.id = d.category_id
            ORDER BY d.id ASC
        """)
        rows = cur.fetchall()
        has_local = True
    except sqlite3.OperationalError:
        cur.execute("""
            SELECT c.title, d.title, d.description, d.file_id, d.file_unique_id, d.mime_type
            FROM docs d
            JOIN doc_categories c ON c.id = d.category_id
            ORDER BY d.id ASC
        """)
        rows = cur.fetchall()
        has_local = False
    con.close()

    for r in rows:
        if has_local:
            cat_title, doc_title, desc, file_id, file_unique_id, mime_type, local_path = r
        else:
            cat_title, doc_title, desc, file_id, file_unique_id, mime_type = r
            local_path = ""
        writer.writerow({
            "kind": "doc",
            "category_title": cat_title,
            "doc_title": doc_title,
            "doc_description": desc or "",
            "doc_file_id": file_id or "",
            "doc_file_unique_id": file_unique_id or "",
            "doc_mime_type": mime_type or "",
            "doc_local_path": local_path or "",
        })

    # profiles
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        SELECT full_name, year_start, city, birthday, about, topics, tg_link
        FROM profiles
        ORDER BY id ASC
    """)
    profs = cur.fetchall()
    con.close()

    for p in profs:
        full_name, year_start, city, birthday, about, topics, tg_link = p
        writer.writerow({
            "kind": "profile",
            "profile_full_name": full_name or "",
            "profile_year_start": year_start or "",
            "profile_city": city or "",
            "profile_birthday": birthday or "",
            "profile_about": about or "",
            "profile_topics": topics or "",
            "profile_tg_link": tg_link or "",
        })

    return buf.getvalue().encode("utf-8")


async def cmd_export_csv(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin_scoped(update, context):
        await update.message.reply_text("Только администраторы.")
        return

    # выгружаем всё в один CSV (kind: category/doc/profile)
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=[
        "kind",
        "category_title",
        "doc_title",
        "doc_description",
        "doc_file_id",
        "doc_file_unique_id",
        "doc_mime_type",
        "doc_local_path",
        "profile_full_name",
        "profile_year_start",
        "profile_city",
        "profile_birthday",
        "profile_about",
        "profile_topics",
        "profile_tg_link",
    ])
    writer.writeheader()

    # categories
    cats = db_docs_list_categories()
    for cid, title in cats:
        writer.writerow({
            "kind": "category",
            "category_title": title,
        })

    # docs
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        SELECT c.title, d.title, d.description, d.file_id, d.file_unique_id, d.mime_type, d.local_path
        FROM docs d
        JOIN doc_categories c ON c.id = d.category_id
        ORDER BY c.title COLLATE NOCASE ASC, d.id ASC
    """)
    for row in cur.fetchall():
        writer.writerow({
            "kind": "doc",
            "category_title": row[0],
            "doc_title": row[1],
            "doc_description": row[2] or "",
            "doc_file_id": row[3] or "",
            "doc_file_unique_id": row[4] or "",
            "doc_mime_type": row[5] or "",
            "doc_local_path": row[6] or "",
        })
    con.close()

    # profiles
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        SELECT full_name, year_start, city, birthday, about, topics, tg_link
        FROM profiles
        ORDER BY full_name COLLATE NOCASE ASC
    """)
    for row in cur.fetchall():
        writer.writerow({
            "kind": "profile",
            "profile_full_name": row[0],
            "profile_year_start": row[1],
            "profile_city": row[2],
            "profile_birthday": row[3] or "",
            "profile_about": row[4],
            "profile_topics": row[5],
            "profile_tg_link": row[6],
        })
    con.close()

    data = buf.getvalue().encode("utf-8-sig")
    bio = io.BytesIO(data)
    bio.name = "bot_backup.csv"

    await context.bot.send_document(
        chat_id=update.effective_chat.id,
        document=bio,
        caption="✅ Бэкап выгружен: bot_backup.csv",
    )

async def cmd_import_csv(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type == "private":
        # можно и в личке, и в чате — но импорт делает админ scoped
        pass

    if not await is_admin_scoped(update, context):
        await update.message.reply_text("Только администраторы могут импортировать CSV.")
        return

    clear_docs_flow(context)
    clear_profile_wiz(context)
    clear_waiting_date(context)

    context.chat_data[WAITING_CSV_IMPORT] = True
    context.chat_data[WAITING_USER_ID] = update.effective_user.id if update.effective_user else None
    context.chat_data[WAITING_SINCE_TS] = int(time.time())

    await update.message.reply_text(
        "📥 <b>Импорт из CSV</b>\n\n"
        "Отправьте файлом CSV (например <code>bot_backup.csv</code>).\n"
        "Бот восстановит категории/документы/анкеты.\n\n"
        "Важно: если в CSV есть <code>doc_local_path</code> и файл сохранён на сервере, "
        "бот сможет пере-залить документ в Telegram и обновить <code>file_id</code> при необходимости.",
        parse_mode=ParseMode.HTML,
    )



# ---------------- CALLBACKS: meetings cancel/reschedule ----------------

async def cb_cancel_open(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        try:
            await query.answer()
        except (TimedOut, NetworkError):
            pass
    except (TimedOut, NetworkError):
        pass
    if not await is_admin_scoped(update, context):
        try:
            await query.answer("Только администраторы могут отменять/переносить.", show_alert=True)
        except (TimedOut, NetworkError):
            pass
        return
    _, _, meeting_type = query.data.split(":")
    await query.edit_message_reply_markup(reply_markup=kb_cancel_options(meeting_type))

async def cb_cancel_close(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not await is_admin_scoped(update, context):
        try:
            await query.answer("Только администраторы.", show_alert=True)
        except (TimedOut, NetworkError):
            pass
        return
    await query.edit_message_reply_markup(reply_markup=None)
    try:
        await query.answer("Ок, не отменяем ✅")
    except (TimedOut, NetworkError):
        pass

async def cb_cancel_reason(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not await is_admin_scoped(update, context):
        try:
            await query.answer("Только администраторы.", show_alert=True)
        except (TimedOut, NetworkError):
            pass
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
        try:
            await query.answer("Отменено.")
        except (TimedOut, NetworkError):
            pass
        return

    if reason_key == "tech":
        reason_text = "Перенесём по техническим причинам"
        db_set_canceled(meeting_type, today_d, reason_text)
        await query.edit_message_reply_markup(reply_markup=None)
        title = "✅ Сегодняшняя планёрка отменена" if meeting_type == MEETING_STANDUP else "✅ Сегодняшняя отраслевая встреча отменена"
        await context.bot.send_message(chat_id=update.effective_chat.id, text=f"{title}\nПричина: {reason_text}")
        try:
            await query.answer("Ок.")
        except (TimedOut, NetworkError):
            pass
        return

    if reason_key == "move":
        await query.edit_message_reply_markup(reply_markup=kb_reschedule_dates(meeting_type, today_d))
        try:
            await query.answer("Выберите дату переноса 📆")
        except (TimedOut, NetworkError):
            pass
        return

async def cb_reschedule_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not await is_admin_scoped(update, context):
        try:
            await query.answer("Только администраторы.", show_alert=True)
        except (TimedOut, NetworkError):
            pass
        return

    parts = query.data.split(":")
    meeting_type = parts[2]
    picked = parts[3]
    today_d = datetime.now(MOSCOW_TZ).date()

    try:
        dd, mm, yy = picked.split(".")
        new_d = date(int("20" + yy), int(mm), int(dd))
    except Exception:
        try:
            await query.answer("Не смог распознать дату.", show_alert=True)
        except (TimedOut, NetworkError):
            pass
        return

    if new_d <= today_d:
        try:
            await query.answer("Дата переноса должна быть в будущем.", show_alert=True)
        except (TimedOut, NetworkError):
            pass
        return

    db_set_canceled(meeting_type, today_d, "Перенос на другой день", reschedule_date=picked)
    db_upsert_reschedule(meeting_type, today_d, new_d)

    await query.edit_message_reply_markup(reply_markup=None)

    title = "✅ Сегодняшняя планёрка перенесена" if meeting_type == MEETING_STANDUP else "✅ Сегодняшняя отраслевая встреча перенесена"
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=f"{title}\nНовая дата: {picked} 📌\nСледите за расписанием или чатом"
    )
    try:
        await query.answer("Перенесено.")
    except (TimedOut, NetworkError):
        pass

async def cb_reschedule_manual(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not await is_admin_scoped(update, context):
        try:
            await query.answer("❌ Только администраторы.", show_alert=True)
        except (TimedOut, NetworkError):
            pass
        return

    parts = query.data.split(":")
    meeting_type = parts[2]

    context.chat_data[WAITING_DATE_FLAG] = True
    context.chat_data[WAITING_USER_ID] = update.effective_user.id
    context.chat_data[WAITING_SINCE_TS] = int(time.time())
    context.chat_data[WAITING_MEETING_TYPE] = meeting_type
    try:
        try:
            await query.answer()
        except (TimedOut, NetworkError):
            pass
    except (TimedOut, NetworkError):
        pass
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
    if not await is_admin_scoped(update, context):
        try:
            await query.answer("❌ Только администраторы.", show_alert=True)
        except (TimedOut, NetworkError):
            pass
        return
    clear_waiting_date(context)
    try:
        await query.answer("Ок, отменил ввод даты ✅")
    except (TimedOut, NetworkError):
        pass
    try:
        await query.edit_message_reply_markup(reply_markup=None)
    except Exception:
        pass
    await context.bot.send_message(chat_id=update.effective_chat.id, text="✅ Ввод даты отменён.")

# ---------------- CALLBACKS: HELP ----------------

async def cb_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    data = q.data
    try:
        try:
            await q.answer()
        except (TimedOut, NetworkError):
            pass
    except (TimedOut, NetworkError):
        pass

    if data == "noop":
        return

    is_adm = await is_admin_scoped(update, context)

    if data == "help:main":
        bot_username = (context.bot.username or "blablabird_bot")
        await q.edit_message_text(
            help_text_main(bot_username),
            parse_mode=ParseMode.HTML,
            reply_markup=kb_help_main(is_admin_user=is_adm),
            disable_web_page_preview=True,
        )
        return


    if data == "help:suggest":
        text = (
            "💡 <b>Предложка</b>\n\n"
            "Тут ты можешь отправить свой вопрос/предложение/жалобу/просьбу и т.д. 🙂\n\n"
            "Для этого воспользуйся одним из режимов ниже 👇"
        )
        await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb_suggest_modes(), disable_web_page_preview=True)
        return

    if data == "help:suggest:cancel":
        clear_suggest_flow(context)
        await q.edit_message_text("✅ Отправка отменена.", parse_mode=ParseMode.HTML, reply_markup=kb_help_main(is_admin_user=is_adm))
        return

    if data.startswith("help:suggest:mode:"):
        mode = data.split(":")[-1]  # anon|named
        scope_chat_id = get_scope_chat_id(update, context)
        if not scope_chat_id:
            try:
                await q.answer("Открой /help из группового чата, чтобы привязать предложку к нему.", show_alert=True)
            except (TimedOut, NetworkError):
                pass
            return

        context.user_data[WAITING_SUGGESTION_TEXT] = True
        context.user_data[SUGGESTION_MODE] = mode

        await q.edit_message_text(
            "✍️ <b>Напиши сообщение для администраторов</b>\n\n"
            "Можно одним сообщением. Я передам его администраторам.\n"
            "Чтобы отменить — нажми «Отмена».",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_suggest_cancel(),
            disable_web_page_preview=True,
        )
        return

    if data == "help:docs":
        text = (
            "📄 <b>Документы</b>\n\n"
            "Выберите категорию — внутри будут файлы.\n"
            "Нажмите на файл, чтобы получить его в чат."
        )
        await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb_help_docs_categories())
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
            await q.edit_message_text("Файл не найден (возможно удалён).", reply_markup=kb_help_main(is_admin_user=is_adm))
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
            try:
                await q.answer("Ссылка не найдена.", show_alert=True)
            except (TimedOut, NetworkError):
                pass
            return
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
            reply_markup=kb_help_link_card(url),
            disable_web_page_preview=True,
        )
        return

    if data == "help:team":
        text = "👥 <b>Познакомиться с командой</b>\n\nВыберите человека:"
        await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb_help_team())
        return

    if data.startswith("help:team:person:"):
        pid = int(data.split(":")[-1])
        p = db_profiles_get(pid)
        if not p:
            await q.edit_message_text("Анкета не найдена (возможно удалена).", reply_markup=kb_help_team())
            return

        bday = (p.get("birthday") or "").strip() or "—"

        card = (
            f"👤 <b>{p['full_name']}</b>\n"
            f"📅 Работает с: <b>{p['year_start']}</b>\n"
            f"🏙️ Город: <b>{p['city']}</b>\n"
            f"🎂 День рождения: <b>{bday}</b>\n\n"
            f"📝 <b>Кратко о себе</b>\n{p['about']}\n\n"
            f"❓ <b>По каким вопросам обращаться</b>\n{p['topics']}\n\n"
            f"🔗 TG: {p['tg_link']}"
        )
        await q.edit_message_text(card, parse_mode=ParseMode.HTML, reply_markup=kb_help_profile_card(p), disable_web_page_preview=True)
        return

    if data == "help:settings":
        if not is_adm:
            try:
                await q.answer("⚠️ Кнопка доступна администраторам чата. Обратитесь к ним 🙂", show_alert=True)
            except (TimedOut, NetworkError):
                pass
            return
        text = (
            "⚙️ <b>Настройки</b>\n\n"
            "Управление документами, категориями и анкетами.\n"
            "Все действия делаются тут, в ЛС — в чате флудить не будем 🙂"
        )
        await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb_help_settings())
        return

    # дальше — настройки (только админы)
    if data.startswith("help:settings:"):
        if not is_adm:
            try:
                await q.answer("⚠️ Доступно администраторам чата.", show_alert=True)
            except (TimedOut, NetworkError):
                pass
            return

        if data == "help:settings:cancel":
            clear_docs_flow(context)
            clear_profile_wiz(context)
            clear_waiting_date(context)
            clear_csv_import(context)
            clear_csv_import(context)
            await q.edit_message_text("✅ Действие отменено.", reply_markup=kb_help_settings(), parse_mode=ParseMode.HTML)
            return


        if data == "help:settings:bcast":
            clear_bcast_flow(context)
            context.user_data[BCAST_ACTIVE] = True
            context.user_data[BCAST_STEP] = "topic"
            context.user_data[BCAST_DATA] = {"topic": None, "text": None, "files": []}
            await q.edit_message_text(
                "📣 <b>Рассылка</b>\n\n"
                "Шаг 1/3: <b>Тема</b> (будет выделена жирным)\n"
                "Отправьте тему одним сообщением.\n"
                "Если тема не нужна — отправьте <code>-</code>.",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_cancel_wizard_settings(),
                disable_web_page_preview=True,
            )
            return

        if data == "help:settings:bcast:cancel":
            clear_bcast_flow(context)
            await q.edit_message_text("✅ Рассылка отменена.", parse_mode=ParseMode.HTML, reply_markup=kb_help_settings())
            return

        if data == "help:settings:bcast:clear_files":
            d = _bcast_get_data(context)
            d["files"] = []
            context.user_data[BCAST_DATA] = d
            await q.answer("Файлы очищены ✅")
            return

        if data == "help:settings:bcast:send":
            d = _bcast_get_data(context)
            topic = d.get("topic")
            body = d.get("text")
            files = d.get("files") or []
            message_html = _bcast_compose_message(topic, body)

            if not message_html and not files:
                await q.answer("Нечего отправлять: добавьте текст или файлы.", show_alert=True)
                return

            ok, fail = await broadcast_to_chats(context, message_html, files)
            clear_bcast_flow(context)
            await q.edit_message_text(
                f"✅ Рассылка отправлена.\n\n"
                f"Успешно: <b>{ok}</b>\n"
                f"Ошибок: <b>{fail}</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_help_settings(),
            )
            return

        if data == "help:settings:export_csv":
            # экспортируем CSV и отправляем в ЛС (тут мы и так в ЛС)
            if update.effective_user:
                try:
                    csv_bytes = export_backup_csv_bytes()
                    bio = io.BytesIO(csv_bytes)
                    bio.name = "bot_backup.csv"
                    await context.bot.send_document(
                        chat_id=update.effective_user.id,
                        document=bio,
                        caption="📤 Отчёт CSV (бэкап) готов. Сохрани файл — он поможет восстановить документы и анкеты.",
                    )
                    try:
                        await q.answer("Отправил CSV ✅")
                    except (TimedOut, NetworkError):
                        pass
                except Exception as e:
                    logger.exception("export_csv failed: %s", e)
                    try:
                        await q.answer("Не смог сформировать CSV 😕", show_alert=True)
                    except (TimedOut, NetworkError):
                        pass
            return

        if data == "help:settings:import_csv":
            # включаем режим ожидания CSV файла
            clear_docs_flow(context)
            clear_profile_wiz(context)
            clear_waiting_date(context)
            context.chat_data[WAITING_CSV_IMPORT] = True
            context.chat_data[WAITING_USER_ID] = update.effective_user.id if update.effective_user else None
            context.chat_data[WAITING_SINCE_TS] = int(time.time())
            await q.edit_message_text(
                "📥 <b>Импорт отчёта CSV</b>\n\n"
                "Отправьте CSV-файл следующим сообщением.\n"
                "После загрузки бот восстановит категории, документы и анкеты.\n\n"
                "Если передумали — нажмите «Отмена».",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_cancel_wizard_settings(),
            )
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
                try:
                    await q.answer("Удалено ✅")
                except (TimedOut, NetworkError):
                    pass
                await q.edit_message_text("✅ Категория удалена.", reply_markup=kb_settings_categories(), parse_mode=ParseMode.HTML)
            else:
                try:
                    await q.answer("Нельзя: категория не пустая", show_alert=True)
                except (TimedOut, NetworkError):
                    pass
            return

        if data == "help:settings:add_doc":
            clear_docs_flow(context)
            context.chat_data[WAITING_DOC_UPLOAD] = True
            context.chat_data[WAITING_DOC_DESC] = False
            context.chat_data[WAITING_USER_ID] = update.effective_user.id
            context.chat_data[WAITING_SINCE_TS] = int(time.time())
            await q.edit_message_text(
                "➕ <b>Добавление файла</b>\n\n"
                "1) Отправьте документ следующим сообщением.\n"
                "2) Затем бот попросит краткое описание.\n"
                "3) Потом выберем категорию.\n\n"
                "Название можно указать в подписи к файлу (caption).",
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
                try:
                    await q.answer("Удалено ✅")
                except (TimedOut, NetworkError):
                    pass
                await q.edit_message_text("✅ Файл удалён.", parse_mode=ParseMode.HTML, reply_markup=kb_help_settings())
            else:
                try:
                    await q.answer("Не найден", show_alert=True)
                except (TimedOut, NetworkError):
                    pass
            return

        if data.startswith("help:settings:add_doc:cat:"):
            cid = int(data.split(":")[-1])
            pending = context.chat_data.get(PENDING_DOC_INFO)
            if not pending:
                try:
                    await q.answer("Нет загруженного файла. Начните заново.", show_alert=True)
                except (TimedOut, NetworkError):
                    pass
                return
            db_docs_add_doc(cid, pending["title"], pending.get("description"), pending["file_id"], pending["file_unique_id"], pending.get("mime"), pending.get("local_path"))
            clear_docs_flow(context)
            await q.edit_message_text("✅ Файл добавлен в документы.", parse_mode=ParseMode.HTML, reply_markup=kb_help_settings())
            return

        if data == "help:settings:add_doc:newcat":
            pending = context.chat_data.get(PENDING_DOC_INFO)
            if not pending:
                try:
                    await q.answer("Сначала отправьте файл.", show_alert=True)
                except (TimedOut, NetworkError):
                    pass
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
                "Шаг 1/7: отправьте <b>Имя и Фамилию</b>.\n"
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
                try:
                    await q.answer("Удалено ✅")
                except (TimedOut, NetworkError):
                    pass
                await q.edit_message_text("✅ Анкета удалена.", parse_mode=ParseMode.HTML, reply_markup=kb_help_settings())
            else:
                try:
                    await q.answer("Не найдено", show_alert=True)
                except (TimedOut, NetworkError):
                    pass
            return

    try:

        await q.answer()

    except (TimedOut, NetworkError):

        pass



# ---------------- HANDLERS: NEW MEMBERS ----------------

async def on_new_members(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    new_members = update.message.new_chat_members or []
    if not new_members:
        return

    # если добавили самого бота — не приветствуем как человека
    bot_id = context.bot.id
    for m in new_members:
        if m.id == bot_id:
            await update.message.reply_text(
                "Привет! Я в чате ✅\n"
                "Чтобы включить уведомления, админ должен выполнить команду /setchat."
            )
            return

    names = []
    for m in new_members:
        nm = (m.full_name or m.first_name or "коллега").strip()
        if nm:
            names.append(nm)

    joined = ", ".join(names) if names else "коллега"
    text = WELCOME_TEXT.format(name=joined)

    await update.message.reply_text(text, disable_web_page_preview=True)

# ---------------- HANDLERS: DOCUMENT UPLOAD ----------------

async def on_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat:
        return

    # рассылка  # bcast attachment: сохраняем документ как вложение (в ЛС админа)
    if context.user_data.get(BCAST_ACTIVE) and context.user_data.get(BCAST_STEP) == "files":
        doc = update.message.document
        if doc:
            d = _bcast_get_data(context)
            d["files"].append({"kind": "document", "file_id": doc.file_id, "file_unique_id": doc.file_unique_id})
            context.user_data[BCAST_DATA] = d
            await update.message.reply_text("✅ Документ добавлен. Можешь добавить ещё или нажми «✅ Отправить».", reply_markup=kb_bcast_files_menu())
        return


    user_id = update.effective_user.id if update.effective_user else None
    waiting_user = context.chat_data.get(WAITING_USER_ID)
    if waiting_user and user_id != waiting_user:
        return

    # ---------------- CSV IMPORT FLOW ----------------
    if context.chat_data.get(WAITING_CSV_IMPORT):
        if not await is_admin_scoped(update, context):
            clear_csv_import(context)
            await update.message.reply_text("❌ Только администраторы могут импортировать CSV.")
            return

        doc = update.message.document
        if not doc:
            return

        # скачиваем CSV во временный файл
        try:
            tg_file = await context.bot.get_file(doc.file_id)
            tmp_path = Path(STORAGE_DIR) / "tmp_import.csv"
            await tg_file.download_to_drive(custom_path=str(tmp_path))
            raw = tmp_path.read_text(encoding="utf-8-sig")
        except Exception as e:
            clear_csv_import(context)
            logger.exception("CSV import download/read failed: %s", e)
            await update.message.reply_text("❌ Не смог скачать/прочитать CSV.")
            return

        ok_docs = ok_profiles = ok_cats = 0
        skipped_docs = 0
        reader = csv.DictReader(io.StringIO(raw))
        for row in reader:
            kind = (row.get("kind") or "").strip().lower()

            if kind == "category":
                title = (row.get("category_title") or "").strip()
                if title:
                    db_docs_ensure_category(title)
                    ok_cats += 1
                continue

            if kind == "profile":
                full_name = (row.get("profile_full_name") or "").strip()
                if not full_name:
                    continue
                year_start = int((row.get("profile_year_start") or "0").strip() or 0)
                city = (row.get("profile_city") or "").strip()
                birthday = (row.get("profile_birthday") or "").strip() or None
                about = (row.get("profile_about") or "").strip()
                topics = (row.get("profile_topics") or "").strip()
                tg_link = (row.get("profile_tg_link") or "").strip()
                if not (year_start and city and about and topics and tg_link):
                    # базовая валидация, чтобы не засорять базу
                    continue
                db_profiles_upsert(full_name, year_start, city, birthday, about, topics, tg_link)
                ok_profiles += 1
                continue

            if kind == "doc":
                cat_title = (row.get("category_title") or "").strip() or "Документы"
                cid = db_docs_ensure_category(cat_title)

                title = (row.get("doc_title") or "").strip() or "Документ"
                description = (row.get("doc_description") or "").strip() or None
                file_id = (row.get("doc_file_id") or "").strip() or None
                file_unique_id = (row.get("doc_file_unique_id") or "").strip() or None
                mime_type = (row.get("doc_mime_type") or "").strip() or None
                local_path = (row.get("doc_local_path") or "").strip() or None

                # Если file_id отсутствует, но есть локальный файл — пере-зальём в TG и обновим file_id
                if (not file_id) and local_path and Path(local_path).exists():
                    target_chat_id = update.effective_user.id if update.effective_user else update.effective_chat.id
                    try:
                        with open(local_path, "rb") as f:
                            msg = await context.bot.send_document(
                                chat_id=target_chat_id,
                                document=f,
                                caption=f"♻️ Восстановление: {title}",
                                disable_notification=True,
                            )
                        if msg and msg.document:
                            file_id = msg.document.file_id
                            file_unique_id = msg.document.file_unique_id
                            mime_type = msg.document.mime_type
                    except Forbidden:
                        # если бот не может в ЛС — отправим в текущий чат
                        try:
                            with open(local_path, "rb") as f:
                                msg = await context.bot.send_document(
                                    chat_id=update.effective_chat.id,
                                    document=f,
                                    caption=f"♻️ Восстановление: {title}",
                                    disable_notification=True,
                                )
                            if msg and msg.document:
                                file_id = msg.document.file_id
                                file_unique_id = msg.document.file_unique_id
                                mime_type = msg.document.mime_type
                        except Exception as e:
                            logger.exception("Reupload local doc failed: %s", e)
                    except Exception as e:
                        logger.exception("Reupload local doc failed: %s", e)

                if not file_id and not (local_path and Path(local_path).exists()):
                    skipped_docs += 1
                    continue

                db_docs_upsert_by_unique(
                    cid,
                    title=title,
                    description=description,
                    file_id=file_id or "",
                    file_unique_id=file_unique_id,
                    mime_type=mime_type,
                    local_path=local_path,
                )
                ok_docs += 1
                continue

        clear_csv_import(context)
        await update.message.reply_text(
            f"✅ Импорт завершён.\n"
            f"Категории: {ok_cats}\n"
            f"Документы: {ok_docs} (пропущено без файла: {skipped_docs})\n"
            f"Анкеты: {ok_profiles}"
        )
        return

    # ---------------- DOC ADD FLOW ----------------
    if not context.chat_data.get(WAITING_DOC_UPLOAD):
        return

    if not await is_admin_scoped(update, context):
        clear_docs_flow(context)
        await update.message.reply_text("❌ Только администраторы могут добавлять документы.")
        return

    doc = update.message.document
    if not doc:
        return

    title = (update.message.caption or "").strip() or (doc.file_name or "Документ")

    # локально бэкапим документ (на случай краша/переезда)
    local_path = None
    try:
        tg_file = await context.bot.get_file(doc.file_id)
        safe_name = (doc.file_name or "document").replace("/", "_")
        local_path = str(Path(STORAGE_DIR) / "docs" / f"{doc.file_unique_id}_{safe_name}")
        await tg_file.download_to_drive(custom_path=local_path)
    except Exception as e:
        logger.exception("Failed to backup doc locally: %s", e)
        local_path = None

    pending = {
        "file_id": doc.file_id,
        "file_unique_id": doc.file_unique_id,
        "mime": doc.mime_type,
        "title": title[:120],
        "description": None,
        "local_path": local_path,
    }
    context.chat_data[PENDING_DOC_INFO] = pending
    context.chat_data[WAITING_DOC_UPLOAD] = False
    context.chat_data[WAITING_DOC_DESC] = True

    await update.message.reply_text(
        "✍️ <b>Краткое описание документа</b>\n\n"
        "Напишите 1–2 предложения.\n"
        "Если описания не нужно — отправьте <code>-</code>",
        parse_mode=ParseMode.HTML,
        reply_markup=kb_cancel_wizard_settings(),
    )


async def on_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat:
        return
    if context.user_data.get(BCAST_ACTIVE) and context.user_data.get(BCAST_STEP) == "files":
        photos = update.message.photo or []
        if photos:
            # берём самый большой
            ph = photos[-1]
            d = _bcast_get_data(context)
            d["files"].append({"kind": "photo", "file_id": ph.file_id, "file_unique_id": ph.file_unique_id})
            context.user_data[BCAST_DATA] = d
            await update.message.reply_text("✅ Фото добавлено. Можешь добавить ещё или нажми «✅ Отправить».", reply_markup=kb_bcast_files_menu())

async def on_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat:
        return
    if context.user_data.get(BCAST_ACTIVE) and context.user_data.get(BCAST_STEP) == "files":
        vid = update.message.video
        if vid:
            d = _bcast_get_data(context)
            d["files"].append({"kind": "video", "file_id": vid.file_id, "file_unique_id": vid.file_unique_id})
            context.user_data[BCAST_DATA] = d
            await update.message.reply_text("✅ Видео добавлено. Можешь добавить ещё или нажми «✅ Отправить».", reply_markup=kb_bcast_files_menu())


# ---------------- HANDLERS: TEXT INPUT (dates / categories / profiles) ----------------

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
        clear_csv_import(context)
        clear_suggest_flow(context)
        clear_bcast_flow(context)
        await update.message.reply_text("⏳ Время ожидания истекло. Начните действие заново через /help.")
        return


    # предложка (в ЛС): ждём текст  # anti-spam
    if context.user_data.get(WAITING_SUGGESTION_TEXT):
        # анти-спам: 1 сообщение в 5 минут на человека
        if user_id:
            last_ts = db_get_suggest_last_ts(user_id) or 0
            now_ts = int(time.time())
            if now_ts - last_ts < 5 * 60:
                left = 5 * 60 - (now_ts - last_ts)
                mins = max(1, (left + 59) // 60)
                await update.message.reply_text(f"⏳ Можно отправлять не чаще 1 раза в 5 минут. Попробуйте через ~{mins} мин.")
                return

        mode = context.user_data.get(SUGGESTION_MODE, "anon")
        scope_chat_id = get_scope_chat_id(update, context)
        if not scope_chat_id:
            clear_suggest_flow(context)
            await update.message.reply_text("⚠️ Не вижу, к какому чату привязать предложку. Открой /help в групповом чате ещё раз.")
            return

        await send_suggestion_to_admins(scope_chat_id, update, context, text, mode)

        if user_id:
            db_set_suggest_last_ts(user_id, int(time.time()))

        clear_suggest_flow(context)
        await update.message.reply_text("✅ Спасибо! Передал администраторам 🙌")
        return

    # рассылка  # bcast attachment (в ЛС админа): шаги тема/текст/файлы
    if context.user_data.get(BCAST_ACTIVE):
        step = context.user_data.get(BCAST_STEP)
        d = _bcast_get_data(context)

        if step == "topic":
            if text != "-":
                topic = text.strip()
                if len(topic) < 2:
                    await update.message.reply_text("❌ Тема слишком короткая. Или отправьте <code>-</code> чтобы пропустить.", parse_mode=ParseMode.HTML)
                    return
                d["topic"] = topic[:200]
            else:
                d["topic"] = None

            context.user_data[BCAST_DATA] = d
            context.user_data[BCAST_STEP] = "text"
            await update.message.reply_text(
                "Шаг 2/3: <b>Текст рассылки</b> 📝\n"
                "Отправьте текст одним сообщением.\n"
                "Если текст не нужен — отправьте <code>-</code>.",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_cancel_wizard_settings(),
            )
            return

        if step == "text":
            if text != "-":
                body = text.strip()
                if len(body) < 2:
                    await update.message.reply_text("❌ Текст слишком короткий. Или отправьте <code>-</code> чтобы пропустить.", parse_mode=ParseMode.HTML)
                    return
                # лимит Telegram ~4096, оставим запас
                d["text"] = body[:3500]
            else:
                d["text"] = None

            context.user_data[BCAST_DATA] = d
            context.user_data[BCAST_STEP] = "files"
            await update.message.reply_text(
                "Шаг 3/3: <b>Файлы</b> 📎\n\n"
                "Можешь прикрепить <b>документы / фото / видео</b> (сколько нужно).\n"
                "Когда закончишь — нажми <b>✅ Отправить</b>.\n"
                "Можно без файлов 🙂",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_bcast_files_menu(),
            )
            return

        # step == files -> ждём вложения или кнопку "Отправить"
        return

    # описание документа
    if context.chat_data.get(WAITING_DOC_DESC):
        if not await is_admin_scoped(update, context):
            clear_docs_flow(context)
            await update.message.reply_text("❌ Только администраторы могут добавлять документы.")
            return

        pending = context.chat_data.get(PENDING_DOC_INFO)
        if not pending:
            clear_docs_flow(context)
            await update.message.reply_text("❌ Не найден загруженный файл. Начните заново через /help.")
            return

        desc = None if text == "-" else text
        if desc is not None:
            desc = desc.strip()
            if len(desc) < 3:
                await update.message.reply_text("❌ Слишком коротко. Напишите чуть подробнее или отправьте <code>-</code>.", parse_mode=ParseMode.HTML)
                return
            desc = desc[:600]

        pending["description"] = desc
        context.chat_data[PENDING_DOC_INFO] = pending
        context.chat_data[WAITING_DOC_DESC] = False

        await update.message.reply_text(
            "✅ Описание сохранено.\n\nТеперь выберите категорию:",
            reply_markup=kb_pick_category_for_new_doc(),
        )
        return

    # перенос даты вручную
    if context.chat_data.get(WAITING_DATE_FLAG):
        if not await is_admin_scoped(update, context):
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

    # ввод названия категории
    if context.chat_data.get(WAITING_NEW_CATEGORY_NAME):
        if not await is_admin_scoped(update, context):
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
            db_docs_add_doc(cid, pending["title"], pending.get("description"), pending["file_id"], pending["file_unique_id"], pending.get("mime"), pending.get("local_path"))
            clear_docs_flow(context)
            await update.message.reply_text("✅ Категория создана и файл добавлен.", reply_markup=kb_help_settings())
            return

        clear_docs_flow(context)
        await update.message.reply_text("✅ Категория добавлена.", reply_markup=kb_help_settings())
        return

    # анкета — шаги
    if context.chat_data.get(PROFILE_WIZ_ACTIVE):
        if not await is_admin_scoped(update, context):
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
            await update.message.reply_text("Шаг 2/7: с какого года работает? Пример: 2022", reply_markup=kb_cancel_wizard_settings())
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
            await update.message.reply_text("Шаг 3/7: город проживания. Пример: Москва", reply_markup=kb_cancel_wizard_settings())
            return

        if step == "city":
            if len(text) < 2:
                await update.message.reply_text("❌ Укажите город.")
                return
            data["city"] = text
            context.chat_data[PROFILE_WIZ_DATA] = data
            context.chat_data[PROFILE_WIZ_STEP] = "birthday"
            await update.message.reply_text(
                "Шаг 4/7: день рождения (формат <b>ДД.ММ</b>)\n"
                "Пример: <code>22.01</code>\n"
                "Если не хотите указывать — отправьте <code>-</code>",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_cancel_wizard_settings()
            )
            return

        if step == "birthday":
            b = text.strip()
            if b == "-":
                data["birthday"] = None
            else:
                if not re.fullmatch(r"\d{2}\.\d{2}", b):
                    await update.message.reply_text("❌ Формат ДД.ММ (пример 22.01) или '-'")
                    return
                dd, mm = b.split(".")
                try:
                    dd_i = int(dd)
                    mm_i = int(mm)
                except Exception:
                    await update.message.reply_text("❌ Формат ДД.ММ (пример 22.01) или '-'")
                    return
                if not (1 <= dd_i <= 31 and 1 <= mm_i <= 12):
                    await update.message.reply_text("❌ Некорректная дата. Пример: 22.01")
                    return
                data["birthday"] = b

            context.chat_data[PROFILE_WIZ_DATA] = data
            context.chat_data[PROFILE_WIZ_STEP] = "about"
            await update.message.reply_text("Шаг 5/7: кратко о себе (1–3 предложения)", reply_markup=kb_cancel_wizard_settings())
            return

        if step == "about":
            if len(text) < 5:
                await update.message.reply_text("❌ Напишите чуть подробнее 🙂")
                return
            data["about"] = text
            context.chat_data[PROFILE_WIZ_DATA] = data
            context.chat_data[PROFILE_WIZ_STEP] = "topics"
            await update.message.reply_text("Шаг 6/7: по каким вопросам обращаться?", reply_markup=kb_cancel_wizard_settings())
            return

        if step == "topics":
            if len(text) < 3:
                await update.message.reply_text("❌ Укажите темы/вопросы.")
                return
            data["topics"] = text
            context.chat_data[PROFILE_WIZ_DATA] = data
            context.chat_data[PROFILE_WIZ_STEP] = "tg_link"
            await update.message.reply_text("Шаг 7/7: Telegram (@username или https://t.me/username)", reply_markup=kb_cancel_wizard_settings())
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
                birthday=data.get("birthday"),
                about=data["about"],
                topics=data["topics"],
                tg_link=data["tg_link"],
            )

            clear_profile_wiz(context)
            await update.message.reply_text(f"✅ Анкета добавлена (ID {pid}).", reply_markup=kb_help_settings())
            return



# ---------------- SUGGEST BOX ----------------

async def send_suggestion_to_admins(scope_chat_id: int, update: Update, context: ContextTypes.DEFAULT_TYPE, message_text: str, mode: str) -> tuple[int, int]:
    """Отправляет сообщение всем админам чата (кроме ботов). Возвращает (sent_ok, sent_fail)."""
    sent_ok = 0
    sent_fail = 0

    user = update.effective_user
    user_name = (user.full_name if user else "Неизвестный пользователь")
    username = ("@" + user.username) if (user and user.username) else ""
    user_id = user.id if user else 0

    try:
        chat = await context.bot.get_chat(scope_chat_id)
        chat_title = chat.title or str(scope_chat_id)
    except Exception:
        chat_title = str(scope_chat_id)

    mode_label = "🕵️ Анонимно" if mode == "anon" else "🙋 Не анонимно"

    admin_text = (
        f"💡 <b>Предложка</b> ({mode_label})\n"
        f"Чат: <b>{chat_title}</b> (<code>{scope_chat_id}</code>)\n"
        f"От: <b>{user_name}</b> {username} (<code>{user_id}</code>)\n\n"
        f"Сообщение:\n{message_text}"
    )

    try:
        admins = await context.bot.get_chat_administrators(scope_chat_id)
    except Exception as e:
        logger.exception("get_chat_administrators failed: %s", e)
        return (0, 0)

    for a in admins:
        try:
            if getattr(a.user, "is_bot", False):
                continue
            await context.bot.send_message(
                chat_id=a.user.id,
                text=admin_text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            sent_ok += 1
        except Forbidden:
            sent_fail += 1
        except Exception:
            sent_fail += 1

    return (sent_ok, sent_fail)



# ---------------- BROADCAST ----------------

def _bcast_get_data(context: ContextTypes.DEFAULT_TYPE) -> dict:
    data = context.user_data.get(BCAST_DATA)
    if not isinstance(data, dict):
        data = {"topic": None, "text": None, "files": []}
        context.user_data[BCAST_DATA] = data
    if "files" not in data or not isinstance(data.get("files"), list):
        data["files"] = []
    return data

def _bcast_compose_message(topic: str | None, body: str | None) -> str:
    topic = (topic or "").strip()
    body = (body or "").strip()
    # Экрануем пользовательский ввод для HTML
    topic_esc = escape(topic) if topic else ""
    body_esc = escape(body) if body else ""
    if topic_esc and body_esc:
        return f"<b>{topic_esc}</b>\n\n{body_esc}"
    if topic_esc:
        return f"<b>{topic_esc}</b>"
    return body_esc

async def broadcast_to_chats(context: ContextTypes.DEFAULT_TYPE, message_html: str, files: list[dict]) -> tuple[int, int]:
    """Рассылка в notify_chats. Возвращает (ok, fail).

    Формат отправки:
      A) нет файлов -> одно текстовое сообщение
      B) ровно 1 файл (document/photo/video) -> одно сообщение с caption
      C) несколько файлов и ВСЕ photo/video -> media_group, caption у первого
      D) иначе -> текст отдельным + файлы по одному (fallback)
    """
    ok = 0
    fail = 0

    # caption лимиты у Telegram ~1024; оставим запас
    def cap(text: str) -> str:
        if not text:
            return ""
        return text[:900]

    chat_ids = db_list_chats()
    files = files or []

    for cid in chat_ids:
        try:
            # A) только текст
            if not files:
                if message_html:
                    await context.bot.send_message(
                        chat_id=cid,
                        text=message_html,
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True,
                    )
                ok += 1
                continue

            # B) один файл -> caption в это же сообщение
            if len(files) == 1:
                f0 = files[0]
                kind = f0.get("kind")
                file_id = f0.get("file_id")
                caption = cap(message_html)

                if kind == "document":
                    await context.bot.send_document(
                        chat_id=cid,
                        document=file_id,
                        caption=caption or None,
                        parse_mode=ParseMode.HTML if caption else None,
                    )
                elif kind == "photo":
                    await context.bot.send_photo(
                        chat_id=cid,
                        photo=file_id,
                        caption=caption or None,
                        parse_mode=ParseMode.HTML if caption else None,
                    )
                elif kind == "video":
                    await context.bot.send_video(
                        chat_id=cid,
                        video=file_id,
                        caption=caption or None,
                        parse_mode=ParseMode.HTML if caption else None,
                    )
                else:
                    # неизвестный тип -> fallback: текст + файл как документ
                    if message_html:
                        await context.bot.send_message(
                            chat_id=cid,
                            text=message_html,
                            parse_mode=ParseMode.HTML,
                            disable_web_page_preview=True,
                        )
                    if file_id:
                        await context.bot.send_document(chat_id=cid, document=file_id)
                ok += 1
                continue

            # C) несколько и все фото/видео -> media_group
            all_media = all((x.get("kind") in ("photo", "video")) for x in files)
            if all_media:
                media = []
                caption = cap(message_html)
                for i, f0 in enumerate(files[:10]):  # лимит TG на альбом 10
                    kind = f0.get("kind")
                    file_id = f0.get("file_id")
                    if not file_id:
                        continue
                    if kind == "photo":
                        media.append(
                            InputMediaPhoto(
                                media=file_id,
                                caption=(caption if i == 0 and caption else None),
                                parse_mode=(ParseMode.HTML if i == 0 and caption else None),
                            )
                        )
                    else:
                        media.append(
                            InputMediaVideo(
                                media=file_id,
                                caption=(caption if i == 0 and caption else None),
                                parse_mode=(ParseMode.HTML if i == 0 and caption else None),
                            )
                        )

                if media:
                    await context.bot.send_media_group(chat_id=cid, media=media)
                    ok += 1
                    continue

            # D) fallback: текст отдельно + файлы по одному
            if message_html:
                await context.bot.send_message(
                    chat_id=cid,
                    text=message_html,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
            for f0 in files:
                kind = f0.get("kind")
                file_id = f0.get("file_id")
                if not file_id:
                    continue
                if kind == "document":
                    await context.bot.send_document(chat_id=cid, document=file_id)
                elif kind == "photo":
                    await context.bot.send_photo(chat_id=cid, photo=file_id)
                elif kind == "video":
                    await context.bot.send_video(chat_id=cid, video=file_id)
            ok += 1
        except Exception as e:
            logger.exception("Broadcast failed to %s: %s", cid, e)
            fail += 1

    return ok, fail

# ---------------- APP ----------------

def main():
    ensure_db_path(DB_PATH)
    ensure_storage_dir(STORAGE_DIR)
    db_init()

    request = HTTPXRequest(connect_timeout=15, read_timeout=30, write_timeout=30, pool_timeout=30)

    app = Application.builder().token(BOT_TOKEN).request(request).build()

    # commands
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("setchat", cmd_setchat))
    app.add_handler(CommandHandler("unsetchat", cmd_unsetchat))
    app.add_handler(CommandHandler("force_standup", cmd_force_standup))
    app.add_handler(CommandHandler("test_industry", cmd_test_industry))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("reset", cmd_reset))
    app.add_handler(CommandHandler("export_csv", cmd_export_csv))
    app.add_handler(CommandHandler("import_csv", cmd_import_csv))

    # callbacks: meetings
    app.add_handler(CallbackQueryHandler(cb_cancel_open, pattern=r"^cancel:open:(standup|industry)$"))
    app.add_handler(CallbackQueryHandler(cb_cancel_close, pattern=r"^cancel:close:(standup|industry)$"))
    app.add_handler(CallbackQueryHandler(cb_cancel_reason, pattern=r"^cancel:reason:(standup|industry):(no_topics|tech|move)$"))
    app.add_handler(CallbackQueryHandler(cb_reschedule_pick, pattern=r"^reschedule:pick:(standup|industry):\d{2}\.\d{2}\.\d{2}$"))
    app.add_handler(CallbackQueryHandler(cb_reschedule_manual, pattern=r"^reschedule:manual:(standup|industry)$"))
    app.add_handler(CallbackQueryHandler(cb_cancel_manual_input, pattern=r"^reschedule:cancel_manual:(standup|industry)$"))

    # callbacks: help
    app.add_handler(CallbackQueryHandler(cb_help, pattern=r"^(help:|noop)"))

    # new members welcome
    app.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, on_new_members))

    # document upload
    app.add_handler(MessageHandler(filters.Document.ALL, on_document))

    # broadcast media (photo/video)
    app.add_handler(MessageHandler(filters.PHOTO, on_photo))
    app.add_handler(MessageHandler(filters.VIDEO, on_video))

    # text input
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    # schedule checker
    app.job_queue.run_repeating(check_and_send_jobs, interval=60, first=10, name="meetings_checker")

    logger.info("Bot started. DB=%s", DB_PATH)
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
