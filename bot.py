import os
import re
import random
import secrets
from dataclasses import dataclass, field
import sqlite3
import logging
import time
import csv
import io
import zipfile
import json
import html as html_lib
import httpx
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


# ---------------- TEXT -> HTML (entities incl. blockquote) ----------------
def _utf16_to_py_index(s: str, u16_index: int) -> int:
    """
    Convert Telegram UTF-16 code unit offset to Python string index.
    Telegram entities offsets/length are based on UTF-16 code units.
    """
    if u16_index <= 0:
        return 0
    count = 0
    for i, ch in enumerate(s):
        # characters outside BMP take 2 UTF-16 code units
        count += 2 if ord(ch) > 0xFFFF else 1
        if count >= u16_index:
            return i + 1
    return len(s)

def _entity_open_close(entity) -> tuple[str, str]:
    t = getattr(entity, "type", "")
    if t == "bold":
        return "<b>", "</b>"
    if t == "italic":
        return "<i>", "</i>"
    if t == "underline":
        return "<u>", "</u>"
    if t == "strikethrough":
        return "<s>", "</s>"
    if t == "spoiler":
        return '<span class="tg-spoiler">', "</span>"
    if t == "code":
        return "<code>", "</code>"
    if t == "pre":
        lang = getattr(entity, "language", None)
        if lang:
            # Telegram HTML supports <pre><code class="language-...">...</code></pre>
            return f'<pre><code class="language-{html_lib.escape(lang)}">', "</code></pre>"
        return "<pre>", "</pre>"
    if t == "text_link":
        url = getattr(entity, "url", "") or ""
        return f'<a href="{html_lib.escape(url, quote=True)}">', "</a>"
    if t == "blockquote":
        return "<blockquote>", "</blockquote>"
    if t == "expandable_blockquote":
        return "<blockquote expandable>", "</blockquote>"
    # Fallback: unsupported entity types are ignored
    return "", ""

def _text_with_entities_to_html(text: str, entities: list) -> str:
    if not text:
        return ""
    entities = list(entities or [])
    if not entities:
        return html_lib.escape(text)

    # Prepare start/end events
    starts: dict[int, list[tuple[int, str]]] = {}
    ends: dict[int, list[tuple[int, str]]] = {}

    for e in entities:
        try:
            off = int(getattr(e, "offset", 0))
            ln = int(getattr(e, "length", 0))
        except Exception:
            continue
        if ln <= 0:
            continue

        start = _utf16_to_py_index(text, off)
        end = _utf16_to_py_index(text, off + ln)
        if end <= start:
            continue

        open_tag, close_tag = _entity_open_close(e)
        if not open_tag:
            continue

        # For stable nesting:
        # - open outer first => sort opens by longer span first (end desc)
        # - close inner first => sort closes by shorter span first (start desc)
        starts.setdefault(start, []).append((end, open_tag))
        ends.setdefault(end, []).append((start, close_tag))

    out: list[str] = []
    for i in range(0, len(text) + 1):
        if i in ends:
            # close inner first => larger start (inner) first
            for _start, tag in sorted(ends[i], key=lambda x: x[0], reverse=True):
                out.append(tag)
        if i in starts:
            # open outer first => larger end first
            for _end, tag in sorted(starts[i], key=lambda x: x[0], reverse=True):
                out.append(tag)
        if i < len(text):
            out.append(html_lib.escape(text[i]))
    return "".join(out)

def message_to_html(message) -> str:
    """
    Returns HTML suitable for ParseMode.HTML from a Telegram Message,
    preserving formatting entities (including blockquote).
    """
    if not message:
        return ""
    if getattr(message, "text", None):
        return _text_with_entities_to_html(message.text, getattr(message, "entities", None) or [])
    if getattr(message, "caption", None):
        return _text_with_entities_to_html(message.caption, getattr(message, "caption_entities", None) or [])
    return ""

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

# ------- MEMES (channel source) -------
MEME_CHANNEL_ID = int(os.getenv("MEME_CHANNEL_ID", "-1003761916249"))

# -------- ACCESS CONTROL --------
ACCESS_CHAT_ID = -1003399576556

NO_ACCESS_TEXT = (
    "🕵️‍♂️ Еще никогда Штирлиц не был так близок к провалу!\n\n"
    "🚫 Не нашёл Вас в чате — данные вам недоступны!"
)

INDUSTRY_WIKI_URL = os.getenv("INDUSTRY_WIKI_URL", "")
STAFF_URL = os.getenv("STAFF_URL", "")
SITE_URL = os.getenv("SITE_URL", "")
LITE_FORM_URL = os.getenv("LITE_FORM_URL", "")
LEAD_CRM_URL = os.getenv("LEAD_CRM_URL", "")
REANIMATION_REQUEST_URL = os.getenv("REANIMATION_REQUEST_URL", "")
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


# ---------------- HOROSCOPE ----------------

ZODIAC = [
    ("aries", "♈ Овен"),
    ("taurus", "♉ Телец"),
    ("gemini", "♊ Близнецы"),
    ("cancer", "♋ Рак"),
    ("leo", "♌ Лев"),
    ("virgo", "♍ Дева"),
    ("libra", "♎ Весы"),
    ("scorpio", "♏ Скорпион"),
    ("sagittarius", "♐ Стрелец"),
    ("capricorn", "♑ Козерог"),
    ("aquarius", "♒ Водолей"),
    ("pisces", "♓ Рыбы"),
]
ZODIAC_NAME = {slug: title for slug, title in ZODIAC}


def kb_horo_signs():
    # Инвертированная "пирамида": сверху более длинные названия, ниже — короче
    # (широкая верхушка -> узкое основание)
    layout = [
        ["sagittarius", "capricorn", "scorpio", "aquarius"],  # самые длинные
        ["gemini", "taurus", "pisces"],                       # средние
        ["virgo", "cancer", "libra"],                         # короче
        ["aries", "leo"],                                     # самые короткие
    ]

    rows = []
    for slugs in layout:
        row = [
            InlineKeyboardButton(ZODIAC_NAME[slug], callback_data=f"horo:sign:{slug}")
            for slug in slugs
        ]
        rows.append(row)
    return InlineKeyboardMarkup(rows)



def kb_horo_after():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Твой персональный мем 😂", callback_data="horo:meme")]
    ])

def zodiac_from_ddmm(ddmm: str) -> str | None:
    # ddmm = "ДД.ММ"
    try:
        dd, mm = ddmm.split(".")
        d = int(dd)
        m = int(mm)
    except Exception:
        return None

    if (m == 3 and d >= 21) or (m == 4 and d <= 19): return "aries"
    if (m == 4 and d >= 20) or (m == 5 and d <= 20): return "taurus"
    if (m == 5 and d >= 21) or (m == 6 and d <= 20): return "gemini"
    if (m == 6 and d >= 21) or (m == 7 and d <= 22): return "cancer"
    if (m == 7 and d >= 23) or (m == 8 and d <= 22): return "leo"
    if (m == 8 and d >= 23) or (m == 9 and d <= 22): return "virgo"
    if (m == 9 and d >= 23) or (m == 10 and d <= 22): return "libra"
    if (m == 10 and d >= 23) or (m == 11 and d <= 21): return "scorpio"
    if (m == 11 and d >= 22) or (m == 12 and d <= 21): return "sagittarius"
    if (m == 12 and d >= 22) or (m == 1 and d <= 19): return "capricorn"
    if (m == 1 and d >= 20) or (m == 2 and d <= 18): return "aquarius"
    if (m == 2 and d >= 19) or (m == 3 and d <= 20): return "pisces"
    return None

def split_sentences_ru(text: str) -> list[str]:
    """
    Very small RU sentence splitter suitable for horoscope paragraphs.
    Keeps punctuation at the end of each sentence.
    """
    t = re.sub(r"\s+", " ", (text or "").strip())
    if not t:
        return []
    # Split on . ! ? … keeping delimiter
    parts = re.split(r"(?<=[\.!\?…])\s+", t)
    out: list[str] = []
    for s in parts:
        s = s.strip()
        if not s:
            continue
        out.append(s)
    return out


def extract_horo_blocks(horo_text: str) -> tuple[str, str, str]:
    """
    Returns (body_text, advice_sentence, focus_sentence).

    IMPORTANT:
    - advice_sentence and focus_sentence are taken STRICTLY from the original horoscope text (no new wording).
    - body_text is the original horoscope text with those two sentences removed (to avoid duplication).
    """
    sents = split_sentences_ru(horo_text)
    src = re.sub(r"\s+", " ", (horo_text or "").strip())
    if not sents:
        t = src.strip()
        return t, t, t

    # Scoring for "advice" (directive-like sentence)
    advice_keywords = [
        "советует", "стоит", "нужно", "не ", "следите", "контролируйте", "постарайтесь",
        "не стоит", "важно", "лучше", "осторож", "держите", "помните",
    ]

    def advice_score(sent: str) -> int:
        sl = sent.lower()
        sc = 0
        for kw in advice_keywords:
            if re.search(kw, sl):
                sc += 3
        # avoid meta sentences like "Гороскоп на сегодня..."
        if sl.startswith("гороскоп"):
            sc -= 4
        # shorter reads better as a separate block
        if len(sent) <= 150:
            sc += 1
        return sc

    ranked_advice = sorted(sents, key=advice_score, reverse=True)
    advice = ranked_advice[0].strip()

    remaining = [s for s in sents if s.strip() != advice]

    # Scoring for "focus" (usually a short "keep an eye on ..." sentence)
    focus_keywords = ["следите", "контрол", "держите", "помните", "осторож", "не спеш", "не тороп", "не кидай"]
    def focus_score(sent: str) -> int:
        sl = sent.lower()
        sc = 0
        for kw in focus_keywords:
            if re.search(kw, sl):
                sc += 4
        # penalize the same "Гороскоп на сегодня..." meta phrasing
        if "гороскоп на сегодня" in sl or sl.startswith("гороскоп"):
            sc -= 6
        # prefer concise focus
        if len(sent) <= 120:
            sc += 2
        elif len(sent) <= 180:
            sc += 1
        return sc

    focus = None
    if remaining:
        ranked_focus = sorted(remaining, key=focus_score, reverse=True)
        focus = ranked_focus[0].strip()

    if not focus:
        focus = (remaining[0] if remaining else advice).strip()

    # Build body without duplicates (remove first occurrences only)
    body_sents = [s.strip() for s in sents if s.strip() not in (advice, focus)]
    body = " ".join(body_sents).strip()
    if not body:
        body = src.strip()

    return body, advice, focus

async def fetch_rambler_horo(sign_slug: str) -> tuple[str, str | None]:
    """
    Fetches Russian daily horoscope text from Rambler and returns:
      (horo_text, date_str)

    We intentionally return ONLY the horoscope body text (no menus/author/like/share).
    """
    url = f"https://horoscopes.rambler.ru/{sign_slug}/"
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; meetings-bot/1.0)",
        "Accept-Language": "ru-RU,ru;q=0.9",
    }

    async with httpx.AsyncClient(timeout=10.0, headers=headers, follow_redirects=True) as client:
        r = await client.get(url)
        r.raise_for_status()
        page_html = r.text

    # Strip scripts/styles to avoid noise
    cleaned = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", page_html)
    cleaned = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", cleaned)

    # Date (e.g. "26 января 2026") – try to find anywhere on the page
    plain_for_date = re.sub(r"(?is)<[^>]+>", " ", cleaned)
    plain_for_date = html_lib.unescape(plain_for_date)
    plain_for_date = re.sub(r"\s+", " ", plain_for_date)
    date_m = re.search(r"\b\d{1,2}\s+[А-Яа-яЁё]+\s+\d{4}\b", plain_for_date)
    date_str = date_m.group(0) if date_m else None

    # Extract paragraphs; Rambler keeps horoscope body in <p> tags
    p_blocks = re.findall(r"(?is)<p\b[^>]*>(.*?)</p>", cleaned)
    paras: list[str] = []
    for p in p_blocks:
        t = re.sub(r"(?is)<[^>]+>", " ", p)
        t = html_lib.unescape(t)
        t = re.sub(r"\s+", " ", t).strip()
        if not t:
            continue
        # Filter obvious UI garbage if it leaks into <p>
        bad = ("Нравится", "Поделиться", "Следующая неделя", "Неделя", "Месяц", "Январь", "Февраль")
        if any(b in t for b in bad):
            continue
        # Keep only meaningful Cyrillic text
        if len(re.findall(r"[А-Яа-яЁё]", t)) < 20:
            continue
        paras.append(t)

    if not paras:
        raise RuntimeError("Не удалось извлечь текст гороскопа (Rambler)")

    # Usually the horoscope is the longest paragraph block
    horo_text = max(paras, key=len).strip()

    return horo_text, date_str

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

    # ------- HORO: rate-limit (1 раз в день) + знак для пользователей без анкеты -------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS horo_rate (
            user_id INTEGER PRIMARY KEY,
            last_date TEXT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS horo_users (
            user_id INTEGER PRIMARY KEY,
            sign_slug TEXT NOT NULL
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

    

    # ------- HELP MENU: FAQ -------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS faq_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            question TEXT NOT NULL,
            answer TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)
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
            tg_user_id INTEGER,
            avg_test_score INTEGER,
            created_at TEXT NOT NULL
        )
    """)

    # миграция для старых БД: birthday
    try:
        cur.execute("ALTER TABLE profiles ADD COLUMN birthday TEXT")
    except sqlite3.OperationalError:
        pass




    # ✅ миграция для старых БД: tg_user_id
    try:
        cur.execute("ALTER TABLE profiles ADD COLUMN tg_user_id INTEGER")
    except sqlite3.OperationalError:
        pass

    # ✅ миграция для старых БД: avg_test_score (средний балл тестирования, %)
    try:
        cur.execute("ALTER TABLE profiles ADD COLUMN avg_test_score INTEGER")
    except sqlite3.OperationalError:
        pass


    # ------- ACHIEVEMENTS: выдачи ачивок -------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS achievement_awards (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        profile_id INTEGER NOT NULL,
        emoji TEXT NOT NULL,
        title TEXT NOT NULL,
        description TEXT NOT NULL,
        awarded_at TEXT NOT NULL,
        awarded_by INTEGER,
        FOREIGN KEY(profile_id) REFERENCES profiles(id) ON DELETE CASCADE
    )
""")
    # ------- MEMES: пул мемов из канала -------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS memes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            kind TEXT NOT NULL,           -- photo|video|document
            file_id TEXT NOT NULL,
            unique_key TEXT UNIQUE,       -- чтобы не дублировать
            created_at TEXT NOT NULL
        )
    """)


    # ------- MEME SENDS: выдача мемов без повторов в день -------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS meme_sends (
            day TEXT NOT NULL,            -- YYYY-MM-DD (по MOSCOW_TZ)
            user_id INTEGER NOT NULL,
            meme_id INTEGER NOT NULL,
            sent_at TEXT NOT NULL,
            PRIMARY KEY (day, user_id),
            UNIQUE (day, meme_id),
            FOREIGN KEY(meme_id) REFERENCES memes(id) ON DELETE CASCADE
        )
    """)



    # ===================== TESTING (employees) DB =====================
    # templates
    cur.execute("""
        CREATE TABLE IF NOT EXISTS test_templates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            created_by INTEGER,
            created_at TEXT NOT NULL,
            is_draft_visible INTEGER NOT NULL DEFAULT 1
        )
    """)
    # миграция для старых БД: is_draft_visible (логическое удаление черновиков)
    try:
        cur.execute("ALTER TABLE test_templates ADD COLUMN is_draft_visible INTEGER NOT NULL DEFAULT 1")
    except sqlite3.OperationalError:
        pass


    # questions
    cur.execute("""
        CREATE TABLE IF NOT EXISTS test_questions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            template_id INTEGER NOT NULL,
            idx INTEGER NOT NULL,
            q_type TEXT NOT NULL, -- open|single|multi
            question_text TEXT NOT NULL,
            options_json TEXT,    -- JSON list[str]
            correct_json TEXT,    -- JSON list[int]
            created_at TEXT NOT NULL,
            FOREIGN KEY(template_id) REFERENCES test_templates(id) ON DELETE CASCADE
        )
    """)

    # assignments
    cur.execute("""
        CREATE TABLE IF NOT EXISTS test_assignments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            template_id INTEGER NOT NULL,
            profile_id INTEGER NOT NULL,
            assigned_by INTEGER,
            assigned_at TEXT NOT NULL,
            time_limit_sec INTEGER,
            deadline_at TEXT,
            status TEXT NOT NULL, -- assigned|in_progress|finished|expired|canceled|saved
            started_at TEXT,
            finished_at TEXT,
            current_idx INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY(template_id) REFERENCES test_templates(id) ON DELETE CASCADE,
            FOREIGN KEY(profile_id) REFERENCES profiles(id) ON DELETE CASCADE
        )
    """)

    # answers
    cur.execute("""
        CREATE TABLE IF NOT EXISTS test_answers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            assignment_id INTEGER NOT NULL,
            question_id INTEGER NOT NULL,
            answer_json TEXT NOT NULL,
            is_correct INTEGER, -- 1/0/NULL
            answered_at TEXT NOT NULL,
            UNIQUE(assignment_id, question_id),
            FOREIGN KEY(assignment_id) REFERENCES test_assignments(id) ON DELETE CASCADE,
            FOREIGN KEY(question_id) REFERENCES test_questions(id) ON DELETE CASCADE
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

def db_get_horo_last_date(user_id: int) -> str | None:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT last_date FROM horo_rate WHERE user_id=?", (int(user_id),))
    row = cur.fetchone()
    con.close()
    return row[0] if row else None


def db_set_horo_last_date(user_id: int, date_iso: str):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """INSERT INTO horo_rate(user_id, last_date) VALUES(?, ?)
           ON CONFLICT(user_id) DO UPDATE SET last_date=excluded.last_date""",
        (int(user_id), date_iso),
    )
    con.commit()
    con.close()



# ---------------- MEMES DB ----------------

def db_meme_add(kind: str, file_id: str, unique_key: str):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """INSERT INTO memes(kind, file_id, unique_key, created_at)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(unique_key) DO NOTHING""",
        (kind, file_id, unique_key, datetime.utcnow().isoformat()),
    )
    con.commit()
    con.close()


def db_meme_user_has_today(user_id: int, day_iso: str) -> bool:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT 1 FROM meme_sends WHERE day=? AND user_id=? LIMIT 1", (day_iso, user_id))
    row = cur.fetchone()
    con.close()
    return bool(row)


def db_meme_pick_for_day(day_iso: str) -> dict | None:
    """
    Выбираем случайный мем, который ещё НЕ выдавался никому в этот день.
    """
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        SELECT m.id, m.kind, m.file_id
        FROM memes m
        LEFT JOIN meme_sends s
            ON s.meme_id = m.id AND s.day = ?
        WHERE s.meme_id IS NULL
        ORDER BY RANDOM()
        LIMIT 1
    """, (day_iso,))
    row = cur.fetchone()
    con.close()
    if not row:
        return None
    return {"id": row[0], "kind": row[1], "file_id": row[2]}


def db_meme_mark_sent(day_iso: str, user_id: int, meme_id: int) -> bool:
    """
    Пишем факт выдачи. Возвращает True если успешно (без конфликтов),
    False если уже есть выдача пользователю сегодня или мем уже занят сегодня.
    """
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    try:
        cur.execute("""
            INSERT INTO meme_sends(day, user_id, meme_id, sent_at)
            VALUES (?, ?, ?, ?)
        """, (day_iso, user_id, meme_id, datetime.utcnow().isoformat()))
        con.commit()
        ok = True
    except sqlite3.IntegrityError:
        ok = False
    finally:
        con.close()
    return ok


def db_horo_get_user_sign(user_id: int) -> str | None:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT sign_slug FROM horo_users WHERE user_id=?", (int(user_id),))
    row = cur.fetchone()
    con.close()
    return row[0] if row else None


def db_horo_set_user_sign(user_id: int, sign_slug: str):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """INSERT INTO horo_users(user_id, sign_slug) VALUES(?, ?)
           ON CONFLICT(user_id) DO UPDATE SET sign_slug=excluded.sign_slug""",
        (int(user_id), sign_slug),
    )
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




# ---------------- HELP DB: FAQ ----------------

def db_faq_list() -> list[tuple[int, str]]:
    """Список FAQ (id, question), последние сверху."""
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT id, question FROM faq_items ORDER BY id DESC")
    rows = cur.fetchall()
    con.close()
    return [(int(r[0]), r[1]) for r in rows]


def db_faq_get(fid: int) -> dict | None:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT id, question, answer FROM faq_items WHERE id=?", (int(fid),))
    row = cur.fetchone()
    con.close()
    if not row:
        return None
    return {"id": int(row[0]), "question": row[1], "answer": row[2]}


def db_faq_add(question: str, answer: str) -> int:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        "INSERT INTO faq_items(question, answer, created_at) VALUES(?, ?, ?)",
        (question.strip(), answer.strip(), datetime.utcnow().isoformat()),
    )
    con.commit()
    fid = cur.lastrowid
    con.close()
    return int(fid)


def db_faq_delete(fid: int) -> bool:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("DELETE FROM faq_items WHERE id=?", (int(fid),))
    ok = cur.rowcount > 0
    con.commit()
    con.close()
    return ok


def db_faq_upsert(question: str, answer: str) -> int:
    """Upsert по question: если вопрос уже есть — обновляем answer."""
    q = (question or "").strip()
    a = (answer or "").strip()
    if not q or not a:
        return 0

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT id FROM faq_items WHERE question=?", (q,))
    row = cur.fetchone()
    if row:
        fid = int(row[0])
        cur.execute("UPDATE faq_items SET answer=? WHERE id=?", (a, fid))
        con.commit()
        con.close()
        return fid

    cur.execute(
        "INSERT INTO faq_items(question, answer, created_at) VALUES(?, ?, ?)",
        (q, a, datetime.utcnow().isoformat()),
    )
    con.commit()
    fid = int(cur.lastrowid)
    con.close()
    return fid

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
        SELECT id, full_name, year_start, city, birthday, about, topics, tg_link, tg_user_id, avg_test_score
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
        "tg_user_id": row[8],
        "avg_test_score": row[9],
    }

def db_profiles_get_by_tg_link(tg_link: str):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        SELECT id, full_name, year_start, city, birthday, about, topics, tg_link, tg_user_id, avg_test_score
        FROM profiles
        WHERE tg_link=?
    """, (tg_link.strip(),))
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
        "tg_user_id": row[8],
        "avg_test_score": row[9],
    }





# ===================== TESTING: TG USER ID SYNC (profiles) =========

def db_profiles_set_tg_user_id(profile_id: int, tg_user_id: int):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("UPDATE profiles SET tg_user_id=? WHERE id=?", (int(tg_user_id), int(profile_id)))
    con.commit()
    con.close()

def db_profiles_set_avg_test_score(profile_id: int, avg_test_score: int | None):
    """Устанавливает средний балл тестирования (в процентах) для карточки сотрудника."""
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("UPDATE profiles SET avg_test_score=? WHERE id=?", (avg_test_score, int(profile_id)))
    con.commit()
    con.close()




def db_profiles_get_by_tg_user_id(tg_user_id: int):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        SELECT id, full_name, year_start, city, birthday, about, topics, tg_link, tg_user_id, avg_test_score
        FROM profiles
        WHERE tg_user_id=?
        """,
        (int(tg_user_id),),
    )
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
        "tg_user_id": row[8],
        "avg_test_score": row[9],
    }


def _normalize_profile_tg_link(username: str | None) -> str | None:
    if not username:
        return None
    u = username.strip().lstrip("@")
    if not u:
        return None
    return "@" + u


async def sync_profile_user_id_from_update(update: Update):
    """
    Если у пользователя есть @username, и в profiles.tg_link есть такой же,
    то записываем tg_user_id = update.effective_user.id.

    Это позволяет слать ЛС по user_id (chat_id), а не по @username.
    """
    user = update.effective_user
    if not user:
        return
    tg_link = _normalize_profile_tg_link(getattr(user, "username", None))
    if not tg_link:
        return
    prof = db_profiles_get_by_tg_link(tg_link)
    if not prof:
        return
    if prof.get("tg_user_id") == user.id:
        return
    db_profiles_set_tg_user_id(int(prof["id"]), int(user.id))

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


# ---------------- ACHIEVEMENTS (awards) ----------------

def db_achievements_list(profile_id: int) -> list[dict]:
    """Список ачивок для профиля (последние сверху)."""
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        SELECT emoji, title, description, awarded_at
        FROM achievement_awards
        WHERE profile_id=?
        ORDER BY id DESC
        """,
        (int(profile_id),),
    )
    rows = cur.fetchall()
    con.close()
    return [
        {"emoji": r[0], "title": r[1], "description": r[2], "awarded_at": r[3]}
        for r in rows
    ]


def db_achievement_award_add(profile_id: int, emoji: str, title: str, description: str, awarded_by: int | None = None) -> int:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO achievement_awards(profile_id, emoji, title, description, awarded_at, awarded_by)
        VALUES(?, ?, ?, ?, ?, ?)
        """,
        (int(profile_id), emoji.strip(), title.strip(), description.strip(), datetime.utcnow().isoformat(), awarded_by),
    )
    con.commit()
    aid = cur.lastrowid
    con.close()
    return aid


def export_achievement_awards_rows() -> list[dict]:
    """Для CSV/ZIP бэкапа: все выданные ачивки."""
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        SELECT a.id, p.id, p.full_name, p.tg_link, a.emoji, a.title, a.description, a.awarded_at, a.awarded_by
        FROM achievement_awards a
        JOIN profiles p ON p.id = a.profile_id
        ORDER BY a.id ASC
        """
    )
    rows = cur.fetchall()
    con.close()
    out = []
    for r in rows:
        out.append({
            "award_id": r[0],
            "profile_id": r[1],
            "full_name": r[2] or "",
            "tg_link": r[3] or "",
            "emoji": r[4] or "",
            "title": r[5] or "",
            "description": r[6] or "",
            "awarded_at": r[7] or "",
            "awarded_by": r[8] or "",
        })
    return out


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
    "Доброе утро, супергерои задач! 🦸♀️🦸♂️",
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
        f""
    )

def build_industry_text(industry_zoom_url: str) -> str:
    return (
        "Коллеги, привет! ☕️✨\n"
        "На горизонте <b>Отраслевая встреча</b> — стартуем через <b>30 минут</b> 🚀\n\n"
        "⏰ Встречаемся в <b>12:00 (МСК)</b>\n\n"
        f'👉 <a href="{industry_zoom_url}">Присоединиться к Zoom</a>\n\n'
        ""
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



async def is_member_of_access_chat(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """
    True если пользователь состоит в ACCESS_CHAT_ID.
    """
    try:
        member = await context.bot.get_chat_member(ACCESS_CHAT_ID, user_id)
        return member.status in ("member", "administrator", "creator")
    except Forbidden:
        logger.warning(
            "Forbidden while checking ACCESS_CHAT_ID. "
            "Bot must be member of the chat and have rights."
        )
        return False
    except Exception as e:
        logger.exception("Error checking access chat membership: %s", e)
        return False


async def deny_no_access(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """
    Если пользователь не в чате — шлём сообщение и запрещаем дальнейшую обработку.
    """
    user = update.effective_user
    if not user:
        return True

    has_access = await is_member_of_access_chat(user.id, context)
    if has_access:
        return False

    try:
        if update.message:
            await update.message.reply_text(NO_ACCESS_TEXT)
        elif update.callback_query:
            await update.callback_query.answer("Нет доступа", show_alert=True)
            await update.callback_query.message.reply_text(NO_ACCESS_TEXT)
        elif update.effective_chat:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=NO_ACCESS_TEXT,
            )
    except Exception:
        pass

    return True

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


# faq add flow
WAITING_FAQ_Q = "waiting_faq_q"
WAITING_FAQ_A = "waiting_faq_a"
PENDING_FAQ = "pending_faq"

WAITING_RESTORE_ZIP = "waiting_restore_zip"
# profiles add flow
PROFILE_WIZ_ACTIVE = "profile_wiz_active"

# csv import flow
WAITING_CSV_IMPORT = "waiting_csv_import"
WAITING_ZIP_IMPORT = "waiting_zip_import"
WAITING_TEST_AVGSCORE = "waiting_test_avgscore"
WAITING_TEST_AVGSCORE_PID = "waiting_test_avgscore_pid"



# bonus calculator (FAQ)
WAITING_BONUS_CALC = "waiting_bonus_calc"
BONUS_STEP = "bonus_step"
BONUS_DATA = "bonus_data"

# achievements award flow
ACH_WIZ_ACTIVE = "ach_wiz_active"
ACH_WIZ_STEP = "ach_wiz_step"
ACH_WIZ_DATA = "ach_wiz_data"
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


def clear_faq_flow(context: ContextTypes.DEFAULT_TYPE):
    context.chat_data[WAITING_FAQ_Q] = False
    context.chat_data[WAITING_FAQ_A] = False
    context.chat_data.pop(PENDING_FAQ, None)



def clear_csv_import(context: ContextTypes.DEFAULT_TYPE):
    context.chat_data[WAITING_CSV_IMPORT] = False
    context.chat_data.pop(WAITING_USER_ID, None)
    context.chat_data.pop(WAITING_SINCE_TS, None)

def clear_restore_zip(context: ContextTypes.DEFAULT_TYPE):
    context.chat_data[WAITING_RESTORE_ZIP] = False


def clear_profile_wiz(context: ContextTypes.DEFAULT_TYPE):
    context.chat_data[PROFILE_WIZ_ACTIVE] = False
    context.chat_data.pop(PROFILE_WIZ_STEP, None)
    context.chat_data.pop(PROFILE_WIZ_DATA, None)

def clear_zip_import(context: ContextTypes.DEFAULT_TYPE):
    context.chat_data[WAITING_ZIP_IMPORT] = False

def clear_ach_wiz(context: ContextTypes.DEFAULT_TYPE):
    context.chat_data[ACH_WIZ_ACTIVE] = False
    context.chat_data.pop(ACH_WIZ_STEP, None)
    context.chat_data.pop(ACH_WIZ_DATA, None)

def clear_suggest_flow(context: ContextTypes.DEFAULT_TYPE):
    context.user_data[WAITING_SUGGESTION_TEXT] = False
    context.user_data.pop(SUGGESTION_MODE, None)

def clear_bcast_flow(context: ContextTypes.DEFAULT_TYPE):
    context.user_data[BCAST_ACTIVE] = False
    context.user_data.pop(BCAST_STEP, None)
    context.user_data.pop(BCAST_DATA, None)


def clear_bonus_calc_flow(context: ContextTypes.DEFAULT_TYPE):
    context.chat_data[WAITING_BONUS_CALC] = False
    context.chat_data.pop(BONUS_STEP, None)
    context.chat_data.pop(BONUS_DATA, None)


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


def format_achievements_for_profile(profile_id: int) -> str:
    items = db_achievements_list(profile_id)
    if not items:
        return "— Всё ещё впереди —"
    parts = []
    for it in items[:10]:
        parts.append(f"{escape(it['emoji'])} <b>{escape(it['title'])}</b>\n{escape(it['description'])}")
    return "\n\n".join(parts)


BDAY_TEMPLATES: list[str] = [
    (
        "🎉 Коллеги, сегодня день рождения у {NAME}!\n\n"
        "Желаем крепкого здоровья, профессиональных побед и отличного настроения каждый день. "
        "Пусть работа радует, а жизнь приносит приятные сюрпризы! 🎂✨"
    ),
    (
        "🎊 Сегодня празднует день рождения {NAME}!\n\n"
        "Пусть впереди будет много интересных задач, сильных результатов и поводов для гордости. "
        "Спасибо, что ты с нами! 🎁😊"
    ),
    (
        "🚀 У нас повод для праздника!\n\n"
        "{NAME}, с днём рождения! Желаем драйва, роста, уверенных решений и кайфа от того, что ты делаешь. "
        "Пусть этот год будет особенно удачным! 🎉🔥"
    ),
    (
        "🌟 Сегодня поздравляем нашего коллегу {NAME} с днём рождения!\n\n"
        "Пусть в команде всегда будет поддержка, в проектах — успех, а вне работы — радость и баланс. "
        "Отличного года впереди! 🎂🤝"
    ),
    (
        "😄 Сегодня без повода работать серьёзно нельзя — у {NAME} день рождения!\n\n"
        "Желаем хорошего настроения, приятных задач и как можно больше классных моментов в этом году. 🎉🥳"
    ),
    (
        "💼 Коллеги, поздравляем {NAME} с днём рождения!\n\n"
        "Желаем стабильного роста, уверенных решений и проектов, которыми можно гордиться. "
        "Пусть всё задуманное реализуется! 🎯🎂"
    ),
    (
        "✨ Сегодня день рождения у {NAME}!\n\n"
        "Пусть каждый новый день приносит вдохновение, хорошие новости и ощущение, что ты на своём месте. "
        "С праздником! 🎉🎁"
    ),
]

def pick_bday_text(template_index: int, full_name: str, mention: str | None) -> str:
    """
    Возвращает текст поздравления по шаблону.

    - template_index: 0..len(BDAY_TEMPLATES)-1
    - Если есть mention -> подставляем @username в {NAME}
    - Иначе -> подставляем имя (первое слово из full_name; если не получилось, то full_name целиком)
    """
    if mention:
        name_for_text = mention
    else:
        full_name = (full_name or "").strip()
        name_for_text = (full_name.split()[0] if full_name else full_name)

    if not BDAY_TEMPLATES:
        return f"🎉 С днём рождения, {name_for_text}! 🎂"

    i = int(template_index) % len(BDAY_TEMPLATES)
    return BDAY_TEMPLATES[i].format(NAME=name_for_text)

async def send_birthday_congrats(context: ContextTypes.DEFAULT_TYPE) -> bool:
    """
    Шлёт поздравления в notify_chats всем, у кого birthday == сегодня (ДД.ММ).
    Использует 7 шаблонов и чередует их по кругу без повторов (до полного круга) через meta.
    """
    now_msk = datetime.now(MOSCOW_TZ)
    today_ddmm = now_msk.strftime("%d.%m")

    chat_ids = db_list_chats()
    if not chat_ids:
        logger.warning("No chats for notifications. Add via /setchat.")
        return False

    people = db_profiles_birthdays(today_ddmm)
    if not people:
        return False

    # какой шаблон следующий (0..len-1)
    next_tpl = db_get_meta("bday_template_next")
    try:
        tpl_idx = int(next_tpl) if next_tpl is not None else 0
    except Exception:
        tpl_idx = 0

    if not BDAY_TEMPLATES:
        tpl_idx = 0
    else:
        tpl_idx = tpl_idx % len(BDAY_TEMPLATES)

    sent_any = False

    for p in people:
        full_name = p.get("full_name", "")
        mention = normalize_tg_mention(p.get("tg_link", ""))

        text = pick_bday_text(tpl_idx, full_name, mention)

        # следующий шаблон по кругу
        if BDAY_TEMPLATES:
            tpl_idx = (tpl_idx + 1) % len(BDAY_TEMPLATES)

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
    db_set_meta("bday_template_next", str(tpl_idx))

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
        "Здесь собраны все полезные материалы и инструменты для команды 👇\n\n"
        "📄 <b>Документы</b>\n"
        "🔗 <b>Полезные ссылки</b>\n"
        "👥 <b>Краткая инфо о команде</b>\n"
        "❓ <b>FAQ и калькулятор премии</b>\n"
        "💡 <b>Предложка</b>\n"
        "🎮 <b>Досуг</b>\n"
    )


def kb_help_main(is_admin_user: bool):
    rows = [
        [InlineKeyboardButton("📄 Документы", callback_data="help:docs")],
        [InlineKeyboardButton("🔗 Полезные ссылки", callback_data="help:links")],
        [InlineKeyboardButton("👥 Краткая инфо о команде", callback_data="help:team")],
        [
            InlineKeyboardButton("❓ FAQ и калькулятор", callback_data="help:faq"),
            InlineKeyboardButton("💡 Предложка", callback_data="help:suggest"),
        ],
        [InlineKeyboardButton("🎮 Досуг", callback_data="help:leisure")],
    ]
    if is_admin_user:
        rows.append([InlineKeyboardButton("⚙️ Настройки", callback_data="help:settings")])
    return InlineKeyboardMarkup(rows)


def help_text_leisure() -> str:
    return (
        "🎮 <b>Досуг</b>\n\n"
        "Здесь можно сыграть с коллегой прямо в личных сообщениях.\n"
        "Никаких сообщений в рабочий чат не отправляется."
    )


def kb_help_leisure():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⚓ Игра «Морской бой»", callback_data="help:leisure:sb")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="help:main")],
    ])




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

def kb_help_faq_list():
    items = db_faq_list()
    rows = []
    rows.append([InlineKeyboardButton("🧮 Калькулятор премии", callback_data="help:faq:bonus")])
    if not items:
        rows.append([InlineKeyboardButton("— пока пусто —", callback_data="noop")])
    else:
        for fid, q in items[:40]:
            plain = html_lib.unescape(re.sub(r"<[^>]+>", "", q or ""))
            label = plain if len(plain) <= 60 else (plain[:57] + "…")
            rows.append([InlineKeyboardButton(label, callback_data=f"help:faq:item:{fid}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="help:main")])
    return InlineKeyboardMarkup(rows)


def kb_help_faq_item():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ Назад к FAQ", callback_data="help:faq")],
        [InlineKeyboardButton("🏠 В главное меню", callback_data="help:main")],
    ])


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
            "title": "Стафф 🧑🤝🧑",
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

    if REANIMATION_REQUEST_URL:
        catalog["reanimation_request"] = {
            "title": "Запрос на реанимацию 🚑",
            "url": REANIMATION_REQUEST_URL,
            "desc": "Этот файл со ссылками на компании, которые требуют поиска новых контактов, возобновление старых",
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
        [InlineKeyboardButton("🏆 Ачивки", callback_data="help:settings:ach")],
        [InlineKeyboardButton("📝 Тестирование", callback_data="help:settings:test")],
        [InlineKeyboardButton("❓ FAQ", callback_data="help:settings:faq")],
        [InlineKeyboardButton("📦 Скачать бэкап ZIP", callback_data="help:settings:backup_zip")],
        [InlineKeyboardButton("📥 Загрузить бэкап ZIP", callback_data="help:settings:restore_zip")],
        [InlineKeyboardButton("📣 Рассылка", callback_data="help:settings:bcast")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="help:main")],
    ])


def kb_settings_faq():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Добавить вопрос", callback_data="help:settings:faq:add")],
        [InlineKeyboardButton("➖ Удалить вопрос", callback_data="help:settings:faq:del")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="help:settings")],
    ])


def kb_pick_faq_to_delete():
    items = db_faq_list()
    rows = []
    if not items:
        rows.append([InlineKeyboardButton("— пусто —", callback_data="noop")])
    else:
        for fid, q in items[:40]:
            plain = html_lib.unescape(re.sub(r"<[^>]+>", "", q or ""))
            label = plain if len(plain) <= 60 else (plain[:57] + "…")
            rows.append([InlineKeyboardButton(f"🗑️ {label}", callback_data=f"help:settings:faq:del:{fid}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="help:settings:faq")])
    return InlineKeyboardMarkup(rows)


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

def kb_achievements_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎁 Выдать ачивку", callback_data="help:settings:ach:give")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="help:settings")],
    ])

def kb_pick_profile_for_achievement():
    people = db_profiles_list()
    rows = []
    if not people:
        rows.append([InlineKeyboardButton("— анкет нет —", callback_data="noop")])
    else:
        for pid, name in people[:60]:
            rows.append([InlineKeyboardButton(name, callback_data=f"help:settings:ach:pick:{pid}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="help:settings:ach")])
    return InlineKeyboardMarkup(rows)


# ===================== TESTING (employees) =====================
# NOTE: Admin wizard state is kept in context.user_data (not chat_data).
TEST_WIZ_ACTIVE = "test_wiz_active"
TEST_WIZ_STEP = "test_wiz_step"
TEST_WIZ_DATA = "test_wiz_data"
TEST_WIZ_SELECTED_PIDS = "test_wiz_selected_pids"
TEST_WIZ_TEMPLATE_ID = "test_wiz_template_id"
TEST_WIZ_FROM_TEMPLATE_ID = "test_wiz_from_template_id"

TEST_WIZ_STEP_TITLE = "title"
TEST_WIZ_STEP_MENU = "menu"
TEST_WIZ_STEP_Q_TYPE = "q_type"
TEST_WIZ_STEP_Q_TEXT = "q_text"
TEST_WIZ_STEP_Q_OPTIONS = "q_options"
TEST_WIZ_STEP_Q_CORRECT = "q_correct"
TEST_WIZ_STEP_TIME = "time"
TEST_WIZ_STEP_TIME_MANUAL = "time_manual"
TEST_WIZ_STEP_PICK_PROFILE = "pick_profile"
TEST_WIZ_STEP_CONFIRM = "confirm"

ACTIVE_TEST_ASSIGNMENT_ID = "active_test_assignment_id"
ACTIVE_TEST_MULTI_SELECTED = "active_test_multi_selected"  # dict[qid] -> set[int]

EMPLOYEE_TEST_FINISH_TEXT = "✅ Отлично. Тест пройден. Результаты сообщит твой руководитель."
EMPLOYEE_TEST_EXPIRED_TEXT = "⏳ Время на тестирование истекло.\n\n" + EMPLOYEE_TEST_FINISH_TEXT

def clear_test_wiz(context: ContextTypes.DEFAULT_TYPE):
    context.user_data[TEST_WIZ_ACTIVE] = False
    context.user_data.pop(TEST_WIZ_STEP, None)
    context.user_data.pop(TEST_WIZ_DATA, None)
    context.user_data.pop(TEST_WIZ_SELECTED_PIDS, None)
    context.user_data.pop(TEST_WIZ_TEMPLATE_ID, None)
    context.user_data.pop(TEST_WIZ_FROM_TEMPLATE_ID, None)

def clear_active_test(context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop(ACTIVE_TEST_ASSIGNMENT_ID, None)
    context.user_data.pop(ACTIVE_TEST_MULTI_SELECTED, None)

def _test_wiz_ensure_template_persisted(context: ContextTypes.DEFAULT_TYPE, created_by: int | None) -> int | None:
    """
    Ensures that current admin wizard has a persisted template (draft).
    Creates test_templates + test_questions once, stores template_id in user_data.
    Returns template_id or None if not enough data.
    """
    existing = context.user_data.get(TEST_WIZ_TEMPLATE_ID)
    if existing:
        try:
            return int(existing)
        except Exception:
            pass

    d = context.user_data.get(TEST_WIZ_DATA) or {}
    title = (d.get("title") or "").strip()
    qs = d.get("questions") or []
    if not title or not qs:
        return None

    template_id = db_test_create_template(title, created_by)
    for i, qq in enumerate(qs, start=1):
        db_test_add_question(
            template_id=template_id,
            idx=i,
            q_type=qq["q_type"],
            question_text=qq["question_text"],
            options=(qq.get("options") if qq["q_type"] in ("single", "multi") else None),
            correct=(qq.get("correct") if qq["q_type"] in ("single", "multi") else None),
        )
    context.user_data[TEST_WIZ_TEMPLATE_ID] = int(template_id)
    return int(template_id)

def _now_iso() -> str:
    return datetime.utcnow().isoformat()

def _safe_json_dumps(obj) -> str:
    return json.dumps(obj, ensure_ascii=False)

def _safe_json_loads(s: str, default):
    try:
        return json.loads(s) if s else default
    except Exception:
        return default

# ---------------- TESTING DB helpers ----------------

def db_test_create_template(title: str, created_by: int | None) -> int:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        "INSERT INTO test_templates(title, created_by, created_at, is_draft_visible) VALUES(?, ?, ?, 1)",
        (title.strip(), created_by, _now_iso()),
    )
    con.commit()
    tid = int(cur.lastrowid)
    con.close()
    return tid

def db_test_add_question(template_id: int, idx: int, q_type: str, question_text: str, options: list[str] | None, correct: list[int] | None) -> int:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """INSERT INTO test_questions(template_id, idx, q_type, question_text, options_json, correct_json, created_at)
             VALUES(?, ?, ?, ?, ?, ?, ?)""",
        (
            int(template_id),
            int(idx),
            q_type,
            question_text.strip(),
            _safe_json_dumps(options) if options is not None else None,
            _safe_json_dumps(correct) if correct is not None else None,
            _now_iso(),
        ),
    )
    con.commit()
    qid = int(cur.lastrowid)
    con.close()
    return qid

def db_test_get_questions(template_id: int) -> list[dict]:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        "SELECT id, idx, q_type, question_text, options_json, correct_json FROM test_questions WHERE template_id=? ORDER BY idx ASC",
        (int(template_id),),
    )
    rows = cur.fetchall()
    con.close()
    out=[]
    for r in rows:
        out.append({
            "id": int(r[0]),
            "idx": int(r[1]),
            "q_type": r[2],
            "question_text": r[3],
            "options": _safe_json_loads(r[4], []),
            "correct": _safe_json_loads(r[5], []),
        })
    return out

def db_test_create_assignment(template_id: int, profile_id: int, assigned_by: int | None, time_limit_sec: int | None) -> int:
    assigned_at = _now_iso()
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """INSERT INTO test_assignments(template_id, profile_id, assigned_by, assigned_at, time_limit_sec, deadline_at, status, started_at, finished_at, current_idx)
             VALUES(?, ?, ?, ?, ?, NULL, 'assigned', NULL, NULL, 0)""",
        (int(template_id), int(profile_id), assigned_by, assigned_at, (int(time_limit_sec) if time_limit_sec is not None else None)),
    )
    con.commit()
    aid = int(cur.lastrowid)
    con.close()
    return aid

def db_test_get_assignment(aid: int) -> dict | None:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """SELECT id, template_id, profile_id, assigned_by, assigned_at, time_limit_sec, deadline_at, status,
                  started_at, finished_at, current_idx
             FROM test_assignments WHERE id=?""",
        (int(aid),),
    )
    r = cur.fetchone()
    con.close()
    if not r:
        return None
    return {
        "id": int(r[0]),
        "template_id": int(r[1]),
        "profile_id": int(r[2]),
        "assigned_by": r[3],
        "assigned_at": r[4],
        "time_limit_sec": r[5],
        "deadline_at": r[6],
        "status": r[7],
        "started_at": r[8],
        "finished_at": r[9],
        "current_idx": int(r[10] or 0),
    }

def db_test_update_assignment_start(aid: int, deadline_at_iso: str | None):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """UPDATE test_assignments
             SET status='in_progress', started_at=?, deadline_at=?, current_idx=0
             WHERE id=?""",
        (_now_iso(), deadline_at_iso, int(aid)),
    )
    con.commit()
    con.close()

def db_test_update_assignment_progress(aid: int, current_idx: int):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("UPDATE test_assignments SET current_idx=? WHERE id=?", (int(current_idx), int(aid)))
    con.commit()
    con.close()

def db_test_finish_assignment(aid: int, status: str):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """UPDATE test_assignments
             SET status=?, finished_at=?
             WHERE id=?""",
        (status, _now_iso(), int(aid)),
    )
    con.commit()
    con.close()

def db_test_set_assignment_status(aid: int, status: str):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("UPDATE test_assignments SET status=? WHERE id=?", (status, int(aid)))
    con.commit()
    con.close()

def db_test_save_answer(assignment_id: int, question_id: int, answer_obj: dict, is_correct: int | None):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """INSERT INTO test_answers(assignment_id, question_id, answer_json, is_correct, answered_at)
             VALUES(?, ?, ?, ?, ?)
             ON CONFLICT(assignment_id, question_id) DO UPDATE SET
               answer_json=excluded.answer_json,
               is_correct=excluded.is_correct,
               answered_at=excluded.answered_at""",
        (int(assignment_id), int(question_id), _safe_json_dumps(answer_obj), is_correct, _now_iso()),
    )
    con.commit()
    con.close()

def db_test_list_recent_results(limit: int = 20) -> list[dict]:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """SELECT a.id, a.profile_id, a.status, a.finished_at, a.assigned_at, t.title
             FROM test_assignments a
             JOIN test_templates t ON t.id = a.template_id
             WHERE a.status IN ('finished','expired','saved')
             ORDER BY COALESCE(a.finished_at, a.assigned_at) DESC
             LIMIT ?""",
        (int(limit),),
    )
    rows = cur.fetchall()
    con.close()
    out=[]
    for r in rows:
        out.append({
            "id": int(r[0]),
            "profile_id": int(r[1]),
            "status": r[2],
            "finished_at": r[3],
            "assigned_at": r[4],
            "title": r[5],
        })
    return out

def db_test_get_answers_for_assignment(aid: int) -> list[dict]:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """SELECT q.idx, q.q_type, q.question_text, q.options_json, q.correct_json,
                    ans.answer_json, ans.is_correct, ans.answered_at
             FROM test_questions q
             LEFT JOIN test_answers ans
               ON ans.question_id = q.id AND ans.assignment_id = ?
             WHERE q.template_id = (SELECT template_id FROM test_assignments WHERE id=?)
             ORDER BY q.idx ASC""",
        (int(aid), int(aid)),
    )
    rows = cur.fetchall()
    con.close()
    out=[]
    for r in rows:
        out.append({
            "idx": int(r[0]),
            "q_type": r[1],
            "question_text": r[2],
            "options": _safe_json_loads(r[3], []),
            "correct": _safe_json_loads(r[4], []),
            "answer": _safe_json_loads(r[5], {}),
            "is_correct": r[6],
            "answered_at": r[7],
        })
    return out


def db_test_delete_assignment_full(aid: int) -> bool:
    """Полностью удаляет тестирование из истории: assignment + ответы + вопросы + шаблон.

    Важно: SQLite по умолчанию может быть без PRAGMA foreign_keys=ON, поэтому удаляем явно.
    """
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    cur.execute("SELECT template_id FROM test_assignments WHERE id=?", (int(aid),))
    row = cur.fetchone()
    if not row:
        con.close()
        return False
    template_id = int(row[0])

    # 1) answers
    cur.execute("DELETE FROM test_answers WHERE assignment_id=?", (int(aid),))
    # 2) assignment
    cur.execute("DELETE FROM test_assignments WHERE id=?", (int(aid),))
    # 3) questions + template (в вашем потоке template создаётся под 1 assignment)
    cur.execute("DELETE FROM test_questions WHERE template_id=?", (int(template_id),))
    cur.execute("DELETE FROM test_templates WHERE id=?", (int(template_id),))

    con.commit()
    con.close()
    return True
def db_test_list_templates(limit: int = 50) -> list[dict]:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """SELECT id, title, created_at
             FROM test_templates
             WHERE is_draft_visible=1
             ORDER BY created_at DESC
             LIMIT ?""",
        (int(limit),),
    )
    rows = cur.fetchall()
    con.close()
    return [{"id": int(r[0]), "title": r[1], "created_at": r[2]} for r in rows]


def db_test_get_template(tid: int) -> dict | None:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT id, title, created_by, created_at FROM test_templates WHERE id=?", (int(tid),))
    r = cur.fetchone()
    con.close()
    if not r:
        return None
    return {"id": int(r[0]), "title": r[1], "created_by": r[2], "created_at": r[3]}


def db_test_get_questions_for_template(tid: int) -> list[dict]:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """SELECT id, idx, q_type, question_text, options_json, correct_json
             FROM test_questions
             WHERE template_id=?
             ORDER BY idx ASC""",
        (int(tid),),
    )
    rows = cur.fetchall()
    con.close()
    out=[]
    for r in rows:
        out.append({
            "id": int(r[0]),
            "idx": int(r[1]),
            "q_type": r[2],
            "question_text": r[3],
            "options": _safe_json_loads(r[4], []),
            "correct": _safe_json_loads(r[5], []),
        })
    return out


def db_test_delete_template_full(tid: int) -> bool:
    """
    Полностью удаляет шаблон теста из 'Черновиков' и всю связанную историю:
    assignments + answers + questions + template.
    """
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    try:
        # collect assignment ids
        cur.execute("SELECT id FROM test_assignments WHERE template_id=?", (int(tid),))
        aids = [int(x[0]) for x in cur.fetchall()]

        if aids:
            cur.executemany("DELETE FROM test_answers WHERE assignment_id=?", [(aid,) for aid in aids])
            cur.executemany("DELETE FROM test_assignments WHERE id=?", [(aid,) for aid in aids])

        cur.execute("DELETE FROM test_questions WHERE template_id=?", (int(tid),))
        cur.execute("DELETE FROM test_templates WHERE id=?", (int(tid),))
        con.commit()
        return True
    finally:
        con.close()



def db_test_template_has_assignments(tid: int) -> bool:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT 1 FROM test_assignments WHERE template_id=? LIMIT 1", (int(tid),))
    ok = cur.fetchone() is not None
    con.close()
    return ok


def db_test_hide_template(tid: int):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("UPDATE test_templates SET is_draft_visible=0 WHERE id=?", (int(tid),))
    con.commit()
    con.close()


def db_test_delete_draft_only(tid: int) -> bool:
    """Удаляет только черновик (шаблон) из списка черновиков.

    Если по шаблону уже есть назначения/результаты — делаем логическое удаление (скрываем из черновиков),
    чтобы результаты в «Результаты» продолжали открываться.
    """
    # Если есть назначения — просто скрываем
    if db_test_template_has_assignments(int(tid)):
        db_test_hide_template(int(tid))
        return True

    # Иначе можно удалить полностью (вместе с вопросами), т.к. результатов нет
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    try:
        cur.execute("DELETE FROM test_questions WHERE template_id=?", (int(tid),))
        cur.execute("DELETE FROM test_templates WHERE id=?", (int(tid),))
        con.commit()
        return True
    finally:
        con.close()


def db_test_delete_assignment_only(aid: int) -> bool:
    """Удаляет только результат (assignment + ответы), не трогая шаблон/черновик."""
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT id FROM test_assignments WHERE id=?", (int(aid),))
    if not cur.fetchone():
        con.close()
        return False
    cur.execute("DELETE FROM test_answers WHERE assignment_id=?", (int(aid),))
    cur.execute("DELETE FROM test_assignments WHERE id=?", (int(aid),))
    con.commit()
    con.close()
    return True

def db_test_delete_answers(aid: int):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("DELETE FROM test_answers WHERE assignment_id=?", (int(aid),))
    con.commit()
    con.close()

# ---------------- TESTING UI helpers ----------------

def kb_settings_test_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Создать и отправить тест", callback_data="help:settings:test:create")],
        [InlineKeyboardButton("🗂 Черновики", callback_data="help:settings:test:drafts")],
        [InlineKeyboardButton("📋 Результаты (последние)", callback_data="help:settings:test:results")],
        [InlineKeyboardButton("📈 Указать средний балл тестирования", callback_data="help:settings:test:avgscore")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="help:settings")],
    ])

def kb_test_wiz_questions_menu(has_any: bool):
    rows = [[InlineKeyboardButton("➕ Добавить вопрос", callback_data="help:settings:test:q:add")]]
    if has_any:
        rows.append([InlineKeyboardButton("✅ Закончить добавление вопросов", callback_data="help:settings:test:q:done")])
    rows.append([InlineKeyboardButton("❌ Отмена", callback_data="help:settings:test:cancel")])
    return InlineKeyboardMarkup(rows)

def kb_test_q_type():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📝 Открытый (open)", callback_data="help:settings:test:q:type:open")],
        [InlineKeyboardButton("🔘 Один вариант (single)", callback_data="help:settings:test:q:type:single")],
        [InlineKeyboardButton("☑️ Несколько (multi)", callback_data="help:settings:test:q:type:multi")],
        [InlineKeyboardButton("❌ Отмена", callback_data="help:settings:test:cancel")],
    ])

def kb_test_options_done(can_done: bool):
    rows=[]
    if can_done:
        rows.append([InlineKeyboardButton("✅ Готово с вариантами", callback_data="help:settings:test:q:opts_done")])
    rows.append([InlineKeyboardButton("❌ Отмена", callback_data="help:settings:test:cancel")])
    return InlineKeyboardMarkup(rows)

def kb_test_correct_single(options: list[str]):
    rows=[]
    for i,opt in enumerate(options):
        rows.append([InlineKeyboardButton(opt, callback_data=f"help:settings:test:q:correct_single:{i}")])
    rows.append([InlineKeyboardButton("❌ Отмена", callback_data="help:settings:test:cancel")])
    return InlineKeyboardMarkup(rows)

def kb_test_correct_multi(options: list[str], selected: set[int]):
    rows=[]
    for i,opt in enumerate(options):
        mark = "☑️" if i in selected else "⬜"
        rows.append([InlineKeyboardButton(f"{mark} {opt}", callback_data=f"help:settings:test:q:correct_toggle:{i}")])
    rows.append([InlineKeyboardButton("✅ Готово", callback_data="help:settings:test:q:correct_done")])
    rows.append([InlineKeyboardButton("❌ Отмена", callback_data="help:settings:test:cancel")])
    return InlineKeyboardMarkup(rows)

def kb_test_time_limit():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("5", callback_data="help:settings:test:time:5"),
            InlineKeyboardButton("10", callback_data="help:settings:test:time:10"),
            InlineKeyboardButton("15", callback_data="help:settings:test:time:15"),
        ],
        [
            InlineKeyboardButton("20", callback_data="help:settings:test:time:20"),
            InlineKeyboardButton("30", callback_data="help:settings:test:time:30"),
        ],
        [InlineKeyboardButton("✍️ Ввести минуты вручную", callback_data="help:settings:test:time:manual")],
        [InlineKeyboardButton("♾️ Без лимита", callback_data="help:settings:test:time:none")],
        [InlineKeyboardButton("❌ Отмена", callback_data="help:settings:test:cancel")],
    ])

def kb_pick_profiles_for_test(selected: set[int], back_cb: str = "help:settings:test"):
    """
    Multi-select profiles for test sending.
    Reuses the same simple list style as achievements selection, but with toggles.
    """
    people = db_profiles_list()
    rows = []
    if not people:
        rows.append([InlineKeyboardButton("— анкет нет —", callback_data="noop")])
    else:
        for pid, name in people[:60]:
            mark = "☑️" if int(pid) in selected else "⬜"
            rows.append([InlineKeyboardButton(f"{mark} {name}", callback_data=f"help:settings:test:pick_toggle:{pid}")])
    rows.append([InlineKeyboardButton("✅ Готово", callback_data="help:settings:test:pick_done")])
    rows.append([InlineKeyboardButton("❌ Отмена", callback_data="help:settings:test:cancel")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=back_cb)])
    return InlineKeyboardMarkup(rows)

def kb_test_confirm_send():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Отправить", callback_data="help:settings:test:send")],
        [InlineKeyboardButton("👥 Изменить получателей", callback_data="help:settings:test:pick_open")],
        [InlineKeyboardButton("💾 Сохранить в черновики", callback_data="help:settings:test:save_draft")],
        [InlineKeyboardButton("❌ Отмена", callback_data="help:settings:test:cancel")],
    ])

# ---------------- TESTING: drafts UI ----------------

def kb_settings_test_drafts_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ Назад", callback_data="help:settings:test")],
    ])

def kb_test_drafts_list(templates: list[dict]):
    rows=[]
    if not templates:
        rows.append([InlineKeyboardButton("— черновиков нет —", callback_data="noop")])
    else:
        for t in templates[:40]:
            title = t.get("title") or "— без названия —"
            rows.append([InlineKeyboardButton(title, callback_data=f"help:settings:test:draft:open:{t['id']}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="help:settings:test")])
    return InlineKeyboardMarkup(rows)

def kb_test_draft_actions(tid: int):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📤 Отправить сотрудникам", callback_data=f"help:settings:test:draft:send:{tid}")],
        [InlineKeyboardButton("🗑 Удалить черновик", callback_data=f"help:settings:test:draft:delete:{tid}")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="help:settings:test:drafts")],
    ])

def kb_test_draft_delete_confirm(tid: int):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🗑 Да, удалить", callback_data=f"help:settings:test:draft:delete_yes:{tid}")],
        [InlineKeyboardButton("⬅️ Назад", callback_data=f"help:settings:test:draft:open:{tid}")],
    ])

def kb_test_results_list(items: list[dict]):
    rows=[]
    if not items:
        rows.append([InlineKeyboardButton("— пока нет —", callback_data="noop")])
    else:
        for it in items[:20]:
            prof = db_profiles_get(int(it["profile_id"]))
            who = prof["full_name"] if prof else f"id={it['profile_id']}"
            title = (it.get("title") or "").strip()
            status = (it.get("status") or "").strip()
            label = f"{who} — {status} — {title}" if title else f"{who} — {status}"
            if len(label) > 64:
                label = label[:61] + "…"
            rows.append([InlineKeyboardButton(label, callback_data=f"help:settings:test:results:open:{it['id']}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="help:settings:test")])
    return InlineKeyboardMarkup(rows)

def kb_test_results_actions(aid: int):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💾 Сохранить", callback_data=f"help:settings:test:results:save:{aid}")],
        [InlineKeyboardButton("🗑 Удалить", callback_data=f"help:settings:test:results:delete:{aid}")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="help:settings:test:results")],
    ])

# ---------------- TESTING runtime (employee) ----------------

def _parse_deadline(deadline_iso: str | None) -> datetime | None:
    if not deadline_iso:
        return None
    try:
        return datetime.fromisoformat(deadline_iso)
    except Exception:
        return None

async def _expire_assignment_if_needed(assignment: dict, context: ContextTypes.DEFAULT_TYPE):
    deadline = _parse_deadline(assignment.get("deadline_at"))
    if deadline and datetime.utcnow() > deadline and assignment.get("status") in ("assigned","in_progress"):
        db_test_finish_assignment(int(assignment["id"]), "expired")
        # clear active state for user if we can infer current update user elsewhere
        return True
    return False

def _is_correct_closed(selected: list[int], correct: list[int]) -> int:
    return 1 if sorted(set(selected)) == sorted(set(correct or [])) else 0

async def send_employee_question(context: ContextTypes.DEFAULT_TYPE, chat_id, assignment: dict):
    questions = db_test_get_questions(int(assignment["template_id"]))
    total = len(questions)
    idx = int(assignment.get("current_idx") or 0)
    if idx >= total:
        return
    q = questions[idx]
    qid = int(q["id"])
    qtype = q["q_type"]
    title = f"Вопрос {idx+1}/{total}:\n{q['question_text']}"
    if qtype == "open":
        await context.bot.send_message(chat_id=chat_id, text=title)
        return
    options = q.get("options") or []
    if qtype == "single":
        rows=[]
        for i,opt in enumerate(options):
            rows.append([InlineKeyboardButton(opt, callback_data=f"test:single:{assignment['id']}:{qid}:{i}")])
        kb = InlineKeyboardMarkup(rows)
        await context.bot.send_message(chat_id=chat_id, text=title, reply_markup=kb)
        return
    if qtype == "multi":
        # init selection state
        selmap = context.user_data.get(ACTIVE_TEST_MULTI_SELECTED) or {}
        selmap[str(qid)] = list(selmap.get(str(qid), []))  # keep if exists
        context.user_data[ACTIVE_TEST_MULTI_SELECTED] = selmap
        kb = kb_employee_multi(assignment["id"], qid, options, set(selmap.get(str(qid), [])))
        await context.bot.send_message(chat_id=chat_id, text=title, reply_markup=kb)
        return

def kb_employee_multi(aid: int, qid: int, options: list[str], selected: set[int]):
    rows=[]
    for i,opt in enumerate(options):
        mark = "☑️" if i in selected else "⬜"
        rows.append([InlineKeyboardButton(f"{mark} {opt}", callback_data=f"test:toggle:{aid}:{qid}:{i}")])
    rows.append([InlineKeyboardButton("✅ Ответить", callback_data=f"test:multi_submit:{aid}:{qid}")])
    return InlineKeyboardMarkup(rows)

async def _notify_admin_test_done(context: ContextTypes.DEFAULT_TYPE, assignment: dict, status_text: str):
    admin_id = assignment.get("assigned_by")
    if not admin_id:
        return
    prof = db_profiles_get(int(assignment["profile_id"]))
    who = prof["full_name"] if prof else f"id={assignment['profile_id']}"
    msg = f"📝 Тест {status_text}: {who}.\nСмотреть: /help → Настройки → Тестирование"
    try:
        await context.bot.send_message(chat_id=int(admin_id), text=msg)
    except Exception:
        pass



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


def kb_pick_profile_for_avgscore():
    people = db_profiles_list()
    rows = []
    if not people:
        rows.append([InlineKeyboardButton("— анкет нет —", callback_data="noop")])
    else:
        for pid, name in people[:60]:
            rows.append([InlineKeyboardButton(name, callback_data=f"help:settings:test:avgscore:pick:{pid}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="help:settings:test")])
    return InlineKeyboardMarkup(rows)


def kb_cancel_wizard_settings():
    return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="help:settings:cancel")]])

# ---------------- COMMANDS ----------------

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await deny_no_access(update, context):
        return

    name = update.effective_user.first_name if update.effective_user else "коллеги"
    text = (
        f"Привет, {name}! 👋\n\n"
        "Готов помочь тебе упростить рабочий день.\n\n"
        "Здесь ты найдёшь полезные ссылки и документы.\n\n"
        "А если появятся идеи или предложения — ты всегда можешь прислать их в разделе 💡 «Предложка» 💡, анонимно или нет.\n\n"
        "Вот команды, которые вызывают меня:\n"
        "• /help — меню «Помогатор»\n"
        "• /horo — твой ежедневный гороскоп\n"

    )
    await update.message.reply_text(text)

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await deny_no_access(update, context):
        return

    bot_username = (context.bot.username or "blablabird_bot")
    text = help_text_main(bot_username)

    orig_msg = update.message  # чтобы (по возможности) удалить /help в группе

    # 1) если команда в личке — просто показываем меню тут
    if update.effective_chat and update.effective_chat.type == "private":
        is_adm = await is_admin_scoped(update, context)
        await update.message.reply_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=kb_help_main(is_admin_user=is_adm),
            disable_web_page_preview=True,
        )
        return

    # 2) если команда в группе — пробуем прислать меню в ЛС пользователю
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

            # успех -> удаляем /help в чате (если есть права)
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
                when=15,
                data={"chat_id": msg.chat_id, "message_id": msg.message_id},
                name=f"del_help_warn_{msg.chat_id}_{msg.message_id}",
            )
            return

        except Exception as e:
            logger.exception("Failed to DM /help: %s", e)

    msg = await update.message.reply_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=kb_help_main(is_admin_user=await is_admin_scoped(update, context)),
        disable_web_page_preview=True,
        reply_to_message_id=update.message.message_id,
    )

    if update.effective_chat and update.effective_chat.type != "private":
        if orig_msg:
            context.job_queue.run_once(
                job_delete_message,
                when=15,
                data={"chat_id": orig_msg.chat_id, "message_id": orig_msg.message_id},
                name=f"del_help_cmd_{orig_msg.chat_id}_{orig_msg.message_id}",
            )
        if msg:
            context.job_queue.run_once(
                job_delete_message,
                when=15,
                data={"chat_id": msg.chat_id, "message_id": msg.message_id},
                name=f"del_help_fallback_{msg.chat_id}_{msg.message_id}",
            )
async def _send_horo_dm(user_id: int, sign_slug: str, context: ContextTypes.DEFAULT_TYPE):
    today_iso = datetime.now(MOSCOW_TZ).date().isoformat()

    # rate-limit: 1 раз в день — сообщение строго в ЛС
    if db_get_horo_last_date(user_id) == today_iso:
        await context.bot.send_message(chat_id=user_id, text="Звёзды свою работу выполнили, приходи завтра 🙂")
        return

    horo_text, date_str = await fetch_rambler_horo(sign_slug)

    title = ZODIAC_NAME.get(sign_slug, sign_slug)
    head = title
    if date_str:
        head += f" • {date_str}"

    body_text, advice, focus = extract_horo_blocks(horo_text)

    sep = "\n────────────\n\n"

    msg = (
        f"<b>{escape(head)}</b>\n\n"
        f"<b>Ваш гороскоп:</b>\n"
        f"{escape(body_text)}"
        f"{sep}"
        f"<b>Совет дня 🧭:</b>\n"
        f"{escape(advice)}"
        f"{sep}"
        f"<b>Фокус 🎯:</b>\n"
        f"{escape(focus)}"
    )

    await context.bot.send_message(
        chat_id=user_id,
        text=msg,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
        reply_markup=kb_horo_after(),
    )

    db_set_horo_last_date(user_id, today_iso)


async def cmd_horo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await deny_no_access(update, context):
        return

    orig_msg = update.message
    user = update.effective_user
    chat = update.effective_chat
    if not orig_msg or not user or not chat:
        return

    user_id = user.id

    # 1) знак по карточке (birthday) если есть
    sign_slug = None
    username = (user.username or "").strip()
    if username:
        prof = db_profiles_get_by_tg_link("@" + username)
        if prof and prof.get("birthday"):
            sign_slug = zodiac_from_ddmm(prof["birthday"])

    # 2) если карточки нет — пробуем сохранённый ранее знак
    if not sign_slug:
        sign_slug = db_horo_get_user_sign(user_id)

    # 3) если знака нет — просим выбрать, но:
    #    - в группе/канале клавиатуру шлём в ЛС
    #    - в личке можно показать сразу тут
    if not sign_slug:
        text_pick = "У тебя нет карточки сотрудника. Выбери свой знак — и я пришлю гороскоп 👇"

        if chat.type == "private":
            await orig_msg.reply_text(text_pick, reply_markup=kb_horo_signs(), disable_web_page_preview=True)
        else:
            try:
                await context.bot.send_message(chat_id=user_id, text=text_pick, reply_markup=kb_horo_signs(), disable_web_page_preview=True)
            except Forbidden:
                bot_username = (context.bot.username or "blablabird_bot")
                warn = (
                    "⚠️ Я не могу написать вам в ЛС.\n"
                    f"Откройте личку: перейдите к боту @{bot_username} и отправьте /start,\n"
                    "после этого снова введите /horo."
                )
                msg = await orig_msg.reply_text(warn, disable_web_page_preview=True)
                # автоудаляем предупреждение в группе
                context.job_queue.run_once(
                    job_delete_message,
                    when=15,
                    data={"chat_id": msg.chat_id, "message_id": msg.message_id},
                    name=f"del_horo_warn_{msg.chat_id}_{msg.message_id}",
                )

        # удаляем команду /horo в группе
        if chat.type != "private":
            try:
                await context.bot.delete_message(chat_id=orig_msg.chat_id, message_id=orig_msg.message_id)
            except Exception:
                pass
        return

    # 4) знак есть — шлём строго в ЛС, в чат ничего не пишем
    try:
        await _send_horo_dm(user_id, sign_slug, context)
    except Forbidden:
        bot_username = (context.bot.username or "blablabird_bot")
        warn = (
            "⚠️ Я не могу написать вам в ЛС.\n"
            f"Откройте личку: перейдите к боту @{bot_username} и отправьте /start,\n"
            "после этого снова введите /horo."
        )
        # предупреждаем только в том месте, где запросили (если это не ЛС)
        if chat.type == "private":
            await orig_msg.reply_text(warn, disable_web_page_preview=True)
        else:
            msg = await orig_msg.reply_text(warn, disable_web_page_preview=True)
            context.job_queue.run_once(
                job_delete_message,
                when=15,
                data={"chat_id": msg.chat_id, "message_id": msg.message_id},
                name=f"del_horo_warn_{msg.chat_id}_{msg.message_id}",
            )

    # удаляем команду /horo в группе
    if chat.type != "private":
        try:
            await context.bot.delete_message(chat_id=orig_msg.chat_id, message_id=orig_msg.message_id)
        except Exception:
            pass


async def cb_horo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await deny_no_access(update, context):
        return

    q = update.callback_query
    if not q or not q.data:
        return

    try:
        await q.answer()
    except (TimedOut, NetworkError):
        pass

    # кнопка мема после гороскопа
    if q.data == "horo:meme":
        day_iso = datetime.now(MOSCOW_TZ).date().isoformat()
        uid = update.effective_user.id

        # 1 раз в день на пользователя
        if db_meme_user_has_today(uid, day_iso):
            await context.bot.send_message(
                chat_id=uid,
                text="Звёзды любят работать, но поработай и ты. Давай завтра 😂",
            )
            return

        # без повторов: один и тот же мем нельзя выдать двум людям в этот день
        meme = None
        for _ in range(5):  # на всякий случай, если одновременно нажали несколько людей
            candidate = db_meme_pick_for_day(day_iso)
            if not candidate:
                meme = None
                break
            if db_meme_mark_sent(day_iso, uid, candidate["id"]):
                meme = candidate
                break

        if not meme:
            await context.bot.send_message(
                chat_id=uid,
                text="Сегодня мемы уже разобрали 😅\nДавай завтра 😂",
            )
            return

        kind = meme["kind"]
        file_id = meme["file_id"]

        if kind == "photo":
            await context.bot.send_photo(chat_id=uid, photo=file_id)
        elif kind == "video":
            await context.bot.send_video(chat_id=uid, video=file_id)
        else:
            await context.bot.send_document(chat_id=uid, document=file_id)
        return

    parts = q.data.split(":")
    if len(parts) != 3 or parts[0] != "horo" or parts[1] != "sign":
        return

    sign_slug = parts[2].strip()
    if sign_slug not in ZODIAC_NAME:
        try:
            await q.answer("Не понял знак 🤔", show_alert=True)
        except (TimedOut, NetworkError):
            pass
        return

    user = update.effective_user
    if not user:
        return
    user_id = user.id

    db_horo_set_user_sign(user_id, sign_slug)

    try:
        await _send_horo_dm(user_id, sign_slug, context)
        # убираем клавиатуру/сообщение выбора — без лишних подтверждений
        try:
            if q.message:
                await context.bot.delete_message(chat_id=q.message.chat_id, message_id=q.message.message_id)
        except Exception:
            try:
                await q.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass

    except Forbidden:
        bot_username = (context.bot.username or "blablabird_bot")
        warn = (
            "⚠️ Я не могу написать вам в ЛС.\n"
            f"Откройте личку: перейдите к боту @{bot_username} и отправьте /start,\n"
            "после этого снова введите /horo."
        )
        try:
            await q.edit_message_text(warn, disable_web_page_preview=True)
        except Exception:
            pass


async def cmd_setchat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await deny_no_access(update, context):
        return

    if update.effective_chat.type == "private":
        await update.message.reply_text("Эта команда работает только в групповом чате.")
        return
    if not await is_admin_scoped(update, context):
        await update.message.reply_text("Только администраторы могут назначить чат для уведомлений.")
        return
    db_add_chat(update.effective_chat.id)
    await update.message.reply_text("✅ Готово! Этот чат добавлен в рассылку уведомлений.")

async def cmd_unsetchat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await deny_no_access(update, context):
        return

    if update.effective_chat.type == "private":
        await update.message.reply_text("Эта команда работает только в групповом чате.")
        return
    if not await is_admin_scoped(update, context):
        await update.message.reply_text("Только администраторы могут отключить уведомления.")
        return
    db_remove_chat(update.effective_chat.id)
    await update.message.reply_text("🧹 Этот чат убран из рассылки уведомлений.")

async def cmd_force_standup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await deny_no_access(update, context):
        return

    if not await is_admin_scoped(update, context):
        await update.message.reply_text("Недостаточно прав.")
        return
    if not db_list_chats():
        await update.message.reply_text("Сначала подключи чат командой /setchat.")
        return
    await send_meeting_message(MEETING_STANDUP, context, force=True)
    await update.message.reply_text("🚀 Отправил принудительное уведомление планёрки.")

async def cmd_test_industry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await deny_no_access(update, context):
        return

    if not await is_admin_scoped(update, context):
        await update.message.reply_text("Недостаточно прав.")
        return
    if not db_list_chats():
        await update.message.reply_text("Сначала подключи чат командой /setchat.")
        return
    await send_meeting_message(MEETING_INDUSTRY, context, force=True)
    await update.message.reply_text("🚀 Отправил тестовое уведомление отраслевой встречи.")

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await deny_no_access(update, context):
        return

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
    if await deny_no_access(update, context):
        return

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


def export_backup_zip_bytes() -> bytes:
    """Формирует ZIP-бэкап с несколькими CSV (profiles/docs/categories/notify_chats/achievements_awards)."""
    files: dict[str, str] = {}

    # doc_categories.csv
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=["title", "created_at"])
    w.writeheader()
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    try:
        cur.execute("SELECT title, created_at FROM doc_categories ORDER BY title COLLATE NOCASE ASC")
        for title, created_at in cur.fetchall():
            w.writerow({"title": title or "", "created_at": created_at or ""})
    finally:
        con.close()
    files["doc_categories.csv"] = buf.getvalue()

    # legacy name (для совместимости со старым импортом, который ищет categories.csv)
    files["categories.csv"] = buf.getvalue()

    # docs.csv
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=[
        "category_title",
        "doc_title",
        "doc_description",
        "doc_file_id",
        "doc_file_unique_id",
        "doc_mime_type",
        "doc_local_path",
    ])
    w.writeheader()
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
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
        w.writerow({
            "category_title": cat_title or "",
            "doc_title": doc_title or "",
            "doc_description": desc or "",
            "doc_file_id": file_id or "",
            "doc_file_unique_id": file_unique_id or "",
            "doc_mime_type": mime_type or "",
            "doc_local_path": local_path or "",
        })
    files["docs.csv"] = buf.getvalue()

    # profiles.csv
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=[
        "profile_id",
        "full_name",
        "year_start",
        "city",
        "birthday",
        "about",
        "topics",
        "tg_link",
        "avg_test_score",
    ])
    w.writeheader()
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        SELECT id, full_name, year_start, city, birthday, about, topics, tg_link, avg_test_score
        FROM profiles
        ORDER BY id ASC
    """)
    for row in cur.fetchall():
        w.writerow({
            "profile_id": row[0],
            "full_name": row[1] or "",
            "year_start": row[2] or "",
            "city": row[3] or "",
            "birthday": row[4] or "",
            "about": row[5] or "",
            "topics": row[6] or "",
            "tg_link": row[7] or "",
            "avg_test_score": row[8] if row[8] is not None else "",
        })
    con.close()
    files["profiles.csv"] = buf.getvalue()

    # notify_chats.csv
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=["chat_id", "added_at"])
    w.writeheader()
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT chat_id, added_at FROM notify_chats ORDER BY chat_id ASC")
    for row in cur.fetchall():
        w.writerow({"chat_id": row[0], "added_at": row[1]})
    con.close()
    files["notify_chats.csv"] = buf.getvalue()

    # achievements_awards.csv
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=[
        "award_id",
        "profile_id",
        "full_name",
        "tg_link",
        "emoji",
        "title",
        "description",
        "awarded_at",
        "awarded_by",
    ])
    w.writeheader()
    for r in export_achievement_awards_rows():
        w.writerow(r)
    files["achievements_awards.csv"] = buf.getvalue()


    # faq.csv
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=["question", "answer", "created_at"])
    w.writeheader()
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    try:
        cur.execute("SELECT question, answer, created_at FROM faq_items ORDER BY id ASC")
        for question, answer, created_at in cur.fetchall():
            w.writerow({"question": question or "", "answer": answer or "", "created_at": created_at or ""})
    finally:
        con.close()
    files["faq.csv"] = buf.getvalue()

    zbuf = io.BytesIO()
    with zipfile.ZipFile(zbuf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, content in files.items():
            zf.writestr(name, content.encode("utf-8-sig"))
    return zbuf.getvalue()


def restore_backup_zip_bytes(data: bytes) -> dict:
    """Восстановление из ZIP бэкапа (CSV). Возвращает статистику по импортированным сущностям."""
    stats = {"profiles": 0, "categories": 0, "docs": 0, "faq": 0, "notify_chats": 0, "achievements_awards": 0}
    zbuf = io.BytesIO(data)
    with zipfile.ZipFile(zbuf, "r") as zf:
        names = set(zf.namelist())

        # 1) profiles.csv
        profile_id_map: dict[str, int] = {}
        if "profiles.csv" in names:
            raw = zf.read("profiles.csv").decode("utf-8", errors="replace")
            rdr = csv.DictReader(io.StringIO(raw))
            con = sqlite3.connect(DB_PATH)
            cur = con.cursor()
            for row in rdr:
                if not row:
                    continue
                pid = (row.get("profile_id") or "").strip()
                full_name = (row.get("full_name") or "").strip()
                year_start = (row.get("year_start") or "").strip() or "2000"
                city = (row.get("city") or "").strip()
                birthday = (row.get("birthday") or "").strip() or None
                about = (row.get("about") or "").strip()
                topics = (row.get("topics") or "").strip()
                tg_link = (row.get("tg_link") or "").strip()

                avg_raw = (row.get("avg_test_score") or "").strip()
                avg_test_score = None
                if avg_raw:
                    try:
                        avg_test_score = int(float(avg_raw))
                    except Exception:
                        avg_test_score = None

                created_at = datetime.utcnow().isoformat()

                # upsert by id if present, else by (tg_link, full_name) heuristic
                if pid.isdigit():
                    cur.execute(
                        """INSERT INTO profiles(id, full_name, year_start, city, birthday, about, topics, tg_link, avg_test_score, created_at)
                               VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                               ON CONFLICT(id) DO UPDATE SET
                                 full_name=excluded.full_name,
                                 year_start=excluded.year_start,
                                 city=excluded.city,
                                 birthday=excluded.birthday,
                                 about=excluded.about,
                                 topics=excluded.topics,
                                 tg_link=excluded.tg_link,
                                 avg_test_score=excluded.avg_test_score
                        """,
                        (int(pid), full_name, int(year_start), city, birthday, about, topics, tg_link, avg_test_score, created_at),
                    )
                    new_id = int(pid)
                else:
                    # try find existing by tg_link first
                    new_id = None
                    if tg_link:
                        cur.execute("SELECT id FROM profiles WHERE tg_link=?", (tg_link,))
                        r = cur.fetchone()
                        if r:
                            new_id = int(r[0])
                    if new_id is None and full_name:
                        cur.execute("SELECT id FROM profiles WHERE full_name=?", (full_name,))
                        r = cur.fetchone()
                        if r:
                            new_id = int(r[0])
                    if new_id is None:
                        cur.execute(
                            """INSERT INTO profiles(full_name, year_start, city, birthday, about, topics, tg_link, avg_test_score, created_at)
                                   VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (full_name, int(year_start), city, birthday, about, topics, tg_link, avg_test_score, created_at),
                        )
                        new_id = int(cur.lastrowid)

                if pid:
                    profile_id_map[pid] = new_id
                stats["profiles"] += 1

            con.commit()
            con.close()

        # 2) doc_categories.csv (или legacy categories.csv)
        cat_filename = None
        if "doc_categories.csv" in names:
            cat_filename = "doc_categories.csv"
        elif "categories.csv" in names:
            cat_filename = "categories.csv"

        if cat_filename:
            raw = zf.read(cat_filename).decode("utf-8", errors="replace")
            rdr = csv.DictReader(io.StringIO(raw))
            con = sqlite3.connect(DB_PATH)
            cur = con.cursor()
            for row in rdr:
                title = (row.get("title") or "").strip()
                created_at = (row.get("created_at") or "").strip() or datetime.utcnow().isoformat()
                if not title:
                    continue
                cur.execute(
                    """INSERT INTO doc_categories(title, created_at)
                           VALUES(?, ?)
                           ON CONFLICT(title) DO UPDATE SET created_at=excluded.created_at
                    """,
                    (title, created_at),
                )
                stats["categories"] += 1
            con.commit()
            con.close()

        # Если файл категорий отсутствует/пустой — попробуем восстановить категории из docs.csv
        # (на случай, если в старом бэкапе категории не выгружались отдельным файлом).
        if stats.get("categories", 0) == 0 and "docs.csv" in names:
            try:
                raw_docs = zf.read("docs.csv").decode("utf-8", errors="replace")
                rdr_docs = csv.DictReader(io.StringIO(raw_docs))
                titles = []
                seen = set()
                for row in rdr_docs:
                    t = (row.get("category_title") or row.get("category") or "").strip()
                    if not t:
                        continue
                    key = t.casefold()
                    if key in seen:
                        continue
                    seen.add(key)
                    titles.append(t)
                if titles:
                    con = sqlite3.connect(DB_PATH)
                    cur = con.cursor()
                    for t in titles:
                        cur.execute(
                            "INSERT INTO doc_categories(title, created_at) VALUES(?, ?) ON CONFLICT(title) DO UPDATE SET created_at=excluded.created_at",
                            (t, datetime.utcnow().isoformat()),
                        )
                        stats["categories"] += 1
                    con.commit()
                    con.close()
            except Exception:
                # не критично — продолжим восстановление документов
                pass

        # helper: get category_id by title (create if missing)
        def _ensure_category(title: str) -> int:
            con = sqlite3.connect(DB_PATH)
            cur = con.cursor()
            cur.execute("SELECT id FROM doc_categories WHERE title=?", (title,))
            r = cur.fetchone()
            if r:
                con.close()
                return int(r[0])
            cur.execute("INSERT INTO doc_categories(title, created_at) VALUES(?, ?)", (title, datetime.utcnow().isoformat()))
            con.commit()
            cid = int(cur.lastrowid)
            con.close()
            return cid

        # 3) docs.csv (by category_title)
        if "docs.csv" in names:
            raw = zf.read("docs.csv").decode("utf-8", errors="replace")
            rdr = csv.DictReader(io.StringIO(raw))
            con = sqlite3.connect(DB_PATH)
            cur = con.cursor()
            for row in rdr:
                cat_title = (row.get("category_title") or "").strip() or "Без категории"
                doc_title = (row.get("doc_title") or "").strip() or "Документ"
                doc_desc = (row.get("doc_description") or "").strip() or None
                file_id = (row.get("doc_file_id") or "").strip()
                file_unique_id = (row.get("doc_file_unique_id") or "").strip() or None
                mime_type = (row.get("doc_mime_type") or "").strip() or None
                if not file_id:
                    continue
                cid = _ensure_category(cat_title)
                # вставляем как новый, но избегаем дублей по (category_id, title, file_id)
                cur.execute(
                    """SELECT id FROM docs WHERE category_id=? AND title=? AND file_id=?""",
                    (cid, doc_title, file_id),
                )
                if cur.fetchone():
                    continue
                cur.execute(
                    """INSERT INTO docs(category_id, title, description, file_id, file_unique_id, mime_type, uploaded_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (cid, doc_title, doc_desc, file_id, file_unique_id, mime_type, datetime.utcnow().isoformat()),
                )
                stats["docs"] += 1
            con.commit()
            con.close()

                # 4) faq.csv
        if "faq.csv" in names:
            raw = zf.read("faq.csv").decode("utf-8-sig", errors="ignore")
            reader = csv.DictReader(io.StringIO(raw))
            for row in reader:
                q = (row.get("question") or "").strip()
                a = (row.get("answer") or "").strip()
                if not q or not a:
                    continue
                db_faq_upsert(q, a)
                stats["faq"] += 1

# 4) notify_chats.csv
        if "notify_chats.csv" in names:
            raw = zf.read("notify_chats.csv").decode("utf-8", errors="replace")
            rdr = csv.DictReader(io.StringIO(raw))
            con = sqlite3.connect(DB_PATH)
            cur = con.cursor()
            for row in rdr:
                chat_id = (row.get("chat_id") or "").strip()
                added_at = (row.get("added_at") or "").strip() or datetime.utcnow().isoformat()
                if not chat_id:
                    continue
                try:
                    cid = int(chat_id)
                except Exception:
                    continue
                cur.execute(
                    """INSERT INTO notify_chats(chat_id, added_at)
                           VALUES(?, ?)
                           ON CONFLICT(chat_id) DO UPDATE SET added_at=excluded.added_at""",
                    (cid, added_at),
                )
                stats["notify_chats"] += 1
            con.commit()
            con.close()

        # 5) achievements_awards.csv
        if "achievements_awards.csv" in names:
            raw = zf.read("achievements_awards.csv").decode("utf-8", errors="replace")
            rdr = csv.DictReader(io.StringIO(raw))
            con = sqlite3.connect(DB_PATH)
            cur = con.cursor()
            for row in rdr:
                pid_old = (row.get("profile_id") or "").strip()
                full_name = (row.get("full_name") or "").strip()
                tg_link = (row.get("tg_link") or "").strip()
                emoji = (row.get("emoji") or "🏆").strip()
                title = (row.get("title") or "Ачивка").strip()
                description = (row.get("description") or "").strip()
                awarded_at = (row.get("awarded_at") or "").strip() or datetime.utcnow().isoformat()
                awarded_by = (row.get("awarded_by") or "").strip()
                awarded_by_val = int(awarded_by) if awarded_by.isdigit() else None

                target_pid = None
                if pid_old and pid_old in profile_id_map:
                    target_pid = profile_id_map[pid_old]
                elif pid_old.isdigit():
                    target_pid = int(pid_old)
                else:
                    # fallback: by tg_link or full_name
                    if tg_link:
                        cur.execute("SELECT id FROM profiles WHERE tg_link=?", (tg_link,))
                        r = cur.fetchone()
                        if r:
                            target_pid = int(r[0])
                    if target_pid is None and full_name:
                        cur.execute("SELECT id FROM profiles WHERE full_name=?", (full_name,))
                        r = cur.fetchone()
                        if r:
                            target_pid = int(r[0])

                if not target_pid:
                    continue

                # avoid duplicate exact same award
                cur.execute(
                    """SELECT id FROM achievement_awards
                           WHERE profile_id=? AND emoji=? AND title=? AND description=?""",
                    (int(target_pid), emoji, title, description),
                )
                if cur.fetchone():
                    continue

                cur.execute(
                    """INSERT INTO achievement_awards(profile_id, emoji, title, description, awarded_at, awarded_by)
                           VALUES(?, ?, ?, ?, ?, ?)""",
                    (int(target_pid), emoji, title, description, awarded_at, awarded_by_val),
                )
                stats["achievements_awards"] += 1

            con.commit()
            con.close()

    return stats

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
        "profile_avg_test_score",
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
    if await deny_no_access(update, context):
        return

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
        "profile_avg_test_score",
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
        SELECT id, full_name, year_start, city, birthday, about, topics, tg_link, avg_test_score
        FROM profiles
        ORDER BY full_name COLLATE NOCASE ASC
    """)
    for row in cur.fetchall():
        writer.writerow({
            "kind": "profile",
            "profile_full_name": row[1] or "",
            "profile_year_start": row[2] or "",
            "profile_city": row[3] or "",
            "profile_birthday": row[4] or "",
            "profile_about": row[5] or "",
            "profile_topics": row[6] or "",
            "profile_tg_link": row[7] or "",
            "profile_avg_test_score": row[8] if row[8] is not None else "",
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
    if await deny_no_access(update, context):
        return

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
    if await deny_no_access(update, context):
        return

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
    if await deny_no_access(update, context):
        return

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
    if await deny_no_access(update, context):
        return

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
    if await deny_no_access(update, context):
        return

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
    if await deny_no_access(update, context):
        return

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
    if await deny_no_access(update, context):
        return

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


# ===================== TESTING (employees) callbacks =====================

async def cb_test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query

    # ===================== TESTING: sync tg_user_id =============
    await sync_profile_user_id_from_update(update)
    data = q.data or ""
    try:
        try:
            await q.answer()
        except (TimedOut, NetworkError):
            pass
    except (TimedOut, NetworkError):
        pass

    if not data.startswith("test:"):
        return

    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else ""
    user_id = update.effective_user.id if update.effective_user else None
    if not user_id:
        return

    # start
    if action == "start" and len(parts) >= 3:
        aid = int(parts[2])
        a = db_test_get_assignment(aid)
        if not a:
            return

        # deadline check (assigned but already expired is rare)
        if await _expire_assignment_if_needed(a, context):
            clear_active_test(context)
            await context.bot.send_message(chat_id=user_id, text=EMPLOYEE_TEST_EXPIRED_TEXT)
            await _notify_admin_test_done(context, a, "истёк")
            return

        # mark started
        deadline_iso = None
        if a.get("time_limit_sec"):
            deadline_iso = (datetime.utcnow() + timedelta(seconds=int(a["time_limit_sec"]))).isoformat()
        db_test_update_assignment_start(aid, deadline_iso)
        context.user_data[ACTIVE_TEST_ASSIGNMENT_ID] = aid
        context.user_data[ACTIVE_TEST_MULTI_SELECTED] = {}
        a = db_test_get_assignment(aid)
        await send_employee_question(context, user_id, a)
        return

    # other actions require active assignment
    if len(parts) < 4:
        return
    aid = int(parts[2])
    qid = int(parts[3])
    a = db_test_get_assignment(aid)
    if not a:
        return

    # deadline check
    if await _expire_assignment_if_needed(a, context):
        clear_active_test(context)
        await context.bot.send_message(chat_id=user_id, text=EMPLOYEE_TEST_EXPIRED_TEXT)
        await _notify_admin_test_done(context, a, "истёк")
        return

    questions = db_test_get_questions(int(a["template_id"]))
    qmap = {int(x["id"]): x for x in questions}
    qinfo = qmap.get(qid)
    if not qinfo:
        return

    if action == "single" and len(parts) >= 5:
        opt = int(parts[4])
        correct = qinfo.get("correct") or []
        is_corr = _is_correct_closed([opt], correct)
        db_test_save_answer(aid, qid, {"selected": [opt]}, is_corr)

        # advance
        next_idx = int(a.get("current_idx") or 0) + 1
        db_test_update_assignment_progress(aid, next_idx)
        a = db_test_get_assignment(aid)

        if next_idx >= len(questions):
            db_test_finish_assignment(aid, "finished")
            clear_active_test(context)
            await context.bot.send_message(chat_id=user_id, text=EMPLOYEE_TEST_FINISH_TEXT)
            await _notify_admin_test_done(context, a, "пройден")
            return

        await send_employee_question(context, user_id, a)
        return

    if action == "toggle" and len(parts) >= 5:
        opt = int(parts[4])
        selmap = context.user_data.get(ACTIVE_TEST_MULTI_SELECTED) or {}
        cur = set(selmap.get(str(qid), []))
        if opt in cur:
            cur.remove(opt)
        else:
            cur.add(opt)
        selmap[str(qid)] = sorted(cur)
        context.user_data[ACTIVE_TEST_MULTI_SELECTED] = selmap

        opts = qinfo.get("options") or []
        await q.edit_message_reply_markup(reply_markup=kb_employee_multi(aid, qid, opts, set(cur)))
        return

    if action == "multi_submit":
        selmap = context.user_data.get(ACTIVE_TEST_MULTI_SELECTED) or {}
        selected = list(selmap.get(str(qid), []))
        correct = qinfo.get("correct") or []
        is_corr = _is_correct_closed(selected, correct)
        db_test_save_answer(aid, qid, {"selected": selected}, is_corr)

        next_idx = int(a.get("current_idx") or 0) + 1
        db_test_update_assignment_progress(aid, next_idx)
        a = db_test_get_assignment(aid)

        if next_idx >= len(questions):
            db_test_finish_assignment(aid, "finished")
            clear_active_test(context)
            await context.bot.send_message(chat_id=user_id, text=EMPLOYEE_TEST_FINISH_TEXT)
            await _notify_admin_test_done(context, a, "пройден")
            return

        await send_employee_question(context, user_id, a)
        return

async def cb_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await deny_no_access(update, context):
        return

    # ===================== TESTING: sync tg_user_id =============
    await sync_profile_user_id_from_update(update)

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

    if data == "help:leisure":
        await q.edit_message_text(
            help_text_leisure(),
            parse_mode=ParseMode.HTML,
            reply_markup=kb_help_leisure(),
            disable_web_page_preview=True,
        )
        return

    if data == "help:leisure:sb":
        await q.edit_message_text(
            "⚓ <b>Морской бой</b>\n\n"
            "Выберите коллегу, которого хотите пригласить:",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_sb_pick_opponent(),
            disable_web_page_preview=True,
        )
        return


    if data == "help:faq":
        clear_bonus_calc_flow(context)
        text = (
            "❓ <b>Часто задаваемые вопросы</b>\n\n"
            "Выберите вопрос из списка ниже 👇"
        )
        await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb_help_faq_list(), disable_web_page_preview=True)
        return


    if data == "help:faq:bonus":
        clear_bonus_calc_flow(context)
        context.chat_data[WAITING_BONUS_CALC] = True
        context.chat_data[BONUS_STEP] = 1
        context.chat_data[BONUS_DATA] = {}

        await q.message.reply_text(
            "🧮 <b>Калькулятор премии</b>\n\n"
            "Требования к премии:\n• Минимальный порог — <b>70%</b> плана\n• Максимальный порог — <b>200%</b> плана\n\nШаг 1/2: введите <b>оклад</b> (например: 40 000)",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Назад к FAQ", callback_data="help:faq")]
            ]),
        )
        return

    if data.startswith("help:faq:item:"):
        fid = int(data.split(":")[-1])
        item = db_faq_get(fid)
        if not item:
            await q.edit_message_text("Вопрос не найден (возможно удалён).", reply_markup=kb_help_main(is_admin_user=is_adm))
            return
        text = (
            f"❓ {item['question']}\n\n"
            f"{item['answer']}"
        )
        await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb_help_faq_item(), disable_web_page_preview=True)
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
            "✍️ <b>Напиши сообщение для тимлида</b>\n\n"
            "Можно одним сообщением. Я передам его тимлиду\n"
            "Чтобы отменить — нажми «Отмена».",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_suggest_cancel(),
            disable_web_page_preview=True,
        )
        return

    if data == "help:docs":
        text = (
            "📄 <b>Документы</b>\n\n"
            "Здесь собраны рабочие документы.\n"
            "Выберите категорию, чтобы перейти к файлам."
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
            "Здесь собраны рабочие ресурсы, используемые в повседневных задачах"
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
        text = "👥 <b>Познакомиться с командой</b>\n\nЗдесь вы можете познакомиться с коллегами.\nВыберите человека, чтобы посмотреть его профиль 👇"
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
            f"👤 <b>{p['full_name']}</b>\n\n"
            f"📅 Работает с: <b>{p['year_start']}</b>\n"
            f"🏙️ Город: <b>{p['city']}</b>\n"
            f"🎂 День рождения: <b>{bday}</b>\n\n"
            f"📝 <b>Кратко о себе</b>\n{p['about']}\n\n"
            f"❓ <b>По каким вопросам обращаться</b>\n{p['topics']}\n\n"
            f"🔗 <b>TG:</b> {p['tg_link']}\n"
            f"📈 <b>Средний балл тестирования:</b> <b>{(str(p.get('avg_test_score')) + '%' ) if (p.get('avg_test_score') is not None and str(p.get('avg_test_score')).strip() != '') else '—'}</b>\n\n"
            f"━━━━━━━━━━━━━━\n\n"
            f"🏆 <b>Ачивки</b>\n\n{format_achievements_for_profile(p['id'])}"
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

    if data == "help:settings:faq":
        clear_faq_flow(context)
        await q.edit_message_text(
            "❓ <b>FAQ</b>\n\nУправление вопросами.",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_settings_faq(),
            disable_web_page_preview=True,
        )
        return

    if data == "help:settings:faq:add":
        clear_faq_flow(context)
        context.chat_data[WAITING_FAQ_Q] = True
        context.chat_data[WAITING_USER_ID] = update.effective_user.id if update.effective_user else None
        context.chat_data[WAITING_SINCE_TS] = int(time.time())
        await q.edit_message_text(
            "➕ <b>Добавление вопроса</b>\n\nОтправьте текст вопроса одним сообщением.",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_cancel_wizard_settings(),
            disable_web_page_preview=True,
        )
        return

    if data == "help:settings:faq:del":
        clear_faq_flow(context)
        await q.edit_message_text(
            "➖ <b>Удаление вопроса</b>\n\nВыберите, что удалить:",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_pick_faq_to_delete(),
            disable_web_page_preview=True,
        )
        return

    if data.startswith("help:settings:faq:del:"):
        fid = int(data.split(":")[-1])
        ok = db_faq_delete(fid)
        try:
            await q.answer("Удалено ✅" if ok else "Не найдено", show_alert=not ok)
        except (TimedOut, NetworkError):
            pass
        await q.edit_message_text(
            "❓ <b>FAQ</b>\n\nУправление вопросами.",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_settings_faq(),
            disable_web_page_preview=True,
        )
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
            clear_faq_flow(context)
            clear_profile_wiz(context)
            clear_waiting_date(context)
            clear_csv_import(context)
            clear_zip_import(context)
            clear_suggest_flow(context)
            clear_ach_wiz(context)
            clear_bcast_flow(context)
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
            clear_faq_flow(context)
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

        if data == "help:settings:backup_zip":
            # сформировать ZIP и отправить документом в текущий чат (обычно ЛС)
            try:
                b = export_backup_zip_bytes()
                bio = io.BytesIO(b)
                bio.name = f"backup_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.zip"
                await context.bot.send_document(
                    chat_id=update.effective_chat.id,
                    document=bio,
                    caption="📦 Бэкап готов. Сохраните ZIP — его можно потом загрузить обратно для восстановления.",
                )
                await q.answer("Бэкап отправлен ✅")
            except Exception as e:
                logger.exception("backup_zip send failed: %s", e)
                await q.answer("Не смог сформировать бэкап 😕", show_alert=True)
            return

        if data == "help:settings:restore_zip":
            clear_restore_zip(context)
            context.chat_data[WAITING_RESTORE_ZIP] = True
            context.chat_data[WAITING_USER_ID] = update.effective_user.id
            context.chat_data[WAITING_SINCE_TS] = int(time.time())
            await q.edit_message_text(
                "📥 <b>Восстановление из ZIP</b>\n\n"
                "Пришлите ZIP-файл бэкапа следующим сообщением.\n"
                "Я восстановлю карточки, документы/категории, подключённые чаты и ачивки.\n\n"
                "Если передумали — нажмите «Отмена».",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_cancel_wizard_settings(),
            )
            await q.answer()
            return


        # ===================== TESTING (employees) help/settings =====================
        if data == "help:settings:test":
            clear_test_wiz(context)
            await q.edit_message_text(
                "📝 <b>Тестирование</b>\n\n"
                "Создание и отправка тестов сотрудникам, а также просмотр результатов.",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_settings_test_menu(),
                disable_web_page_preview=True,
            )
            return

        
        

        if data == "help:settings:test:avgscore":
            # ручная установка среднего балла тестирования в карточке сотрудника
            clear_test_wiz(context)
            context.chat_data[WAITING_TEST_AVGSCORE] = False
            context.chat_data.pop(WAITING_TEST_AVGSCORE_PID, None)

            await q.edit_message_text(
                "📈 <b>Средний балл тестирования</b>\n\n"
                "Выберите сотрудника, чтобы вручную указать значение в процентах.",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_pick_profile_for_avgscore(),
            )
            return

        if data.startswith("help:settings:test:avgscore:pick:"):
            pid = int(data.split(":")[-1])
            p = db_profiles_get(pid)
            if not p:
                await q.answer("Анкета не найдена.", show_alert=True)
                return

            context.chat_data[WAITING_TEST_AVGSCORE] = True
            context.chat_data[WAITING_TEST_AVGSCORE_PID] = pid

            current = p.get("avg_test_score")
            cur_txt = f"{int(current)}%" if current is not None and str(current).strip() != "" else "—"

            await q.edit_message_text(
                f"👤 <b>{escape(p['full_name'])}</b>\n"
                f"Текущее значение: <b>{escape(cur_txt)}</b>\n\n"
                "Отправьте следующим сообщением число от <b>0</b> до <b>100</b> (в процентах).\n"
                "Чтобы очистить значение — отправьте <code>0</code>.\n\n"
                "Отмена: /help → Настройки → Тестирование",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_cancel_wizard_settings(),
            )
            return

        if data == "help:settings:test:drafts":
            clear_test_wiz(context)
            templates = db_test_list_templates(limit=50)
            await q.edit_message_text(
                "🗂 <b>Черновики</b> (шаблоны тестов)\n\n"
                "Здесь хранятся все созданные шаблоны тестов. Вы можете открыть шаблон и отправить его одному или нескольким сотрудникам.",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_test_drafts_list(templates),
                disable_web_page_preview=True,
            )
            return

        if data.startswith("help:settings:test:draft:open:"):
            tid = int(data.split(":")[-1])
            tpl = db_test_get_template(tid)
            if not tpl:
                await q.answer("Черновик не найден.", show_alert=True)
                return
            qs = db_test_get_questions_for_template(tid)
            body = [f"🗂 <b>Черновик</b>\n", f"Название: <b>{escape(tpl.get('title') or '')}</b>", f"Вопросов: <b>{len(qs)}</b>"]
            await q.edit_message_text("\n".join(body), parse_mode=ParseMode.HTML, reply_markup=kb_test_draft_actions(tid))
            return

        if data.startswith("help:settings:test:draft:delete:"):
            tid = int(data.split(":")[-1])
            await q.edit_message_text(
                "🗑 <b>Удалить черновик?</b>\n\n"
                "Будет удалён только черновик. Результаты (если есть) останутся в разделе «Результаты».",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_test_draft_delete_confirm(tid),
            )
            return

        if data.startswith("help:settings:test:draft:delete_yes:"):
            tid = int(data.split(":")[-1])
            db_test_delete_draft_only(tid)
            templates = db_test_list_templates(limit=50)
            await q.edit_message_text(
                "✅ Черновик удалён.",
                reply_markup=kb_test_drafts_list(templates),
            )
            return

        if data.startswith("help:settings:test:draft:send:"):
            tid = int(data.split(":")[-1])
            tpl = db_test_get_template(tid)
            if not tpl:
                await q.answer("Черновик не найден.", show_alert=True)
                return
            # Start a lightweight wizard for sending existing template
            clear_test_wiz(context)
            context.user_data[TEST_WIZ_ACTIVE] = True
            context.user_data[TEST_WIZ_STEP] = TEST_WIZ_STEP_TIME
            context.user_data[TEST_WIZ_DATA] = {"time_limit_sec": None, "profile_ids": []}
            context.user_data[TEST_WIZ_SELECTED_PIDS] = set()
            context.user_data[TEST_WIZ_TEMPLATE_ID] = int(tid)
            context.user_data[TEST_WIZ_FROM_TEMPLATE_ID] = int(tid)

            await q.edit_message_text(
                f"📝 <b>Отправка черновика</b>\n\n"
                f"Шаг 1/3: выберите лимит времени для теста «<b>{escape(tpl.get('title') or '')}</b>»:",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_test_time_limit(),
            )
            return
        if data == "help:settings:test:cancel":
            clear_test_wiz(context)
            await q.edit_message_text(
                "⚙️ <b>Настройки</b>\n\nВыберите действие:",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_help_settings(),
            )
            return

        if data == "help:settings:test:create":
            clear_test_wiz(context)
            context.user_data[TEST_WIZ_ACTIVE] = True
            context.user_data[TEST_WIZ_STEP] = TEST_WIZ_STEP_TITLE
            context.user_data[TEST_WIZ_DATA] = {"questions": []}
            context.user_data[TEST_WIZ_SELECTED_PIDS] = set()
            context.user_data.pop(TEST_WIZ_TEMPLATE_ID, None)
            await q.edit_message_text(
                "📝 <b>Создание теста</b>\n\nШаг 1/5: введите <b>название теста</b> одним сообщением.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="help:settings:test:cancel")]]),
            )
            return

        if data == "help:settings:test:q:add":
            if not context.user_data.get(TEST_WIZ_ACTIVE):
                await q.answer()
                return
            d = context.user_data.get(TEST_WIZ_DATA) or {}
            if len(d.get("questions", [])) >= 10:
                await q.answer("Максимум 10 вопросов.", show_alert=True)
                return
            d["pending_q"] = {"options": [], "correct": []}
            context.user_data[TEST_WIZ_DATA] = d
            context.user_data[TEST_WIZ_STEP] = TEST_WIZ_STEP_Q_TYPE
            await q.edit_message_text(
                "Шаг 2/5: выберите тип вопроса:",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_test_q_type(),
            )
            return

        if data.startswith("help:settings:test:q:type:"):
            if not context.user_data.get(TEST_WIZ_ACTIVE):
                await q.answer()
                return
            qtype = data.split(":")[-1]
            d = context.user_data.get(TEST_WIZ_DATA) or {}
            pq = d.get("pending_q") or {"options": [], "correct": []}
            pq["q_type"] = qtype
            d["pending_q"] = pq
            context.user_data[TEST_WIZ_DATA] = d
            context.user_data[TEST_WIZ_STEP] = TEST_WIZ_STEP_Q_TEXT
            await q.edit_message_text(
                "Введите <b>текст вопроса</b> одним сообщением.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="help:settings:test:cancel")]]),
            )
            return

        if data == "help:settings:test:q:opts_done":
            if not context.user_data.get(TEST_WIZ_ACTIVE):
                await q.answer()
                return
            d = context.user_data.get(TEST_WIZ_DATA) or {}
            pq = d.get("pending_q") or {}
            opts = pq.get("options") or []
            if len(opts) < 2:
                await q.answer("Нужно минимум 2 варианта.", show_alert=True)
                return
            context.user_data[TEST_WIZ_STEP] = TEST_WIZ_STEP_Q_CORRECT
            if pq.get("q_type") == "single":
                await q.edit_message_text(
                    "Выберите <b>правильный</b> вариант:",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb_test_correct_single(opts),
                )
            else:
                pq["correct_set"] = set()
                d["pending_q"] = pq
                context.user_data[TEST_WIZ_DATA] = d
                await q.edit_message_text(
                    "Отметьте <b>правильные</b> варианты (можно несколько), затем нажмите «Готово»:",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb_test_correct_multi(opts, set()),
                )
            return

        if data.startswith("help:settings:test:q:correct_single:"):
            if not context.user_data.get(TEST_WIZ_ACTIVE):
                await q.answer()
                return
            i = int(data.split(":")[-1])
            d = context.user_data.get(TEST_WIZ_DATA) or {}
            pq = d.get("pending_q") or {}
            pq["correct"] = [i]
            d["pending_q"] = pq
            # commit question
            qs = d.get("questions") or []
            qs.append({
                "q_type": pq.get("q_type"),
                "question_text": pq.get("question_text"),
                "options": pq.get("options") or [],
                "correct": pq.get("correct") or [],
            })
            d["questions"] = qs
            d.pop("pending_q", None)
            context.user_data[TEST_WIZ_DATA] = d
            context.user_data[TEST_WIZ_STEP] = TEST_WIZ_STEP_MENU
            await q.edit_message_text(
                f"Вопрос добавлен. Сейчас вопросов: <b>{len(qs)}</b>.",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_test_wiz_questions_menu(has_any=len(qs)>0),
            )
            return

        if data.startswith("help:settings:test:q:correct_toggle:"):
            if not context.user_data.get(TEST_WIZ_ACTIVE):
                await q.answer()
                return
            i = int(data.split(":")[-1])
            d = context.user_data.get(TEST_WIZ_DATA) or {}
            pq = d.get("pending_q") or {}
            sel = set(pq.get("correct_set") or [])
            if i in sel:
                sel.remove(i)
            else:
                sel.add(i)
            pq["correct_set"] = sel
            d["pending_q"] = pq
            context.user_data[TEST_WIZ_DATA] = d
            opts = pq.get("options") or []
            await q.edit_message_text(
                "Отметьте правильные варианты и нажмите «Готово»:",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_test_correct_multi(opts, sel),
            )
            return

        if data == "help:settings:test:q:correct_done":
            if not context.user_data.get(TEST_WIZ_ACTIVE):
                await q.answer()
                return
            d = context.user_data.get(TEST_WIZ_DATA) or {}
            pq = d.get("pending_q") or {}
            sel = sorted(set(pq.get("correct_set") or []))
            if not sel:
                await q.answer("Нужно выбрать хотя бы 1 вариант.", show_alert=True)
                return
            pq["correct"] = sel
            qs = d.get("questions") or []
            qs.append({
                "q_type": pq.get("q_type"),
                "question_text": pq.get("question_text"),
                "options": pq.get("options") or [],
                "correct": pq.get("correct") or [],
            })
            d["questions"] = qs
            d.pop("pending_q", None)
            context.user_data[TEST_WIZ_DATA] = d
            context.user_data[TEST_WIZ_STEP] = TEST_WIZ_STEP_MENU
            await q.edit_message_text(
                f"Вопрос добавлен. Сейчас вопросов: <b>{len(qs)}</b>.",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_test_wiz_questions_menu(has_any=len(qs)>0),
            )
            return

        if data == "help:settings:test:q:done":
            if not context.user_data.get(TEST_WIZ_ACTIVE):
                await q.answer()
                return
            d = context.user_data.get(TEST_WIZ_DATA) or {}
            qs = d.get("questions") or []
            if not qs:
                await q.answer("Добавьте хотя бы 1 вопрос.", show_alert=True)
                return
            context.user_data[TEST_WIZ_STEP] = TEST_WIZ_STEP_TIME
            await q.edit_message_text(
                "Шаг 3/5: выберите лимит времени (в минутах) или «без лимита»:",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_test_time_limit(),
            )
            return

        if data.startswith("help:settings:test:time:"):
            if not context.user_data.get(TEST_WIZ_ACTIVE):
                await q.answer()
                return
            tail = data.split(":")[-1]
            d = context.user_data.get(TEST_WIZ_DATA) or {}
            if tail == "manual":
                context.user_data[TEST_WIZ_STEP] = TEST_WIZ_STEP_TIME_MANUAL
                await q.edit_message_text(
                    "Введите количество минут (целое число), одним сообщением:",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="help:settings:test:cancel")]]),
                )
                return
            if tail == "none":
                d["time_limit_sec"] = None
            else:
                mins = int(tail)
                d["time_limit_sec"] = mins * 60
            context.user_data[TEST_WIZ_DATA] = d
            context.user_data[TEST_WIZ_STEP] = TEST_WIZ_STEP_PICK_PROFILE
            await q.edit_message_text(
                "Шаг 4/5: выберите сотрудников (можно несколько):",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_pick_profiles_for_test(set(), back_cb="help:settings:test"),
            )
            return


        if data == "help:settings:test:pick_open":
            if not context.user_data.get(TEST_WIZ_ACTIVE):
                await q.answer()
                return
            selected = set(context.user_data.get(TEST_WIZ_SELECTED_PIDS) or set())
            await q.edit_message_text(
                "Шаг 4/5: выберите сотрудников (можно несколько):",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_pick_profiles_for_test(selected, back_cb="help:settings:test"),
            )
            return

        if data.startswith("help:settings:test:pick_toggle:"):
            if not context.user_data.get(TEST_WIZ_ACTIVE):
                await q.answer()
                return
            pid = int(data.split(":")[-1])
            selected = set(context.user_data.get(TEST_WIZ_SELECTED_PIDS) or set())
            if pid in selected:
                selected.remove(pid)
            else:
                selected.add(pid)
            context.user_data[TEST_WIZ_SELECTED_PIDS] = selected
            await q.edit_message_reply_markup(reply_markup=kb_pick_profiles_for_test(selected, back_cb="help:settings:test"))
            await q.answer()
            return

        if data == "help:settings:test:pick_done":
            if not context.user_data.get(TEST_WIZ_ACTIVE):
                await q.answer()
                return
            selected = list(context.user_data.get(TEST_WIZ_SELECTED_PIDS) or [])
            if not selected:
                await q.answer("Выберите хотя бы одного сотрудника.", show_alert=True)
                return

            d = context.user_data.get(TEST_WIZ_DATA) or {}
            # store for summary (legacy key kept for compatibility)
            d["profile_ids"] = selected
            context.user_data[TEST_WIZ_DATA] = d
            context.user_data[TEST_WIZ_STEP] = TEST_WIZ_STEP_CONFIRM

            # ensure template is persisted as a draft once we reached confirmation
            admin_id = update.effective_user.id if update.effective_user else None
            template_id = _test_wiz_ensure_template_persisted(context, admin_id)
            if template_id:
                context.user_data[TEST_WIZ_TEMPLATE_ID] = template_id

            qs = d.get("questions") or []
            tl = d.get("time_limit_sec")
            tl_txt = "без лимита" if not tl else f"{int(tl//60)} мин"

            names = []
            for pid in selected[:8]:
                prof = db_profiles_get(pid)
                names.append(prof["full_name"] if prof else f"id={pid}")
            who_txt = ", ".join([escape(x) for x in names])
            if len(selected) > 8:
                who_txt += f" и ещё {len(selected) - 8}"

            summary = (
                "📝 <b>Проверьте данные</b>\n\n"
                f"Название: <b>{escape(d.get('title',''))}</b>\n"
                f"Вопросов: <b>{len(qs)}</b>\n"
                f"Лимит: <b>{tl_txt}</b>\n"
                f"Сотрудники: <b>{who_txt}</b>"
            )
            await q.edit_message_text(
                summary,
                parse_mode=ParseMode.HTML,
                reply_markup=kb_test_confirm_send(),
                disable_web_page_preview=True
            )
            return

        if data == "help:settings:test:save_draft":
            if not context.user_data.get(TEST_WIZ_ACTIVE):
                await q.answer()
                return
            admin_id = update.effective_user.id if update.effective_user else None
            tid = _test_wiz_ensure_template_persisted(context, admin_id)
            # draft is saved by persisting template; exit wizard but keep template in DB
            clear_test_wiz(context)
            if tid:
                await q.edit_message_text(
                    "💾 Черновик сохранён.\n\nВы можете найти его в меню «Черновики».",
                    reply_markup=kb_settings_test_menu(),
                )
            else:
                await q.edit_message_text(
                    "⚠️ Не удалось сохранить черновик: не хватает данных (название/вопросы).",
                    reply_markup=kb_settings_test_menu(),
                )
            return

        if data == "help:settings:test:send":

            if not context.user_data.get(TEST_WIZ_ACTIVE):
                await q.answer()
                return

            admin_id = update.effective_user.id if update.effective_user else None
            d = context.user_data.get(TEST_WIZ_DATA) or {}

            # recipients
            profile_ids = d.get("profile_ids") or list(context.user_data.get(TEST_WIZ_SELECTED_PIDS) or [])
            profile_ids = [int(x) for x in profile_ids]
            if not profile_ids:
                await q.answer("Выберите хотя бы одного сотрудника.", show_alert=True)
                return

            # template (persisted draft)
            template_id = context.user_data.get(TEST_WIZ_TEMPLATE_ID)
            if not template_id:
                template_id = _test_wiz_ensure_template_persisted(context, admin_id)
                if not template_id:
                    await q.answer("Не хватает данных для создания теста.", show_alert=True)
                    return
                context.user_data[TEST_WIZ_TEMPLATE_ID] = template_id

            tpl = db_test_get_template(int(template_id))
            title = (tpl.get("title") if tpl else "") or (d.get("title") or "").strip() or "Тест"

            time_limit_sec = d.get("time_limit_sec")

            delivered = []
            failed = []

            for pid in profile_ids:
                aid = db_test_create_assignment(int(template_id), int(pid), admin_id, time_limit_sec)

                prof = db_profiles_get(int(pid))
                who = (prof.get("full_name") if prof else f"id={pid}")

                tg_user_id = int(prof.get("tg_user_id")) if prof and prof.get("tg_user_id") else None
                ok = False
                if tg_user_id:
                    try:
                        # compose employee notification with title + duration + motivation
                        if time_limit_sec:
                            duration_text = f"{int(time_limit_sec)//60} минут"
                        else:
                            duration_text = "без ограничения по времени"

                        notify_text = (
                            f"📝 Назначен тест: {title}\n"
                            f"⏱ Длительность: {duration_text}\n\n"
                            "Результаты теста покажут твою подкованность в данной тематике 💪"
                            "Нажми кнопку ниже, чтобы начать."
                        )
                        await context.bot.send_message(
                            chat_id=tg_user_id,
                            text=notify_text,
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("▶️ Начать тест", callback_data=f"test:start:{aid}")]]),
                            disable_web_page_preview=True,
                        )
                        ok = True
                    except Exception:
                        ok = False

                if ok:
                    delivered.append(who)
                else:
                    # mark assignment as canceled (not delivered)
                    try:
                        db_test_set_assignment_status(int(aid), "canceled")
                    except Exception:
                        pass
                    failed.append(who)

            # keep wizard active so admin can pick other recipients if needed; clear current selection
            context.user_data[TEST_WIZ_SELECTED_PIDS] = set()
            d["profile_ids"] = []
            context.user_data[TEST_WIZ_DATA] = d
            context.user_data[TEST_WIZ_STEP] = TEST_WIZ_STEP_PICK_PROFILE

            if delivered and not failed:
                msg = "✅ Тест отправлен сотрудникам в личные сообщения."
            elif delivered and failed:
                msg = (
                    "⚠️ Тест отправлен не всем.\n\n"
                    f"Доставлено: {len(delivered)}\n"
                    f"Не доставлено: {len(failed)}\n\n"
                    "Если не доставлено — сотрудник должен запустить бота (/start) и написать любое сообщение, "
                    "чтобы бот смог запомнить его Telegram user_id, либо сотрудник запретил сообщения."
                )
            else:
                msg = (
                    "⚠️ Тест создан, но не доставлен никому.\n\n"
                    "Сотрудники должны запустить бота (/start) и написать боту любое сообщение, "
                    "чтобы бот смог запомнить их Telegram user_id."
                )

            await q.edit_message_text(
                msg,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("👥 Выбрать других сотрудников", callback_data="help:settings:test:pick_open")],
                    [InlineKeyboardButton("🗂 Черновики", callback_data="help:settings:test:drafts")],
                    [InlineKeyboardButton("🏠 В меню тестирования", callback_data="help:settings:test")],
                ]),
            )
            return

        if data == "help:settings:test:results":
            items = db_test_list_recent_results(20)
            await q.edit_message_text(
                "📋 <b>Результаты (последние)</b>\n\nВыберите тест:",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_test_results_list(items),
                disable_web_page_preview=True,
            )
            return

        if data.startswith("help:settings:test:results:open:"):

            aid = int(data.split(":")[-1])

            a = db_test_get_assignment(aid)

            if not a:

                await q.answer("Не найдено.", show_alert=True)

                return

            prof = db_profiles_get(int(a["profile_id"]))

            who = prof["full_name"] if prof else f"id={a['profile_id']}"

            answers = db_test_get_answers_for_assignment(aid)



            # ----- итог по закрытым вопросам (single/multi), open не считаем -----

            total_closed = 0

            correct_closed = 0

            for item in answers:

                if item.get("q_type") == "open":

                    continue

                total_closed += 1

                is_corr = item.get("is_correct")

                # fallback для старых записей, где is_correct может быть NULL

                if is_corr is None:

                    try:

                        ans = item.get("answer")

                        sel = (ans.get("selected") if isinstance(ans, dict) else []) or []

                        correct = item.get("correct") or []

                        is_corr = 1 if _is_correct_closed([int(x) for x in sel], [int(x) for x in correct]) else 0

                    except Exception:

                        is_corr = 0

                if int(is_corr) == 1:

                    correct_closed += 1



            pct = int(round((correct_closed / total_closed) * 100)) if total_closed else 0



            parts = [

                f"📝 <b>{escape(who)}</b> — статус: <b>{escape(a['status'])}</b>",

                f"📊 <b>Закрытые вопросы:</b> {correct_closed} / {total_closed} ({pct}%)\n",

            ]



            for item in answers:

                parts.append(f"<b>Q{item['idx']}.</b> {escape(item['question_text'])}")



                if item["q_type"] == "open":

                    txt = (item["answer"].get("text") if isinstance(item["answer"], dict) else "") or "—"

                    parts.append(f"Ответ: {escape(txt)}")



                else:

                    sel = (item["answer"].get("selected") if isinstance(item["answer"], dict) else []) or []

                    opts = item["options"] or []

                    chosen = []

                    for si in sel:

                        if 0 <= int(si) < len(opts):

                            chosen.append(opts[int(si)])



                    parts.append("Ответ: " + escape(", ".join(chosen) if chosen else "—"))



                    is_corr = item.get("is_correct")

                    if is_corr is None:

                        try:

                            correct = item.get("correct") or []

                            is_corr = 1 if _is_correct_closed([int(x) for x in sel], [int(x) for x in correct]) else 0

                        except Exception:

                            is_corr = None



                    if is_corr == 1 or is_corr == "1":

                        parts.append("Результат: ✅ <b>Верно</b>")

                    elif is_corr == 0 or is_corr == "0":

                        parts.append("Результат: ❌ <b>Неверно</b>")



                parts.append("")



            await q.edit_message_text(

                "\n".join(parts).strip(),

                parse_mode=ParseMode.HTML,

                reply_markup=kb_test_results_actions(aid),

                disable_web_page_preview=True,

            )

            return

        if data.startswith("help:settings:test:results:save:"):
            aid = int(data.split(":")[-1])
            db_test_set_assignment_status(aid, "saved")
            await q.answer("Сохранено")
            # refresh view
            a = db_test_get_assignment(aid)
            if a:
                await _notify_admin_test_done(context, a, "сохранён")
            await q.edit_message_reply_markup(reply_markup=kb_test_results_actions(aid))
            return

        if data.startswith("help:settings:test:results:delete:"):
            aid = int(data.split(":")[-1])
            ok = db_test_delete_assignment_only(aid)
            await q.answer("Удалено" if ok else "Не найдено")
            items = db_test_list_recent_results(20)
            await q.edit_message_text(
                "📋 <b>Результаты (последние)</b>\n\nВыберите тест:",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_test_results_list(items),
                disable_web_page_preview=True,
            )
            return

        if data == "help:settings:ach":
            clear_bcast_flow(context)
            clear_ach_wiz(context)
            await q.edit_message_text(
                "🏆 <b>Ачивки</b>\n\n"
                "Здесь можно выдать ачивку сотруднику из анкеты.\n"
                "Ачивки гибкие: эмодзи, название и описание задаются при выдаче.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🎁 Выдать ачивку", callback_data="help:settings:ach:give")],
                    [InlineKeyboardButton("⬅️ Назад", callback_data="help:settings")],
                ]),
            )
            return

        if data == "help:settings:ach:give":
            clear_bcast_flow(context)
            clear_ach_wiz(context)
            await q.edit_message_text(
                "🎁 <b>Выдать ачивку</b>\n\nВыберите сотрудника:",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_pick_profile_for_achievement(),
            )
            return

        if data.startswith("help:settings:ach:pick:"):
            pid = int(data.split(":")[-1])
            p = db_profiles_get(pid)
            if not p:
                await q.answer("Анкета не найдена.", show_alert=True)
                return
            clear_ach_wiz(context)
            clear_bcast_flow(context)
            context.chat_data[ACH_WIZ_ACTIVE] = True
            context.chat_data[ACH_WIZ_STEP] = "emoji"
            context.chat_data[ACH_WIZ_DATA] = {
                "profile_id": pid,
                "full_name": p.get("full_name", ""),
                "tg_link": p.get("tg_link", ""),
            }
            context.chat_data[WAITING_USER_ID] = update.effective_user.id
            context.chat_data[WAITING_SINCE_TS] = int(time.time())
            await q.edit_message_text(
                f"🎁 Выдаём ачивку для: <b>{escape(p.get('full_name',''))}</b>\n\n"
                "Шаг 2/4: отправьте <b>эмодзи</b> (пример: 🏅)",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_cancel_wizard_settings(),
            )
            return

        if data == "help:settings:backup_zip":
            # (obsolete alias if any)
            return

        if data == "help:settings:restore_zip":
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

    # restore ZIP backup
    if context.chat_data.get(WAITING_RESTORE_ZIP):
        user_id = update.effective_user.id if update.effective_user else None
        waiting_user = context.chat_data.get(WAITING_USER_ID)
        if waiting_user and user_id != waiting_user:
            return

        if not await is_admin_scoped(update, context):
            clear_restore_zip(context)
            await update.message.reply_text("❌ Только администраторы могут загружать бэкап.")
            return

        doc = update.message.document
        if not doc:
            return

        # принимаем только .zip (по имени или mime)
        fname = (doc.file_name or "").lower()
        if not (fname.endswith(".zip") or (doc.mime_type or "").lower() in ("application/zip", "application/x-zip-compressed")):
            await update.message.reply_text("❌ Нужен ZIP-файл (backup.zip). Пришлите корректный файл или нажмите «Отмена».")
            return

        try:
            tg_file = await context.bot.get_file(doc.file_id)
            b = await tg_file.download_as_bytearray()
            stats = restore_backup_zip_bytes(bytes(b))
            clear_restore_zip(context)
            await update.message.reply_text(
                "✅ Бэкап загружен и восстановлен.\n\n"
                f"👥 Профили: <b>{stats.get('profiles', 0)}</b>\n"
                f"🗂️ Категории: <b>{stats.get('categories', 0)}</b>\n"
                f"📄 Документы: <b>{stats.get('docs', 0)}</b>\n"
                f"💬 Чаты рассылки: <b>{stats.get('notify_chats', 0)}</b>\n"
                f"🏆 Ачивки: <b>{stats.get('achievements_awards', 0)}</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_help_settings(),
            )
        except Exception as e:
            logger.exception("restore zip failed: %s", e)
            await update.message.reply_text("❌ Не смог восстановить из ZIP. Проверьте файл и попробуйте ещё раз.")
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

    # ---------------- ZIP IMPORT FLOW ----------------
    if context.chat_data.get(WAITING_ZIP_IMPORT):
        if not await is_admin_scoped(update, context):
            clear_zip_import(context)
            await update.message.reply_text("❌ Только администраторы могут восстанавливать бэкап.")
            return

        doc = update.message.document
        if not doc:
            return

        # скачиваем ZIP во временный файл
        try:
            tg_file = await context.bot.get_file(doc.file_id)
            tmp_path = Path(STORAGE_DIR) / "tmp_backup.zip"
            await tg_file.download_to_drive(custom_path=str(tmp_path))
        except Exception as e:
            clear_zip_import(context)
            logger.exception("ZIP download failed: %s", e)
            await update.message.reply_text("❌ Не смог скачать ZIP.")
            return

        def _read_csv_from_zip(zf: zipfile.ZipFile, name: str) -> str | None:
            try:
                data = zf.read(name)
            except KeyError:
                return None
            try:
                return data.decode("utf-8-sig")
            except Exception:
                return data.decode("utf-8", errors="ignore")

        ok_cats = ok_docs = ok_profiles = ok_ach = ok_faq = 0
        skipped_docs = 0

        try:
            with zipfile.ZipFile(tmp_path, "r") as zf:
                # categories
                raw = _read_csv_from_zip(zf, "categories.csv")
                if raw:
                    reader = csv.DictReader(io.StringIO(raw))
                    for row in reader:
                        title = (row.get("title") or "").strip()
                        if title:
                            db_docs_ensure_category(title)
                            ok_cats += 1

                                # если categories.csv отсутствует или пустой — восстановим категории из docs.csv
                if ok_cats == 0:
                    raw_docs = _read_csv_from_zip(zf, "docs.csv")
                    if raw_docs:
                        try:
                            rdr_docs = csv.DictReader(io.StringIO(raw_docs))
                            seen = set()
                            for r0 in rdr_docs:
                                t = (r0.get("category_title") or r0.get("category") or "").strip()
                                if not t:
                                    continue
                                key = t.casefold()
                                if key in seen:
                                    continue
                                seen.add(key)
                                db_docs_ensure_category(t)
                                ok_cats += 1
                        except Exception:
                            pass

# profiles
                raw = _read_csv_from_zip(zf, "profiles.csv")
                id_map: dict[str, int] = {}
                if raw:
                    reader = csv.DictReader(io.StringIO(raw))
                    for row in reader:
                        full_name = (row.get("full_name") or "").strip()
                        if not full_name:
                            continue
                        year_start = int((row.get("year_start") or "0").strip() or 0)
                        city = (row.get("city") or "").strip()
                        birthday = (row.get("birthday") or "").strip() or None
                        about = (row.get("about") or "").strip()
                        topics = (row.get("topics") or "").strip()
                        tg_link = (row.get("tg_link") or "").strip()
                        if not (year_start and city and about and topics and tg_link):
                            continue
                        pid = db_profiles_upsert(full_name, year_start, city, birthday, about, topics, tg_link)
                        if avg_val is not None:
                            if avg_val < 0:
                                avg_val = 0
                            if avg_val > 100:
                                avg_val = 100
                            db_profiles_set_avg_test_score(int(pid), None if int(avg_val) == 0 else int(avg_val))
                        ok_profiles += 1
                        if tg_link:
                            id_map[tg_link] = pid

                # docs
                raw = _read_csv_from_zip(zf, "docs.csv")
                if raw:
                    reader = csv.DictReader(io.StringIO(raw))
                    for row in reader:
                        cat_title = (row.get("category_title") or "").strip() or "Документы"
                        cid = db_docs_ensure_category(cat_title)

                        title = (row.get("doc_title") or "").strip() or "Документ"
                        description = (row.get("doc_description") or "").strip() or None
                        file_id = (row.get("doc_file_id") or "").strip() or None
                        file_unique_id = (row.get("doc_file_unique_id") or "").strip() or None
                        mime_type = (row.get("doc_mime_type") or "").strip() or None
                        local_path = (row.get("doc_local_path") or "").strip() or None

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

                                # faq
                raw = _read_csv_from_zip(zf, "faq.csv")
                if raw:
                    reader = csv.DictReader(io.StringIO(raw))
                    for row in reader:
                        q_text = (row.get("question") or "").strip()
                        a_text = (row.get("answer") or "").strip()
                        if not q_text or not a_text:
                            continue
                        db_faq_upsert(q_text, a_text)
                        ok_faq += 1

# achievements
                raw = _read_csv_from_zip(zf, "achievements_awards.csv")
                if raw:
                    reader = csv.DictReader(io.StringIO(raw))
                    for row in reader:
                        tg_link = (row.get("tg_link") or "").strip()
                        pid = id_map.get(tg_link) if tg_link else None
                        if not pid and tg_link:
                            # попробуем найти в БД
                            con = sqlite3.connect(DB_PATH)
                            cur = con.cursor()
                            cur.execute("SELECT id FROM profiles WHERE tg_link=?", (tg_link,))
                            r = cur.fetchone()
                            con.close()
                            pid = r[0] if r else None
                        if not pid:
                            continue
                        emoji = (row.get("emoji") or "").strip() or "🏆"
                        title = (row.get("title") or "").strip() or "Ачивка"
                        description = (row.get("description") or "").strip() or ""
                        # не тащим awarded_at/awarded_by в точности — создаём новую запись восстановления
                        db_achievement_award_add(int(pid), emoji, title, description, None)
                        ok_ach += 1

        except zipfile.BadZipFile:
            clear_zip_import(context)
            await update.message.reply_text("❌ Это не ZIP или файл повреждён.")
            return
        except Exception as e:
            clear_zip_import(context)
            logger.exception("ZIP import failed: %s", e)
            await update.message.reply_text("❌ Ошибка при восстановлении ZIP.")
            return

        clear_zip_import(context)
        await update.message.reply_text(
            "✅ Восстановление завершено.\n\n"
            f"Категории: <b>{ok_cats}</b>\n"
            f"Анкеты: <b>{ok_profiles}</b>\n"
            f"Документы: <b>{ok_docs}</b> (пропущено без file_id: <b>{skipped_docs}</b>)\n"
            f"FAQ: <b>{ok_faq}</b>\n"
            f"Ачивки: <b>{ok_ach}</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_help_settings(),
        )
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
                avg_raw = (row.get("profile_avg_test_score") or "").strip()
                avg_val = None
                if avg_raw:
                    try:
                        avg_val = int(float(avg_raw.replace("%","").strip()))
                    except Exception:
                        avg_val = None
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



# ---------------- HANDLERS: MEME CHANNEL (collect memes) ----------------

async def on_meme_channel_post(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg or not msg.chat:
        return

    # только нужный канал
    if int(msg.chat_id) != int(MEME_CHANNEL_ID):
        return

    # PHOTO (берём самый большой размер)
    if getattr(msg, "photo", None):
        ph = msg.photo[-1]
        unique_key = f"photo:{ph.file_unique_id}"
        db_meme_add("photo", ph.file_id, unique_key)
        return

    # VIDEO
    if getattr(msg, "video", None):
        vd = msg.video
        unique_key = f"video:{vd.file_unique_id}"
        db_meme_add("video", vd.file_id, unique_key)
        return

    # DOCUMENT (например gif/видео/картинка документом)
    if getattr(msg, "document", None):
        doc = msg.document
        unique_key = f"document:{doc.file_unique_id}"
        db_meme_add("document", doc.file_id, unique_key)
        return




# ---------------- HANDLERS: TEXT INPUT (dates / categories / profiles) ----------------

async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat:
        return


    # ===================== TESTING: sync tg_user_id =============
    await sync_profile_user_id_from_update(update)

    user_id = update.effective_user.id if update.effective_user else None
    text = (update.message.text or "").strip()

    text_html = (message_to_html(update.message) or "").strip()


    # ===================== TESTING (employees) on_text routing =====================

    # (1) Employee active test: open question answer
    active_aid = context.user_data.get(ACTIVE_TEST_ASSIGNMENT_ID)
    if active_aid:
        a = db_test_get_assignment(int(active_aid))
        if a:
            # deadline check
            if await _expire_assignment_if_needed(a, context):
                clear_active_test(context)
                try:
                    await update.message.reply_text(EMPLOYEE_TEST_EXPIRED_TEXT)
                except Exception:
                    pass
                await _notify_admin_test_done(context, a, "истёк")
                return

            questions = db_test_get_questions(int(a["template_id"]))
            idx = int(a.get("current_idx") or 0)
            if 0 <= idx < len(questions):
                qinfo = questions[idx]
                if qinfo["q_type"] == "open":
                    # Save text answer (never show any scoring to employee)
                    db_test_save_answer(int(active_aid), int(qinfo["id"]), {"text": text}, None)

                    next_idx = idx + 1
                    db_test_update_assignment_progress(int(active_aid), next_idx)
                    a = db_test_get_assignment(int(active_aid))

                    if next_idx >= len(questions):
                        db_test_finish_assignment(int(active_aid), "finished")
                        clear_active_test(context)
                        await update.message.reply_text(EMPLOYEE_TEST_FINISH_TEXT)
                        await _notify_admin_test_done(context, a, "пройден")
                        return

                    await send_employee_question(context, update.effective_chat.id, a)
                    return

    # (2) Admin test wizard: free-text inputs
    if context.user_data.get(TEST_WIZ_ACTIVE):
        step = context.user_data.get(TEST_WIZ_STEP) or ""
        d = context.user_data.get(TEST_WIZ_DATA) or {}

        # title input
        if step == TEST_WIZ_STEP_TITLE:
            d["title"] = text
            context.user_data[TEST_WIZ_DATA] = d
            context.user_data[TEST_WIZ_STEP] = TEST_WIZ_STEP_MENU
            await update.message.reply_text(
                f"Шаг 2/5: добавление вопросов. Сейчас вопросов: <b>{len(d.get('questions') or [])}</b>.",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_test_wiz_questions_menu(has_any=len(d.get('questions') or [])>0),
            )
            return

        # question text input
        if step == TEST_WIZ_STEP_Q_TEXT:
            pq = d.get("pending_q") or {"options": [], "correct": []}
            pq["question_text"] = text
            d["pending_q"] = pq
            context.user_data[TEST_WIZ_DATA] = d
            if pq.get("q_type") == "open":
                # commit open question
                qs = d.get("questions") or []
                qs.append({"q_type": "open", "question_text": pq["question_text"], "options": [], "correct": []})
                d["questions"] = qs
                d.pop("pending_q", None)
                context.user_data[TEST_WIZ_DATA] = d
                context.user_data[TEST_WIZ_STEP] = TEST_WIZ_STEP_MENU
                await update.message.reply_text(
                    f"Вопрос добавлен. Сейчас вопросов: <b>{len(qs)}</b>.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb_test_wiz_questions_menu(has_any=True),
                )
                return

            # need options
            context.user_data[TEST_WIZ_STEP] = TEST_WIZ_STEP_Q_OPTIONS
            await update.message.reply_text(
                "Пришлите варианты ответа по одному сообщению (мин 2, макс 8).\nКогда закончите — нажмите «Готово с вариантами».",
                reply_markup=kb_test_options_done(can_done=False),
            )
            return

        # options input
        if step == TEST_WIZ_STEP_Q_OPTIONS:
            pq = d.get("pending_q") or {}
            opts = pq.get("options") or []
            if len(opts) >= 8:
                await update.message.reply_text("Максимум 8 вариантов. Нажмите «Готово с вариантами».", reply_markup=kb_test_options_done(can_done=True))
                return
            opts.append(text)
            pq["options"] = opts
            d["pending_q"] = pq
            context.user_data[TEST_WIZ_DATA] = d
            await update.message.reply_text(
                f"Вариант добавлен ({len(opts)}/8).",
                reply_markup=kb_test_options_done(can_done=(len(opts) >= 2)),
            )
            return

        # manual time
        if step == TEST_WIZ_STEP_TIME_MANUAL:
            try:
                mins = int(re.sub(r"\D", "", text))
                if mins <= 0:
                    raise ValueError()
            except Exception:
                await update.message.reply_text("Введите целое число минут, например 12.")
                return
            d["time_limit_sec"] = mins * 60
            context.user_data[TEST_WIZ_DATA] = d
            context.user_data[TEST_WIZ_STEP] = TEST_WIZ_STEP_PICK_PROFILE
            await update.message.reply_text("Шаг 4/5: выберите сотрудников (можно несколько):", reply_markup=kb_pick_profiles_for_test(set(), back_cb="help:settings:test"))
            return

    # ---------------- BONUS CALC (FAQ) ----------------
    if context.chat_data.get(WAITING_BONUS_CALC):
        step = int(context.chat_data.get(BONUS_STEP) or 1)
        data = context.chat_data.get(BONUS_DATA) or {}

        raw = (text or "")
        raw = raw.replace("\u00A0", " ")  # nbsp
        raw_num = raw.replace(" ", "").replace(",", ".").strip()
        try:
            val = float(raw_num)
        except Exception:
            await update.message.reply_text("Не понял число. Введите ещё раз:")
            return

        if step == 1:
            if val <= 0:
                await update.message.reply_text("Оклад должен быть больше 0. Введите ещё раз:")
                return
            data["salary"] = val
            context.chat_data[BONUS_DATA] = data
            context.chat_data[BONUS_STEP] = 2
            await update.message.reply_text(
                "✅ Оклад принят.\n\n"
                "Шаг 2/2: введите <b>% выполнения плана</b> (например: 100)",
                parse_mode=ParseMode.HTML,
            )
            return

        # step == 2
        salary = float(data.get("salary") or 0)
        percent_in = val

        # clamp rules
        if percent_in < 70:
            bonus = 0.0
        else:
            percent_eff = min(percent_in, 200.0)
            bonus_gross = (salary / 2.0) * (percent_eff / 100.0)
            bonus = bonus_gross * 0.87  # 13% tax

        clear_bonus_calc_flow(context)

        def fmt_money(x: float) -> str:
            if abs(x - round(x)) < 1e-9:
                return f"{x:,.0f}".replace(",", " ")
            return f"{x:,.2f}".replace(",", " ")

        note = ""
        if percent_in > 200:
            note = "\n\n<b>🔥 Вау, я поражён твоими результатами!</b>\nТак держать — видно, что ты умеешь выходить за рамки!"
        elif percent_in < 70:
            note = "\n\n<b>🌱 Каждый результат — это шаг вперёд.</b>\nПродолжай — и всё обязательно получится"

        percent_used = 0.0 if percent_in < 0 else min(percent_in, 200.0)

        await update.message.reply_text(
            "🧾 <b>Результат</b>\n\n"
            f"Оклад: <b>{fmt_money(salary)}</b>\n"
            f"% выполнения (твой показатель): <b>{percent_in:.2f}</b>\n"
            f"% выполнения (учитываем в расчётах): <b>{percent_used:.2f}</b>\n"
            f"Премия: <b>{fmt_money(bonus)}</b>"
            f"{note}",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Назад к FAQ", callback_data="help:faq")],
            ]),
        )


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
        await update.message.reply_text("✅ Спасибо! Передал тимлиду 🙌")
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

    # ачивки — выдача
    if context.chat_data.get(ACH_WIZ_ACTIVE):
        if not await is_admin_scoped(update, context):
            clear_ach_wiz(context)
            await update.message.reply_text("❌ Только администраторы могут выдавать ачивки.")
            return

        step = context.chat_data.get(ACH_WIZ_STEP)
        d = context.chat_data.get(ACH_WIZ_DATA) or {}

        if step == "emoji":
            emoji = text.strip()
            if len(emoji) < 1 or len(emoji) > 16:
                await update.message.reply_text("❌ Отправьте один эмодзи (или короткую связку). Пример: 🏅")
                return
            d["emoji"] = emoji
            context.chat_data[ACH_WIZ_DATA] = d
            context.chat_data[ACH_WIZ_STEP] = "title"
            await update.message.reply_text(
                "Шаг 3/4: отправьте <b>название ачивки</b> (будет жирным).",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_cancel_wizard_settings(),
            )
            return

        if step == "title":
            title = text.strip()
            if len(title) < 2:
                await update.message.reply_text("❌ Слишком коротко. Напишите название ачивки.")
                return
            d["title"] = title[:80]
            context.chat_data[ACH_WIZ_DATA] = d
            context.chat_data[ACH_WIZ_STEP] = "description"
            await update.message.reply_text(
                "Шаг 4/4: напишите <b>описание</b> — за что выдаётся ачивка 🙂",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_cancel_wizard_settings(),
            )
            return

        if step == "description":
            desc = text.strip()
            if len(desc) < 3:
                await update.message.reply_text("❌ Напишите чуть подробнее 🙂")
                return
            d["description"] = desc[:600]

            pid = d.get("profile_id")
            if not pid:
                clear_ach_wiz(context)
                await update.message.reply_text("❌ Не выбран сотрудник. Начните заново через /help → Настройки → Ачивки.")
                return

            admin_id = update.effective_user.id if update.effective_user else None
            db_achievement_award_add(int(pid), d.get("emoji", "🏆"), d.get("title", "Ачивка"), d.get("description", ""), admin_id)

            scope_chat_id = get_scope_chat_id(update, context)
            mention = normalize_tg_mention(d.get("tg_link", "") or "")
            who = mention if mention else f"<b>{escape(d.get('full_name', 'Сотрудник'))}</b>"
            msg = (
                f"🎉 <b>Поздравляем, {who}!</b>\n\n"
                f"В твой профиль добавлена новая ачивка: <b>{escape(d.get('emoji', '🏆'))} {escape(d.get('title', 'Ачивка'))}</b>\n\n"
                f"Достижение получено за: «{escape(d.get('description', ''))}»\n\n"
                f"Так держать! 🚀🔥\n\n"
                f"Посмотреть можно в /help"
            )

            sent = False
            if scope_chat_id:
                try:
                    await context.bot.send_message(chat_id=scope_chat_id, text=msg, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
                    sent = True
                except Exception as e:
                    logger.exception("Cannot send achievement notify to scope chat: %s", e)

            if not sent:
                for chat_id in db_list_chats():
                    try:
                        await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
                        sent = True
                        break
                    except Exception:
                        pass

            clear_ach_wiz(context)
            await update.message.reply_text("✅ Ачивка выдана и опубликована в чате.", reply_markup=kb_help_settings())
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

    # ---------------- TEST AVG SCORE (manual) ----------------
    if context.chat_data.get(WAITING_TEST_AVGSCORE):
        pid = context.chat_data.get(WAITING_TEST_AVGSCORE_PID)
        if not pid:
            context.chat_data[WAITING_TEST_AVGSCORE] = False
        else:
            raw = (text or "").replace("%", "").strip()
            try:
                val = int(float(raw))
            except Exception:
                await update.message.reply_text("Введите число от 0 до 100 (можно с %).")
                return

            if val < 0 or val > 100:
                await update.message.reply_text("Значение должно быть в диапазоне 0–100.")
                return

            # 0 трактуем как очистку значения
            db_profiles_set_avg_test_score(int(pid), None if val == 0 else val)

            context.chat_data[WAITING_TEST_AVGSCORE] = False
            context.chat_data.pop(WAITING_TEST_AVGSCORE_PID, None)

            p = db_profiles_get(int(pid))
            who = p["full_name"] if p else f"id={pid}"
            shown = "—" if val == 0 else f"{val}%"

            await update.message.reply_text(
                f"✅ Сохранено. {who}: средний балл тестирования = {shown}"
            )
        return


        # ---------------- FAQ ADD FLOW ----------------
    if context.chat_data.get(WAITING_FAQ_Q):
        context.chat_data[WAITING_FAQ_Q] = False
        context.chat_data[WAITING_FAQ_A] = True

        q_html = (text_html or text or "").strip()
        q_plain = (text or "").strip()
        context.chat_data[PENDING_FAQ] = {"question_html": q_html, "question_plain": q_plain}

        await update.message.reply_text(
            "✅ Вопрос сохранён.\n\nТеперь отправьте <b>ответ</b> одним сообщением.",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_cancel_wizard_settings(),
        )
        return

    if context.chat_data.get(WAITING_FAQ_A):
        pending = context.chat_data.get(PENDING_FAQ) or {}
        q_html = (pending.get("question_html") or "").strip()
        a_html = (text_html or text or "").strip()
        clear_faq_flow(context)

        if not q_html or not a_html:
            await update.message.reply_text("❌ Не удалось сохранить: пустой вопрос или ответ.")
            return

        db_faq_add(q_html, a_html)
        await update.message.reply_text(
            "✅ Вопрос добавлен в FAQ.",
            reply_markup=kb_help_settings(),
        )
        return
        db_faq_add(q_text, a_text)
        await update.message.reply_text(
            "✅ Вопрос добавлен в FAQ.",
            reply_markup=kb_help_settings(),
        )
        return


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


# ---------------- ERROR HANDLER ----------------

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Логируем любые необработанные ошибки, чтобы бот не падал молча."""
    try:
        logger.exception("Unhandled exception while processing update: %s", context.error)
    except Exception:
        pass



# ---------------- LEISURE: SEA BATTLE (BATTLESHIP) ----------------

SB_GAMES = "sb_games"         # context.application.bot_data[SB_GAMES] -> dict
SB_USER_GAME = "sb_user_game" # context.application.bot_data[SB_USER_GAME] -> dict user_id->game_id

SB_SIZE = 10
SB_SHIPS = [4, 3, 3, 2, 2, 2, 1, 1, 1, 1]
SB_TURN_SECONDS = 180  # 3 minutes
SB_INVITE_SECONDS = 300  # 5 минут ожидания ответа на приглашение


@dataclass
class SBPlayerState:
    user_id: int
    full_name: str
    ships: set[tuple[int, int]] = field(default_factory=set)      # ship cells
    hits: set[tuple[int, int]] = field(default_factory=set)       # hits by enemy
    misses_by_enemy: set[tuple[int, int]] = field(default_factory=set)
    setup_confirmed: bool = False


@dataclass
class SBGame:
    game_id: str
    host_id: int
    p1: SBPlayerState
    p2: SBPlayerState
    status: str = "inviting"   # inviting/setup/playing/finished
    turn_user_id: int | None = None
    last_turn_job_name: str | None = None
    last_invite_job_name: str | None = None


def _sb_store(context: ContextTypes.DEFAULT_TYPE):
    bd = context.application.bot_data
    bd.setdefault(SB_GAMES, {})
    bd.setdefault(SB_USER_GAME, {})
    return bd[SB_GAMES], bd[SB_USER_GAME]


def _sb_render_own_board(p: SBPlayerState) -> str:
    header = "   " + " ".join([str(i) for i in range(1, 11)])
    lines = [header]
    for r in range(SB_SIZE):
        row_label = chr(ord("A") + r)
        cells = []
        for c in range(SB_SIZE):
            cc = (r, c)
            if cc in p.ships and cc in p.hits:
                cells.append("🔥")
            elif cc in p.ships:
                cells.append("🚢")
            elif cc in p.misses_by_enemy:
                cells.append("❌")
            else:
                cells.append("🟦")
        lines.append(f"{row_label}  " + " ".join(cells))
    return "\n".join(lines)


def _sb_render_enemy_board(p_enemy: SBPlayerState) -> str:
    header = "   " + " ".join([str(i) for i in range(1, 11)])
    lines = [header]
    for r in range(SB_SIZE):
        row_label = chr(ord("A") + r)
        cells = []
        for c in range(SB_SIZE):
            cc = (r, c)
            if cc in p_enemy.hits:
                cells.append("🔥")
            elif cc in p_enemy.misses_by_enemy:
                cells.append("❌")
            else:
                cells.append("🟦")
        lines.append(f"{row_label}  " + " ".join(cells))
    return "\n".join(lines)


def _sb_can_place(occ: set[tuple[int, int]], coords: list[tuple[int, int]]) -> bool:
    # No adjacency (including diagonals)
    for r, c in coords:
        if not (0 <= r < SB_SIZE and 0 <= c < SB_SIZE):
            return False
        if (r, c) in occ:
            return False
        for dr in (-1, 0, 1):
            for dc in (-1, 0, 1):
                if (r + dr, c + dc) in occ:
                    return False
    return True


def _sb_random_place_ships() -> set[tuple[int, int]]:
    occ: set[tuple[int, int]] = set()
    for ln in SB_SHIPS:
        for _try in range(700):
            vertical = random.choice([True, False])
            if vertical:
                r = random.randint(0, SB_SIZE - ln)
                c = random.randint(0, SB_SIZE - 1)
                coords = [(r + k, c) for k in range(ln)]
            else:
                r = random.randint(0, SB_SIZE - 1)
                c = random.randint(0, SB_SIZE - ln)
                coords = [(r, c + k) for k in range(ln)]
            if _sb_can_place(occ, coords):
                occ.update(coords)
                break
    return occ


def kb_sb_pick_opponent():
    people = db_profiles_list()  # [(pid, full_name), ...]
    rows = []
    if not people:
        rows.append([InlineKeyboardButton("— анкет нет —", callback_data="noop")])
    else:
        for pid, name in people[:80]:
            prof = db_profiles_get(int(pid))
            tg_uid = int(prof.get("tg_user_id") or 0) if prof else 0
            if not tg_uid:
                # user hasn't started bot yet -> cannot DM
                continue
            rows.append([InlineKeyboardButton(name, callback_data=f"sb:new:pick:{pid}")])

        if not rows:
            rows.append([InlineKeyboardButton("— нет доступных игроков (нужно нажать /start) —", callback_data="noop")])

    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="help:leisure")])
    return InlineKeyboardMarkup(rows)


def kb_sb_invite(game_id: str):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Принять", callback_data=f"sb:invite_accept:{game_id}"),
            InlineKeyboardButton("❌ Отказаться", callback_data=f"sb:invite_decline:{game_id}"),
        ]
    ])



def kb_sb_host_wait(game_id: str):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("❌ Отменить приглашение", callback_data=f"sb:invite_cancel:{game_id}")],
        [InlineKeyboardButton("🏠 В меню", callback_data="help:main")],
    ])

def kb_sb_setup(game_id: str):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Сгенерировать другое расположение", callback_data=f"sb:setup_reroll:{game_id}")],
        [InlineKeyboardButton("✅ Подтвердить. В бой!", callback_data=f"sb:setup_confirm:{game_id}")],
    ])


def kb_sb_pick_row(game_id: str):
    letters = [chr(ord("A") + i) for i in range(10)]
    rows = [
        [InlineKeyboardButton(ch, callback_data=f"sb:shot_row:{game_id}:{ch}") for ch in letters[:5]],
        [InlineKeyboardButton(ch, callback_data=f"sb:shot_row:{game_id}:{ch}") for ch in letters[5:]],
    ]
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"sb:back_to_game:{game_id}")])
    return InlineKeyboardMarkup(rows)


def kb_sb_pick_cell(game_id: str, row_letter: str):
    nums = list(range(1, 11))
    rows = [
        [InlineKeyboardButton(str(n), callback_data=f"sb:shot_cell:{game_id}:{row_letter}:{n}") for n in nums[:5]],
        [InlineKeyboardButton(str(n), callback_data=f"sb:shot_cell:{game_id}:{row_letter}:{n}") for n in nums[5:]],
    ]
    rows.append([InlineKeyboardButton("⬅️ К буквам", callback_data=f"sb:shot_pick:{game_id}")])
    return InlineKeyboardMarkup(rows)


async def _sb_send_setup(context: ContextTypes.DEFAULT_TYPE, g: SBGame, p: SBPlayerState):
    board = _sb_render_own_board(p)
    text = (
        "⚓ <b>Морской бой</b>\n\n"
        "Перед началом игры:\n"
        "• Бот автоматически расставит корабли.\n"
        "• Можно перерасставить сколько угодно раз.\n"
        "• Когда оба подтвердят — игра начнётся.\n\n"
        "<b>Правило времени:</b> на каждый ход даётся <b>3 минуты</b>. "
        "Если время вышло — поражение.\n\n"
        "<b>Твоё поле:</b>\n<pre>" + board + "</pre>"
    )
    await context.bot.send_message(
        chat_id=p.user_id,
        text=text,
        parse_mode=ParseMode.HTML,
        reply_markup=kb_sb_setup(g.game_id),
    )


def _sb_cancel_turn_job(context: ContextTypes.DEFAULT_TYPE, g: SBGame):
    if g.last_turn_job_name:
        jobs = context.job_queue.get_jobs_by_name(g.last_turn_job_name)
        for j in jobs:
            try:
                j.schedule_removal()
            except Exception:
                pass
        g.last_turn_job_name = None



def _sb_cancel_invite_job(context: ContextTypes.DEFAULT_TYPE, g: SBGame):
    if g.last_invite_job_name:
        jobs = context.job_queue.get_jobs_by_name(g.last_invite_job_name)
        for j in jobs:
            try:
                j.schedule_removal()
            except Exception:
                pass
        g.last_invite_job_name = None


async def _sb_invite_timeout_job(context: ContextTypes.DEFAULT_TYPE):
    data = getattr(context.job, "data", None) or {}
    game_id = data.get("game_id")
    games, user_map = _sb_store(context)
    g: SBGame | None = games.get(game_id)
    if not g or g.status != "inviting":
        return

    g.status = "finished"
    _sb_cancel_invite_job(context, g)

    try:
        await context.bot.send_message(
            chat_id=g.p1.user_id,
            text="⏰ Приглашение истекло (5 минут без ответа). Ты можешь пригласить другого коллегу.",
        )
    except Exception:
        pass
    try:
        await context.bot.send_message(
            chat_id=g.p2.user_id,
            text="⏰ Приглашение в «Морской бой» истекло (5 минут без ответа).",
        )
    except Exception:
        pass

    games.pop(game_id, None)
    user_map.pop(g.p1.user_id, None)
    user_map.pop(g.p2.user_id, None)


def _sb_schedule_invite_timer(context: ContextTypes.DEFAULT_TYPE, g: SBGame):
    _sb_cancel_invite_job(context, g)
    name = f"sb_invite:{g.game_id}"
    g.last_invite_job_name = name
    context.job_queue.run_once(
        _sb_invite_timeout_job,
        when=SB_INVITE_SECONDS,
        name=name,
        data={"game_id": g.game_id},
    )

async def _sb_turn_timeout_job(context: ContextTypes.DEFAULT_TYPE):
    data = getattr(context.job, "data", None) or {}
    game_id = data.get("game_id")

    games, user_map = _sb_store(context)
    g: SBGame | None = games.get(game_id)
    if not g or g.status != "playing" or not g.turn_user_id:
        return

    loser = g.turn_user_id
    winner = g.p1.user_id if loser == g.p2.user_id else g.p2.user_id

    g.status = "finished"
    _sb_cancel_turn_job(context, g)

    try:
        await context.bot.send_message(chat_id=winner, text="🏆 Победа! Соперник не успел сделать ход за 3 минуты.")
    except Exception:
        pass
    try:
        await context.bot.send_message(chat_id=loser, text="⏰ Поражение: ты не успел сделать ход за 3 минуты.")
    except Exception:
        pass

    # remove from memory (do not store results)
    games.pop(game_id, None)
    user_map.pop(g.p1.user_id, None)
    user_map.pop(g.p2.user_id, None)


def _sb_schedule_turn_timer(context: ContextTypes.DEFAULT_TYPE, g: SBGame):
    _sb_cancel_turn_job(context, g)
    name = f"sb_turn:{g.game_id}"
    g.last_turn_job_name = name
    context.job_queue.run_once(
        _sb_turn_timeout_job,
        when=SB_TURN_SECONDS,
        name=name,
        data={"game_id": g.game_id},
    )


async def _sb_send_turn_state(context: ContextTypes.DEFAULT_TYPE, g: SBGame):
    for p, enemy in [(g.p1, g.p2), (g.p2, g.p1)]:
        own = _sb_render_own_board(p)
        opp = _sb_render_enemy_board(enemy)
        turn_line = "🎯 <b>Твой ход</b>" if g.turn_user_id == p.user_id else "⏳ Ход соперника"
        text = (
            "⚓ <b>Морской бой</b>\n\n"
            f"{turn_line}\n"
            f"<b>Твоё поле:</b>\n<pre>{own}</pre>\n"
            f"<b>Поле соперника:</b>\n<pre>{opp}</pre>\n"
        )
        kb = kb_sb_pick_row(g.game_id) if g.turn_user_id == p.user_id else None
        await context.bot.send_message(chat_id=p.user_id, text=text, parse_mode=ParseMode.HTML, reply_markup=kb)


def _sb_target_defeated(target: SBPlayerState) -> bool:
    return target.ships.issubset(target.hits)


async def cb_seabattle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await deny_no_access(update, context):
        return

    # sync tg_user_id
    await sync_profile_user_id_from_update(update)

    q = update.callback_query
    data = (q.data or "")
    try:
        await q.answer()
    except Exception:
        pass

    games, user_map = _sb_store(context)
    user_id = update.effective_user.id if update.effective_user else None
    if not user_id:
        return

    # new game: pick opponent profile id
    if data.startswith("sb:new:pick:"):
        pid = int(data.split(":")[-1])
        opp = db_profiles_get(pid)
        if not opp or not opp.get("tg_user_id"):
            await q.answer("Этот коллега ещё не запускал бота (нужно нажать /start).", show_alert=True)
            return

        opp_user_id = int(opp["tg_user_id"])
        if opp_user_id == user_id:
            await q.answer("Нельзя пригласить самого себя 🙂", show_alert=True)
            return

        if user_map.get(user_id) or user_map.get(opp_user_id):
            await q.answer("Кто-то из вас уже в игре. Завершите текущую игру.", show_alert=True)
            return

        game_id = secrets.token_hex(4)
        p1 = SBPlayerState(user_id=user_id, full_name=(update.effective_user.full_name or "Игрок 1"))
        p2 = SBPlayerState(user_id=opp_user_id, full_name=(opp.get("full_name") or "Игрок 2"))
        g = SBGame(game_id=game_id, host_id=user_id, p1=p1, p2=p2, status="inviting")

        games[game_id] = g
        user_map[user_id] = game_id
        user_map[opp_user_id] = game_id


        _sb_schedule_invite_timer(context, g)

        await q.edit_message_text(
            f"✅ Приглашение отправлено: <b>{p2.full_name}</b>\n\n"
            "Ждём принятия приглашения (всё в личке).",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_sb_host_wait(game_id),
        )

        try:
            await context.bot.send_message(
                chat_id=opp_user_id,
                text=(
                    "⚓ Тебя пригласили сыграть в <b>«Морской бой»</b>\n\n"
                    f"👤 Противник: <b>{p1.full_name}</b>"
                ),
                parse_mode=ParseMode.HTML,
                reply_markup=kb_sb_invite(game_id),
            )
        except Exception:
            games.pop(game_id, None)
            user_map.pop(user_id, None)
            user_map.pop(opp_user_id, None)
            await q.answer("Не удалось отправить приглашение в личку.", show_alert=True)
        return

    # invite accept/decline
    if data.startswith("sb:invite_accept:") or data.startswith("sb:invite_decline:"):
        game_id = data.split(":")[-1]
        g: SBGame | None = games.get(game_id)
        if not g:
            await q.edit_message_text("Игра уже недоступна.")
            return

        if user_id != g.p2.user_id:
            await q.answer("Это приглашение не для тебя.", show_alert=True)
            return

        _sb_cancel_invite_job(context, g)

        if data.startswith("sb:invite_decline:"):
            try:
                await context.bot.send_message(chat_id=g.p1.user_id, text="❌ Коллега отказался от игры.")
            except Exception:
                pass
            await q.edit_message_text("Ок, приглашение отклонено.")
            games.pop(game_id, None)
            user_map.pop(g.p1.user_id, None)
            user_map.pop(g.p2.user_id, None)
            return

        # accept -> setup
        g.status = "setup"
        g.p1.ships = _sb_random_place_ships()
        g.p2.ships = _sb_random_place_ships()
        g.p1.setup_confirmed = False
        g.p2.setup_confirmed = False

        await q.edit_message_text("✅ Принято! Сейчас бот пришлёт настройку игры в личке.")
        await _sb_send_setup(context, g, g.p1)
        await _sb_send_setup(context, g, g.p2)
        return

    # setup reroll/confirm
    if data.startswith("sb:setup_reroll:") or data.startswith("sb:setup_confirm:"):
        game_id = data.split(":")[-1]
        g: SBGame | None = games.get(game_id)
        if not g or g.status != "setup":
            await q.answer("Сейчас нельзя.", show_alert=True)
            return

        p = g.p1 if user_id == g.p1.user_id else (g.p2 if user_id == g.p2.user_id else None)
        if not p:
            await q.answer("Не твоя игра.", show_alert=True)
            return

        if data.startswith("sb:setup_reroll:"):
            p.ships = _sb_random_place_ships()
            p.setup_confirmed = False
            board = _sb_render_own_board(p)
            await q.edit_message_text(
                "⚓ <b>Морской бой</b>\n\n"
                "<b>Твоё поле:</b>\n<pre>" + board + "</pre>",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_sb_setup(g.game_id),
            )
            return

        # confirm
        p.setup_confirmed = True
        await q.answer("Готов ✅")

        if g.p1.setup_confirmed and g.p2.setup_confirmed:
            g.status = "playing"
            g.turn_user_id = random.choice([g.p1.user_id, g.p2.user_id])
            _sb_schedule_turn_timer(context, g)

            intro = (
                "⚓ <b>Морской бой начинается!</b>\n\n"
                "Коротко о процессе:\n"
                "• Ходы по очереди, выбираете клетку кнопками.\n"
                "• 🔥 — попадание, ❌ — промах.\n"
                "• Если попал — ход продолжается.\n"
                "• Победа — уничтожить весь флот.\n\n"
                "<b>Важно:</b> на каждый ход даётся <b>3 минуты</b>. Не успел — поражение.\n"
            )
            for uid in [g.p1.user_id, g.p2.user_id]:
                try:
                    await context.bot.send_message(chat_id=uid, text=intro, parse_mode=ParseMode.HTML)
                except Exception:
                    pass

            await _sb_send_turn_state(context, g)
            return

        await q.edit_message_text("✅ Поле подтверждено. Ждём, когда соперник подтвердит своё поле.")
        return

    # pick row for shot
    if data.startswith("sb:shot_pick:"):
        game_id = data.split(":")[-1]
        g = games.get(game_id)
        if not g or g.status != "playing":
            await q.answer("Игра недоступна.", show_alert=True)
            return
        if g.turn_user_id != user_id:
            await q.answer("Сейчас ход соперника.", show_alert=True)
            return
        await q.edit_message_reply_markup(reply_markup=kb_sb_pick_row(game_id))
        return

    if data.startswith("sb:shot_row:"):
        _, _, game_id, row_letter = data.split(":")
        g = games.get(game_id)
        if not g or g.status != "playing":
            await q.answer("Игра недоступна.", show_alert=True)
            return
        if g.turn_user_id != user_id:
            await q.answer("Сейчас ход соперника.", show_alert=True)
            return
        await q.edit_message_reply_markup(reply_markup=kb_sb_pick_cell(game_id, row_letter))
        return

    if data.startswith("sb:shot_cell:"):
        _, _, game_id, row_letter, n_str = data.split(":")
        g: SBGame | None = games.get(game_id)
        if not g or g.status != "playing":
            await q.answer("Игра недоступна.", show_alert=True)
            return
        if g.turn_user_id != user_id:
            await q.answer("Сейчас ход соперника.", show_alert=True)
            return

        row = ord(row_letter) - ord("A")
        col = int(n_str) - 1
        cell = (row, col)

        shooter = g.p1 if user_id == g.p1.user_id else g.p2
        target = g.p2 if shooter is g.p1 else g.p1

        if cell in target.hits or cell in target.misses_by_enemy:
            await q.answer("Ты уже стрелял сюда.", show_alert=True)
            return

        hit = cell in target.ships
        if hit:
            target.hits.add(cell)
            await q.answer("🔥 Попадание!")
        else:
            target.misses_by_enemy.add(cell)
            await q.answer("❌ Мимо")
            g.turn_user_id = target.user_id

        _sb_schedule_turn_timer(context, g)

        if _sb_target_defeated(target):
            g.status = "finished"
            _sb_cancel_turn_job(context, g)
            try:
                await context.bot.send_message(chat_id=shooter.user_id, text="🏆 Победа! Ты уничтожил весь флот соперника.")
            except Exception:
                pass
            try:
                await context.bot.send_message(chat_id=target.user_id, text="☠️ Поражение. Твой флот уничтожен.")
            except Exception:
                pass
            games.pop(game_id, None)
            user_map.pop(g.p1.user_id, None)
            user_map.pop(g.p2.user_id, None)
            return

        await _sb_send_turn_state(context, g)
        return

    if data.startswith("sb:back_to_game:"):
        game_id = data.split(":")[-1]
        g: SBGame | None = games.get(game_id)
        if not g:
            await q.answer("Игра недоступна.", show_alert=True)
            return
        kb = kb_sb_pick_row(game_id) if (g.status == "playing" and g.turn_user_id == user_id) else None
        await q.edit_message_reply_markup(reply_markup=kb)
        return


# ---------------- APP ----------------

def main():
    ensure_db_path(DB_PATH)
    ensure_storage_dir(STORAGE_DIR)
    db_init()

    request = HTTPXRequest(connect_timeout=15, read_timeout=30, write_timeout=30, pool_timeout=30)

    app = Application.builder().token(BOT_TOKEN).request(request).build()

    # log errors
    app.add_error_handler(error_handler)

    # commands
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("horo", cmd_horo))
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
    app.add_handler(CallbackQueryHandler(cb_horo, pattern=r"^horo:"))
    app.add_handler(CallbackQueryHandler(cb_cancel_close, pattern=r"^cancel:close:(standup|industry)$"))
    app.add_handler(CallbackQueryHandler(cb_cancel_reason, pattern=r"^cancel:reason:(standup|industry):(no_topics|tech|move)$"))
    app.add_handler(CallbackQueryHandler(cb_reschedule_pick, pattern=r"^reschedule:pick:(standup|industry):\d{2}\.\d{2}\.\d{2}$"))
    app.add_handler(CallbackQueryHandler(cb_reschedule_manual, pattern=r"^reschedule:manual:(standup|industry)$"))
    app.add_handler(CallbackQueryHandler(cb_cancel_manual_input, pattern=r"^reschedule:cancel_manual:(standup|industry)$"))

    # callbacks: testing
    app.add_handler(CallbackQueryHandler(cb_test, pattern=r"^test:"))

    app.add_handler(CallbackQueryHandler(cb_seabattle, pattern=r"^sb:"))

# callbacks: help
    app.add_handler(CallbackQueryHandler(cb_help, pattern=r"^(help:|noop)"))

    # new members welcome
    app.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, on_new_members))

    # meme channel collector
    app.add_handler(MessageHandler(
        filters.Chat(MEME_CHANNEL_ID) & (filters.PHOTO | filters.VIDEO | filters.Document.ALL),
        on_meme_channel_post
    ))

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
    try:
        app.run_polling(allowed_updates=Update.ALL_TYPES)
    except Exception as e:
        logger.exception("run_polling crashed: %s", e)
        raise

if __name__ == "__main__":
    main()
