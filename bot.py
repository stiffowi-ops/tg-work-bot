# -*- coding: utf-8 -*-
import os
import re
import random
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
BUILD_VERSION = "CASES-CATALOG-2026-07-23-V2"

BOT_TOKEN = os.getenv("BOT_TOKEN")
ZOOM_URL = os.getenv("ZOOM_URL")  # планёрка
INDUSTRY_ZOOM_URL = os.getenv("INDUSTRY_ZOOM_URL")  # отраслевая

# ✅ поддержка DATABASE_PATH и DB_PATH
DB_PATH = os.getenv("DATABASE_PATH") or os.getenv("DB_PATH", "bot.db")

STORAGE_DIR = os.getenv("STORAGE_DIR", "storage")


# -------- ACCESS CONTROL --------
ACCESS_CHAT_ID = -1003399576556

NO_ACCESS_TEXT = (
    "🕵️♂️ Еще никогда Штирлиц не был так близок к провалу!\n\n"
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

# Уведомления о встречах и изменениях расписания живут в чате 10 минут.
MEETING_MESSAGE_TTL_SECONDS = 10 * 60

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


def schedule_message_delete(
    context: ContextTypes.DEFAULT_TYPE,
    message,
    delay_seconds: int = MEETING_MESSAGE_TTL_SECONDS,
):
    """Ставит отправленное ботом сообщение в очередь на автоудаление."""
    if not message or not context.job_queue:
        return

    context.job_queue.run_once(
        job_delete_message,
        when=delay_seconds,
        data={
            "chat_id": message.chat_id,
            "message_id": message.message_id,
        },
        name=f"delete:{message.chat_id}:{message.message_id}",
    )

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
            reschedule_time TEXT,
            PRIMARY KEY (meeting_type, meeting_date)
        )
    """)

    # переносы встреч
    cur.execute("""
        CREATE TABLE IF NOT EXISTS meeting_reschedules (
            meeting_type TEXT NOT NULL,
            original_date TEXT NOT NULL,
            new_date TEXT NOT NULL,
            new_time TEXT,
            created_at TEXT NOT NULL,
            sent INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (meeting_type, original_date)
        )
    """)

    # Миграции времени перенесённых регулярных встреч для старых баз.
    try:
        cur.execute("ALTER TABLE meeting_state ADD COLUMN reschedule_time TEXT")
    except sqlite3.OperationalError:
        pass
    try:
        cur.execute("ALTER TABLE meeting_reschedules ADD COLUMN new_time TEXT")
    except sqlite3.OperationalError:
        pass

    # Старые переносы продолжают работать в прежнее стандартное время.
    cur.execute(
        """UPDATE meeting_reschedules
           SET new_time=CASE
               WHEN meeting_type='standup' THEN '09:15'
               WHEN meeting_type='industry' THEN '11:30'
               ELSE '09:15'
           END
           WHERE new_time IS NULL OR new_time=''"""
    )
    cur.execute(
        """UPDATE meeting_state
           SET reschedule_time=CASE
               WHEN meeting_type='standup' THEN '09:15'
               WHEN meeting_type='industry' THEN '11:30'
               ELSE '09:15'
           END
           WHERE reschedule_date IS NOT NULL
             AND reschedule_date<>''
             AND (reschedule_time IS NULL OR reschedule_time='')"""
    )

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

    # Дата последнего изменения карточки или файла.
    try:
        cur.execute("ALTER TABLE docs ADD COLUMN updated_at TEXT")
    except sqlite3.OperationalError:
        pass
    cur.execute("UPDATE docs SET updated_at=uploaded_at WHERE updated_at IS NULL OR updated_at='' ")

    # ------- DOCUMENTS: теги, избранное, история и подборки -------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS doc_tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL UNIQUE COLLATE NOCASE,
            created_at TEXT NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS doc_tag_links (
            doc_id INTEGER NOT NULL,
            tag_id INTEGER NOT NULL,
            PRIMARY KEY (doc_id, tag_id),
            FOREIGN KEY(doc_id) REFERENCES docs(id) ON DELETE CASCADE,
            FOREIGN KEY(tag_id) REFERENCES doc_tags(id) ON DELETE CASCADE
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS doc_favorites (
            user_id INTEGER NOT NULL,
            doc_id INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (user_id, doc_id),
            FOREIGN KEY(doc_id) REFERENCES docs(id) ON DELETE CASCADE
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS doc_views (
            user_id INTEGER NOT NULL,
            doc_id INTEGER NOT NULL,
            last_viewed_at TEXT NOT NULL,
            view_count INTEGER NOT NULL DEFAULT 1,
            PRIMARY KEY (user_id, doc_id),
            FOREIGN KEY(doc_id) REFERENCES docs(id) ON DELETE CASCADE
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS doc_collections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL UNIQUE COLLATE NOCASE,
            description TEXT,
            created_at TEXT NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS doc_collection_items (
            collection_id INTEGER NOT NULL,
            doc_id INTEGER NOT NULL,
            position INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (collection_id, doc_id),
            FOREIGN KEY(collection_id) REFERENCES doc_collections(id) ON DELETE CASCADE,
            FOREIGN KEY(doc_id) REFERENCES docs(id) ON DELETE CASCADE
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_docs_uploaded_at ON docs(uploaded_at DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_doc_views_user_time ON doc_views(user_id, last_viewed_at DESC)")

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
            photo_file_id TEXT,
            is_active INTEGER NOT NULL DEFAULT 1,
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

    # Фото сотрудника хранится как Telegram file_id.
    try:
        cur.execute("ALTER TABLE profiles ADD COLUMN photo_file_id TEXT")
    except sqlite3.OperationalError:
        pass

    # Мягкое удаление карточки: при выходе сотрудника из рабочего чата
    # карточка скрывается, но связанные тесты, ачивки и история сохраняются.
    try:
        cur.execute("ALTER TABLE profiles ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1")
    except sqlite3.OperationalError:
        pass
    cur.execute("UPDATE profiles SET is_active=1 WHERE is_active IS NULL")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_profiles_active_name ON profiles(is_active, full_name)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_profiles_tg_user_id ON profiles(tg_user_id)")


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
        achievement_key TEXT,
        level INTEGER NOT NULL DEFAULT 1,
        FOREIGN KEY(profile_id) REFERENCES profiles(id) ON DELETE CASCADE
    )
""")

    # Миграции для старых БД: ключ и уровень ачивки.
    try:
        cur.execute("ALTER TABLE achievement_awards ADD COLUMN achievement_key TEXT")
    except sqlite3.OperationalError:
        pass
    try:
        cur.execute("ALTER TABLE achievement_awards ADD COLUMN level INTEGER NOT NULL DEFAULT 1")
    except sqlite3.OperationalError:
        pass

    # ------- ACHIEVEMENT NOMINATIONS: номинации от коллег -------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS achievement_nominations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scope_chat_id INTEGER NOT NULL,
            nominator_user_id INTEGER,
            nominator_profile_id INTEGER,
            nominee_profile_id INTEGER NOT NULL,
            category_key TEXT NOT NULL DEFAULT 'team_help',
            reason TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at TEXT NOT NULL,
            reviewed_at TEXT,
            reviewed_by INTEGER,
            award_id INTEGER,
            FOREIGN KEY(nominator_profile_id) REFERENCES profiles(id) ON DELETE SET NULL,
            FOREIGN KEY(nominee_profile_id) REFERENCES profiles(id) ON DELETE CASCADE,
            FOREIGN KEY(award_id) REFERENCES achievement_awards(id) ON DELETE SET NULL
        )
    """)
    # Категория номинации для старых баз.
    try:
        cur.execute("ALTER TABLE achievement_nominations ADD COLUMN category_key TEXT NOT NULL DEFAULT 'team_help'")
    except sqlite3.OperationalError:
        pass

    # ------- NOTIFICATIONS: внутренний центр уведомлений -------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            notification_type TEXT NOT NULL,
            title TEXT NOT NULL,
            body TEXT,
            callback_data TEXT,
            is_read INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_notifications_user_read ON notifications(user_id, is_read, id DESC)")

    # ------- ACHIEVEMENT REACTIONS: реакции на публичные благодарности -------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS achievement_reactions (
            award_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            reaction TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (award_id, user_id),
            FOREIGN KEY(award_id) REFERENCES achievement_awards(id) ON DELETE CASCADE
        )
    """)

    # ------- COMMUNICATIONS: saved broadcast tags -------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS broadcast_tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL
        )
    """)

    # ------- COMMUNICATIONS: durable scheduled sends -------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS scheduled_communications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            kind TEXT NOT NULL,                    -- meeting|broadcast
            payload_json TEXT NOT NULL,
            send_at_utc TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending', -- pending|sending|sent|failed
            created_by INTEGER,
            created_at TEXT NOT NULL,
            sent_at TEXT,
            result_json TEXT,
            last_error TEXT
        )
    """)
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_scheduled_communications_due "
        "ON scheduled_communications(status, send_at_utc)"
    )
    # If the process stopped after reserving a task, retry it after restart.
    cur.execute("UPDATE scheduled_communications SET status='pending' WHERE status='sending'")

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


def normalize_broadcast_tag_name(value: str) -> str:
    """Returns a Telegram-friendly hashtag name without the leading #."""
    clean = (value or "").strip().lstrip("#")
    clean = re.sub(r"[^0-9A-Za-zА-Яа-яЁё_]+", "_", clean)
    clean = re.sub(r"_+", "_", clean).strip("_")
    return clean[:60]


def db_broadcast_tags_list() -> list[dict]:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT id, name FROM broadcast_tags ORDER BY name COLLATE NOCASE ASC")
    rows = cur.fetchall()
    con.close()
    return [{"id": int(r[0]), "name": r[1]} for r in rows]


def db_broadcast_tag_get(tag_id: int) -> dict | None:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT id, name FROM broadcast_tags WHERE id=?", (int(tag_id),))
    row = cur.fetchone()
    con.close()
    return {"id": int(row[0]), "name": row[1]} if row else None


def db_broadcast_tag_add(name: str) -> dict | None:
    clean = normalize_broadcast_tag_name(name)
    if not clean:
        return None
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        "INSERT INTO broadcast_tags(name, created_at) VALUES(?, ?) "
        "ON CONFLICT(name) DO NOTHING",
        (clean, datetime.utcnow().isoformat()),
    )
    con.commit()
    cur.execute("SELECT id, name FROM broadcast_tags WHERE name=?", (clean,))
    row = cur.fetchone()
    con.close()
    return {"id": int(row[0]), "name": row[1]} if row else None


def db_broadcast_tag_delete(tag_id: int) -> bool:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("DELETE FROM broadcast_tags WHERE id=?", (int(tag_id),))
    ok = cur.rowcount > 0
    con.commit()
    con.close()
    return ok


def db_profiles_list_for_delivery() -> list[dict]:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        "SELECT id, full_name, tg_user_id FROM profiles "
        "WHERE COALESCE(is_active, 1)=1 "
        "ORDER BY full_name COLLATE NOCASE ASC"
    )
    rows = cur.fetchall()
    con.close()
    return [
        {"id": int(r[0]), "full_name": r[1], "tg_user_id": r[2]}
        for r in rows
    ]


def db_scheduled_communication_add(
    kind: str,
    payload: dict,
    send_at_utc: str,
    created_by: int | None,
) -> int:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO scheduled_communications(
            kind, payload_json, send_at_utc, status, created_by, created_at
        ) VALUES(?, ?, ?, 'pending', ?, ?)
        """,
        (
            kind,
            json.dumps(payload, ensure_ascii=False),
            send_at_utc,
            int(created_by) if created_by else None,
            datetime.utcnow().isoformat(),
        ),
    )
    con.commit()
    item_id = int(cur.lastrowid)
    con.close()
    return item_id


def db_scheduled_communications_due(limit: int = 20) -> list[dict]:
    now_utc = datetime.utcnow().isoformat()
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        SELECT id, kind, payload_json, send_at_utc
        FROM scheduled_communications
        WHERE status='pending' AND send_at_utc<=?
        ORDER BY send_at_utc ASC, id ASC
        LIMIT ?
        """,
        (now_utc, int(limit)),
    )
    rows = cur.fetchall()
    con.close()
    return [
        {
            "id": int(r[0]),
            "kind": r[1],
            "payload_json": r[2],
            "send_at_utc": r[3],
        }
        for r in rows
    ]


def db_scheduled_communication_reserve(item_id: int) -> bool:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        "UPDATE scheduled_communications SET status='sending' "
        "WHERE id=? AND status='pending'",
        (int(item_id),),
    )
    ok = cur.rowcount > 0
    con.commit()
    con.close()
    return ok


def db_scheduled_communication_finish(
    item_id: int,
    status: str,
    result: dict | None = None,
    error: str | None = None,
):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        UPDATE scheduled_communications
        SET status=?, sent_at=?, result_json=?, last_error=?
        WHERE id=?
        """,
        (
            status,
            datetime.utcnow().isoformat(),
            json.dumps(result or {}, ensure_ascii=False),
            (error or None),
            int(item_id),
        ),
    )
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
        "SELECT canceled, reason, reschedule_date, reschedule_time "
        "FROM meeting_state WHERE meeting_type=? AND meeting_date=?",
        (meeting_type, d.isoformat()),
    )
    row = cur.fetchone()
    con.close()
    if not row:
        return {
            "canceled": 0,
            "reason": None,
            "reschedule_date": None,
            "reschedule_time": None,
        }
    return {
        "canceled": row[0],
        "reason": row[1],
        "reschedule_date": row[2],
        "reschedule_time": row[3],
    }


def db_set_canceled(
    meeting_type: str,
    d: date,
    reason: str,
    reschedule_date: str | None = None,
    reschedule_time: str | None = None,
):
    if reschedule_date:
        reschedule_time = (
            parse_regular_meeting_time(reschedule_time)
            or regular_meeting_default_time(meeting_type)
        )
    else:
        reschedule_time = None

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        INSERT INTO meeting_state (
            meeting_type, meeting_date, canceled, reason,
            reschedule_date, reschedule_time
        )
        VALUES (?, ?, 1, ?, ?, ?)
        ON CONFLICT(meeting_type, meeting_date) DO UPDATE SET
            canceled=1,
            reason=excluded.reason,
            reschedule_date=excluded.reschedule_date,
            reschedule_time=excluded.reschedule_time
    """, (
        meeting_type, d.isoformat(), reason,
        reschedule_date, reschedule_time,
    ))
    con.commit()
    con.close()


def db_upsert_reschedule(
    meeting_type: str,
    original_d: date,
    new_d: date,
    new_time: str | None = None,
):
    clean_time = (
        parse_regular_meeting_time(new_time)
        or regular_meeting_default_time(meeting_type)
    )
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        INSERT INTO meeting_reschedules(
            meeting_type, original_date, new_date, new_time, created_at, sent
        )
        VALUES (?, ?, ?, ?, ?, 0)
        ON CONFLICT(meeting_type, original_date) DO UPDATE SET
            new_date=excluded.new_date,
            new_time=excluded.new_time,
            created_at=excluded.created_at,
            sent=0
    """, (
        meeting_type, original_d.isoformat(), new_d.isoformat(),
        clean_time, datetime.utcnow().isoformat(),
    ))
    con.commit()
    con.close()


def db_delete_reschedule(meeting_type: str, original_d: date) -> bool:
    """Удаляет ранее созданный перенос регулярной встречи."""
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        "DELETE FROM meeting_reschedules WHERE meeting_type=? AND original_date=?",
        (meeting_type, original_d.isoformat()),
    )
    ok = cur.rowcount > 0
    con.commit()
    con.close()
    return ok


def db_get_due_reschedules(
    meeting_type: str,
    target_day: date,
    as_of_time: str | None = None,
) -> list[str]:
    """
    Возвращает ожидающие переносы на указанную дату.

    Если передано as_of_time в формате ЧЧ:ММ, выбираются только переносы,
    время уведомления которых уже наступило. Это позволяет безопасно
    догнать уведомление после краткого перезапуска бота.
    """
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    if as_of_time is None:
        cur.execute("""
            SELECT original_date
            FROM meeting_reschedules
            WHERE meeting_type=? AND sent=0 AND new_date=?
            ORDER BY COALESCE(new_time, ''), original_date ASC
        """, (meeting_type, target_day.isoformat()))
    else:
        clean_time = parse_regular_meeting_time(as_of_time) or "00:00"
        default_time = regular_meeting_default_time(meeting_type)
        cur.execute("""
            SELECT original_date
            FROM meeting_reschedules
            WHERE meeting_type=?
              AND sent=0
              AND new_date=?
              AND COALESCE(NULLIF(new_time, ''), ?)<=?
            ORDER BY COALESCE(NULLIF(new_time, ''), ?), original_date ASC
        """, (
            meeting_type, target_day.isoformat(), default_time, clean_time,
            default_time,
        ))
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

def db_docs_get_category(category_id: int) -> dict | None:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT id, title FROM doc_categories WHERE id=?", (int(category_id),))
    row = cur.fetchone()
    con.close()
    if not row:
        return None
    return {"id": int(row[0]), "title": row[1]}


def db_docs_rename_category(category_id: int, new_title: str) -> bool:
    title = (new_title or "").strip()
    if len(title) < 2:
        return False

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("UPDATE doc_categories SET title=? WHERE id=?", (title, int(category_id)))
    ok = cur.rowcount > 0
    con.commit()
    con.close()
    return ok

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
    did = int(doc_id)
    cur.execute("DELETE FROM doc_tag_links WHERE doc_id=?", (did,))
    cur.execute("DELETE FROM doc_favorites WHERE doc_id=?", (did,))
    cur.execute("DELETE FROM doc_views WHERE doc_id=?", (did,))
    cur.execute("DELETE FROM doc_collection_items WHERE doc_id=?", (did,))
    cur.execute("DELETE FROM docs WHERE id=?", (did,))
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

def db_profiles_upsert(
    full_name: str,
    year_start: int,
    city: str,
    birthday: str | None,
    about: str,
    topics: str,
    tg_link: str,
    photo_file_id: str | None = None,
) -> int:
    """Upsert анкеты по tg_link (если есть), иначе по full_name. Фото сохраняем, если оно передано."""
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
               SET full_name=?, year_start=?, city=?, birthday=?, about=?, topics=?, tg_link=?,
                   photo_file_id=COALESCE(?, photo_file_id),
                   is_active=1
               WHERE id=?""",
            (
                full_name.strip(), int(year_start), city.strip(), birthday, about.strip(),
                topics.strip(), (tg_link or "").strip(), photo_file_id, pid,
            ),
        )
        con.commit()
        con.close()
        return pid

    cur.execute(
        """INSERT INTO profiles(
               full_name, year_start, city, birthday, about, topics, tg_link, photo_file_id, created_at
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            full_name.strip(), int(year_start), city.strip(), birthday, about.strip(),
            topics.strip(), (tg_link or "").strip(), photo_file_id, datetime.utcnow().isoformat(),
        ),
    )
    con.commit()
    pid = cur.lastrowid
    con.close()
    return int(pid)




# ---------------- DOCUMENTS: KNOWLEDGE BASE DB ----------------

def _db_doc_rows_to_dicts(rows) -> list[dict]:
    return [
        {
            "id": int(r[0]),
            "category_id": int(r[1]),
            "title": r[2],
            "description": r[3] or "",
            "file_id": r[4],
            "file_unique_id": r[5],
            "mime": r[6],
            "local_path": r[7],
            "uploaded_at": r[8],
            "updated_at": r[9] or r[8],
            "category_title": r[10],
        }
        for r in rows
    ]


def db_docs_list_all(limit: int = 100) -> list[dict]:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        SELECT d.id, d.category_id, d.title, d.description, d.file_id,
               d.file_unique_id, d.mime_type, d.local_path, d.uploaded_at,
               COALESCE(d.updated_at, d.uploaded_at), c.title
        FROM docs d
        JOIN doc_categories c ON c.id=d.category_id
        ORDER BY d.title COLLATE NOCASE ASC
        LIMIT ?
        """,
        (int(limit),),
    )
    rows = cur.fetchall()
    con.close()
    return _db_doc_rows_to_dicts(rows)


def db_docs_search(query: str, limit: int = 40) -> list[dict]:
    q = (query or "").strip()
    if not q:
        return []
    like = f"%{q}%"
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        SELECT DISTINCT d.id, d.category_id, d.title, d.description, d.file_id,
               d.file_unique_id, d.mime_type, d.local_path, d.uploaded_at,
               COALESCE(d.updated_at, d.uploaded_at), c.title
        FROM docs d
        JOIN doc_categories c ON c.id=d.category_id
        LEFT JOIN doc_tag_links l ON l.doc_id=d.id
        LEFT JOIN doc_tags t ON t.id=l.tag_id
        WHERE d.title LIKE ? COLLATE NOCASE
           OR COALESCE(d.description, '') LIKE ? COLLATE NOCASE
           OR c.title LIKE ? COLLATE NOCASE
           OR COALESCE(t.title, '') LIKE ? COLLATE NOCASE
        ORDER BY d.title COLLATE NOCASE ASC
        LIMIT ?
        """,
        (like, like, like, like, int(limit)),
    )
    rows = cur.fetchall()
    con.close()
    return _db_doc_rows_to_dicts(rows)


def db_docs_new(days: int = 30, limit: int = 40) -> list[dict]:
    threshold = (datetime.utcnow() - timedelta(days=max(1, int(days)))).isoformat()
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        SELECT d.id, d.category_id, d.title, d.description, d.file_id,
               d.file_unique_id, d.mime_type, d.local_path, d.uploaded_at,
               COALESCE(d.updated_at, d.uploaded_at), c.title
        FROM docs d
        JOIN doc_categories c ON c.id=d.category_id
        WHERE d.uploaded_at>=?
        ORDER BY d.uploaded_at DESC, d.id DESC
        LIMIT ?
        """,
        (threshold, int(limit)),
    )
    rows = cur.fetchall()
    con.close()
    return _db_doc_rows_to_dicts(rows)


def db_doc_record_view(user_id: int | None, doc_id: int):
    if not user_id:
        return
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO doc_views(user_id, doc_id, last_viewed_at, view_count)
        VALUES(?, ?, ?, 1)
        ON CONFLICT(user_id, doc_id) DO UPDATE SET
            last_viewed_at=excluded.last_viewed_at,
            view_count=doc_views.view_count+1
        """,
        (int(user_id), int(doc_id), datetime.utcnow().isoformat()),
    )
    con.commit()
    con.close()


def db_docs_recent(user_id: int | None, limit: int = 40) -> list[dict]:
    if not user_id:
        return []
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        SELECT d.id, d.category_id, d.title, d.description, d.file_id,
               d.file_unique_id, d.mime_type, d.local_path, d.uploaded_at,
               COALESCE(d.updated_at, d.uploaded_at), c.title
        FROM doc_views v
        JOIN docs d ON d.id=v.doc_id
        JOIN doc_categories c ON c.id=d.category_id
        WHERE v.user_id=?
        ORDER BY v.last_viewed_at DESC
        LIMIT ?
        """,
        (int(user_id), int(limit)),
    )
    rows = cur.fetchall()
    con.close()
    return _db_doc_rows_to_dicts(rows)


def db_doc_is_favorite(user_id: int | None, doc_id: int) -> bool:
    if not user_id:
        return False
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT 1 FROM doc_favorites WHERE user_id=? AND doc_id=?", (int(user_id), int(doc_id)))
    result = cur.fetchone() is not None
    con.close()
    return result


def db_doc_toggle_favorite(user_id: int | None, doc_id: int) -> bool:
    if not user_id:
        return False
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT 1 FROM doc_favorites WHERE user_id=? AND doc_id=?", (int(user_id), int(doc_id)))
    if cur.fetchone():
        cur.execute("DELETE FROM doc_favorites WHERE user_id=? AND doc_id=?", (int(user_id), int(doc_id)))
        enabled = False
    else:
        cur.execute(
            "INSERT INTO doc_favorites(user_id, doc_id, created_at) VALUES(?, ?, ?)",
            (int(user_id), int(doc_id), datetime.utcnow().isoformat()),
        )
        enabled = True
    con.commit()
    con.close()
    return enabled


def db_docs_favorites(user_id: int | None, limit: int = 40) -> list[dict]:
    if not user_id:
        return []
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        SELECT d.id, d.category_id, d.title, d.description, d.file_id,
               d.file_unique_id, d.mime_type, d.local_path, d.uploaded_at,
               COALESCE(d.updated_at, d.uploaded_at), c.title
        FROM doc_favorites f
        JOIN docs d ON d.id=f.doc_id
        JOIN doc_categories c ON c.id=d.category_id
        WHERE f.user_id=?
        ORDER BY f.created_at DESC
        LIMIT ?
        """,
        (int(user_id), int(limit)),
    )
    rows = cur.fetchall()
    con.close()
    return _db_doc_rows_to_dicts(rows)


def db_doc_tags_list() -> list[dict]:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        SELECT t.id, t.title, COUNT(l.doc_id)
        FROM doc_tags t
        LEFT JOIN doc_tag_links l ON l.tag_id=t.id
        GROUP BY t.id, t.title
        ORDER BY t.title COLLATE NOCASE ASC
        """
    )
    rows = cur.fetchall()
    con.close()
    return [{"id": int(r[0]), "title": r[1], "count": int(r[2] or 0)} for r in rows]


def db_doc_tag_add(title: str) -> int:
    clean = re.sub(r"\s+", " ", (title or "").strip()).lstrip("#")
    if len(clean) < 2:
        raise ValueError("Название тега слишком короткое")
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        "INSERT INTO doc_tags(title, created_at) VALUES(?, ?)",
        (clean[:50], datetime.utcnow().isoformat()),
    )
    con.commit()
    tag_id = int(cur.lastrowid)
    con.close()
    return tag_id


def db_doc_tag_delete(tag_id: int) -> bool:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("DELETE FROM doc_tag_links WHERE tag_id=?", (int(tag_id),))
    cur.execute("DELETE FROM doc_tags WHERE id=?", (int(tag_id),))
    ok = cur.rowcount > 0
    con.commit()
    con.close()
    return ok


def db_doc_get_tags(doc_id: int) -> list[dict]:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        SELECT t.id, t.title
        FROM doc_tag_links l
        JOIN doc_tags t ON t.id=l.tag_id
        WHERE l.doc_id=?
        ORDER BY t.title COLLATE NOCASE ASC
        """,
        (int(doc_id),),
    )
    rows = cur.fetchall()
    con.close()
    return [{"id": int(r[0]), "title": r[1]} for r in rows]


def db_doc_toggle_tag(doc_id: int, tag_id: int) -> bool:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT 1 FROM doc_tag_links WHERE doc_id=? AND tag_id=?", (int(doc_id), int(tag_id)))
    if cur.fetchone():
        cur.execute("DELETE FROM doc_tag_links WHERE doc_id=? AND tag_id=?", (int(doc_id), int(tag_id)))
        enabled = False
    else:
        cur.execute("INSERT OR IGNORE INTO doc_tag_links(doc_id, tag_id) VALUES(?, ?)", (int(doc_id), int(tag_id)))
        enabled = True
    cur.execute("UPDATE docs SET updated_at=? WHERE id=?", (datetime.utcnow().isoformat(), int(doc_id)))
    con.commit()
    con.close()
    return enabled


def db_doc_update_title(doc_id: int, title: str) -> bool:
    clean = re.sub(r"\s+", " ", (title or "").strip())[:120]
    if len(clean) < 2:
        return False
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("UPDATE docs SET title=?, updated_at=? WHERE id=?", (clean, datetime.utcnow().isoformat(), int(doc_id)))
    ok = cur.rowcount > 0
    con.commit()
    con.close()
    return ok


def db_doc_update_description(doc_id: int, description: str | None) -> bool:
    clean = (description or "").strip()[:1200] or None
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("UPDATE docs SET description=?, updated_at=? WHERE id=?", (clean, datetime.utcnow().isoformat(), int(doc_id)))
    ok = cur.rowcount > 0
    con.commit()
    con.close()
    return ok


def db_doc_update_category(doc_id: int, category_id: int) -> bool:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        "UPDATE docs SET category_id=?, updated_at=? WHERE id=?",
        (int(category_id), datetime.utcnow().isoformat(), int(doc_id)),
    )
    ok = cur.rowcount > 0
    con.commit()
    con.close()
    return ok


def db_doc_replace_file(
    doc_id: int,
    file_id: str,
    file_unique_id: str | None,
    mime_type: str | None,
    local_path: str | None,
) -> bool:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        UPDATE docs
        SET file_id=?, file_unique_id=?, mime_type=?, local_path=?, updated_at=?
        WHERE id=?
        """,
        (
            file_id,
            file_unique_id,
            mime_type,
            local_path,
            datetime.utcnow().isoformat(),
            int(doc_id),
        ),
    )
    ok = cur.rowcount > 0
    con.commit()
    con.close()
    return ok


def db_doc_collections_list() -> list[dict]:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        SELECT c.id, c.title, c.description, COUNT(i.doc_id)
        FROM doc_collections c
        LEFT JOIN doc_collection_items i ON i.collection_id=c.id
        GROUP BY c.id, c.title, c.description
        ORDER BY c.title COLLATE NOCASE ASC
        """
    )
    rows = cur.fetchall()
    con.close()
    return [
        {"id": int(r[0]), "title": r[1], "description": r[2] or "", "count": int(r[3] or 0)}
        for r in rows
    ]


def db_doc_collection_get(collection_id: int) -> dict | None:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT id, title, description FROM doc_collections WHERE id=?", (int(collection_id),))
    row = cur.fetchone()
    con.close()
    if not row:
        return None
    return {"id": int(row[0]), "title": row[1], "description": row[2] or ""}


def db_doc_collection_add(title: str, description: str | None = None) -> int:
    clean = re.sub(r"\s+", " ", (title or "").strip())
    if len(clean) < 2:
        raise ValueError("Название подборки слишком короткое")
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        "INSERT INTO doc_collections(title, description, created_at) VALUES(?, ?, ?)",
        (clean[:80], (description or "").strip()[:500] or None, datetime.utcnow().isoformat()),
    )
    con.commit()
    collection_id = int(cur.lastrowid)
    con.close()
    return collection_id


def db_doc_collection_delete(collection_id: int) -> bool:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("DELETE FROM doc_collection_items WHERE collection_id=?", (int(collection_id),))
    cur.execute("DELETE FROM doc_collections WHERE id=?", (int(collection_id),))
    ok = cur.rowcount > 0
    con.commit()
    con.close()
    return ok


def db_doc_collection_items(collection_id: int) -> list[dict]:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        SELECT d.id, d.category_id, d.title, d.description, d.file_id,
               d.file_unique_id, d.mime_type, d.local_path, d.uploaded_at,
               COALESCE(d.updated_at, d.uploaded_at), c.title
        FROM doc_collection_items i
        JOIN docs d ON d.id=i.doc_id
        JOIN doc_categories c ON c.id=d.category_id
        WHERE i.collection_id=?
        ORDER BY i.position ASC, d.title COLLATE NOCASE ASC
        """,
        (int(collection_id),),
    )
    rows = cur.fetchall()
    con.close()
    return _db_doc_rows_to_dicts(rows)


def db_doc_collection_add_item(collection_id: int, doc_id: int) -> bool:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT COALESCE(MAX(position), -1)+1 FROM doc_collection_items WHERE collection_id=?", (int(collection_id),))
    pos = int((cur.fetchone() or [0])[0] or 0)
    cur.execute(
        "INSERT OR IGNORE INTO doc_collection_items(collection_id, doc_id, position) VALUES(?, ?, ?)",
        (int(collection_id), int(doc_id), pos),
    )
    ok = cur.rowcount > 0
    con.commit()
    con.close()
    return ok


def db_doc_collection_remove_item(collection_id: int, doc_id: int) -> bool:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        "DELETE FROM doc_collection_items WHERE collection_id=? AND doc_id=?",
        (int(collection_id), int(doc_id)),
    )
    ok = cur.rowcount > 0
    con.commit()
    con.close()
    return ok

# ---------------- HELP DB: FAQ ----------------

def db_faq_list() -> list[tuple[int, str]]:
    """Список FAQ (id, question), последние сверху."""
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT id, question FROM faq_items ORDER BY id DESC")
    rows = cur.fetchall()
    con.close()
    return [(int(r[0]), r[1]) for r in rows]


def db_faq_list_full() -> list[dict]:
    """Полный список FAQ для общей таблицы: новые записи добавляются в конец."""
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        "SELECT id, question, answer FROM faq_items ORDER BY id ASC"
    )
    rows = cur.fetchall()
    con.close()
    return [
        {"id": int(r[0]), "question": r[1] or "", "answer": r[2] or ""}
        for r in rows
    ]


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
    cur.execute(
        "SELECT id, full_name FROM profiles "
        "WHERE COALESCE(is_active, 1)=1 "
        "ORDER BY full_name COLLATE NOCASE ASC"
    )
    rows = cur.fetchall()
    con.close()
    return [(r[0], r[1]) for r in rows]

def db_profiles_get(pid: int):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        SELECT id, full_name, year_start, city, birthday, about, topics, tg_link,
               tg_user_id, avg_test_score, photo_file_id
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
        "photo_file_id": row[10],
    }


def db_profiles_get_by_tg_link(tg_link: str):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        SELECT id, full_name, year_start, city, birthday, about, topics, tg_link,
               tg_user_id, avg_test_score, photo_file_id
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
        "photo_file_id": row[10],
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
        SELECT id, full_name, year_start, city, birthday, about, topics, tg_link,
               tg_user_id, avg_test_score, photo_file_id
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
        "photo_file_id": row[10],
    }



def _normalize_profile_tg_link(username: str | None) -> str | None:
    if not username:
        return None
    u = username.strip().lstrip("@")
    if not u:
        return None
    return "@" + u


def db_profiles_ensure_from_tg_user(user) -> tuple[int, bool]:
    """
    Создаёт шаблон карточки Telegram-пользователя или активирует существующую.

    Возвращает (profile_id, created), где created=True только для новой записи.
    Сначала ищет по неизменяемому tg_user_id, затем по текущему @username,
    чтобы связать ранее созданную вручную карточку и не делать дубль.
    """
    tg_user_id = int(user.id)
    full_name = (
        getattr(user, "full_name", None)
        or getattr(user, "first_name", None)
        or f"Сотрудник {tg_user_id}"
    ).strip()
    tg_link = _normalize_profile_tg_link(getattr(user, "username", None)) or ""

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    try:
        cur.execute(
            "SELECT id FROM profiles WHERE tg_user_id=? ORDER BY id ASC LIMIT 1",
            (tg_user_id,),
        )
        row = cur.fetchone()

        if not row and tg_link:
            cur.execute(
                "SELECT id FROM profiles WHERE tg_link=? ORDER BY id ASC LIMIT 1",
                (tg_link,),
            )
            row = cur.fetchone()

        if row:
            profile_id = int(row[0])
            cur.execute(
                """
                UPDATE profiles
                SET full_name=?, tg_link=?, tg_user_id=?, is_active=1
                WHERE id=?
                """,
                (full_name, tg_link, tg_user_id, profile_id),
            )
            created = False
        else:
            # Незаполненные поля отображаются в карточке как «—».
            cur.execute(
                """
                INSERT INTO profiles(
                    full_name, year_start, city, birthday, about, topics,
                    tg_link, tg_user_id, is_active, created_at
                ) VALUES (?, 0, '', NULL, '', '', ?, ?, 1, ?)
                """,
                (full_name, tg_link, tg_user_id, datetime.utcnow().isoformat()),
            )
            profile_id = int(cur.lastrowid)
            created = True

        con.commit()
        return profile_id, created
    finally:
        con.close()


def db_profiles_deactivate_by_tg_user(user) -> int | None:
    """
    Скрывает карточку покинувшего рабочий чат сотрудника без удаления истории.
    Возвращает id карточки или None, если подходящая карточка не найдена.
    """
    tg_user_id = int(user.id)
    tg_link = _normalize_profile_tg_link(getattr(user, "username", None))

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    try:
        cur.execute(
            "SELECT id FROM profiles WHERE tg_user_id=? ORDER BY id ASC LIMIT 1",
            (tg_user_id,),
        )
        row = cur.fetchone()

        if not row and tg_link:
            cur.execute(
                "SELECT id FROM profiles WHERE tg_link=? ORDER BY id ASC LIMIT 1",
                (tg_link,),
            )
            row = cur.fetchone()

        if not row:
            return None

        profile_id = int(row[0])
        cur.execute(
            "UPDATE profiles SET is_active=0 WHERE id=?",
            (profile_id,),
        )
        con.commit()
        return profile_id
    finally:
        con.close()


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

def db_profiles_add(
    full_name: str,
    year_start: int,
    city: str,
    birthday: str | None,
    about: str,
    topics: str,
    tg_link: str,
    photo_file_id: str | None = None,
) -> int:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        INSERT INTO profiles(
            full_name, year_start, city, birthday, about, topics, tg_link, photo_file_id, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        full_name.strip(), int(year_start), city.strip(), (birthday or None), about.strip(),
        topics.strip(), tg_link.strip(), photo_file_id, datetime.utcnow().isoformat(),
    ))
    con.commit()
    pid = cur.lastrowid
    con.close()
    return int(pid)


def db_profiles_update(
    pid: int,
    full_name: str,
    year_start: int,
    city: str,
    birthday: str | None,
    about: str,
    topics: str,
    tg_link: str,
    photo_file_id: str | None = None,
    keep_existing_photo: bool = True,
) -> bool:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    if keep_existing_photo and photo_file_id is None:
        cur.execute(
            """
            UPDATE profiles
            SET full_name=?, year_start=?, city=?, birthday=?, about=?, topics=?, tg_link=?
            WHERE id=?
            """,
            (full_name.strip(), int(year_start), city.strip(), (birthday or None), about.strip(), topics.strip(), tg_link.strip(), int(pid)),
        )
    else:
        cur.execute(
            """
            UPDATE profiles
            SET full_name=?, year_start=?, city=?, birthday=?, about=?, topics=?, tg_link=?, photo_file_id=?
            WHERE id=?
            """,
            (
                full_name.strip(), int(year_start), city.strip(), (birthday or None), about.strip(),
                topics.strip(), tg_link.strip(), photo_file_id, int(pid),
            ),
        )
    ok = cur.rowcount > 0
    con.commit()
    con.close()
    return ok


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
          AND COALESCE(is_active, 1)=1
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


def db_profiles_with_birthdays() -> list[dict]:
    """Возвращает сотрудников, у которых заполнена дата рождения ДД.ММ."""
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        SELECT id, full_name, birthday
        FROM profiles
        WHERE birthday IS NOT NULL AND TRIM(birthday) != ''
        ORDER BY full_name COLLATE NOCASE ASC
        """
    )
    rows = cur.fetchall()
    con.close()
    return [
        {
            "id": int(row[0]),
            "full_name": row[1],
            "birthday": row[2],
        }
        for row in rows
    ]


# ---------------- ACHIEVEMENTS, NOMINATIONS, REACTIONS ----------------

NOMINATION_CATEGORIES = {
    "team_help": {
        "emoji": "🤝",
        "title": "Командный вклад",
        "short": "Помощь",
        "description": "Помощь коллегам и поддержка общей работы.",
    },
    "initiative": {
        "emoji": "💡",
        "title": "Инициатор",
        "short": "Инициатива",
        "description": "Полезная идея или улучшение рабочего процесса.",
    },
    "result": {
        "emoji": "🚀",
        "title": "Сильный результат",
        "short": "Результат",
        "description": "Заметный результат, важный для команды или проекта.",
    },
    "development": {
        "emoji": "📚",
        "title": "Исследователь",
        "short": "Развитие",
        "description": "Развитие экспертизы и передача новых знаний команде.",
    },
    "atmosphere": {
        "emoji": "☀️",
        "title": "Душа команды",
        "short": "Атмосфера",
        "description": "Поддержка, уважение и вклад в здоровую атмосферу.",
    },
    "mentoring": {
        "emoji": "🧭",
        "title": "Наставник",
        "short": "Наставничество",
        "description": "Помощь в адаптации, обучении и профессиональном росте коллег.",
    },
}

ACHIEVEMENT_LEVEL_THRESHOLDS = (1, 3, 7)
ACHIEVEMENT_REACTIONS = {
    "clap": "👏",
    "fire": "🔥",
    "heart": "❤️",
}


def nomination_category(category_key: str | None) -> dict:
    return NOMINATION_CATEGORIES.get(category_key or "team_help", NOMINATION_CATEGORIES["team_help"])


def normalize_achievement_key(title: str) -> str:
    """Стабильный ключ для группировки одинаковых ачивок по уровням."""
    value = (title or "achievement").strip().lower().replace("ё", "е")
    value = re.sub(r"[^a-zа-я0-9]+", "_", value, flags=re.IGNORECASE)
    value = value.strip("_")
    return value[:80] or "achievement"


def achievement_level_label(level: int | None) -> str:
    try:
        value = max(1, int(level or 1))
    except (TypeError, ValueError):
        value = 1
    roman = {1: "I", 2: "II", 3: "III"}
    return roman.get(value, str(value))


def achievement_level_from_count(count: int) -> int:
    """I уровень — 1 награда, II — 3, III — 7."""
    count = max(0, int(count or 0))
    if count >= ACHIEVEMENT_LEVEL_THRESHOLDS[2]:
        return 3
    if count >= ACHIEVEMENT_LEVEL_THRESHOLDS[1]:
        return 2
    return 1


def achievement_progress_from_count(count: int) -> dict:
    count = max(0, int(count or 0))
    level = achievement_level_from_count(max(1, count))
    if count >= ACHIEVEMENT_LEVEL_THRESHOLDS[2]:
        return {
            "count": count,
            "level": 3,
            "next_threshold": None,
            "remaining": 0,
            "label": "максимальный уровень",
        }
    next_threshold = ACHIEVEMENT_LEVEL_THRESHOLDS[1] if count < ACHIEVEMENT_LEVEL_THRESHOLDS[1] else ACHIEVEMENT_LEVEL_THRESHOLDS[2]
    return {
        "count": count,
        "level": level,
        "next_threshold": next_threshold,
        "remaining": max(0, next_threshold - count),
        "label": f"{count} из {next_threshold} до уровня {achievement_level_label(level + 1)}",
    }


def db_achievements_list(profile_id: int) -> list[dict]:
    """Список ачивок профиля: последние сверху, с уровнем."""
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        SELECT id, emoji, title, description, awarded_at, awarded_by,
               COALESCE(achievement_key, ''), COALESCE(level, 1)
        FROM achievement_awards
        WHERE profile_id=?
        ORDER BY id DESC
        """,
        (int(profile_id),),
    )
    rows = cur.fetchall()
    con.close()
    return [
        {
            "id": int(r[0]),
            "emoji": r[1],
            "title": r[2],
            "description": r[3],
            "awarded_at": r[4],
            "awarded_by": r[5],
            "achievement_key": r[6] or normalize_achievement_key(r[2]),
            "level": int(r[7] or 1),
        }
        for r in rows
    ]


def db_achievement_get(award_id: int) -> dict | None:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        SELECT a.id, a.profile_id, a.emoji, a.title, a.description, a.awarded_at,
               a.awarded_by, COALESCE(a.achievement_key, ''), COALESCE(a.level, 1),
               p.full_name
        FROM achievement_awards a
        JOIN profiles p ON p.id=a.profile_id
        WHERE a.id=?
        """,
        (int(award_id),),
    )
    r = cur.fetchone()
    con.close()
    if not r:
        return None
    return {
        "id": int(r[0]),
        "profile_id": int(r[1]),
        "emoji": r[2],
        "title": r[3],
        "description": r[4],
        "awarded_at": r[5],
        "awarded_by": r[6],
        "achievement_key": r[7] or normalize_achievement_key(r[3]),
        "level": int(r[8] or 1),
        "profile_name": r[9],
    }


def db_achievement_award_add(
    profile_id: int,
    emoji: str,
    title: str,
    description: str,
    awarded_by: int | None = None,
    level: int = 1,
    achievement_key: str | None = None,
) -> int:
    clean_title = (title or "Ачивка").strip()
    clean_key = (achievement_key or normalize_achievement_key(clean_title)).strip()[:80]
    clean_level = max(1, min(int(level or 1), 99))

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO achievement_awards(
            profile_id, emoji, title, description, awarded_at,
            awarded_by, achievement_key, level
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(profile_id),
            (emoji or "🏆").strip(),
            clean_title,
            (description or "").strip(),
            datetime.utcnow().isoformat(),
            awarded_by,
            clean_key,
            clean_level,
        ),
    )
    con.commit()
    aid = int(cur.lastrowid)
    con.close()
    return aid


def db_achievement_key_count(profile_id: int, achievement_key: str) -> int:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM achievement_awards WHERE profile_id=? AND achievement_key=?",
        (int(profile_id), str(achievement_key)),
    )
    count = int((cur.fetchone() or [0])[0] or 0)
    con.close()
    return count


def db_achievement_progress(profile_id: int, achievement_key: str) -> dict:
    return achievement_progress_from_count(db_achievement_key_count(profile_id, achievement_key))


def db_achievement_progress_summary(profile_id: int) -> list[dict]:
    """По одной строке прогресса на каждый тип ачивки."""
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        SELECT COALESCE(achievement_key, ''), MAX(id), COUNT(*)
        FROM achievement_awards
        WHERE profile_id=?
        GROUP BY COALESCE(achievement_key, '')
        ORDER BY MAX(id) DESC
        """,
        (int(profile_id),),
    )
    groups = cur.fetchall()
    out = []
    for key, latest_id, count in groups:
        cur.execute("SELECT emoji, title FROM achievement_awards WHERE id=?", (int(latest_id),))
        latest = cur.fetchone() or ("🏆", "Ачивка")
        clean_key = key or normalize_achievement_key(latest[1])
        progress = achievement_progress_from_count(int(count or 0))
        out.append({
            "achievement_key": clean_key,
            "emoji": latest[0] or "🏆",
            "title": latest[1] or "Ачивка",
            **progress,
        })
    con.close()
    return out


def db_achievements_count(profile_id: int) -> int:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT COUNT(*) FROM achievement_awards WHERE profile_id=?", (int(profile_id),))
    count = int((cur.fetchone() or [0])[0] or 0)
    con.close()
    return count


# ---------------- ACHIEVEMENT REACTIONS ----------------

def db_achievement_reaction_set(award_id: int, user_id: int, reaction: str) -> bool:
    if reaction not in ACHIEVEMENT_REACTIONS:
        return False
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO achievement_reactions(award_id, user_id, reaction, created_at)
        VALUES(?, ?, ?, ?)
        ON CONFLICT(award_id, user_id) DO UPDATE SET
            reaction=excluded.reaction,
            created_at=excluded.created_at
        """,
        (int(award_id), int(user_id), reaction, datetime.utcnow().isoformat()),
    )
    con.commit()
    con.close()
    return True


def db_achievement_reaction_counts(award_id: int) -> dict[str, int]:
    counts = {key: 0 for key in ACHIEVEMENT_REACTIONS}
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        "SELECT reaction, COUNT(*) FROM achievement_reactions WHERE award_id=? GROUP BY reaction",
        (int(award_id),),
    )
    for reaction, count in cur.fetchall():
        if reaction in counts:
            counts[reaction] = int(count or 0)
    con.close()
    return counts


# ---------------- INTERNAL NOTIFICATIONS ----------------

def db_notification_add(
    user_id: int | None,
    notification_type: str,
    title: str,
    body: str = "",
    callback_data: str | None = None,
) -> int | None:
    if not user_id:
        return None
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO notifications(user_id, notification_type, title, body, callback_data, is_read, created_at)
        VALUES(?, ?, ?, ?, ?, 0, ?)
        """,
        (
            int(user_id),
            (notification_type or "info")[:40],
            (title or "Уведомление")[:180],
            (body or "")[:2000],
            (callback_data or None),
            datetime.utcnow().isoformat(),
        ),
    )
    con.commit()
    nid = int(cur.lastrowid)
    con.close()
    return nid


def db_notification_add_once(
    user_id: int | None,
    notification_type: str,
    title: str,
    body: str = "",
    callback_data: str | None = None,
) -> int | None:
    """Добавляет внутреннее уведомление один раз для одного события."""
    if not user_id:
        return None
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        SELECT id
        FROM notifications
        WHERE user_id=? AND notification_type=?
          AND COALESCE(callback_data, '')=COALESCE(?, '')
        ORDER BY id DESC
        LIMIT 1
        """,
        (int(user_id), (notification_type or "info")[:40], callback_data or None),
    )
    row = cur.fetchone()
    con.close()
    if row:
        return int(row[0])
    return db_notification_add(
        user_id,
        notification_type,
        title,
        body,
        callback_data=callback_data,
    )


def db_notifications_unread_count(user_id: int | None) -> int:
    if not user_id:
        return 0
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT COUNT(*) FROM notifications WHERE user_id=? AND is_read=0", (int(user_id),))
    count = int((cur.fetchone() or [0])[0] or 0)
    con.close()
    return count


def db_notifications_list(user_id: int, page: int = 0, page_size: int = 8) -> dict:
    """
    Возвращает только непрочитанные уведомления.

    Прочитанные записи остаются в БД как техническая история доставки, чтобы
    одноразовые напоминания не создавались повторно, но в пользовательском
    разделе «Уведомления» они больше не отображаются.
    """
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM notifications WHERE user_id=? AND is_read=0",
        (int(user_id),),
    )
    total = int((cur.fetchone() or [0])[0] or 0)
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = max(0, min(int(page), total_pages - 1))
    cur.execute(
        """
        SELECT id, notification_type, title, body, callback_data, is_read, created_at
        FROM notifications
        WHERE user_id=? AND is_read=0
        ORDER BY id DESC
        LIMIT ? OFFSET ?
        """,
        (int(user_id), int(page_size), int(page * page_size)),
    )
    items = [
        {
            "id": int(r[0]),
            "notification_type": r[1],
            "title": r[2],
            "body": r[3] or "",
            "callback_data": r[4],
            "is_read": bool(r[5]),
            "created_at": r[6],
        }
        for r in cur.fetchall()
    ]
    con.close()
    return {"items": items, "page": page, "total": total, "total_pages": total_pages}


def db_notification_get(notification_id: int, user_id: int) -> dict | None:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        SELECT id, notification_type, title, body, callback_data, is_read, created_at
        FROM notifications
        WHERE id=? AND user_id=?
        """,
        (int(notification_id), int(user_id)),
    )
    r = cur.fetchone()
    con.close()
    if not r:
        return None
    return {
        "id": int(r[0]),
        "notification_type": r[1],
        "title": r[2],
        "body": r[3] or "",
        "callback_data": r[4],
        "is_read": bool(r[5]),
        "created_at": r[6],
    }


def db_notification_mark_read(notification_id: int, user_id: int) -> bool:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        "UPDATE notifications SET is_read=1 WHERE id=? AND user_id=?",
        (int(notification_id), int(user_id)),
    )
    ok = cur.rowcount > 0
    con.commit()
    con.close()
    return ok


def db_notifications_mark_all_read(user_id: int) -> int:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("UPDATE notifications SET is_read=1 WHERE user_id=? AND is_read=0", (int(user_id),))
    count = int(cur.rowcount or 0)
    con.commit()
    con.close()
    return count


# ---------------- NOMINATIONS ----------------

def db_nomination_check_allowed(
    nominator_user_id: int,
    nominator_profile_id: int,
    nominee_profile_id: int,
    category_key: str,
) -> tuple[bool, str]:
    if int(nominator_profile_id) == int(nominee_profile_id):
        return False, "Нельзя номинировать самого себя."
    if category_key not in NOMINATION_CATEGORIES:
        return False, "Неизвестная категория номинации."

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        SELECT COUNT(*) FROM achievement_nominations
        WHERE nominator_user_id=? AND date(created_at)=date('now')
        """,
        (int(nominator_user_id),),
    )
    if int((cur.fetchone() or [0])[0] or 0) >= 5:
        con.close()
        return False, "Сегодня уже отправлено 5 номинаций. Попробуйте завтра."

    cur.execute(
        """
        SELECT 1 FROM achievement_nominations
        WHERE nominator_user_id=? AND nominee_profile_id=? AND category_key=? AND status='pending'
        LIMIT 1
        """,
        (int(nominator_user_id), int(nominee_profile_id), category_key),
    )
    if cur.fetchone():
        con.close()
        return False, "Такая номинация уже ожидает решения администратора."

    cur.execute(
        """
        SELECT 1 FROM achievement_nominations
        WHERE nominator_user_id=? AND nominee_profile_id=? AND category_key=?
          AND datetime(created_at) >= datetime('now', '-7 days')
        LIMIT 1
        """,
        (int(nominator_user_id), int(nominee_profile_id), category_key),
    )
    recent = cur.fetchone()
    con.close()
    if recent:
        return False, "Этого коллегу в выбранной категории можно номинировать не чаще одного раза в 7 дней."
    return True, ""


def db_nomination_create(
    scope_chat_id: int,
    nominator_user_id: int | None,
    nominator_profile_id: int | None,
    nominee_profile_id: int,
    category_key: str,
    reason: str,
) -> int:
    if category_key not in NOMINATION_CATEGORIES:
        category_key = "team_help"
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO achievement_nominations(
            scope_chat_id, nominator_user_id, nominator_profile_id,
            nominee_profile_id, category_key, reason, status, created_at
        )
        VALUES(?, ?, ?, ?, ?, ?, 'pending', ?)
        """,
        (
            int(scope_chat_id),
            int(nominator_user_id) if nominator_user_id else None,
            int(nominator_profile_id) if nominator_profile_id else None,
            int(nominee_profile_id),
            category_key,
            reason.strip(),
            datetime.utcnow().isoformat(),
        ),
    )
    con.commit()
    nomination_id = int(cur.lastrowid)
    con.close()
    return nomination_id


def db_nomination_get(nomination_id: int) -> dict | None:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        SELECT n.id, n.scope_chat_id, n.nominator_user_id,
               n.nominator_profile_id, n.nominee_profile_id,
               COALESCE(n.category_key, 'team_help'), n.reason, n.status, n.created_at, n.reviewed_at,
               n.reviewed_by, n.award_id,
               COALESCE(p_from.full_name, 'Сотрудник'),
               COALESCE(p_to.full_name, 'Сотрудник'),
               COALESCE(p_to.tg_link, ''), p_to.tg_user_id
        FROM achievement_nominations n
        LEFT JOIN profiles p_from ON p_from.id = n.nominator_profile_id
        JOIN profiles p_to ON p_to.id = n.nominee_profile_id
        WHERE n.id=?
        """,
        (int(nomination_id),),
    )
    r = cur.fetchone()
    con.close()
    if not r:
        return None
    return {
        "id": int(r[0]),
        "scope_chat_id": int(r[1]),
        "nominator_user_id": r[2],
        "nominator_profile_id": r[3],
        "nominee_profile_id": int(r[4]),
        "category_key": r[5] or "team_help",
        "reason": r[6],
        "status": r[7],
        "created_at": r[8],
        "reviewed_at": r[9],
        "reviewed_by": r[10],
        "award_id": r[11],
        "nominator_name": r[12],
        "nominee_name": r[13],
        "nominee_tg_link": r[14],
        "nominee_tg_user_id": r[15],
    }


def db_nominations_pending(limit: int = 30) -> list[dict]:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        SELECT n.id, n.created_at, n.reason, COALESCE(n.category_key, 'team_help'),
               COALESCE(p_from.full_name, 'Сотрудник'),
               COALESCE(p_to.full_name, 'Сотрудник')
        FROM achievement_nominations n
        LEFT JOIN profiles p_from ON p_from.id = n.nominator_profile_id
        JOIN profiles p_to ON p_to.id = n.nominee_profile_id
        WHERE n.status='pending'
        ORDER BY n.id DESC
        LIMIT ?
        """,
        (int(limit),),
    )
    rows = cur.fetchall()
    con.close()
    return [
        {
            "id": int(r[0]),
            "created_at": r[1],
            "reason": r[2],
            "category_key": r[3] or "team_help",
            "nominator_name": r[4],
            "nominee_name": r[5],
        }
        for r in rows
    ]


def db_nomination_approve(nomination_id: int, reviewed_by: int) -> dict | None:
    """Атомарно одобряет номинацию, выдаёт категорийную ачивку и считает уровень по порогам 1/3/7."""
    con = sqlite3.connect(DB_PATH)
    try:
        cur = con.cursor()
        cur.execute("BEGIN IMMEDIATE")
        cur.execute(
            """SELECT nominee_profile_id, reason, COALESCE(category_key, 'team_help')
               FROM achievement_nominations
               WHERE id=? AND status='pending'""",
            (int(nomination_id),),
        )
        row = cur.fetchone()
        if not row:
            con.rollback()
            return None

        nominee_profile_id = int(row[0])
        reason = row[1]
        category_key = row[2] or "team_help"
        category = nomination_category(category_key)
        achievement_key = f"nomination_{category_key}"

        cur.execute(
            "SELECT COUNT(*) FROM achievement_awards WHERE profile_id=? AND achievement_key=?",
            (nominee_profile_id, achievement_key),
        )
        new_count = int((cur.fetchone() or [0])[0] or 0) + 1
        new_level = achievement_level_from_count(new_count)

        cur.execute(
            """
            INSERT INTO achievement_awards(
                profile_id, emoji, title, description, awarded_at,
                awarded_by, achievement_key, level
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                nominee_profile_id,
                category["emoji"],
                category["title"],
                reason,
                datetime.utcnow().isoformat(),
                int(reviewed_by),
                achievement_key,
                new_level,
            ),
        )
        award_id = int(cur.lastrowid)
        cur.execute(
            """
            UPDATE achievement_nominations
            SET status='approved', reviewed_at=?, reviewed_by=?, award_id=?
            WHERE id=? AND status='pending'
            """,
            (datetime.utcnow().isoformat(), int(reviewed_by), award_id, int(nomination_id)),
        )
        if cur.rowcount != 1:
            con.rollback()
            return None
        con.commit()
        return {
            "award_id": award_id,
            "level": new_level,
            "count": new_count,
            "progress": achievement_progress_from_count(new_count),
            "category_key": category_key,
            "emoji": category["emoji"],
            "title": category["title"],
            "achievement_key": achievement_key,
        }
    finally:
        con.close()


def db_nomination_reject(nomination_id: int, reviewed_by: int) -> bool:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        UPDATE achievement_nominations
        SET status='rejected', reviewed_at=?, reviewed_by=?
        WHERE id=? AND status='pending'
        """,
        (datetime.utcnow().isoformat(), int(reviewed_by), int(nomination_id)),
    )
    ok = cur.rowcount == 1
    con.commit()
    con.close()
    return ok


def export_achievement_awards_rows() -> list[dict]:
    """Для CSV/ZIP-бэкапа: все выданные ачивки, включая уровни."""
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        SELECT a.id, p.id, p.full_name, p.tg_link,
               a.emoji, a.title, a.description, a.awarded_at, a.awarded_by,
               COALESCE(a.achievement_key, ''), COALESCE(a.level, 1)
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
            "achievement_key": r[9] or normalize_achievement_key(r[5] or "Ачивка"),
            "level": int(r[10] or 1),
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
    "Готовы к новым вершинам сегодня? 💪",
    "С добрым утром! Пусть день будет продуктивным и радостным 😊",
]


WELCOME_TEXT = """👋 Привет, {name}! Рады видеть тебя в команде! 🎉
Желаем лёгкого старта, крутых задач и вдохновения каждый день 🌟
Если что — не стесняйся, всегда поможем 🙌

Для знакомства с рабочим ботом и полезными разделами используй команду /help ✅"""

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

INDUSTRY_GREETINGS = [
    "Коллеги, время отраслевой встречи — зарядимся идеями и синхронизируемся по важному 🚀",
    "Привет! Скоро отраслевая встреча: обсудим новости, вопросы и полезные инсайты ☕✨",
    "Команда, готовимся к отраслевой встрече — будет полезно и по делу 🔎",
    "Коллеги, через полчаса встречаемся на отраслевой — не забудьте подключиться 💬",
]

def build_industry_text(industry_zoom_url: str) -> str:
    greet = random.choice(INDUSTRY_GREETINGS)
    return (
        f"{greet}\n\n"
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


def regular_meeting_title(meeting_type: str) -> str:
    return "Планёрка" if meeting_type == MEETING_STANDUP else "Отраслевая встреча"


def regular_meeting_default_time(meeting_type: str) -> str:
    return "09:15" if meeting_type == MEETING_STANDUP else "11:30"


def parse_regular_meeting_time(value: str | None) -> str | None:
    clean = (value or "").strip()
    if re.fullmatch(r"\d{4}", clean):
        clean = f"{clean[:2]}:{clean[2:]}"
    if not re.fullmatch(r"\d{1,2}:\d{2}", clean):
        return None
    try:
        parsed = datetime.strptime(clean, "%H:%M")
    except ValueError:
        return None
    return parsed.strftime("%H:%M")


def format_regular_meeting_datetime(value: date, time_value: str | None) -> str:
    clean_time = parse_regular_meeting_time(time_value) or "—"
    return f"{format_regular_meeting_date(value)} в {clean_time} МСК"


def regular_meeting_is_due(meeting_type: str, meeting_date: date) -> bool:
    if meeting_type == MEETING_STANDUP:
        return standup_due_on_weekday(meeting_date)
    if meeting_type == MEETING_INDUSTRY:
        return industry_due_on_weekday(meeting_date)
    return False


def parse_regular_meeting_date(value: str) -> date | None:
    clean = (value or "").strip()
    for fmt in ("%d.%m.%Y", "%d.%m.%y"):
        try:
            return datetime.strptime(clean, fmt).date()
        except ValueError:
            continue
    return None


def format_regular_meeting_date(value: date) -> str:
    return value.strftime("%d.%m.%Y")


def regular_meeting_week_bounds(reference_date: date | None = None) -> tuple[date, date]:
    """Возвращает понедельник и воскресенье недели по московской дате."""
    current = reference_date or datetime.now(MOSCOW_TZ).date()
    monday = current - timedelta(days=current.weekday())
    return monday, monday + timedelta(days=6)


def regular_meetings_for_current_week(reference_date: date | None = None) -> list[dict]:
    """
    Список регулярных встреч текущей недели, которые ещё можно изменить.
    Встречи до текущей московской даты не показываются.
    """
    today_d = reference_date or datetime.now(MOSCOW_TZ).date()
    week_start, week_end = regular_meeting_week_bounds(today_d)
    items: list[dict] = []

    cursor = week_start
    while cursor <= week_end:
        if cursor >= today_d:
            if regular_meeting_is_due(MEETING_STANDUP, cursor):
                items.append({"meeting_type": MEETING_STANDUP, "meeting_date": cursor})
            if regular_meeting_is_due(MEETING_INDUSTRY, cursor):
                items.append({"meeting_type": MEETING_INDUSTRY, "meeting_date": cursor})
        cursor += timedelta(days=1)

    return items


def regular_meeting_week_text(reference_date: date | None = None) -> str:
    today_d = reference_date or datetime.now(MOSCOW_TZ).date()
    week_start, week_end = regular_meeting_week_bounds(today_d)
    return (
        "🗓 <b>Управление встречами текущей недели</b>\n\n"
        f"Неделя: <b>{week_start.strftime('%d.%m')}–{week_end.strftime('%d.%m.%Y')}</b>\n"
        "Выберите конкретную встречу, которую нужно отменить или перенести. "
        "Показываются только сегодняшние и будущие встречи этой недели."
    )


def kb_regular_meetings_root(reference_date: date | None = None):
    rows = []
    for item in regular_meetings_for_current_week(reference_date):
        meeting_type = item["meeting_type"]
        meeting_date = item["meeting_date"]
        state = db_get_state(meeting_type, meeting_date)

        if state.get("canceled") and state.get("reschedule_date"):
            try:
                moved_to = date.fromisoformat(state["reschedule_date"]).strftime("%d.%m")
            except (TypeError, ValueError):
                moved_to = "другая дата"
            moved_time = (
                parse_regular_meeting_time(state.get("reschedule_time"))
                or regular_meeting_default_time(meeting_type)
            )
            icon = "🔄"
            suffix = f" → {moved_to} {moved_time}"
        elif state.get("canceled"):
            icon = "❌"
            suffix = " — отменена"
        else:
            icon = "🟢" if meeting_type == MEETING_STANDUP else "🔵"
            suffix = ""

        day_name = DAY_RU_UPPER.get(meeting_date.weekday(), "")
        label = (
            f"{icon} {day_name} {meeting_date.strftime('%d.%m')} — "
            f"{regular_meeting_title(meeting_type)}{suffix}"
        )
        callback = (
            "help:settings:regular_meeting:pick:"
            f"{meeting_type}:{meeting_date.isoformat()}"
        )
        rows.append([InlineKeyboardButton(label, callback_data=callback)])

    if not rows:
        rows.append([InlineKeyboardButton("— На этой неделе встреч больше нет —", callback_data="noop")])

    rows.append([InlineKeyboardButton("🔄 Обновить список", callback_data="help:settings:regular_meetings")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="help:settings:communications")])
    return InlineKeyboardMarkup(rows)


def kb_regular_meeting_actions(meeting_type: str, original_date: date):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(
            "❌ Отменить / удалить встречу",
            callback_data="help:settings:regular_meeting:selected_action:cancel",
        )],
        [InlineKeyboardButton(
            "🔄 Перенести дату и время уведомления",
            callback_data="help:settings:regular_meeting:selected_action:move",
        )],
        [InlineKeyboardButton("⬅️ К встречам недели", callback_data="help:settings:regular_meetings")],
    ])


def kb_regular_meeting_time_picker(meeting_type: str):
    """Кнопки времени уведомления о перенесённой встрече."""
    rows = [[InlineKeyboardButton(
        f"⭐ Обычное время — {regular_meeting_default_time(meeting_type)}",
        callback_data=(
            "help:settings:regular_meeting:new_time:"
            + regular_meeting_default_time(meeting_type).replace(":", "")
        ),
    )]]

    options: list[str] = []
    current = datetime(2000, 1, 1, 8, 0)
    finish = datetime(2000, 1, 1, 20, 0)
    while current <= finish:
        options.append(current.strftime("%H:%M"))
        current += timedelta(minutes=30)

    for idx in range(0, len(options), 3):
        rows.append([
            InlineKeyboardButton(
                value,
                callback_data=(
                    "help:settings:regular_meeting:new_time:"
                    + value.replace(":", "")
                ),
            )
            for value in options[idx:idx + 3]
        ])

    rows.append([InlineKeyboardButton(
        "✍️ Указать другое время",
        callback_data="help:settings:regular_meeting:new_time_manual",
    )])
    rows.append([InlineKeyboardButton(
        "❌ Отмена",
        callback_data="help:settings:regular_meeting:cancel",
    )])
    return InlineKeyboardMarkup(rows)


def kb_regular_meeting_notify():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔔 Да, уведомить в чате", callback_data="help:settings:regular_meeting:notify:yes")],
        [InlineKeyboardButton("🔕 Нет, не уведомлять", callback_data="help:settings:regular_meeting:notify:no")],
        [InlineKeyboardButton("❌ Отмена", callback_data="help:settings:regular_meeting:cancel")],
    ])


def kb_regular_meeting_confirm():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Подтвердить", callback_data="help:settings:regular_meeting:confirm")],
        [InlineKeyboardButton("❌ Отмена", callback_data="help:settings:regular_meeting:cancel")],
    ])


def regular_meeting_confirmation_html(data: dict) -> str:
    meeting_type = data.get("meeting_type")
    action = data.get("action")
    original_d = parse_regular_meeting_date(data.get("original_date") or "")
    new_d = parse_regular_meeting_date(data.get("new_date") or "") if data.get("new_date") else None
    new_time = parse_regular_meeting_time(data.get("new_time"))
    reason = escape((data.get("reason") or "").strip())
    notify_text = "да" if data.get("notify") else "нет"
    action_text = "Отмена" if action == "cancel" else "Перенос"
    lines = [
        "📋 <b>Проверьте изменение</b>",
        "",
        f"Встреча: <b>{escape(regular_meeting_title(meeting_type))}</b>",
        f"Действие: <b>{action_text}</b>",
        f"Дата встречи: <b>{format_regular_meeting_date(original_d) if original_d else '—'}</b>",
    ]
    if new_d:
        lines.append(
            "Новая дата и время уведомления: "
            f"<b>{format_regular_meeting_datetime(new_d, new_time)}</b>"
        )
    lines.extend([
        f"Причина: {reason}",
        f"Уведомить сотрудников в чатах: <b>{notify_text}</b>",
    ])
    return "\n".join(lines)


async def notify_regular_meeting_change(context: ContextTypes.DEFAULT_TYPE, data: dict) -> tuple[int, int]:
    meeting_type = data.get("meeting_type")
    action = data.get("action")
    original_d = parse_regular_meeting_date(data.get("original_date") or "")
    new_d = parse_regular_meeting_date(data.get("new_date") or "") if data.get("new_date") else None
    new_time = parse_regular_meeting_time(data.get("new_time"))
    reason = escape((data.get("reason") or "").strip())
    title = escape(regular_meeting_title(meeting_type))

    if action == "move":
        message_text = (
            f"🔄 <b>{title} перенесена</b>\n\n"
            f"Было: <b>{format_regular_meeting_date(original_d)}</b>\n"
            "Новое уведомление: "
            f"<b>{format_regular_meeting_datetime(new_d, new_time)}</b>\n"
            f"Причина: {reason}"
        )
    else:
        message_text = (
            f"❌ <b>{title} отменена</b>\n\n"
            f"Дата: <b>{format_regular_meeting_date(original_d)}</b>\n"
            f"Причина: {reason}"
        )

    ok = 0
    fail = 0
    for chat_id in db_list_chats():
        try:
            sent = await context.bot.send_message(
                chat_id=chat_id,
                text=message_text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            schedule_message_delete(context, sent)
            ok += 1
        except Exception as exc:
            logger.exception("Cannot notify meeting change to %s: %s", chat_id, exc)
            fail += 1
    return ok, fail


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
WAITING_EDIT_CATEGORY_ID = "waiting_edit_category_id"

# documents knowledge-base flows
WAITING_DOC_SEARCH = "waiting_doc_search"
WAITING_DOC_EDIT_TITLE_ID = "waiting_doc_edit_title_id"
WAITING_DOC_EDIT_DESC_ID = "waiting_doc_edit_desc_id"
WAITING_DOC_REPLACE_ID = "waiting_doc_replace_id"
WAITING_DOC_TAG_NAME = "waiting_doc_tag_name"
WAITING_DOC_COLLECTION_NAME = "waiting_doc_collection_name"
DOCS_RETURN_CB = "docs_return_cb"


# faq add flow
WAITING_FAQ_Q = "waiting_faq_q"
WAITING_FAQ_A = "waiting_faq_a"
PENDING_FAQ = "pending_faq"

# faq employee search flow
WAITING_FAQ_SEARCH = "waiting_faq_search"
FAQ_SEARCH_QUERY = "faq_search_query"

WAITING_RESTORE_ZIP = "waiting_restore_zip"
# profiles add flow
PROFILE_WIZ_ACTIVE = "profile_wiz_active"

# csv import flow
WAITING_CSV_IMPORT = "waiting_csv_import"
WAITING_ZIP_IMPORT = "waiting_zip_import"
WAITING_TEST_AVGSCORE = "waiting_test_avgscore"
WAITING_TEST_AVGSCORE_PID = "waiting_test_avgscore_pid"



# achievements award flow
ACH_WIZ_ACTIVE = "ach_wiz_active"
ACH_WIZ_STEP = "ach_wiz_step"
ACH_WIZ_DATA = "ach_wiz_data"

# colleague nomination flow (обычный сотрудник)
NOMINATION_ACTIVE = "nomination_active"
NOMINATION_STEP = "nomination_step"
NOMINATION_DATA = "nomination_data"
PROFILE_WIZ_STEP = "profile_wiz_step"
PROFILE_WIZ_DATA = "profile_wiz_data"
PROFILE_WIZ_MODE = "profile_wiz_mode"          # admin_add|admin_edit|self_create|self_edit
PROFILE_WIZ_EDIT_PID = "profile_wiz_edit_pid"

# suggest box flow
WAITING_SUGGESTION_TEXT = "waiting_suggestion_text"
SUGGESTION_MODE = "suggestion_mode"  # anon|named

# broadcast flow
BCAST_ACTIVE = "bcast_active"
BCAST_STEP = "bcast_step"  # heading_choice|topic|text|files|schedule_time
BCAST_DATA = "bcast_data"
WAITING_BCAST_TAG_NAME = "waiting_bcast_tag_name"
BCAST_TAG_MODE = "bcast_tag_mode"  # manage|wizard

# custom meeting flow (Communications)
COMM_MEETING_ACTIVE = "comm_meeting_active"
COMM_MEETING_STEP = "comm_meeting_step"  # topic|description|link|recipients|schedule_time
COMM_MEETING_DATA = "comm_meeting_data"
COMM_MEETING_SELECTED_PIDS = "comm_meeting_selected_pids"

# management of recurring stand-up / industry meetings (Communications)
REGULAR_MEETING_ACTIVE = "regular_meeting_active"
REGULAR_MEETING_STEP = "regular_meeting_step"  # original_date|new_date|new_time|new_time_manual|reason|notify
REGULAR_MEETING_DATA = "regular_meeting_data"

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
    context.chat_data.pop(WAITING_EDIT_CATEGORY_ID, None)
    context.chat_data[WAITING_DOC_SEARCH] = False
    context.chat_data.pop(WAITING_DOC_EDIT_TITLE_ID, None)
    context.chat_data.pop(WAITING_DOC_EDIT_DESC_ID, None)
    context.chat_data.pop(WAITING_DOC_REPLACE_ID, None)
    context.chat_data[WAITING_DOC_TAG_NAME] = False
    context.chat_data[WAITING_DOC_COLLECTION_NAME] = False


def clear_faq_flow(context: ContextTypes.DEFAULT_TYPE):
    context.chat_data[WAITING_FAQ_Q] = False
    context.chat_data[WAITING_FAQ_A] = False
    context.chat_data.pop(PENDING_FAQ, None)


def clear_faq_search_flow(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    drop_query: bool = True,
):
    context.chat_data[WAITING_FAQ_SEARCH] = False
    if drop_query:
        context.chat_data.pop(FAQ_SEARCH_QUERY, None)



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
    context.chat_data.pop(PROFILE_WIZ_MODE, None)
    context.chat_data.pop(PROFILE_WIZ_EDIT_PID, None)

def clear_zip_import(context: ContextTypes.DEFAULT_TYPE):
    context.chat_data[WAITING_ZIP_IMPORT] = False

def clear_ach_wiz(context: ContextTypes.DEFAULT_TYPE):
    context.chat_data[ACH_WIZ_ACTIVE] = False
    context.chat_data.pop(ACH_WIZ_STEP, None)
    context.chat_data.pop(ACH_WIZ_DATA, None)


def clear_nomination_flow(context: ContextTypes.DEFAULT_TYPE):
    context.user_data[NOMINATION_ACTIVE] = False
    context.user_data.pop(NOMINATION_STEP, None)
    context.user_data.pop(NOMINATION_DATA, None)

def clear_suggest_flow(context: ContextTypes.DEFAULT_TYPE):
    context.user_data[WAITING_SUGGESTION_TEXT] = False
    context.user_data.pop(SUGGESTION_MODE, None)

def clear_bcast_tag_waiting(context: ContextTypes.DEFAULT_TYPE):
    context.user_data[WAITING_BCAST_TAG_NAME] = False
    context.user_data.pop(BCAST_TAG_MODE, None)


def clear_bcast_flow(context: ContextTypes.DEFAULT_TYPE):
    context.user_data[BCAST_ACTIVE] = False
    context.user_data.pop(BCAST_STEP, None)
    context.user_data.pop(BCAST_DATA, None)
    clear_bcast_tag_waiting(context)


def clear_comm_meeting_flow(context: ContextTypes.DEFAULT_TYPE):
    context.user_data[COMM_MEETING_ACTIVE] = False
    context.user_data.pop(COMM_MEETING_STEP, None)
    context.user_data.pop(COMM_MEETING_DATA, None)
    context.user_data.pop(COMM_MEETING_SELECTED_PIDS, None)


def clear_regular_meeting_flow(context: ContextTypes.DEFAULT_TYPE):
    context.user_data[REGULAR_MEETING_ACTIVE] = False
    context.user_data.pop(REGULAR_MEETING_STEP, None)
    context.user_data.pop(REGULAR_MEETING_DATA, None)



# ---------------- DUE RULES ----------------

def standup_due_on_weekday(d: date) -> bool:
    return d.weekday() in (0, 2, 4)

def industry_due_on_weekday(d: date) -> bool:
    return d.weekday() == 2  # 2 = среда

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


def format_achievements_for_profile(profile_id: int, limit: int = 10) -> str:
    items = db_achievements_list(profile_id)
    if not items:
        return "— Всё ещё впереди —"
    parts = []
    for it in items[:max(1, int(limit))]:
        level = achievement_level_label(it.get("level"))
        counts = db_achievement_reaction_counts(int(it["id"]))
        reactions = "  ".join(
            f"{ACHIEVEMENT_REACTIONS[key]} {counts.get(key, 0)}"
            for key in ACHIEVEMENT_REACTIONS
            if counts.get(key, 0)
        )
        date_text = _format_short_date(it.get("awarded_at"))
        part = (
            f"{escape(it['emoji'])} <b>{escape(it['title'])} · уровень {level}</b>\n"
            f"{escape(it['description'])}\n"
            f"📅 {escape(date_text)}"
        )
        if reactions:
            part += f"\n{reactions}"
        parts.append(part)
    return "\n\n".join(parts)


def format_achievement_progress_for_profile(profile_id: int, limit: int = 8) -> str:
    items = db_achievement_progress_summary(profile_id)
    if not items:
        return "— Прогресс появится после первой ачивки —"
    lines = []
    for item in items[:max(1, int(limit))]:
        level = achievement_level_label(item["level"])
        if item["next_threshold"] is None:
            progress = "максимальный уровень"
        else:
            progress = item["label"]
        lines.append(
            f"{escape(item['emoji'])} <b>{escape(item['title'])} · {level}</b> — {escape(progress)}"
        )
    return "\n".join(lines)



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

async def send_meeting_message(
    meeting_type: str,
    context: ContextTypes.DEFAULT_TYPE,
    force: bool = False,
    *,
    include_standard: bool = True,
    due_time: str | None = None,
) -> bool:
    now_msk = datetime.now(MOSCOW_TZ)
    today_d = now_msk.date()
    due_time = parse_regular_meeting_time(due_time) or now_msk.strftime("%H:%M")

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
    standard_due = include_standard and weekday_due and state["canceled"] != 1

    due_orig_isos = db_get_due_reschedules(meeting_type, today_d, due_time)
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
            sent_message = await context.bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=kb_cancel_menu(meeting_type),
            )
            schedule_message_delete(context, sent_message)
        except Exception as e:
            logger.exception("Cannot send %s to %s: %s", meeting_type, chat_id, e)

    if reschedule_due:
        db_mark_reschedules_sent(meeting_type, due_orig_isos)

    return True


async def check_and_send_jobs(context: ContextTypes.DEFAULT_TYPE):
    try:
        await process_due_communications(context)
    except Exception as e:
        logger.exception("Scheduled communications checker failed: %s", e)

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
            await send_meeting_message(
                MEETING_STANDUP, context, force=False,
                include_standard=True, due_time=now_msk.strftime("%H:%M"),
            )
            db_set_meta(key, today_iso)

    if now_msk.hour == 11 and now_msk.minute == 30:
        key = "last_auto_sent_date:industry"
        if db_get_meta(key) != today_iso:
            await send_meeting_message(
                MEETING_INDUSTRY, context, force=False,
                include_standard=True, due_time=now_msk.strftime("%H:%M"),
            )
            db_set_meta(key, today_iso)

    # Перенесённые встречи могут иметь собственное время уведомления.
    # Проверяем их каждую минуту; sent=1 защищает от повторной отправки.
    current_hhmm = now_msk.strftime("%H:%M")
    for meeting_type in (MEETING_STANDUP, MEETING_INDUSTRY):
        if db_get_due_reschedules(meeting_type, now_msk.date(), current_hhmm):
            await send_meeting_message(
                meeting_type, context, force=False,
                include_standard=False, due_time=current_hhmm,
            )

# ---------------- HELP MENUS ----------------

def help_text_main(
    bot_username: str,
    profile: dict | None = None,
    unread_count: int = 0,
    is_admin_user: bool = False,
    user_full_name: str | None = None,
) -> str:
    if profile:
        # Для приветствия используем полное имя Telegram в естественном порядке
        # (имя + фамилия). Если фамилия в Telegram не заполнена, берём полное
        # имя из анкеты сотрудника без обрезания до первого слова.
        profile_full_name = (profile.get("full_name") or "Коллега").strip()
        display_name = (user_full_name or "").strip() or profile_full_name
        tests = db_profile_test_summary(int(profile["id"]))
        achievements_count = db_achievements_count(int(profile["id"]))
        attention = []
        if tests.get("assigned"):
            attention.append(f"📝 новых тестов: <b>{tests['assigned']}</b>")
        if tests.get("in_progress"):
            attention.append(f"⏳ тестов в процессе: <b>{tests['in_progress']}</b>")
        if unread_count:
            attention.append(f"🔔 непрочитанных уведомлений: <b>{unread_count}</b>")
        if not attention:
            attention.append("✅ срочных задач сейчас нет")
        admin_line = ""
        if is_admin_user:
            pending = len(db_nominations_pending(100))
            if pending:
                admin_line = f"\n⚙️ Ожидают решения номинации: <b>{pending}</b>\n"
        return (
            f"👋 <b>Привет, {escape(display_name)}!</b>\n\n"
            "Это твоя рабочая панель. Здесь видно, что требует внимания, "
            "и доступны основные разделы команды.\n\n"
            "📌 <b>Сейчас:</b>\n• " + "\n• ".join(attention) + "\n"
            f"🏆 Всего достижений: <b>{achievements_count}</b>"
            f"{admin_line}\n\n"
            "Выберите нужный раздел 👇"
        )

    return (
        "🤖 <b>Меню «Помогатор Говорун»</b>\n\n"
        "Не удалось связать Telegram с анкетой, но основные разделы доступны.\n\n"
        "Выберите нужный раздел 👇"
    )


def kb_help_main(is_admin_user: bool, unread_count: int = 0):
    notification_label = "🔔 Уведомления"
    if unread_count:
        notification_label += f" · {unread_count}"
    rows = [
        [
            InlineKeyboardButton("👤 Мой кабинет", callback_data="help:me"),
            InlineKeyboardButton(notification_label, callback_data="help:notifications"),
        ],
        [
            InlineKeyboardButton("🙌 Номинация", callback_data="help:nomination"),
            InlineKeyboardButton("👥 Наша команда", callback_data="help:team"),
        ],
        [
            InlineKeyboardButton("📄 Документы", callback_data="help:docs"),
            InlineKeyboardButton("🔗 Полезные ссылки", callback_data="help:links"),
        ],
        [
            InlineKeyboardButton("❓ FAQ", callback_data="help:faq"),
            InlineKeyboardButton("💡 Предложка", callback_data="help:suggest"),
        ],
    ]
    if is_admin_user:
        rows.append([InlineKeyboardButton("⚙️ Управление ботом", callback_data="help:settings")])
    return InlineKeyboardMarkup(rows)



def _profile_team_page(profile_id: int) -> int:
    people = db_profiles_list()
    for index, (pid, _name) in enumerate(people):
        if int(pid) == int(profile_id):
            return index // TEAM_PAGE_SIZE
    return 0


def _format_short_date(value: str | None) -> str:
    if not value:
        return "—"
    try:
        return datetime.fromisoformat(value).strftime("%d.%m.%Y")
    except Exception:
        return str(value)[:10]


def build_my_account_text(profile: dict) -> str:
    tests = db_profile_test_summary(int(profile["id"]))
    achievements_count = db_achievements_count(int(profile["id"]))
    avg = profile.get("avg_test_score")
    avg_text = f"{int(avg)}%" if avg is not None and str(avg).strip() else "—"
    birthday = (profile.get("birthday") or "—").strip() or "—"
    return (
        f"👤 <b>Мой кабинет</b>\n\n"
        f"<b>{escape(profile['full_name'])}</b>\n"
        f"🏙️ {escape(profile.get('city') or '—')}\n"
        f"📅 В команде с: <b>{escape(str(profile.get('year_start') or '—'))}</b>\n"
        f"🎂 День рождения: <b>{escape(birthday)}</b>\n\n"
        f"🏆 Достижений: <b>{achievements_count}</b>\n"
        f"📝 Назначено тестов: <b>{tests['assigned']}</b>\n"
        f"⏳ В процессе: <b>{tests['in_progress']}</b>\n"
        f"✅ Завершено: <b>{tests['finished']}</b>\n"
        f"📈 Средний балл: <b>{escape(avg_text)}</b>"
    )


def kb_my_account(profile: dict):
    page = _profile_team_page(int(profile["id"]))
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🏆 Мои достижения", callback_data="help:me:achievements"),
            InlineKeyboardButton("📝 Мои тесты", callback_data="help:me:tests"),
        ],
        [
            InlineKeyboardButton("✏️ Редактировать мою анкету", callback_data="help:me:edit")
        ],
        [
            InlineKeyboardButton(
                "👥 Моя карточка в команде",
                callback_data=f"help:team:person:{int(profile['id'])}:{page}",
            )
        ],
        [InlineKeyboardButton("⬅️ Назад", callback_data="help:main")],
    ])


def kb_my_tests(profile_id: int):
    tests = db_profile_tests(profile_id, limit=10)
    rows = []
    status_label = {
        "assigned": "▶️ Назначен",
        "in_progress": "⏳ В процессе",
        "finished": "✅ Завершён",
        "saved": "💾 Сохранён",
        "expired": "⌛ Истёк",
        "canceled": "❌ Отменён",
    }
    if not tests:
        rows.append([InlineKeyboardButton("— тестов пока нет —", callback_data="noop")])
    else:
        for item in tests:
            title = item["title"] if len(item["title"]) <= 34 else item["title"][:31] + "…"
            status = item.get("status") or ""
            if status == "assigned":
                rows.append([
                    InlineKeyboardButton(
                        f"▶️ {title}",
                        callback_data=f"test:start:{item['id']}",
                    )
                ])
            elif status == "in_progress":
                rows.append([
                    InlineKeyboardButton(
                        f"⏳ Продолжить: {title}",
                        callback_data=f"help:me:test:continue:{item['id']}",
                    )
                ])
            else:
                rows.append([
                    InlineKeyboardButton(
                        f"{status_label.get(status, '•')} · {title}",
                        callback_data="noop",
                    )
                ])
    rows.append([InlineKeyboardButton("⬅️ В мой кабинет", callback_data="help:me")])
    rows.append([InlineKeyboardButton("🏠 Главное меню", callback_data="help:main")])
    return InlineKeyboardMarkup(rows)


def kb_no_profile_for_account(can_create: bool):
    rows = []
    if can_create:
        rows.append([InlineKeyboardButton("➕ Создать мою анкету", callback_data="help:team:create_profile")])
    rows.append([InlineKeyboardButton("👥 Открыть команду", callback_data="help:team")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="help:main")])
    return InlineKeyboardMarkup(rows)


NOMINATION_PAGE_SIZE = 8


def kb_nomination_intro():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🙌 Номинировать коллегу", callback_data="help:nomination:start")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="help:main")],
    ])


def kb_nomination_people(page: int, exclude_profile_id: int):
    people = [(pid, name) for pid, name in db_profiles_list() if int(pid) != int(exclude_profile_id)]
    total_pages = max(1, (len(people) + NOMINATION_PAGE_SIZE - 1) // NOMINATION_PAGE_SIZE)
    page = max(0, min(int(page), total_pages - 1))
    chunk = people[page * NOMINATION_PAGE_SIZE:(page + 1) * NOMINATION_PAGE_SIZE]

    rows = []
    for i in range(0, len(chunk), 2):
        row = []
        for pid, name in chunk[i:i + 2]:
            label = name if len(name) <= 24 else name[:21] + "…"
            row.append(InlineKeyboardButton(label, callback_data=f"help:nomination:pick:{pid}:{page}"))
        rows.append(row)
    if not chunk:
        rows.append([InlineKeyboardButton("— других анкет пока нет —", callback_data="noop")])

    if total_pages > 1:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("◀️", callback_data=f"help:nomination:page:{page - 1}"))
        nav.append(InlineKeyboardButton(f"{page + 1} / {total_pages}", callback_data="noop"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton("▶️", callback_data=f"help:nomination:page:{page + 1}"))
        rows.append(nav)

    rows.append([InlineKeyboardButton("❌ Отмена", callback_data="help:nomination:cancel")])
    return InlineKeyboardMarkup(rows)


def kb_nomination_categories():
    rows = []
    items = list(NOMINATION_CATEGORIES.items())
    for i in range(0, len(items), 2):
        row = []
        for key, item in items[i:i + 2]:
            row.append(
                InlineKeyboardButton(
                    f"{item['emoji']} {item['short']}",
                    callback_data=f"help:nomination:category:{key}",
                )
            )
        rows.append(row)
    rows.append([InlineKeyboardButton("❌ Отмена", callback_data="help:nomination:cancel")])
    return InlineKeyboardMarkup(rows)


def kb_achievement_reactions(award_id: int):
    counts = db_achievement_reaction_counts(int(award_id))
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(
            f"{ACHIEVEMENT_REACTIONS['clap']} {counts.get('clap', 0)}",
            callback_data=f"help:achievement:react:{int(award_id)}:clap",
        ),
        InlineKeyboardButton(
            f"{ACHIEVEMENT_REACTIONS['fire']} {counts.get('fire', 0)}",
            callback_data=f"help:achievement:react:{int(award_id)}:fire",
        ),
        InlineKeyboardButton(
            f"{ACHIEVEMENT_REACTIONS['heart']} {counts.get('heart', 0)}",
            callback_data=f"help:achievement:react:{int(award_id)}:heart",
        ),
    ]])


def kb_notifications(user_id: int, page: int = 0):
    data = db_notifications_list(int(user_id), page=page, page_size=8)
    rows = []
    for item in data["items"]:
        title = item["title"] if len(item["title"]) <= 45 else item["title"][:42] + "…"
        rows.append([
            InlineKeyboardButton(
                f"🔴 {title}",
                callback_data=f"help:notifications:open:{item['id']}:{data['page']}",
            )
        ])
    if not data["items"]:
        rows.append([InlineKeyboardButton("— уведомлений пока нет —", callback_data="noop")])
    if data["total_pages"] > 1:
        nav = []
        if data["page"] > 0:
            nav.append(InlineKeyboardButton("◀️", callback_data=f"help:notifications:page:{data['page'] - 1}"))
        nav.append(InlineKeyboardButton(f"{data['page'] + 1} / {data['total_pages']}", callback_data="noop"))
        if data["page"] < data["total_pages"] - 1:
            nav.append(InlineKeyboardButton("▶️", callback_data=f"help:notifications:page:{data['page'] + 1}"))
        rows.append(nav)
    if db_notifications_unread_count(int(user_id)):
        rows.append([InlineKeyboardButton("🧹 Прочитать и очистить все", callback_data="help:notifications:read_all")])
    rows.append([InlineKeyboardButton("⬅️ Главное меню", callback_data="help:main")])
    return InlineKeyboardMarkup(rows)


def kb_danger_confirm(confirm_callback: str, cancel_callback: str, confirm_text: str = "🗑 Да, продолжить"):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(confirm_text, callback_data=confirm_callback)],
        [InlineKeyboardButton("↩️ Отмена", callback_data=cancel_callback)],
    ])


def kb_profile_photo_step(has_current_photo: bool = False):
    rows = []
    if has_current_photo:
        rows.append([InlineKeyboardButton("✅ Оставить текущее фото", callback_data="help:profile:photo:keep")])
        rows.append([InlineKeyboardButton("🗑 Удалить текущее фото", callback_data="help:profile:photo:remove")])
    else:
        rows.append([InlineKeyboardButton("⏭ Без фото", callback_data="help:profile:photo:skip")])
    rows.append([InlineKeyboardButton("❌ Отмена", callback_data="help:flow:cancel")])
    return InlineKeyboardMarkup(rows)


def kb_nomination_cancel():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("❌ Отменить номинацию", callback_data="help:nomination:cancel")]
    ])


def kb_nomination_admin_actions(nomination_id: int):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Одобрить", callback_data=f"help:nomination:admin:approve:{nomination_id}"),
            InlineKeyboardButton("❌ Отклонить", callback_data=f"help:nomination:admin:reject:{nomination_id}"),
        ]
    ])


def kb_pending_nominations():
    items = db_nominations_pending(30)
    rows = []
    if not items:
        rows.append([InlineKeyboardButton("— новых номинаций нет —", callback_data="noop")])
    else:
        for item in items:
            category = nomination_category(item.get("category_key"))
            label = f"{category['emoji']} {item['nominee_name']} ← {item['nominator_name']}"
            if len(label) > 48:
                label = label[:45] + "…"
            rows.append([
                InlineKeyboardButton(label, callback_data=f"help:nomination:admin:open:{item['id']}")
            ])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="help:settings:ach")])
    return InlineKeyboardMarkup(rows)


def kb_achievement_level_select():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🥉 Уровень I", callback_data="help:settings:ach:level:1"),
            InlineKeyboardButton("🥈 Уровень II", callback_data="help:settings:ach:level:2"),
        ],
        [InlineKeyboardButton("🥇 Уровень III", callback_data="help:settings:ach:level:3")],
        [InlineKeyboardButton("❌ Отмена", callback_data="help:settings:cancel")],
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


def kb_send_timing(prefix: str):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🚀 Отправить сейчас", callback_data=f"{prefix}:timing:now")],
        [InlineKeyboardButton("🕒 Запланировать время", callback_data=f"{prefix}:timing:later")],
        [InlineKeyboardButton("❌ Отмена", callback_data=f"{prefix}:cancel")],
    ])


def kb_bcast_heading_choice():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✍️ Ввести тему", callback_data="help:settings:bcast:heading:topic")],
        [InlineKeyboardButton("🏷 Выбрать сохранённый тег", callback_data="help:settings:bcast:heading:tag")],
        [InlineKeyboardButton("➖ Без темы и тега", callback_data="help:settings:bcast:heading:none")],
        [InlineKeyboardButton("❌ Отмена", callback_data="help:settings:bcast:cancel")],
    ])


def kb_bcast_tag_pick():
    rows = []
    for item in db_broadcast_tags_list()[:40]:
        rows.append([
            InlineKeyboardButton(
                f"#{item['name']}",
                callback_data=f"help:settings:bcast:tag:{item['id']}",
            )
        ])
    if not rows:
        rows.append([InlineKeyboardButton("— сохранённых тегов нет —", callback_data="noop")])
    rows.append([InlineKeyboardButton("➕ Создать новый тег", callback_data="help:settings:bcast:tag_create")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="help:settings:bcast")])
    return InlineKeyboardMarkup(rows)


def kb_broadcast_tags_manage():
    rows = [[InlineKeyboardButton("➕ Создать тег", callback_data="help:settings:bcast_tags:add")]]
    tags = db_broadcast_tags_list()
    for item in tags[:40]:
        rows.append([
            InlineKeyboardButton(
                f"🗑 #{item['name']}",
                callback_data=f"help:settings:bcast_tags:del:{item['id']}",
            )
        ])
    if not tags:
        rows.append([InlineKeyboardButton("— тегов пока нет —", callback_data="noop")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="help:settings:communications")])
    return InlineKeyboardMarkup(rows)


def kb_bcast_files_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Продолжить", callback_data="help:settings:bcast:send")],
        [InlineKeyboardButton("🗑️ Очистить файлы", callback_data="help:settings:bcast:clear_files")],
        [InlineKeyboardButton("❌ Отмена", callback_data="help:settings:bcast:cancel")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="help:settings:communications")],
    ])


def kb_meeting_recipient_mode():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💬 Общий чат", callback_data="help:settings:meeting:recipients:chats")],
        [InlineKeyboardButton("👥 Выбрать сотрудников", callback_data="help:settings:meeting:recipients:profiles:0")],
        [InlineKeyboardButton("❌ Отмена", callback_data="help:settings:meeting:cancel")],
    ])


def kb_meeting_profile_picker(selected: set[int], page: int = 0):
    people = db_profiles_list_for_delivery()
    page_size = 8
    pages = max(1, (len(people) + page_size - 1) // page_size)
    page = max(0, min(int(page), pages - 1))
    rows = []
    for item in people[page * page_size:(page + 1) * page_size]:
        pid = int(item["id"])
        checked = "✅" if pid in selected else "▫️"
        delivery = "" if item.get("tg_user_id") else " ⚠️"
        rows.append([
            InlineKeyboardButton(
                f"{checked} {item['full_name'][:45]}{delivery}",
                callback_data=f"help:settings:meeting:profile_toggle:{pid}:{page}",
            )
        ])
    if not people:
        rows.append([InlineKeyboardButton("— анкет сотрудников нет —", callback_data="noop")])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀️", callback_data=f"help:settings:meeting:recipients:profiles:{page-1}"))
    nav.append(InlineKeyboardButton(f"{page+1}/{pages}", callback_data="noop"))
    if page + 1 < pages:
        nav.append(InlineKeyboardButton("▶️", callback_data=f"help:settings:meeting:recipients:profiles:{page+1}"))
    rows.append(nav)
    rows.append([
        InlineKeyboardButton(
            f"✅ Готово ({len(selected)})",
            callback_data="help:settings:meeting:profiles_done",
        )
    ])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="help:settings:meeting:recipients_back")])
    return InlineKeyboardMarkup(rows)

def kb_help_docs_categories():
    cats = db_docs_list_categories()
    rows = []
    if not cats:
        rows.append([InlineKeyboardButton("— категорий нет —", callback_data="noop")])
    else:
        for cid, title in cats:
            rows.append([InlineKeyboardButton(title, callback_data=f"help:docs:cat:{cid}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="help:docs")])
    return InlineKeyboardMarkup(rows)

FAQ_CARDS_PER_PAGE = 5
FAQ_PAGE_TEXT_LIMIT = 3300
FAQ_SINGLE_CARD_TEXT_LIMIT = 3050


def ru_word_form(number: int, one: str, few: str, many: str) -> str:
    """Return the correct Russian noun form for an integer count."""
    number = abs(int(number))
    last_two = number % 100
    if 11 <= last_two <= 14:
        return many

    last = number % 10
    if last == 1:
        return one
    if 2 <= last <= 4:
        return few
    return many


def faq_question_count(count: int) -> str:
    """Examples: 1 вопрос, 2 вопроса, 5 вопросов, 21 вопрос."""
    return f"{int(count)} {ru_word_form(count, 'вопрос', 'вопроса', 'вопросов')}"


def faq_plain_text(value: str | None) -> str:
    """Telegram HTML -> readable plain text for search and safe length checks."""
    value = value or ""
    plain = re.sub(r"(?is)<[^>]+>", " ", value)
    plain = html_lib.unescape(plain)
    plain = re.sub(r"[ \t\r\f\v]+", " ", plain)
    plain = re.sub(r"\n{3,}", "\n\n", plain)
    return plain.strip()


def faq_search_items(query: str) -> list[dict]:
    """Search all query words in both the question and the answer."""
    tokens = [part.casefold() for part in re.findall(r"\S+", query or "")]
    if not tokens:
        return []

    result: list[dict] = []
    for item in db_faq_list_full():
        haystack = (
            faq_plain_text(item.get("question"))
            + "\n"
            + faq_plain_text(item.get("answer"))
        ).casefold()
        if all(token in haystack for token in tokens):
            result.append(item)
    return result


def faq_split_plain_text(value: str, limit: int) -> list[str]:
    """Split oversized plain text without cutting words where possible."""
    clean = re.sub(r"\n{3,}", "\n\n", (value or "").strip())
    if not clean:
        return [""]
    if len(clean) <= limit:
        return [clean]

    parts: list[str] = []
    remaining = clean
    while remaining:
        if len(remaining) <= limit:
            parts.append(remaining.strip())
            break

        cut = remaining.rfind("\n\n", 0, limit + 1)
        if cut < max(100, limit // 3):
            cut = remaining.rfind("\n", 0, limit + 1)
        if cut < max(100, limit // 3):
            cut = remaining.rfind(" ", 0, limit + 1)
        if cut <= 0:
            cut = limit

        parts.append(remaining[:cut].strip())
        remaining = remaining[cut:].lstrip()

    return [part for part in parts if part] or [""]


def faq_card_html(number: int, item: dict) -> list[str]:
    """
    Build one FAQ card. Normally it returns one HTML block.
    An exceptionally long answer is split into continuation blocks so Telegram's
    message limit is never reached. The full question remains on the first block.
    """
    question_html = (item.get("question") or "Без названия").strip()
    answer_html = (item.get("answer") or "Ответ пока не указан.").strip()
    question_plain = faq_plain_text(question_html) or "Без названия"
    answer_plain = faq_plain_text(answer_html) or "Ответ пока не указан."

    normal = (
        f"❓ <b>Вопрос {number}</b>\n"
        f"<b>{html_lib.escape(question_plain)}</b>\n\n"
        f"💬 <b>Ответ</b>\n"
        f"{answer_html}"
    )
    if len(faq_plain_text(normal)) <= FAQ_SINGLE_CARD_TEXT_LIMIT:
        return [normal]

    # For an oversized entry, use escaped plain text while splitting. This keeps
    # every generated HTML message valid even when the stored answer has entities.
    first_prefix = (
        f"❓ <b>Вопрос {number}</b>\n"
        f"<b>{html_lib.escape(question_plain)}</b>\n\n"
        f"💬 <b>Ответ</b>\n"
    )
    first_budget = max(
        500,
        FAQ_SINGLE_CARD_TEXT_LIMIT - len(faq_plain_text(first_prefix)) - 80,
    )
    answer_parts = faq_split_plain_text(answer_plain, first_budget)

    blocks = [first_prefix + html_lib.escape(answer_parts[0])]
    continuation_budget = FAQ_SINGLE_CARD_TEXT_LIMIT - 120
    remaining = "\n\n".join(answer_parts[1:])
    if remaining:
        for index, part in enumerate(
            faq_split_plain_text(remaining, continuation_budget),
            start=2,
        ):
            blocks.append(
                f"💬 <b>Продолжение ответа на вопрос {number} · часть {index}</b>\n\n"
                f"{html_lib.escape(part)}"
            )
    return blocks


def faq_pack_pages(items: list[dict]) -> list[list[str]]:
    """Pack complete cards into pages by both card count and text length."""
    blocks: list[str] = []
    for number, item in enumerate(items, start=1):
        blocks.extend(faq_card_html(number, item))

    if not blocks:
        return [[]]

    pages: list[list[str]] = []
    current: list[str] = []
    current_length = 0

    for block in blocks:
        block_length = len(faq_plain_text(block))
        separator_length = 24 if current else 0
        should_break = bool(current) and (
            len(current) >= FAQ_CARDS_PER_PAGE
            or current_length + separator_length + block_length > FAQ_PAGE_TEXT_LIMIT
        )
        if should_break:
            pages.append(current)
            current = []
            current_length = 0

        current.append(block)
        current_length += separator_length + block_length

    if current:
        pages.append(current)
    return pages or [[]]


def build_help_faq_menu() -> tuple[str, InlineKeyboardMarkup]:
    """Main FAQ screen without a separate button for every question."""
    count = len(db_faq_list_full())
    count_line = (
        f"В базе знаний: <b>{count}</b> "
        f"{ru_word_form(count, 'вопрос', 'вопроса', 'вопросов')}"
        if count
        else "Пока вопросов и ответов нет."
    )
    text = (
        "❓ <b>FAQ</b>\n\n"
        f"{count_line}\n\n"
        "Откройте ответы, чтобы увидеть вопросы целиком вместе с ответами, "
        "или воспользуйтесь поиском."
    )
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(
            "📚 Ответы на вопросы",
            callback_data="help:faq:answers:0",
        )],
        [InlineKeyboardButton(
            "🔎 Найти ответ",
            callback_data="help:faq:search",
        )],
        [InlineKeyboardButton("⬅️ Назад", callback_data="help:main")],
    ])
    return text, keyboard


def build_help_faq_cards_page(
    items: list[dict],
    page: int = 0,
    *,
    title: str = "📚 Ответы на вопросы",
    subtitle: str | None = None,
    callback_prefix: str = "help:faq:answers",
    show_search: bool = True,
) -> tuple[str, InlineKeyboardMarkup]:
    pages = faq_pack_pages(items)
    total_pages = max(1, len(pages))
    page = max(0, min(int(page), total_pages - 1))
    page_blocks = pages[page]

    text_lines = [f"<b>{title}</b>"]
    if subtitle:
        text_lines.extend(["", subtitle])
    if items:
        text_lines.extend([
            "",
            f"Страница <b>{page + 1}</b> из <b>{total_pages}</b> · "
            f"всего: <b>{len(items)}</b> "
            f"{ru_word_form(len(items), 'вопрос', 'вопроса', 'вопросов')}",
            "",
        ])
        text_lines.append("\n\n━━━━━━━━━━━━━━\n\n".join(page_blocks))
    else:
        text_lines.extend(["", "Ничего не найдено."])

    rows: list[list[InlineKeyboardButton]] = []
    if total_pages > 1:
        nav_row: list[InlineKeyboardButton] = []
        if page > 0:
            nav_row.append(InlineKeyboardButton(
                "⬅️ Предыдущая",
                callback_data=f"{callback_prefix}:{page - 1}",
            ))
        nav_row.append(InlineKeyboardButton(
            f"{page + 1}/{total_pages}",
            callback_data="noop",
        ))
        if page < total_pages - 1:
            nav_row.append(InlineKeyboardButton(
                "Следующая ➡️",
                callback_data=f"{callback_prefix}:{page + 1}",
            ))
        rows.append(nav_row)

    if show_search:
        rows.append([InlineKeyboardButton(
            "🔎 Найти ответ",
            callback_data="help:faq:search",
        )])
    else:
        rows.append([InlineKeyboardButton(
            "📚 Все вопросы",
            callback_data="help:faq:answers:0",
        )])
        rows.append([InlineKeyboardButton(
            "🔎 Новый поиск",
            callback_data="help:faq:search",
        )])

    rows.append([InlineKeyboardButton("⬅️ В FAQ", callback_data="help:faq")])
    rows.append([InlineKeyboardButton("🏠 В главное меню", callback_data="help:main")])
    return "\n".join(text_lines).rstrip(), InlineKeyboardMarkup(rows)


def build_help_faq_answers_page(page: int = 0) -> tuple[str, InlineKeyboardMarkup]:
    return build_help_faq_cards_page(
        db_faq_list_full(),
        page,
        callback_prefix="help:faq:answers",
        show_search=True,
    )


def build_help_faq_search_page(
    query: str,
    page: int = 0,
) -> tuple[str, InlineKeyboardMarkup]:
    items = faq_search_items(query)
    return build_help_faq_cards_page(
        items,
        page,
        title="🔎 Результаты поиска",
        subtitle=f"Запрос: <b>{html_lib.escape(query)}</b>",
        callback_prefix="help:faq:search_results",
        show_search=False,
    )


def build_help_faq_page(page: int = 0) -> tuple[str, InlineKeyboardMarkup]:
    """Backward compatibility with previous FAQ list callbacks."""
    return build_help_faq_answers_page(page)


def kb_help_faq_list(page: int = 0):
    """Backward compatibility: return the dynamic FAQ cards keyboard."""
    _text, keyboard = build_help_faq_answers_page(page)
    return keyboard


def kb_help_faq_item(page: int = 0):
    """Keyboard for old messages where a question opened separately."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(
            "⬅️ К ответам на вопросы",
            callback_data=f"help:faq:answers:{max(0, int(page))}",
        )],
        [InlineKeyboardButton("🏠 В главное меню", callback_data="help:main")],
    ])

def kb_help_docs_files(category_id: int):
    items = db_docs_list_by_category(category_id)
    rows = []
    if not items:
        rows.append([InlineKeyboardButton("— файлов нет —", callback_data="noop")])
    else:
        for did, title in items[:40]:
            rows.append([InlineKeyboardButton(title, callback_data=f"help:docs:open:{did}")])
    rows.append([InlineKeyboardButton("⬅️ Назад к категориям", callback_data="help:docs")])
    rows.append([InlineKeyboardButton("🏠 В главное меню", callback_data="help:main")])
    return InlineKeyboardMarkup(rows)



def kb_help_docs_main(is_admin_user: bool):
    rows = [
        [InlineKeyboardButton("🔎 Найти документ", callback_data="help:docs:search")],
        [
            InlineKeyboardButton("⭐ Избранное", callback_data="help:docs:favorites"),
            InlineKeyboardButton("🕘 Недавние", callback_data="help:docs:recent"),
        ],
        [InlineKeyboardButton("🆕 Новые документы", callback_data="help:docs:new")],
        [InlineKeyboardButton("📂 Все категории", callback_data="help:docs:categories")],
        [InlineKeyboardButton("🎓 Подборки", callback_data="help:docs:collections")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="help:main")],
    ]
    if is_admin_user:
        rows.extend([
            [InlineKeyboardButton("➕ Добавить документ", callback_data="help:settings:add_doc")],
            [InlineKeyboardButton("✏️ Редактировать документ", callback_data="help:docs:admin:edit")],
            [InlineKeyboardButton("🔄 Заменить файл", callback_data="help:docs:admin:replace")],
            [InlineKeyboardButton("➖ Удалить документ", callback_data="help:docs:admin:delete")],
            [InlineKeyboardButton("🗂 Управление категориями", callback_data="help:settings:cats")],
            [InlineKeyboardButton("🏷 Управление тегами", callback_data="help:docs:admin:tags")],
            [InlineKeyboardButton("🎓 Управление подборками", callback_data="help:docs:admin:collections")],
        ])
    return InlineKeyboardMarkup(rows)


def kb_docs_result_list(items: list[dict], empty_text: str = "— документов нет —", back_cb: str = "help:docs"):
    rows = []
    if not items:
        rows.append([InlineKeyboardButton(empty_text, callback_data="noop")])
    else:
        for item in items[:40]:
            title = str(item.get("title") or "Документ")
            category = str(item.get("category_title") or "")
            label = f"📄 {title}"
            if category:
                label += f" · {category}"
            if len(label) > 60:
                label = label[:57] + "…"
            rows.append([InlineKeyboardButton(label, callback_data=f"help:docs:open:{int(item['id'])}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=back_cb)])
    return InlineKeyboardMarkup(rows)


def kb_doc_card(doc_id: int, user_id: int | None, back_cb: str = "help:docs"):
    fav = db_doc_is_favorite(user_id, doc_id)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📥 Получить файл", callback_data=f"help:docs:download:{doc_id}")],
        [InlineKeyboardButton("★ Убрать из избранного" if fav else "⭐ В избранное", callback_data=f"help:docs:favorite:{doc_id}")],
        [InlineKeyboardButton("⬅️ Назад к списку", callback_data=back_cb)],
        [InlineKeyboardButton("🏠 Документы", callback_data="help:docs")],
    ])


def kb_doc_collections(back_cb: str = "help:docs"):
    collections = db_doc_collections_list()
    rows = []
    for item in collections:
        rows.append([
            InlineKeyboardButton(
                f"🎓 {item['title']} · {item['count']}",
                callback_data=f"help:docs:collection:{item['id']}",
            )
        ])
    if not rows:
        rows.append([InlineKeyboardButton("— подборок пока нет —", callback_data="noop")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=back_cb)])
    return InlineKeyboardMarkup(rows)


def kb_doc_admin_picker(action: str, back_cb: str = "help:docs"):
    items = db_docs_list_all(60)
    rows = []
    icons = {"edit": "✏️", "replace": "🔄", "delete": "🗑"}
    icon = icons.get(action, "📄")
    for item in items:
        label = f"{icon} {item['title']} · {item['category_title']}"
        if len(label) > 60:
            label = label[:57] + "…"
        rows.append([
            InlineKeyboardButton(label, callback_data=f"help:docs:admin:{action}:{item['id']}")
        ])
    if not rows:
        rows.append([InlineKeyboardButton("— документов нет —", callback_data="noop")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=back_cb)])
    return InlineKeyboardMarkup(rows)


def kb_doc_edit_menu(doc_id: int):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✏️ Название", callback_data=f"help:docs:admin:editfield:title:{doc_id}")],
        [InlineKeyboardButton("📝 Описание", callback_data=f"help:docs:admin:editfield:description:{doc_id}")],
        [InlineKeyboardButton("📂 Категория", callback_data=f"help:docs:admin:editfield:category:{doc_id}")],
        [InlineKeyboardButton("🏷 Теги", callback_data=f"help:docs:admin:editfield:tags:{doc_id}")],
        [InlineKeyboardButton("⬅️ К списку", callback_data="help:docs:admin:edit")],
        [InlineKeyboardButton("🏠 Документы", callback_data="help:docs")],
    ])


def kb_doc_category_picker(doc_id: int):
    rows = [
        [InlineKeyboardButton(title, callback_data=f"help:docs:admin:setcat:{doc_id}:{cid}")]
        for cid, title in db_docs_list_categories()
    ]
    if not rows:
        rows.append([InlineKeyboardButton("— категорий нет —", callback_data="noop")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"help:docs:admin:edit:{doc_id}")])
    return InlineKeyboardMarkup(rows)


def kb_doc_tag_picker(doc_id: int):
    assigned = {item["id"] for item in db_doc_get_tags(doc_id)}
    rows = []
    for tag in db_doc_tags_list():
        mark = "✅" if tag["id"] in assigned else "▫️"
        rows.append([
            InlineKeyboardButton(
                f"{mark} #{tag['title']}",
                callback_data=f"help:docs:admin:tagtoggle:{doc_id}:{tag['id']}",
            )
        ])
    if not rows:
        rows.append([InlineKeyboardButton("— сначала создайте теги —", callback_data="noop")])
    rows.append([InlineKeyboardButton("✅ Готово", callback_data=f"help:docs:admin:edit:{doc_id}")])
    return InlineKeyboardMarkup(rows)


def kb_doc_tags_manage():
    rows = [[InlineKeyboardButton("➕ Создать тег", callback_data="help:docs:admin:tags:add")]]
    for tag in db_doc_tags_list():
        rows.append([
            InlineKeyboardButton(
                f"🗑 #{tag['title']} · {tag['count']}",
                callback_data=f"help:docs:admin:tags:delete:{tag['id']}",
            )
        ])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="help:docs")])
    return InlineKeyboardMarkup(rows)


def kb_doc_collections_manage():
    rows = [[InlineKeyboardButton("➕ Создать подборку", callback_data="help:docs:admin:collections:add")]]
    for item in db_doc_collections_list():
        rows.append([
            InlineKeyboardButton(
                f"🎓 {item['title']} · {item['count']}",
                callback_data=f"help:docs:admin:collection:{item['id']}",
            )
        ])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="help:docs")])
    return InlineKeyboardMarkup(rows)


def kb_doc_collection_manage(collection_id: int):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Добавить документы", callback_data=f"help:docs:admin:collection:addlist:{collection_id}")],
        [InlineKeyboardButton("➖ Убрать документы", callback_data=f"help:docs:admin:collection:removelist:{collection_id}")],
        [InlineKeyboardButton("🗑 Удалить подборку", callback_data=f"help:docs:admin:collection:delete:{collection_id}")],
        [InlineKeyboardButton("⬅️ К подборкам", callback_data="help:docs:admin:collections")],
    ])


def kb_doc_collection_doc_picker(collection_id: int, mode: str):
    current = {item["id"] for item in db_doc_collection_items(collection_id)}
    source = db_docs_list_all(100)
    if mode == "add":
        source = [item for item in source if item["id"] not in current]
        prefix = "➕"
    else:
        source = [item for item in source if item["id"] in current]
        prefix = "➖"
    rows = []
    for item in source[:60]:
        label = f"{prefix} {item['title']} · {item['category_title']}"
        if len(label) > 60:
            label = label[:57] + "…"
        rows.append([
            InlineKeyboardButton(
                label,
                callback_data=f"help:docs:admin:collection:{mode}:{collection_id}:{item['id']}",
            )
        ])
    if not rows:
        rows.append([InlineKeyboardButton("— подходящих документов нет —", callback_data="noop")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"help:docs:admin:collection:{collection_id}")])
    return InlineKeyboardMarkup(rows)


def build_doc_card_text(doc: dict) -> str:
    tags = db_doc_get_tags(int(doc["id"]))
    tags_text = " ".join(f"#{escape(t['title'])}" for t in tags) if tags else "—"
    description = (doc.get("description") or "").strip()
    uploaded = _format_short_date(doc.get("uploaded_at"))
    updated = _format_short_date(doc.get("updated_at"))
    text = (
        f"📄 <b>{escape(doc.get('title') or 'Документ')}</b>\n\n"
        f"📂 Категория: <b>{escape(doc.get('category_title') or 'Без категории')}</b>\n"
        f"🏷 Теги: {tags_text}\n"
        f"📅 Добавлен: {escape(uploaded)}"
    )
    if updated != uploaded:
        text += f"\n🔄 Обновлён: {escape(updated)}"
    if description:
        text += f"\n\n{escape(description)}"
    return text

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

# ---------------- TEAM CATALOG: pagination + profile carousel ----------------

TEAM_PAGE_SIZE = 8
TEAM_COLUMNS = 2
BIRTHDAY_PERIOD_DAYS = 60
BIRTHDAY_COUNTER_DAYS = 30
BIRTHDAY_MAX_OFFSET_DAYS = 300

RU_MONTHS_GENITIVE = (
    "",
    "января",
    "февраля",
    "марта",
    "апреля",
    "мая",
    "июня",
    "июля",
    "августа",
    "сентября",
    "октября",
    "ноября",
    "декабря",
)


def _parse_birthday_ddmm(value: str | None) -> tuple[int, int] | None:
    """Проверяет дату ДД.ММ; 29.02 считается корректной датой."""
    text = (value or "").strip()
    match = re.fullmatch(r"(\d{2})\.(\d{2})", text)
    if not match:
        return None
    day = int(match.group(1))
    month = int(match.group(2))
    try:
        # 2000 — високосный год, поэтому 29.02 проходит проверку.
        date(2000, month, day)
    except ValueError:
        return None
    return day, month


def _birthday_occurrences(start_day: date, end_day: date) -> list[dict]:
    """Собирает дни рождения сотрудников в заданном включительном периоде."""
    events: list[dict] = []
    for profile in db_profiles_with_birthdays():
        parsed = _parse_birthday_ddmm(profile.get("birthday"))
        if not parsed:
            continue
        day, month = parsed
        for year in range(start_day.year, end_day.year + 1):
            try:
                event_day = date(year, month, day)
            except ValueError:
                # Например, 29 февраля в невисокосном году.
                continue
            if start_day <= event_day <= end_day:
                events.append({
                    "profile_id": int(profile["id"]),
                    "full_name": profile["full_name"],
                    "birthday": profile["birthday"],
                    "event_date": event_day,
                })
    events.sort(key=lambda item: (item["event_date"], item["full_name"].casefold()))
    return events


def upcoming_birthdays(offset_days: int = 0, period_days: int = BIRTHDAY_PERIOD_DAYS) -> dict:
    """Возвращает события и границы периода относительно текущей даты по Москве."""
    offset = max(0, min(int(offset_days), BIRTHDAY_MAX_OFFSET_DAYS))
    period = max(1, int(period_days))
    today = datetime.now(MOSCOW_TZ).date()
    start_day = today + timedelta(days=offset)
    end_day = start_day + timedelta(days=period - 1)
    return {
        "today": today,
        "offset": offset,
        "start": start_day,
        "end": end_day,
        "events": _birthday_occurrences(start_day, end_day),
    }


def upcoming_birthdays_count(period_days: int = BIRTHDAY_COUNTER_DAYS) -> int:
    return len(upcoming_birthdays(offset_days=0, period_days=period_days)["events"])


def _birthday_date_text(value: date, include_year: bool = False) -> str:
    text = f"{value.day} {RU_MONTHS_GENITIVE[value.month]}"
    if include_year:
        text += f" {value.year}"
    return text


def build_upcoming_birthdays_text(offset_days: int = 0) -> tuple[str, list[dict], int]:
    data = upcoming_birthdays(offset_days=offset_days)
    events = data["events"]
    today = data["today"]
    start_day = data["start"]
    end_day = data["end"]
    offset = int(data["offset"])

    period_label = (
        f"{_birthday_date_text(start_day, include_year=start_day.year != today.year)} — "
        f"{_birthday_date_text(end_day, include_year=end_day.year != start_day.year)}"
    )
    lines = [
        "🎂 <b>Ближайшие дни рождения</b>",
        "",
        "Здесь собраны дни рождения нашей команды.",
        f"Период: <b>{escape(period_label)}</b>",
    ]

    if not events:
        lines.extend(["", f"В ближайшие {BIRTHDAY_PERIOD_DAYS} дней дней рождения в команде нет."])
        return "\n".join(lines), events, offset

    groups: list[tuple[str, list[dict]]] = []
    used_ids: set[int] = set()

    def add_group(title: str, predicate):
        matched_indexes = [
            index
            for index, item in enumerate(events)
            if index not in used_ids and predicate(item)
        ]
        if matched_indexes:
            used_ids.update(matched_indexes)
            groups.append((title, [events[index] for index in matched_indexes]))

    if offset == 0:
        tomorrow = today + timedelta(days=1)
        end_of_week = today + timedelta(days=6 - today.weekday())
        add_group("🔥 <b>Сегодня</b>", lambda item: item["event_date"] == today)
        add_group("⏰ <b>Завтра</b>", lambda item: item["event_date"] == tomorrow)
        add_group(
            "📆 <b>На этой неделе</b>",
            lambda item: tomorrow < item["event_date"] <= end_of_week,
        )

    remaining = [item for index, item in enumerate(events) if index not in used_ids]
    if remaining:
        groups.append(("🗓 <b>Позже</b>" if offset == 0 else "🗓 <b>Дни рождения</b>", remaining))

    for title, items in groups:
        lines.extend(["", title, ""])
        for item in items:
            if item["event_date"] == today:
                lines.append(f"🎉 {escape(item['full_name'])}")
            elif item["event_date"] == today + timedelta(days=1):
                lines.append(f"🎂 {escape(item['full_name'])}")
            else:
                lines.append(
                    f"🎂 {_birthday_date_text(item['event_date'])} — {escape(item['full_name'])}"
                )

    return "\n".join(lines), events, offset


def compact_team_name(name: str, limit: int = 22) -> str:
    """Укорачивает подпись кнопки, чтобы две кнопки помещались в одну строку."""
    value = re.sub(r"\s+", " ", (name or "Без имени").strip())
    if len(value) <= limit:
        return value
    return value[: max(1, limit - 1)].rstrip() + "…"


def _team_total_pages(people_count: int) -> int:
    return max(1, (int(people_count) + TEAM_PAGE_SIZE - 1) // TEAM_PAGE_SIZE)


def _team_clamp_page(page: int, people_count: int) -> int:
    total_pages = _team_total_pages(people_count)
    return max(0, min(int(page), total_pages - 1))


def kb_help_team(page: int = 0, can_create_profile: bool = False):
    """Компактный каталог: 8 сотрудников на странице, по 2 кнопки в строке."""
    people = db_profiles_list()
    page = _team_clamp_page(page, len(people))
    total_pages = _team_total_pages(len(people))

    start = page * TEAM_PAGE_SIZE
    page_people = people[start:start + TEAM_PAGE_SIZE]

    rows = []

    if can_create_profile:
        rows.append([
            InlineKeyboardButton(
                "➕ Создать анкету",
                callback_data="help:team:create_profile",
            )
        ])

    if not page_people:
        rows.append([
            InlineKeyboardButton(
                "— анкет пока нет —",
                callback_data="noop",
            )
        ])
    else:
        for index in range(0, len(page_people), TEAM_COLUMNS):
            keyboard_row = []
            for pid, name in page_people[index:index + TEAM_COLUMNS]:
                keyboard_row.append(
                    InlineKeyboardButton(
                        compact_team_name(name),
                        callback_data=f"help:team:person:{pid}:{page}",
                    )
                )
            rows.append(keyboard_row)

    # Навигация появляется только тогда, когда страниц больше одной.
    if total_pages > 1:
        navigation_row = []

        if page > 0:
            navigation_row.append(
                InlineKeyboardButton(
                    "◀️",
                    callback_data=f"help:team:page:{page - 1}",
                )
            )

        navigation_row.append(
            InlineKeyboardButton(
                f"{page + 1} / {total_pages}",
                callback_data="noop",
            )
        )

        if page < total_pages - 1:
            navigation_row.append(
                InlineKeyboardButton(
                    "▶️",
                    callback_data=f"help:team:page:{page + 1}",
                )
            )

        rows.append(navigation_row)

    birthday_count = upcoming_birthdays_count(BIRTHDAY_COUNTER_DAYS)
    birthday_label = "🎂 Ближайшие дни рождения"
    if birthday_count:
        birthday_label += f" · {birthday_count}"
    rows.append([
        InlineKeyboardButton(
            birthday_label,
            callback_data="help:team:birthdays:0",
        )
    ])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="help:main")])
    return InlineKeyboardMarkup(rows)


def kb_upcoming_birthdays(events: list[dict], offset_days: int = 0):
    offset = max(0, min(int(offset_days), BIRTHDAY_MAX_OFFSET_DAYS))
    rows = []

    for event in events:
        label = (
            f"🎂 {event['event_date'].strftime('%d.%m')} · "
            f"{compact_team_name(event['full_name'], 34)}"
        )
        rows.append([
            InlineKeyboardButton(
                label,
                callback_data=f"help:team:birthday_person:{int(event['profile_id'])}:{offset}",
            )
        ])

    navigation = []
    if offset > 0:
        navigation.append(
            InlineKeyboardButton(
                "◀️ Предыдущие 60 дней",
                callback_data=f"help:team:birthdays:{max(0, offset - BIRTHDAY_PERIOD_DAYS)}",
            )
        )
    if offset + BIRTHDAY_PERIOD_DAYS <= BIRTHDAY_MAX_OFFSET_DAYS:
        navigation.append(
            InlineKeyboardButton(
                "Следующие 60 дней ▶️",
                callback_data=f"help:team:birthdays:{offset + BIRTHDAY_PERIOD_DAYS}",
            )
        )
    if navigation:
        # При двух длинных подписях каждая получает полную ширину строки.
        for button in navigation:
            rows.append([button])

    rows.append([InlineKeyboardButton("⬅️ Наша команда", callback_data="help:team")])
    return InlineKeyboardMarkup(rows)


def kb_help_profile_card(
    profile: dict,
    page: int = 0,
    back_callback: str | None = None,
    back_label: str = "⬅️ Назад к списку",
    show_carousel: bool = True,
):
    """Карточка сотрудника с переходами к предыдущему и следующему профилю."""
    rows = []
    people = db_profiles_list()

    # Находим текущего сотрудника в общем алфавитном списке.
    current_index = next(
        (index for index, (pid, _name) in enumerate(people) if int(pid) == int(profile["id"])),
        None,
    )

    if current_index is not None:
        current_page = current_index // TEAM_PAGE_SIZE
    else:
        current_page = _team_clamp_page(page, len(people))

    # Циклическая карусель: после последнего сотрудника открывается первый.
    if show_carousel and current_index is not None and len(people) > 1:
        if len(people) == 2:
            # При двух сотрудниках предыдущий и следующий совпадают,
            # поэтому оставляем одну понятную кнопку.
            other_index = 1 - current_index
            other_pid, other_name = people[other_index]
            other_page = other_index // TEAM_PAGE_SIZE
            rows.append([
                InlineKeyboardButton(
                    f"Следующий: {compact_team_name(other_name, 25)} ▶️",
                    callback_data=f"help:team:person:{other_pid}:{other_page}",
                )
            ])
        else:
            previous_index = (current_index - 1) % len(people)
            next_index = (current_index + 1) % len(people)

            previous_pid, previous_name = people[previous_index]
            next_pid, next_name = people[next_index]

            previous_page = previous_index // TEAM_PAGE_SIZE
            next_page = next_index // TEAM_PAGE_SIZE

            rows.append([
                InlineKeyboardButton(
                    f"◀️ {compact_team_name(previous_name, 15)}",
                    callback_data=f"help:team:person:{previous_pid}:{previous_page}",
                ),
                InlineKeyboardButton(
                    f"{compact_team_name(next_name, 15)} ▶️",
                    callback_data=f"help:team:person:{next_pid}:{next_page}",
                ),
            ])

    tg = (profile.get("tg_link") or "").strip()
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

    rows.append([
        InlineKeyboardButton(
            back_label,
            callback_data=back_callback or f"help:team:page:{current_page}",
        )
    ])
    rows.append([InlineKeyboardButton("🏠 В главное меню", callback_data="help:main")])
    return InlineKeyboardMarkup(rows)

def _truncate_profile_field(value: str | None, limit: int) -> str:
    text = re.sub(r"\s+", " ", (value or "—").strip()) or "—"
    if len(text) <= limit:
        return text
    return text[: max(1, limit - 1)].rstrip() + "…"


def _build_profile_card_text(
    profile: dict,
    *,
    about_limit: int = 1200,
    topics_limit: int = 1200,
    progress_limit: int = 8,
    achievements_limit: int = 5,
    name_limit: int = 180,
    city_limit: int = 120,
    tg_limit: int = 120,
) -> str:
    """Собирает карточку в едином формате с настраиваемыми лимитами полей."""
    full_name = _truncate_profile_field(profile.get("full_name"), name_limit)
    year_start = str(profile.get("year_start") or "—")
    city = _truncate_profile_field(profile.get("city"), city_limit)
    bday = _truncate_profile_field(profile.get("birthday"), 30)
    tg_link = _truncate_profile_field(profile.get("tg_link"), tg_limit)

    avg = profile.get("avg_test_score")
    avg_text = f"{avg}%" if avg is not None and str(avg).strip() else "—"
    about = _truncate_profile_field(profile.get("about"), about_limit)
    topics = _truncate_profile_field(profile.get("topics"), topics_limit)

    progress_items = db_achievement_progress_summary(int(profile["id"]))
    progress_lines = []
    if progress_limit > 0:
        for item in progress_items[:int(progress_limit)]:
            level = achievement_level_label(item["level"])
            if item["next_threshold"] is None:
                progress_text_item = "максимальный уровень"
            else:
                progress_text_item = item["label"]
            progress_lines.append(
                f"{escape(item['emoji'])} {escape(item['title'])} · "
                f"{level} — {escape(progress_text_item)}"
            )
    progress_text = "\n".join(progress_lines) if progress_lines else "— Всё ещё впереди —"

    if achievements_limit > 0:
        achievements_text = format_achievements_for_profile(
            int(profile["id"]),
            limit=int(achievements_limit),
        )
    else:
        achievements_text = "— Всё ещё впереди —"

    return (
        f"👤 <b>{escape(full_name)}</b>\n\n"
        f"📅 Работает с: <b>{escape(year_start)}</b>\n"
        f"🏙️ Город: <b>{escape(city)}</b>\n"
        f"🎂 День рождения: <b>{escape(bday)}</b>\n\n"
        f"📝 <b>Кратко о себе</b>\n{escape(about)}\n\n"
        f"❓ <b>По каким вопросам обращаться</b>\n{escape(topics)}\n\n"
        f"🔗 <b>TG:</b> {escape(tg_link)}\n"
        f"📈 <b>Средний балл тестирования:</b> <b>{escape(avg_text)}</b>\n\n"
        f"━━━━━━━━━━━━━━\n\n"
        f"🏆 <b>Прогресс уровней</b>\n{progress_text}\n\n"
        f"🏅 <b>Последние ачивки</b>\n\n{achievements_text}"
    )


def build_profile_card_text(profile: dict, compact: bool = False) -> str:
    """Полная текстовая карточка для профилей без фотографии."""
    return _build_profile_card_text(profile)


def build_profile_card_caption(profile: dict) -> str:
    """
    Полная карточка для подписи к фотографии.

    Сначала сохраняем исходный полный вид. Если карточка длиннее лимита
    Telegram-caption, постепенно сокращаем только значения полей и число
    элементов ачивок, не удаляя заголовки и не меняя порядок разделов.
    """
    variants = [
        # Обычная карточка: визуально совпадает с текстовой версией.
        dict(about_limit=1200, topics_limit=1200, progress_limit=8, achievements_limit=5),
        # Мягкое сокращение для насыщенных карточек.
        dict(about_limit=360, topics_limit=360, progress_limit=5, achievements_limit=3),
        dict(about_limit=240, topics_limit=240, progress_limit=4, achievements_limit=2),
        dict(about_limit=160, topics_limit=160, progress_limit=3, achievements_limit=1),
        # Гарантированный компактный вариант с сохранением всех разделов.
        dict(
            about_limit=90,
            topics_limit=90,
            progress_limit=1,
            achievements_limit=0,
            name_limit=90,
            city_limit=60,
            tg_limit=70,
        ),
    ]

    for params in variants:
        caption = _build_profile_card_text(profile, **params)
        if len(_html_plain_text(caption)) <= 1024:
            return caption

    # Практически недостижимый резервный вариант.
    return _build_profile_card_text(
        profile,
        about_limit=40,
        topics_limit=40,
        progress_limit=0,
        achievements_limit=0,
        name_limit=50,
        city_limit=35,
        tg_limit=40,
    )


# Совместимость с предыдущей версией, где фото и текст карточки
# отправлялись раздельно. После перехода на симметричную карточку
# эта связь используется только для удаления старых отдельных фото.
PROFILE_CARD_PHOTO_MESSAGES = "profile_card_photo_messages"


def _profile_card_message_key(chat_id: int, text_message_id: int) -> str:
    return f"{int(chat_id)}:{int(text_message_id)}"


async def delete_profile_card_photo_for_message(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    text_message_id: int,
):
    photo_messages = context.chat_data.get(PROFILE_CARD_PHOTO_MESSAGES)
    if not isinstance(photo_messages, dict):
        return

    key = _profile_card_message_key(chat_id, text_message_id)
    photo_message_id = photo_messages.pop(key, None)
    if not photo_message_id:
        return

    try:
        await context.bot.delete_message(
            chat_id=int(chat_id),
            message_id=int(photo_message_id),
        )
    except Exception:
        # Фото могло быть удалено пользователем или Telegram ранее.
        pass


async def replace_callback_message_with_text(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
    parse_mode: str | None = ParseMode.HTML,
    disable_web_page_preview: bool = True,
):
    """Заменяет callback-сообщение текстом и убирает связанное фото карточки."""
    await delete_profile_card_photo_for_message(
        context,
        chat_id=query.message.chat_id,
        text_message_id=query.message.message_id,
    )

    if getattr(query.message, "photo", None):
        sent = await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=text,
            parse_mode=parse_mode,
            reply_markup=reply_markup,
            disable_web_page_preview=disable_web_page_preview,
        )
        if sent:
            try:
                await context.bot.delete_message(
                    chat_id=query.message.chat_id,
                    message_id=query.message.message_id,
                )
            except Exception:
                pass
        return

    await query.edit_message_text(
        text,
        parse_mode=parse_mode,
        reply_markup=reply_markup,
        disable_web_page_preview=disable_web_page_preview,
    )


async def render_profile_card(
    query,
    profile: dict,
    page: int,
    context: ContextTypes.DEFAULT_TYPE,
    back_callback: str | None = None,
    back_label: str = "⬅️ Назад к списку",
    show_carousel: bool = True,
):
    """
    Показывает симметричную карточку:
    - с фотографией: фото и полный текст находятся в одном медиасообщении;
    - без фотографии: используется обычное текстовое сообщение.

    За счёт caption ширина описания всегда совпадает с шириной фотографии.
    """
    markup = kb_help_profile_card(
        profile,
        page=page,
        back_callback=back_callback,
        back_label=back_label,
        show_carousel=show_carousel,
    )
    chat_id = int(query.message.chat_id)
    current_message_id = int(query.message.message_id)
    photo_file_id = (profile.get("photo_file_id") or "").strip()
    current_is_photo = bool(getattr(query.message, "photo", None))

    # Удаляем отдельное фото, если карточка была открыта старой версией кода.
    await delete_profile_card_photo_for_message(
        context,
        chat_id=chat_id,
        text_message_id=current_message_id,
    )

    if photo_file_id:
        caption = build_profile_card_caption(profile)

        if current_is_photo:
            await query.edit_message_media(
                media=InputMediaPhoto(
                    media=photo_file_id,
                    caption=caption,
                    parse_mode=ParseMode.HTML,
                ),
                reply_markup=markup,
            )
            return

        sent = await context.bot.send_photo(
            chat_id=chat_id,
            photo=photo_file_id,
            caption=caption,
            parse_mode=ParseMode.HTML,
            reply_markup=markup,
        )
        if sent:
            try:
                await context.bot.delete_message(
                    chat_id=chat_id,
                    message_id=current_message_id,
                )
            except Exception:
                pass
        return

    text = build_profile_card_text(profile, compact=False)

    if current_is_photo:
        sent = await context.bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode=ParseMode.HTML,
            reply_markup=markup,
            disable_web_page_preview=True,
        )
        if sent:
            try:
                await context.bot.delete_message(
                    chat_id=chat_id,
                    message_id=current_message_id,
                )
            except Exception:
                pass
        return

    await query.edit_message_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=markup,
        disable_web_page_preview=True,
    )


def kb_help_settings():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📄 Контент", callback_data="help:settings:content"),
            InlineKeyboardButton("👥 Сотрудники", callback_data="help:settings:people"),
        ],
        [
            InlineKeyboardButton("🏆 Ачивки и номинации", callback_data="help:settings:ach"),
            InlineKeyboardButton("📝 Тестирование", callback_data="help:settings:test"),
        ],
        [
            InlineKeyboardButton("📣 Коммуникации", callback_data="help:settings:communications"),
            InlineKeyboardButton("🛠 Система", callback_data="help:settings:system"),
        ],
        [InlineKeyboardButton("⬅️ Назад", callback_data="help:main")],
    ])


def kb_settings_content():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📚 Открыть раздел «Документы»", callback_data="help:docs")],
        [InlineKeyboardButton("❓ Управление FAQ", callback_data="help:settings:faq")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="help:settings")],
    ])


def kb_settings_people():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Добавить анкету", callback_data="help:settings:add_profile")],
        [InlineKeyboardButton("✏️ Редактировать анкету", callback_data="help:settings:edit_profile")],
        [InlineKeyboardButton("➖ Удалить анкету", callback_data="help:settings:del_profile")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="help:settings")],
    ])


def kb_settings_communications():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🗓 Управление планёркой и отраслевой", callback_data="help:settings:regular_meetings")],
        [InlineKeyboardButton("📅 Запланировать другую встречу", callback_data="help:settings:meeting")],
        [InlineKeyboardButton("📣 Создать рассылку", callback_data="help:settings:bcast")],
        [InlineKeyboardButton("🏷 Теги рассылок", callback_data="help:settings:bcast_tags")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="help:settings")],
    ])


def kb_settings_system():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📦 Скачать бэкап ZIP", callback_data="help:settings:backup_zip")],
        [InlineKeyboardButton("📥 Восстановить бэкап ZIP", callback_data="help:settings:restore_zip")],
        [
            InlineKeyboardButton("📤 Экспорт CSV", callback_data="help:settings:export_csv"),
            InlineKeyboardButton("📥 Импорт CSV", callback_data="help:settings:import_csv"),
        ],
        [InlineKeyboardButton("⬅️ Назад", callback_data="help:settings")],
    ])



def kb_settings_faq():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Добавить вопрос", callback_data="help:settings:faq:add")],
        [InlineKeyboardButton("➖ Удалить вопрос", callback_data="help:settings:faq:del")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="help:settings:content")],
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
        rows.append([InlineKeyboardButton("✏️ Переименовать категорию", callback_data="help:settings:cats:rename")])
        rows.append([InlineKeyboardButton("➖ Удалить категорию (только пустую)", callback_data="help:settings:cats:del")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="help:settings:content")])
    return InlineKeyboardMarkup(rows)



def kb_pick_category_to_rename():
    cats = db_docs_list_categories()
    rows = []
    if not cats:
        rows.append([InlineKeyboardButton("— категорий нет —", callback_data="noop")])
    else:
        for cid, title in cats:
            rows.append([InlineKeyboardButton(f"✏️ {title}", callback_data=f"help:settings:cats:rename:{cid}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="help:settings:cats")])
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

    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="help:settings:content")])
    return InlineKeyboardMarkup(rows)

def kb_achievements_menu():
    pending_count = len(db_nominations_pending(1000))
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎁 Выдать ачивку", callback_data="help:settings:ach:give")],
        [
            InlineKeyboardButton(
                f"📨 Номинации на рассмотрении ({pending_count})",
                callback_data="help:settings:ach:nominations",
            )
        ],
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

# Для каждого назначения сотруднику доступна ровно одна попытка. Повторное
# прохождение появляется только после нового назначения администратором.
TEST_MAX_ATTEMPTS = 1
TEST_RETAKE_POLICY_TEXT = (
    "Доступна только одна попытка. После завершения тест нельзя пройти повторно; "
    "новая попытка появится только после повторного назначения администратором."
)

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
             SET status='in_progress', started_at=COALESCE(started_at,?), deadline_at=COALESCE(deadline_at,?),
                 current_idx=CASE WHEN status='assigned' THEN 0 ELSE current_idx END
             WHERE id=? AND status IN ('assigned', 'in_progress')""",
        (_now_iso(), deadline_at_iso, int(aid)),
    )
    ok = cur.rowcount > 0
    con.commit()
    con.close()
    return ok

def db_test_update_assignment_progress(aid: int, current_idx: int):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        "UPDATE test_assignments SET current_idx=? "
        "WHERE id=? AND status IN ('assigned', 'in_progress', 'saved')",
        (int(current_idx), int(aid)),
    )
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
        "SELECT status FROM test_assignments WHERE id=?",
        (int(assignment_id),),
    )
    status_row = cur.fetchone()
    if not status_row or status_row[0] not in ("assigned", "in_progress", "saved"):
        con.close()
        return False
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
    return True

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
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="help:settings:people")])
    return InlineKeyboardMarkup(rows)


def kb_pick_profile_to_edit():
    people = db_profiles_list()
    rows = []
    if not people:
        rows.append([InlineKeyboardButton("— анкет нет —", callback_data="noop")])
    else:
        for pid, name in people[:40]:
            rows.append([InlineKeyboardButton(name, callback_data=f"help:settings:edit_profile:{pid}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="help:settings:people")])
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
    return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="help:flow:cancel")]])


def _profile_wiz_finish_text(mode: str, profile_id: int, is_admin: bool, updated: bool = False) -> tuple[str, InlineKeyboardMarkup]:
    action = "обновлена" if updated else "добавлена"
    if mode == "self_create":
        return f"✅ Ваша анкета {action}.", kb_help_team(can_create_profile=False)
    if mode == "self_edit":
        return (
            "✅ Ваша анкета обновлена. Изменения уже отображаются в карточке команды.",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("👤 Вернуться в мой кабинет", callback_data="help:me")],
                [InlineKeyboardButton("👥 Открыть мою карточку", callback_data=f"help:team:person:{int(profile_id)}:{_profile_team_page(int(profile_id))}")],
                [InlineKeyboardButton("🏠 Главное меню", callback_data="help:main")],
            ]),
        )
    return f"✅ Анкета {action} (ID {profile_id}).", (kb_help_settings() if is_admin else kb_help_main(is_admin_user=False))


def start_profile_wizard(context: ContextTypes.DEFAULT_TYPE, user_id: int, mode: str, initial_data: dict | None = None, edit_pid: int | None = None):
    clear_profile_wiz(context)
    context.chat_data[PROFILE_WIZ_ACTIVE] = True
    context.chat_data[PROFILE_WIZ_STEP] = "full_name"
    context.chat_data[PROFILE_WIZ_DATA] = dict(initial_data or {})
    context.chat_data[PROFILE_WIZ_MODE] = mode
    if edit_pid is not None:
        context.chat_data[PROFILE_WIZ_EDIT_PID] = int(edit_pid)
    context.chat_data[WAITING_USER_ID] = user_id
    context.chat_data[WAITING_SINCE_TS] = int(time.time())


async def finalize_profile_wizard(update: Update, context: ContextTypes.DEFAULT_TYPE) -> tuple[bool, str, InlineKeyboardMarkup]:
    """Сохраняет анкету после шага с фотографией."""
    mode = context.chat_data.get(PROFILE_WIZ_MODE) or "admin_add"
    data = context.chat_data.get(PROFILE_WIZ_DATA) or {}
    is_admin_here = await is_admin_scoped(update, context)

    required = ("full_name", "year_start", "city", "about", "topics", "tg_link")
    if any(not data.get(key) for key in required):
        clear_profile_wiz(context)
        return False, "❌ Не хватает данных анкеты. Начните заполнение заново.", kb_help_main(is_admin_user=is_admin_here)

    if mode in ("admin_edit", "self_edit"):
        edit_pid = context.chat_data.get(PROFILE_WIZ_EDIT_PID)
        if not edit_pid:
            clear_profile_wiz(context)
            fallback = kb_help_settings() if mode == "admin_edit" else kb_help_main(is_admin_user=is_admin_here)
            return False, "❌ Не удалось определить редактируемую анкету.", fallback

        # Самостоятельно можно изменять только собственную анкету.
        if mode == "self_edit":
            owner_profile = get_profile_for_user(update)
            current_user = update.effective_user
            if (
                not current_user
                or not owner_profile
                or int(owner_profile["id"]) != int(edit_pid)
            ):
                clear_profile_wiz(context)
                return (
                    False,
                    "❌ Не удалось подтвердить, что эта анкета принадлежит вам. Редактирование отменено.",
                    kb_help_main(is_admin_user=is_admin_here),
                )

        keep_existing_photo = data.get("photo_action") not in ("replace", "remove")
        ok = db_profiles_update(
            pid=int(edit_pid),
            full_name=data["full_name"],
            year_start=data["year_start"],
            city=data["city"],
            birthday=data.get("birthday"),
            about=data["about"],
            topics=data["topics"],
            tg_link=data["tg_link"],
            photo_file_id=data.get("photo_file_id"),
            keep_existing_photo=keep_existing_photo,
        )
        if not ok:
            clear_profile_wiz(context)
            fallback = kb_help_settings() if mode == "admin_edit" else kb_help_main(is_admin_user=is_admin_here)
            return False, "❌ Не удалось обновить анкету.", fallback
        pid = int(edit_pid)
    else:
        pid = db_profiles_add(
            full_name=data["full_name"],
            year_start=data["year_start"],
            city=data["city"],
            birthday=data.get("birthday"),
            about=data["about"],
            topics=data["topics"],
            tg_link=data["tg_link"],
            photo_file_id=data.get("photo_file_id"),
        )

    if mode in ("self_create", "self_edit") and update.effective_user:
        # Закрепляем анкету за Telegram ID владельца, даже если @username изменился.
        db_profiles_set_tg_user_id(pid, int(update.effective_user.id))

    clear_profile_wiz(context)
    msg, markup = _profile_wiz_finish_text(
        mode,
        pid,
        is_admin_here,
        updated=(mode in ("admin_edit", "self_edit")),
    )
    return True, msg, markup


def get_profile_for_user(update: Update) -> dict | None:
    user = update.effective_user
    if not user:
        return None
    prof = db_profiles_get_by_tg_user_id(int(user.id))
    if prof:
        return prof
    tg_link = _normalize_profile_tg_link(getattr(user, "username", None))
    if tg_link:
        return db_profiles_get_by_tg_link(tg_link)
    return None


async def can_create_own_profile(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if not update.effective_user:
        return False
    if await is_admin_scoped(update, context):
        return False
    return get_profile_for_user(update) is None



def db_profile_test_summary(profile_id: int) -> dict:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        SELECT COUNT(*),
               SUM(CASE WHEN status='assigned' THEN 1 ELSE 0 END),
               SUM(CASE WHEN status='in_progress' THEN 1 ELSE 0 END),
               SUM(CASE WHEN status IN ('finished','saved') THEN 1 ELSE 0 END),
               SUM(CASE WHEN status='expired' THEN 1 ELSE 0 END)
        FROM test_assignments
        WHERE profile_id=?
        """,
        (int(profile_id),),
    )
    row = cur.fetchone() or (0, 0, 0, 0, 0)
    con.close()
    return {
        "total": int(row[0] or 0),
        "assigned": int(row[1] or 0),
        "in_progress": int(row[2] or 0),
        "finished": int(row[3] or 0),
        "expired": int(row[4] or 0),
    }


def db_profile_tests(profile_id: int, limit: int = 10) -> list[dict]:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        SELECT a.id, t.title, a.status, a.assigned_at, a.started_at, a.finished_at
        FROM test_assignments a
        JOIN test_templates t ON t.id = a.template_id
        WHERE a.profile_id=?
        ORDER BY a.id DESC
        LIMIT ?
        """,
        (int(profile_id), int(limit)),
    )
    rows = cur.fetchall()
    con.close()
    return [
        {
            "id": int(r[0]),
            "title": r[1],
            "status": r[2],
            "assigned_at": r[3],
            "started_at": r[4],
            "finished_at": r[5],
        }
        for r in rows
    ]

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

    await sync_profile_user_id_from_update(update)
    bot_username = (context.bot.username or "blablabird_bot")
    is_adm = await is_admin_scoped(update, context)
    profile = get_profile_for_user(update)
    unread_count = db_notifications_unread_count(update.effective_user.id if update.effective_user else None)
    text = help_text_main(
        bot_username,
        profile=profile,
        unread_count=unread_count,
        is_admin_user=is_adm,
        user_full_name=(update.effective_user.full_name if update.effective_user else None),
    )

    orig_msg = update.message  # чтобы (по возможности) удалить /help в группе

    # 1) если команда в личке — просто показываем меню тут
    if update.effective_chat and update.effective_chat.type == "private":
        await update.message.reply_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=kb_help_main(is_admin_user=is_adm, unread_count=unread_count),
            disable_web_page_preview=True,
        )
        return

    # 2) если команда в группе — пробуем прислать меню в ЛС пользователю
    if update.effective_user:
        context.user_data[HELP_SCOPE_CHAT_ID] = update.effective_chat.id

    user_id = update.effective_user.id if update.effective_user else None
    if user_id:
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=text,
                parse_mode=ParseMode.HTML,
                reply_markup=kb_help_main(is_admin_user=is_adm, unread_count=unread_count),
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
        reply_markup=kb_help_main(is_admin_user=is_adm, unread_count=unread_count),
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
                rs_time = state.get("reschedule_time") or "—"
                return (
                    f"• <b>{title}</b>: ❌ отменено/перенесено сегодня\n"
                    f"  Причина: {reason}\n"
                    f"  Новое уведомление: {rs} в {rs_time} МСК"
                )
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
    clear_nomination_flow(context)
    clear_bcast_flow(context)
    clear_comm_meeting_flow(context)
    clear_regular_meeting_flow(context)
    clear_bcast_tag_waiting(context)
    await update.message.reply_text("✅ Сбросил состояния ожидания (дата/документы/анкеты/CSV/предложка/рассылка/встреча).")



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

    # doc_tags.csv — связи тегов с документами
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=[
        "doc_title", "category_title", "doc_file_unique_id", "doc_file_id", "tag_title"
    ])
    w.writeheader()
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        SELECT d.title, c.title, d.file_unique_id, d.file_id, t.title
        FROM doc_tag_links l
        JOIN docs d ON d.id=l.doc_id
        JOIN doc_categories c ON c.id=d.category_id
        JOIN doc_tags t ON t.id=l.tag_id
        ORDER BY t.title COLLATE NOCASE, d.title COLLATE NOCASE
    """)
    for doc_title, cat_title, unique_id, file_id, tag_title in cur.fetchall():
        w.writerow({
            "doc_title": doc_title or "",
            "category_title": cat_title or "",
            "doc_file_unique_id": unique_id or "",
            "doc_file_id": file_id or "",
            "tag_title": tag_title or "",
        })
    con.close()
    files["doc_tags.csv"] = buf.getvalue()

    # doc_collections.csv — подборки и их состав
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=[
        "collection_title", "collection_description", "position",
        "doc_title", "category_title", "doc_file_unique_id", "doc_file_id"
    ])
    w.writeheader()
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        SELECT col.title, col.description, i.position, d.title, cat.title, d.file_unique_id, d.file_id
        FROM doc_collections col
        LEFT JOIN doc_collection_items i ON i.collection_id=col.id
        LEFT JOIN docs d ON d.id=i.doc_id
        LEFT JOIN doc_categories cat ON cat.id=d.category_id
        ORDER BY col.title COLLATE NOCASE, COALESCE(i.position, 0), d.title COLLATE NOCASE
    """)
    for collection_title, description, position, doc_title, cat_title, unique_id, file_id in cur.fetchall():
        w.writerow({
            "collection_title": collection_title or "",
            "collection_description": description or "",
            "position": position if position is not None else "",
            "doc_title": doc_title or "",
            "category_title": cat_title or "",
            "doc_file_unique_id": unique_id or "",
            "doc_file_id": file_id or "",
        })
    con.close()
    files["doc_collections.csv"] = buf.getvalue()

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
        "tg_user_id",
        "is_active",
        "avg_test_score",
        "photo_file_id",
    ])
    w.writeheader()
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        SELECT id, full_name, year_start, city, birthday, about, topics, tg_link,
               tg_user_id, is_active, avg_test_score, photo_file_id
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
            "tg_user_id": row[8] if row[8] is not None else "",
            "is_active": int(row[9]) if row[9] is not None else 1,
            "avg_test_score": row[10] if row[10] is not None else "",
            "photo_file_id": row[11] or "",
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
        "achievement_key",
        "level",
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
    stats = {"profiles": 0, "categories": 0, "docs": 0, "doc_tags": 0, "doc_collections": 0, "faq": 0, "notify_chats": 0, "achievements_awards": 0}
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
                photo_file_id = (row.get("photo_file_id") or "").strip() or None

                tg_user_id_raw = (row.get("tg_user_id") or "").strip()
                tg_user_id = int(tg_user_id_raw) if tg_user_id_raw.lstrip("-").isdigit() else None
                active_raw = (row.get("is_active") or "1").strip().lower()
                is_active = 0 if active_raw in ("0", "false", "no", "нет") else 1

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
                        """INSERT INTO profiles(
                               id, full_name, year_start, city, birthday, about, topics, tg_link,
                               tg_user_id, is_active, avg_test_score, photo_file_id, created_at
                           ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                               ON CONFLICT(id) DO UPDATE SET
                                 full_name=excluded.full_name,
                                 year_start=excluded.year_start,
                                 city=excluded.city,
                                 birthday=excluded.birthday,
                                 about=excluded.about,
                                 topics=excluded.topics,
                                 tg_link=excluded.tg_link,
                                 tg_user_id=COALESCE(excluded.tg_user_id, profiles.tg_user_id),
                                 is_active=excluded.is_active,
                                 avg_test_score=excluded.avg_test_score,
                                 photo_file_id=COALESCE(excluded.photo_file_id, profiles.photo_file_id)
                        """,
                        (
                            int(pid), full_name, int(year_start), city, birthday, about, topics,
                            tg_link, tg_user_id, is_active, avg_test_score, photo_file_id, created_at,
                        ),
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
                            """INSERT INTO profiles(
                                   full_name, year_start, city, birthday, about, topics, tg_link,
                                   tg_user_id, is_active, avg_test_score, photo_file_id, created_at
                               ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                full_name, int(year_start), city, birthday, about, topics, tg_link,
                                tg_user_id, is_active, avg_test_score, photo_file_id, created_at,
                            ),
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

        def _find_restored_doc_id(row: dict) -> int | None:
            unique_id = (row.get("doc_file_unique_id") or "").strip()
            file_id = (row.get("doc_file_id") or "").strip()
            doc_title = (row.get("doc_title") or "").strip()
            cat_title = (row.get("category_title") or "").strip()
            con = sqlite3.connect(DB_PATH)
            cur = con.cursor()
            found = None
            if unique_id:
                cur.execute("SELECT id FROM docs WHERE file_unique_id=? ORDER BY id DESC LIMIT 1", (unique_id,))
                r = cur.fetchone()
                if r:
                    found = int(r[0])
            if found is None and file_id:
                cur.execute("SELECT id FROM docs WHERE file_id=? ORDER BY id DESC LIMIT 1", (file_id,))
                r = cur.fetchone()
                if r:
                    found = int(r[0])
            if found is None and doc_title and cat_title:
                cur.execute(
                    """SELECT d.id FROM docs d JOIN doc_categories c ON c.id=d.category_id
                       WHERE d.title=? AND c.title=? ORDER BY d.id DESC LIMIT 1""",
                    (doc_title, cat_title),
                )
                r = cur.fetchone()
                if r:
                    found = int(r[0])
            con.close()
            return found

        # 4) теги документов
        if "doc_tags.csv" in names:
            raw = zf.read("doc_tags.csv").decode("utf-8-sig", errors="ignore")
            reader = csv.DictReader(io.StringIO(raw))
            con = sqlite3.connect(DB_PATH)
            cur = con.cursor()
            for row in reader:
                tag_title = (row.get("tag_title") or "").strip().lstrip("#")
                doc_id = _find_restored_doc_id(row)
                if not tag_title or not doc_id:
                    continue
                cur.execute(
                    "INSERT INTO doc_tags(title, created_at) VALUES(?, ?) ON CONFLICT(title) DO NOTHING",
                    (tag_title[:50], datetime.utcnow().isoformat()),
                )
                cur.execute("SELECT id FROM doc_tags WHERE title=? COLLATE NOCASE", (tag_title[:50],))
                tag_row = cur.fetchone()
                if tag_row:
                    cur.execute(
                        "INSERT OR IGNORE INTO doc_tag_links(doc_id, tag_id) VALUES(?, ?)",
                        (int(doc_id), int(tag_row[0])),
                    )
                    stats["doc_tags"] += 1
            con.commit()
            con.close()

        # 5) подборки документов
        if "doc_collections.csv" in names:
            raw = zf.read("doc_collections.csv").decode("utf-8-sig", errors="ignore")
            reader = csv.DictReader(io.StringIO(raw))
            con = sqlite3.connect(DB_PATH)
            cur = con.cursor()
            restored_collections = set()
            for row in reader:
                title = (row.get("collection_title") or "").strip()
                description = (row.get("collection_description") or "").strip() or None
                if not title:
                    continue
                cur.execute(
                    """INSERT INTO doc_collections(title, description, created_at) VALUES(?, ?, ?)
                       ON CONFLICT(title) DO UPDATE SET description=excluded.description""",
                    (title[:80], description, datetime.utcnow().isoformat()),
                )
                cur.execute("SELECT id FROM doc_collections WHERE title=? COLLATE NOCASE", (title[:80],))
                collection_row = cur.fetchone()
                if not collection_row:
                    continue
                collection_id = int(collection_row[0])
                restored_collections.add(collection_id)
                doc_id = _find_restored_doc_id(row)
                if doc_id:
                    try:
                        position = int(row.get("position") or 0)
                    except Exception:
                        position = 0
                    cur.execute(
                        "INSERT OR IGNORE INTO doc_collection_items(collection_id, doc_id, position) VALUES(?, ?, ?)",
                        (collection_id, int(doc_id), position),
                    )
            stats["doc_collections"] += len(restored_collections)
            con.commit()
            con.close()

                # 6) faq.csv
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
                achievement_key = (row.get("achievement_key") or normalize_achievement_key(title)).strip()
                try:
                    level = max(1, int(row.get("level") or 1))
                except (TypeError, ValueError):
                    level = 1

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
                    """INSERT INTO achievement_awards(
                           profile_id, emoji, title, description, awarded_at, awarded_by,
                           achievement_key, level
                       ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        int(target_pid), emoji, title, description, awarded_at, awarded_by_val,
                        achievement_key, level,
                    ),
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
        notice = await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"{title}\nПричина: {reason_text}",
        )
        schedule_message_delete(context, notice)
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
        notice = await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"{title}\nПричина: {reason_text}",
        )
        schedule_message_delete(context, notice)
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
    notice = await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=f"{title}\nНовая дата: {picked} 📌\nСледите за расписанием или чатом",
    )
    schedule_message_delete(context, notice)
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
        user_profile = get_profile_for_user(update)
        if not user_profile or int(a.get("profile_id") or 0) != int(user_profile["id"]):
            await q.answer("Этот тест назначен другому сотруднику.", show_alert=True)
            return

        # Старые уведомления могли содержать прямую ссылку `test:start`.
        # Она не должна возвращать сотрудника в уже завершённое назначение.
        if a.get("status") not in ("assigned", "in_progress"):
            await q.answer(
                "Этот тест уже завершён или недоступен. Новая попытка возможна "
                "только после повторного назначения администратором.",
                show_alert=True,
            )
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
        if not db_test_update_assignment_start(aid, deadline_iso):
            await q.answer(
                "Этот тест уже завершён или недоступен.",
                show_alert=True,
            )
            return
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

    if a.get("status") not in ("in_progress", "assigned"):
        await q.answer(
            "Этот тест уже завершён или недоступен. Новая попытка возможна "
            "только после повторного назначения администратором.",
            show_alert=True,
        )
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
        profile = get_profile_for_user(update)
        unread_count = db_notifications_unread_count(update.effective_user.id if update.effective_user else None)
        await replace_callback_message_with_text(
            q,
            context,
            help_text_main(
                bot_username,
                profile=profile,
                unread_count=unread_count,
                is_admin_user=is_adm,
                user_full_name=(update.effective_user.full_name if update.effective_user else None),
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=kb_help_main(is_admin_user=is_adm, unread_count=unread_count),
            disable_web_page_preview=True,
        )
        return


    # ---------------- Реакции на публичные ачивки ----------------
    if data.startswith("help:achievement:react:"):
        parts = data.split(":")
        try:
            award_id = int(parts[3])
            reaction = parts[4]
        except (IndexError, ValueError):
            await q.answer("Не удалось поставить реакцию.", show_alert=True)
            return
        award = db_achievement_get(award_id)
        if not award:
            await q.answer("Ачивка не найдена.", show_alert=True)
            return
        user_id = update.effective_user.id if update.effective_user else None
        if not user_id or not db_achievement_reaction_set(award_id, user_id, reaction):
            await q.answer("Не удалось поставить реакцию.", show_alert=True)
            return
        try:
            await q.edit_message_reply_markup(reply_markup=kb_achievement_reactions(award_id))
        except Exception:
            pass
        await q.answer("Реакция сохранена 🙌")
        return

    # ---------------- Центр уведомлений ----------------
    if data == "help:notifications" or data.startswith("help:notifications:page:"):
        user_id = update.effective_user.id if update.effective_user else None
        if not user_id:
            return
        page = 0
        if data.startswith("help:notifications:page:"):
            try:
                page = int(data.rsplit(":", 1)[-1])
            except ValueError:
                page = 0
        unread = db_notifications_unread_count(user_id)
        await q.edit_message_text(
            "🔔 <b>Уведомления</b>\n\n"
            f"Непрочитанных: <b>{unread}</b>\n"
            "После прочтения уведомление автоматически исчезнет из раздела.",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_notifications(user_id, page),
        )
        return

    if data.startswith("help:notifications:open:"):
        user_id = update.effective_user.id if update.effective_user else None
        if not user_id:
            return
        parts = data.split(":")
        try:
            notification_id = int(parts[3])
            page = int(parts[4]) if len(parts) > 4 else 0
        except (IndexError, ValueError):
            await q.answer("Уведомление не найдено.", show_alert=True)
            return
        item = db_notification_get(notification_id, user_id)
        if not item:
            await q.answer("Уведомление не найдено.", show_alert=True)
            return
        db_notification_mark_read(notification_id, user_id)
        rows = []
        callback_data = (item.get("callback_data") or "").strip()
        # Старые записи центра уведомлений вели напрямую в `test:start`.
        # Перенаправляем их на информационную карточку назначения: карточка
        # сама покажет кнопку только для реально назначенного/незавершённого
        # теста и никогда не запускает повторное прохождение.
        if callback_data.startswith("test:start:"):
            try:
                legacy_aid = int(callback_data.rsplit(":", 1)[-1])
            except (TypeError, ValueError):
                legacy_aid = 0
            legacy_assignment = db_test_get_assignment(legacy_aid) if legacy_aid else None
            callback_data = (
                f"help:testv2:myopen:{legacy_aid}"
                if legacy_assignment
                else ""
            )
        if callback_data.startswith(("help:", "test:")) and len(callback_data.encode("utf-8")) <= 64:
            rows.append([InlineKeyboardButton("➡️ Перейти", callback_data=callback_data)])
        rows.append([InlineKeyboardButton("⬅️ К уведомлениям", callback_data=f"help:notifications:page:{page}")])
        rows.append([InlineKeyboardButton("🏠 Главное меню", callback_data="help:main")])
        await q.edit_message_text(
            f"🔔 <b>{escape(item['title'])}</b>\n\n"
            f"{escape(item.get('body') or 'Без дополнительного описания.')}\n\n"
            f"📅 {_format_short_date(item.get('created_at'))}",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(rows),
        )
        return

    if data == "help:notifications:read_all":
        user_id = update.effective_user.id if update.effective_user else None
        if not user_id:
            return
        count = db_notifications_mark_all_read(user_id)
        await q.answer(f"Прочитано и убрано: {count}")
        await q.edit_message_text(
            "🔔 <b>Уведомления</b>\n\nРаздел очищен — непрочитанных уведомлений нет.",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_notifications(user_id, 0),
        )
        return


    # ---------------- Фото анкеты: пропуск/сохранение/удаление ----------------
    if data in ("help:profile:photo:skip", "help:profile:photo:keep", "help:profile:photo:remove"):
        if not context.chat_data.get(PROFILE_WIZ_ACTIVE) or context.chat_data.get(PROFILE_WIZ_STEP) != "photo":
            await q.answer("Мастер анкеты уже завершён или отменён.", show_alert=True)
            return
        waiting_user = context.chat_data.get(WAITING_USER_ID)
        current_user = update.effective_user.id if update.effective_user else None
        if waiting_user and current_user != waiting_user:
            await q.answer("Это действие запущено другим пользователем.", show_alert=True)
            return
        profile_data = context.chat_data.get(PROFILE_WIZ_DATA) or {}
        if data.endswith(":remove"):
            profile_data["photo_file_id"] = None
            profile_data["photo_action"] = "remove"
        elif data.endswith(":keep"):
            profile_data["photo_action"] = "keep"
        else:
            profile_data["photo_file_id"] = None
            profile_data["photo_action"] = "skip"
        context.chat_data[PROFILE_WIZ_DATA] = profile_data
        _ok, message_text, markup = await finalize_profile_wizard(update, context)
        await q.edit_message_text(message_text, reply_markup=markup)
        return


    # ---------------- Мой кабинет ----------------
    if data == "help:me":
        profile = get_profile_for_user(update)
        if not profile:
            can_create = await can_create_own_profile(update, context)
            await q.edit_message_text(
                "👤 <b>Мой кабинет</b>\n\n"
                "Не удалось связать ваш Telegram с анкетой сотрудника.\n"
                "Проверьте, что в анкете указан актуальный @username.",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_no_profile_for_account(can_create),
            )
            return
        await q.edit_message_text(
            build_my_account_text(profile),
            parse_mode=ParseMode.HTML,
            reply_markup=kb_my_account(profile),
            disable_web_page_preview=True,
        )
        return

    if data == "help:me:edit":
        profile = get_profile_for_user(update)
        if not profile:
            await q.answer("Ваша анкета не найдена.", show_alert=True)
            return

        # В мастер передаётся только анкета текущего пользователя.
        start_profile_wizard(
            context,
            update.effective_user.id,
            mode="self_edit",
            initial_data=profile,
            edit_pid=int(profile["id"]),
        )
        await q.edit_message_text(
            "✏️ <b>Редактирование моей анкеты</b>\n\n"
            "Вы можете обновить имя, год начала работы, город, день рождения, "
            "описание, темы для обращений, Telegram и фотографию.\n\n"
            "Шаг 1/8: отправьте <b>Имя и Фамилию</b>.\n"
            f"Текущее значение: <code>{html_lib.escape(profile['full_name'])}</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_cancel_wizard_settings(),
        )
        return
    if data == "help:me:achievements":
        profile = get_profile_for_user(update)
        if not profile:
            await q.answer("Анкета не найдена.", show_alert=True)
            return
        count = db_achievements_count(int(profile["id"]))
        text = (
            f"🏆 <b>Мои достижения</b>\n\n"
            f"Всего: <b>{count}</b>\n\n"
            f"{format_achievements_for_profile(int(profile['id']), limit=30)}"
        )
        await q.edit_message_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ В мой кабинет", callback_data="help:me")],
                [InlineKeyboardButton("🏠 Главное меню", callback_data="help:main")],
            ]),
        )
        return

    if data == "help:me:tests":
        profile = get_profile_for_user(update)
        if not profile:
            await q.answer("Анкета не найдена.", show_alert=True)
            return
        summary = db_profile_test_summary(int(profile["id"]))
        text = (
            "📝 <b>Мои тесты</b>\n\n"
            f"Новые: <b>{summary['assigned']}</b>\n"
            f"В процессе: <b>{summary['in_progress']}</b>\n"
            f"Завершено: <b>{summary['finished']}</b>\n"
            f"Истекло: <b>{summary['expired']}</b>\n\n"
            "Ниже показаны последние тесты."
        )
        await q.edit_message_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=kb_my_tests(int(profile["id"])),
        )
        return

    if data.startswith("help:me:test:continue:"):
        profile = get_profile_for_user(update)
        if not profile:
            await q.answer("Анкета не найдена.", show_alert=True)
            return
        assignment_id = int(data.split(":")[-1])
        assignment = db_test_get_assignment(assignment_id)
        if not assignment or int(assignment["profile_id"]) != int(profile["id"]):
            await q.answer("Тест не найден.", show_alert=True)
            return
        if assignment.get("status") != "in_progress":
            await q.answer("Этот тест уже не находится в процессе.", show_alert=True)
            return
        if await _expire_assignment_if_needed(assignment, context):
            await q.edit_message_text(
                EMPLOYEE_TEST_EXPIRED_TEXT,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ В мой кабинет", callback_data="help:me")]]),
            )
            return
        context.user_data[ACTIVE_TEST_ASSIGNMENT_ID] = assignment_id
        context.user_data[ACTIVE_TEST_MULTI_SELECTED] = {}
        await q.edit_message_text(
            "⏳ Продолжаем тест. Следующий вопрос отправлен отдельным сообщением.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ В мой кабинет", callback_data="help:me")]]),
        )
        await send_employee_question(context, update.effective_user.id, assignment)
        return

    # ---------------- Номинация коллеги ----------------
    if data == "help:nomination":
        clear_nomination_flow(context)
        await q.edit_message_text(
            "🙌 <b>Номинация коллеги</b>\n\n"
            "Здесь можно отметить коллегу за помощь, инициативу, сильный результат, "
            "развитие, атмосферу или наставничество.\n\n"
            "После одобрения в командном чате появится благодарность с реакциями. "
            "Уровни растут автоматически: I — 1 одобрение, II — 3, III — 7.\n\n"
            "Ограничения: нельзя номинировать себя, повтор одной категории для одного коллеги — "
            "не чаще раза в 7 дней, максимум 5 номинаций в день.",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_nomination_intro(),
        )
        return

    if data == "help:nomination:start" or data.startswith("help:nomination:page:"):
        profile = get_profile_for_user(update)
        if not profile:
            await q.edit_message_text(
                "🙌 <b>Номинация</b>\n\n"
                "Сначала нужна анкета сотрудника, связанная с вашим Telegram.",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_no_profile_for_account(await can_create_own_profile(update, context)),
            )
            return
        page = 0
        if data.startswith("help:nomination:page:"):
            try:
                page = int(data.rsplit(":", 1)[-1])
            except ValueError:
                page = 0
        clear_nomination_flow(context)
        scope_chat_id = get_scope_chat_id(update, context) or ACCESS_CHAT_ID
        context.user_data[NOMINATION_DATA] = {
            "nominator_profile_id": int(profile["id"]),
            "nominator_name": profile["full_name"],
            "scope_chat_id": int(scope_chat_id),
            "created_ts": int(time.time()),
        }
        await q.edit_message_text(
            "🙌 <b>Номинация коллеги</b>\n\n"
            "Шаг 1/3: выберите коллегу, чей вклад хотите отметить.",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_nomination_people(page, int(profile["id"])),
        )
        return

    if data.startswith("help:nomination:pick:"):
        parts = data.split(":")
        try:
            nominee_id = int(parts[3])
        except (IndexError, ValueError):
            await q.answer("Не удалось выбрать сотрудника.", show_alert=True)
            return
        profile = get_profile_for_user(update)
        nominee = db_profiles_get(nominee_id)
        if not profile or not nominee or int(profile["id"]) == nominee_id:
            await q.answer("Нельзя выбрать эту анкету.", show_alert=True)
            return
        nomination_data = context.user_data.get(NOMINATION_DATA) or {}
        nomination_data.update({
            "nominator_profile_id": int(profile["id"]),
            "nominator_name": profile["full_name"],
            "nominee_profile_id": nominee_id,
            "nominee_name": nominee["full_name"],
            "scope_chat_id": int(nomination_data.get("scope_chat_id") or get_scope_chat_id(update, context) or ACCESS_CHAT_ID),
            "created_ts": int(time.time()),
        })
        context.user_data[NOMINATION_DATA] = nomination_data
        context.user_data[NOMINATION_ACTIVE] = True
        context.user_data[NOMINATION_STEP] = "category"
        await q.edit_message_text(
            f"🙌 Номинируем: <b>{escape(nominee['full_name'])}</b>\n\n"
            "Шаг 2/3: выберите, за какой вклад хотите отметить коллегу.",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_nomination_categories(),
        )
        return

    if data.startswith("help:nomination:category:"):
        category_key = data.rsplit(":", 1)[-1]
        if category_key not in NOMINATION_CATEGORIES:
            await q.answer("Категория не найдена.", show_alert=True)
            return
        nomination_data = context.user_data.get(NOMINATION_DATA) or {}
        nominee_profile_id = nomination_data.get("nominee_profile_id")
        nominator_profile_id = nomination_data.get("nominator_profile_id")
        user_id = update.effective_user.id if update.effective_user else None
        if not nominee_profile_id or not nominator_profile_id or not user_id:
            clear_nomination_flow(context)
            await q.answer("Начните номинацию заново.", show_alert=True)
            return
        allowed, reason_text = db_nomination_check_allowed(
            nominator_user_id=int(user_id),
            nominator_profile_id=int(nominator_profile_id),
            nominee_profile_id=int(nominee_profile_id),
            category_key=category_key,
        )
        if not allowed:
            clear_nomination_flow(context)
            await q.edit_message_text(
                f"⚠️ <b>Номинацию нельзя отправить</b>\n\n{escape(reason_text)}",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🙌 Начать заново", callback_data="help:nomination:start")],
                    [InlineKeyboardButton("🏠 Главное меню", callback_data="help:main")],
                ]),
            )
            return
        category = nomination_category(category_key)
        nomination_data["category_key"] = category_key
        nomination_data["category_title"] = category["title"]
        nomination_data["category_emoji"] = category["emoji"]
        nomination_data["created_ts"] = int(time.time())
        context.user_data[NOMINATION_DATA] = nomination_data
        context.user_data[NOMINATION_ACTIVE] = True
        context.user_data[NOMINATION_STEP] = "reason"
        await q.edit_message_text(
            f"🙌 Номинируем: <b>{escape(nomination_data.get('nominee_name') or 'коллегу')}</b>\n"
            f"Категория: {escape(category['emoji'])} <b>{escape(category['title'])}</b>\n\n"
            "Шаг 3/3: напишите конкретную причину номинации.\n\n"
            "Минимум 25 символов: что сделал коллега и почему это было важно для команды.",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_nomination_cancel(),
        )
        return

    if data == "help:nomination:cancel":
        clear_nomination_flow(context)
        await q.edit_message_text(
            "✅ Номинация отменена.",
            reply_markup=kb_help_main(is_admin_user=is_adm),
        )
        return

    if data == "help:faq":
        clear_faq_search_flow(context)
        text, keyboard = build_help_faq_menu()
        await q.edit_message_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard,
            disable_web_page_preview=True,
        )
        return

    if (
        data == "help:faq:answers"
        or data.startswith("help:faq:answers:")
        or data.startswith("help:faq:page:")  # старые сообщения
    ):
        clear_faq_search_flow(context)
        try:
            page = int(data.rsplit(":", 1)[-1])
        except (TypeError, ValueError):
            page = 0

        text, keyboard = build_help_faq_answers_page(page)
        await q.edit_message_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard,
            disable_web_page_preview=True,
        )
        return

    if data == "help:faq:search":
        clear_faq_search_flow(context)
        context.chat_data[WAITING_FAQ_SEARCH] = True
        await q.edit_message_text(
            "🔎 <b>Поиск по FAQ</b>\n\n"
            "Напишите слово или фразу. Поиск выполняется одновременно "
            "по вопросам и ответам.\n\n"
            "Например: <code>РОС</code>.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Отмена", callback_data="help:faq")]
            ]),
            disable_web_page_preview=True,
        )
        return

    if data == "help:faq:search_results" or data.startswith("help:faq:search_results:"):
        clear_faq_search_flow(context, drop_query=False)
        query_text = (context.chat_data.get(FAQ_SEARCH_QUERY) or "").strip()
        if not query_text:
            context.chat_data[WAITING_FAQ_SEARCH] = True
            await q.edit_message_text(
                "🔎 Напишите слово или фразу для поиска по FAQ.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ Отмена", callback_data="help:faq")]
                ]),
            )
            return
        try:
            page = int(data.rsplit(":", 1)[-1])
        except (TypeError, ValueError):
            page = 0
        text, keyboard = build_help_faq_search_page(query_text, page)
        await q.edit_message_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard,
            disable_web_page_preview=True,
        )
        return


    if data.startswith("help:faq:item:"):
        parts = data.split(":")
        try:
            fid = int(parts[3])
        except (IndexError, TypeError, ValueError):
            await q.answer("Некорректный вопрос", show_alert=True)
            return

        # В новых callback хранится номер страницы. Старые callback без страницы
        # продолжают работать и возвращают пользователя на первую страницу.
        try:
            faq_page = max(0, int(parts[4]))
        except (IndexError, TypeError, ValueError):
            faq_page = 0

        item = db_faq_get(fid)
        if not item:
            await q.edit_message_text(
                "Вопрос не найден (возможно удалён).",
                reply_markup=kb_help_main(is_admin_user=is_adm),
            )
            return

        text = (
            f"❓ {item['question']}\n\n"
            f"{item['answer']}"
        )
        await q.edit_message_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=kb_help_faq_item(faq_page),
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
            "✍️ <b>Напиши сообщение для тимлида</b>\n\n"
            "Можно одним сообщением. Я передам его тимлиду\n"
            "Чтобы отменить — нажми «Отмена».",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_suggest_cancel(),
            disable_web_page_preview=True,
        )
        return

    if data == "help:docs":
        clear_docs_flow(context)
        context.user_data[DOCS_RETURN_CB] = "help:docs"
        text = (
            "📚 <b>Документы</b>\n\n"
            "Поиск, избранное, история просмотров, новые материалы, категории и подборки."
        )
        await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb_help_docs_main(is_adm))
        return

    if data == "help:docs:categories":
        context.user_data[DOCS_RETURN_CB] = "help:docs:categories"
        await q.edit_message_text(
            "📂 <b>Все категории</b>\n\nВыберите категорию:",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_help_docs_categories(),
        )
        return

    if data.startswith("help:docs:cat:"):
        cid = int(data.split(":")[-1])
        cats = dict(db_docs_list_categories())
        title = cats.get(cid, "Категория")
        context.user_data[DOCS_RETURN_CB] = f"help:docs:cat:{cid}"
        text = f"📂 <b>{escape(title)}</b>\n\nВыберите документ:"
        await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb_help_docs_files(cid))
        return

    if data == "help:docs:search":
        clear_docs_flow(context)
        context.chat_data[WAITING_DOC_SEARCH] = True
        context.chat_data[WAITING_USER_ID] = update.effective_user.id if update.effective_user else None
        context.chat_data[WAITING_SINCE_TS] = int(time.time())
        await q.edit_message_text(
            "🔎 <b>Поиск документов</b>\n\n"
            "Введите название, фразу из описания, категорию или тег.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="help:docs")]]),
        )
        return

    if data == "help:docs:favorites":
        items = db_docs_favorites(update.effective_user.id if update.effective_user else None)
        context.user_data[DOCS_RETURN_CB] = "help:docs:favorites"
        await q.edit_message_text(
            "⭐ <b>Избранные документы</b>\n\nВаши сохранённые материалы:",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_docs_result_list(items, "— в избранном пока пусто —"),
        )
        return

    if data == "help:docs:recent":
        items = db_docs_recent(update.effective_user.id if update.effective_user else None)
        context.user_data[DOCS_RETURN_CB] = "help:docs:recent"
        await q.edit_message_text(
            "🕘 <b>Недавно открытые</b>\n\nПоследние просмотренные документы:",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_docs_result_list(items, "— история просмотров пока пуста —"),
        )
        return

    if data == "help:docs:new":
        items = db_docs_new(30)
        context.user_data[DOCS_RETURN_CB] = "help:docs:new"
        await q.edit_message_text(
            "🆕 <b>Новые документы</b>\n\nДобавленные за последние 30 дней:",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_docs_result_list(items, "— за 30 дней новых документов нет —"),
        )
        return

    if data == "help:docs:collections":
        context.user_data[DOCS_RETURN_CB] = "help:docs:collections"
        await q.edit_message_text(
            "🎓 <b>Подборки</b>\n\nГотовые наборы документов по темам:",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_doc_collections(),
        )
        return

    if data.startswith("help:docs:collection:") and not data.startswith("help:docs:admin:"):
        collection_id = int(data.split(":")[-1])
        collection = db_doc_collection_get(collection_id)
        if not collection:
            await q.answer("Подборка не найдена.", show_alert=True)
            return
        items = db_doc_collection_items(collection_id)
        context.user_data[DOCS_RETURN_CB] = f"help:docs:collection:{collection_id}"
        description = f"\n\n{escape(collection['description'])}" if collection.get("description") else ""
        await q.edit_message_text(
            f"🎓 <b>{escape(collection['title'])}</b>{description}\n\nВыберите документ:",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_docs_result_list(items, "— подборка пока пуста —", "help:docs:collections"),
        )
        return

    if data.startswith("help:docs:open:") or data.startswith("help:docs:file:"):
        doc_id = int(data.split(":")[-1])
        doc = db_docs_get(doc_id)
        if not doc:
            await q.edit_message_text("Документ не найден или был удалён.", reply_markup=kb_help_docs_main(is_adm))
            return
        category = db_docs_get_category(int(doc["category_id"])) or {"title": "Без категории"}
        doc["category_title"] = category["title"]
        try:
            con = sqlite3.connect(DB_PATH)
            cur = con.cursor()
            cur.execute("SELECT uploaded_at, COALESCE(updated_at, uploaded_at) FROM docs WHERE id=?", (doc_id,))
            date_row = cur.fetchone()
            con.close()
            doc["uploaded_at"] = date_row[0] if date_row else None
            doc["updated_at"] = date_row[1] if date_row else None
        except Exception:
            doc["uploaded_at"] = None
            doc["updated_at"] = None
        db_doc_record_view(update.effective_user.id if update.effective_user else None, doc_id)
        back_cb = context.user_data.get(DOCS_RETURN_CB) or "help:docs"
        await q.edit_message_text(
            build_doc_card_text(doc),
            parse_mode=ParseMode.HTML,
            reply_markup=kb_doc_card(doc_id, update.effective_user.id if update.effective_user else None, back_cb),
            disable_web_page_preview=True,
        )
        return

    if data.startswith("help:docs:download:"):
        doc_id = int(data.split(":")[-1])
        doc = db_docs_get(doc_id)
        if not doc:
            await q.answer("Документ не найден.", show_alert=True)
            return
        db_doc_record_view(update.effective_user.id if update.effective_user else None, doc_id)
        try:
            caption = f"📄 <b>{escape(doc['title'])}</b>"
            if doc.get("description"):
                caption += f"\n\n{escape(doc['description'])}"
            await context.bot.send_document(
                chat_id=update.effective_chat.id,
                document=doc["file_id"],
                caption=caption[:1024],
                parse_mode=ParseMode.HTML,
            )
            await q.answer("Файл отправлен")
        except Exception as e:
            logger.exception("send_document failed: %s", e)
            await q.answer("Не смог отправить файл.", show_alert=True)
        return

    if data.startswith("help:docs:favorite:"):
        doc_id = int(data.split(":")[-1])
        enabled = db_doc_toggle_favorite(update.effective_user.id if update.effective_user else None, doc_id)
        back_cb = context.user_data.get(DOCS_RETURN_CB) or "help:docs"
        try:
            await q.edit_message_reply_markup(
                reply_markup=kb_doc_card(doc_id, update.effective_user.id if update.effective_user else None, back_cb)
            )
        except Exception:
            pass
        await q.answer("Добавлено в избранное" if enabled else "Удалено из избранного")
        return

    # -------- Администрирование документов --------
    if data.startswith("help:docs:admin:"):
        if not is_adm:
            await q.answer("Доступно администраторам.", show_alert=True)
            return

        if data == "help:docs:admin:edit":
            await q.edit_message_text(
                "✏️ <b>Редактирование документа</b>\n\nВыберите документ:",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_doc_admin_picker("edit"),
            )
            return

        if data.startswith("help:docs:admin:edit:"):
            doc_id = int(data.split(":")[-1])
            doc = db_docs_get(doc_id)
            if not doc:
                await q.answer("Документ не найден.", show_alert=True)
                return
            await q.edit_message_text(
                f"✏️ <b>{escape(doc['title'])}</b>\n\nЧто изменить?",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_doc_edit_menu(doc_id),
            )
            return

        if data.startswith("help:docs:admin:editfield:title:"):
            doc_id = int(data.split(":")[-1])
            clear_docs_flow(context)
            context.chat_data[WAITING_DOC_EDIT_TITLE_ID] = doc_id
            context.chat_data[WAITING_USER_ID] = update.effective_user.id
            context.chat_data[WAITING_SINCE_TS] = int(time.time())
            await q.edit_message_text(
                "✏️ <b>Новое название</b>\n\nОтправьте название одним сообщением.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data=f"help:docs:admin:edit:{doc_id}")]]),
            )
            return

        if data.startswith("help:docs:admin:editfield:description:"):
            doc_id = int(data.split(":")[-1])
            clear_docs_flow(context)
            context.chat_data[WAITING_DOC_EDIT_DESC_ID] = doc_id
            context.chat_data[WAITING_USER_ID] = update.effective_user.id
            context.chat_data[WAITING_SINCE_TS] = int(time.time())
            await q.edit_message_text(
                "📝 <b>Новое описание</b>\n\nОтправьте описание или <code>-</code>, чтобы удалить его.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data=f"help:docs:admin:edit:{doc_id}")]]),
            )
            return

        if data.startswith("help:docs:admin:editfield:category:"):
            doc_id = int(data.split(":")[-1])
            await q.edit_message_text(
                "📂 <b>Категория документа</b>\n\nВыберите новую категорию:",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_doc_category_picker(doc_id),
            )
            return

        if data.startswith("help:docs:admin:setcat:"):
            parts = data.split(":")
            doc_id, category_id = int(parts[-2]), int(parts[-1])
            db_doc_update_category(doc_id, category_id)
            await q.edit_message_text(
                "✅ Категория изменена.",
                reply_markup=kb_doc_edit_menu(doc_id),
            )
            return

        if data.startswith("help:docs:admin:editfield:tags:"):
            doc_id = int(data.split(":")[-1])
            await q.edit_message_text(
                "🏷 <b>Теги документа</b>\n\nНажимайте теги, чтобы добавить или убрать их:",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_doc_tag_picker(doc_id),
            )
            return

        if data.startswith("help:docs:admin:tagtoggle:"):
            parts = data.split(":")
            doc_id, tag_id = int(parts[-2]), int(parts[-1])
            db_doc_toggle_tag(doc_id, tag_id)
            await q.edit_message_reply_markup(reply_markup=kb_doc_tag_picker(doc_id))
            return

        if data == "help:docs:admin:replace":
            await q.edit_message_text(
                "🔄 <b>Замена файла</b>\n\nВыберите документ. Название, описание, теги, избранное и подборки сохранятся:",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_doc_admin_picker("replace"),
            )
            return

        if data.startswith("help:docs:admin:replace:"):
            doc_id = int(data.split(":")[-1])
            doc = db_docs_get(doc_id)
            if not doc:
                await q.answer("Документ не найден.", show_alert=True)
                return
            clear_docs_flow(context)
            context.chat_data[WAITING_DOC_REPLACE_ID] = doc_id
            context.chat_data[WAITING_USER_ID] = update.effective_user.id
            context.chat_data[WAITING_SINCE_TS] = int(time.time())
            await q.edit_message_text(
                f"🔄 <b>Замена файла</b>\n\nДокумент: <b>{escape(doc['title'])}</b>\n\nОтправьте новый файл следующим сообщением.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="help:docs:admin:replace")]]),
            )
            return

        if data == "help:docs:admin:delete":
            await q.edit_message_text(
                "➖ <b>Удаление документа</b>\n\nВыберите документ:",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_doc_admin_picker("delete"),
            )
            return

        if data.startswith("help:docs:admin:delete:") and not data.startswith("help:docs:admin:delete:confirm:"):
            doc_id = int(data.split(":")[-1])
            doc = db_docs_get(doc_id)
            if not doc:
                await q.answer("Документ не найден.", show_alert=True)
                return
            await q.edit_message_text(
                f"⚠️ <b>Удалить документ?</b>\n\n<b>{escape(doc['title'])}</b>\n\n"
                "Документ исчезнет из категорий, тегов, избранного и подборок.",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_danger_confirm(
                    f"help:docs:admin:delete:confirm:{doc_id}",
                    "help:docs:admin:delete",
                    "🗑 Да, удалить документ",
                ),
            )
            return

        if data.startswith("help:docs:admin:delete:confirm:"):
            doc_id = int(data.split(":")[-1])
            ok = db_docs_delete_doc(doc_id)
            await q.edit_message_text(
                "✅ Документ удалён." if ok else "⚠️ Документ уже отсутствует.",
                reply_markup=kb_help_docs_main(True),
            )
            return

        if data == "help:docs:admin:tags":
            await q.edit_message_text(
                "🏷 <b>Управление тегами</b>\n\nЧисло справа показывает количество документов с тегом.",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_doc_tags_manage(),
            )
            return

        if data == "help:docs:admin:tags:add":
            clear_docs_flow(context)
            context.chat_data[WAITING_DOC_TAG_NAME] = True
            context.chat_data[WAITING_USER_ID] = update.effective_user.id
            context.chat_data[WAITING_SINCE_TS] = int(time.time())
            await q.edit_message_text(
                "➕ <b>Новый тег</b>\n\nОтправьте название без символа #.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="help:docs:admin:tags")]]),
            )
            return

        if data.startswith("help:docs:admin:tags:delete:"):
            tag_id = int(data.split(":")[-1])
            db_doc_tag_delete(tag_id)
            await q.edit_message_text(
                "✅ Тег удалён.",
                reply_markup=kb_doc_tags_manage(),
            )
            return

        if data == "help:docs:admin:collections":
            await q.edit_message_text(
                "🎓 <b>Управление подборками</b>\n\nСоздавайте тематические наборы документов.",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_doc_collections_manage(),
            )
            return

        if data == "help:docs:admin:collections:add":
            clear_docs_flow(context)
            context.chat_data[WAITING_DOC_COLLECTION_NAME] = True
            context.chat_data[WAITING_USER_ID] = update.effective_user.id
            context.chat_data[WAITING_SINCE_TS] = int(time.time())
            await q.edit_message_text(
                "➕ <b>Новая подборка</b>\n\nОтправьте название подборки.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="help:docs:admin:collections")]]),
            )
            return

        if data.startswith("help:docs:admin:collection:"):
            parts = data.split(":")
            action = parts[4] if len(parts) > 4 else ""

            if action.isdigit():
                collection_id = int(action)
                collection = db_doc_collection_get(collection_id)
                if not collection:
                    await q.answer("Подборка не найдена.", show_alert=True)
                    return
                await q.edit_message_text(
                    f"🎓 <b>{escape(collection['title'])}</b>\n\nДокументов: {len(db_doc_collection_items(collection_id))}",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb_doc_collection_manage(collection_id),
                )
                return

            if action == "addlist":
                collection_id = int(parts[-1])
                await q.edit_message_text(
                    "➕ <b>Добавление документов</b>\n\nВыберите документ:",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb_doc_collection_doc_picker(collection_id, "add"),
                )
                return

            if action == "removelist":
                collection_id = int(parts[-1])
                await q.edit_message_text(
                    "➖ <b>Удаление из подборки</b>\n\nВыберите документ:",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb_doc_collection_doc_picker(collection_id, "remove"),
                )
                return

            if action in ("add", "remove"):
                collection_id, doc_id = int(parts[-2]), int(parts[-1])
                if action == "add":
                    db_doc_collection_add_item(collection_id, doc_id)
                else:
                    db_doc_collection_remove_item(collection_id, doc_id)
                await q.edit_message_reply_markup(reply_markup=kb_doc_collection_doc_picker(collection_id, action))
                return

            if action == "delete":
                collection_id = int(parts[-1])
                collection = db_doc_collection_get(collection_id)
                if not collection:
                    await q.answer("Подборка не найдена.", show_alert=True)
                    return
                await q.edit_message_text(
                    f"⚠️ Удалить подборку <b>{escape(collection['title'])}</b>?\n\nСами документы останутся в каталоге.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb_danger_confirm(
                        f"help:docs:admin:collection:deleteconfirm:{collection_id}",
                        "help:docs:admin:collections",
                        "🗑 Да, удалить подборку",
                    ),
                )
                return

            if action == "deleteconfirm":
                collection_id = int(parts[-1])
                db_doc_collection_delete(collection_id)
                await q.edit_message_text("✅ Подборка удалена.", reply_markup=kb_doc_collections_manage())
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

    if data == "help:team" or data.startswith("help:team:page:"):
        page = 0
        if data.startswith("help:team:page:"):
            try:
                page = int(data.rsplit(":", 1)[-1])
            except (TypeError, ValueError):
                page = 0

        people_count = len(db_profiles_list())
        page = _team_clamp_page(page, people_count)
        total_pages = _team_total_pages(people_count)

        text = (
            "👥 <b>Познакомиться с командой</b>\n\n"
            "Здесь вы можете познакомиться с коллегами.\n"
            "Выберите человека, чтобы посмотреть его профиль 👇\n\n"
            f"Коллег: <b>{people_count}</b>"
        )
        if total_pages > 1:
            text += f" · страница <b>{page + 1}/{total_pages}</b>"

        await replace_callback_message_with_text(
            q,
            context,
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=kb_help_team(
                page=page,
                can_create_profile=await can_create_own_profile(update, context),
            ),
        )
        return

    if data.startswith("help:team:birthdays:"):
        try:
            offset = int(data.rsplit(":", 1)[-1])
        except (TypeError, ValueError):
            offset = 0
        offset = max(0, min(offset, BIRTHDAY_MAX_OFFSET_DAYS))
        text, events, offset = build_upcoming_birthdays_text(offset)
        await replace_callback_message_with_text(
            q,
            context,
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=kb_upcoming_birthdays(events, offset_days=offset),
        )
        return

    if data.startswith("help:team:birthday_person:"):
        parts = data.split(":")
        try:
            pid = int(parts[3])
            offset = int(parts[4]) if len(parts) > 4 else 0
        except (IndexError, TypeError, ValueError):
            await q.answer("Не удалось открыть анкету.", show_alert=True)
            return
        profile = db_profiles_get(pid)
        if not profile:
            await q.answer("Анкета не найдена.", show_alert=True)
            return
        offset = max(0, min(offset, BIRTHDAY_MAX_OFFSET_DAYS))
        await render_profile_card(
            q,
            profile,
            page=_profile_team_page(pid),
            context=context,
            back_callback=f"help:team:birthdays:{offset}",
            back_label="⬅️ К дням рождения",
            show_carousel=False,
        )
        return

    if data == "help:team:create_profile":
        existing = get_profile_for_user(update)
        if existing:
            await q.answer("У вас уже есть анкета ✅", show_alert=True)
            await q.edit_message_text(
                "👥 <b>Познакомиться с командой</b>\n\nВаша анкета уже создана. Ниже можно посмотреть анкеты коллег 👇",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_help_team(can_create_profile=False),
            )
            return

        start_profile_wizard(context, update.effective_user.id, mode="self_create")
        await q.edit_message_text(
            "➕ <b>Создание анкеты</b>\n\n"
            "Шаг 1/8: отправьте <b>Имя и Фамилию</b>.\n"
            "Пример: <code>Иван Петров</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_cancel_wizard_settings(),
        )
        return

    if data.startswith("help:team:person:"):
        parts = data.split(":")
        try:
            pid = int(parts[3])
            page = int(parts[4]) if len(parts) > 4 else 0
        except (IndexError, TypeError, ValueError):
            await q.answer("Не удалось открыть анкету.", show_alert=True)
            return

        p = db_profiles_get(pid)
        if not p:
            await q.edit_message_text(
                "Анкета не найдена (возможно, она была удалена).",
                reply_markup=kb_help_team(
                    page=page,
                    can_create_profile=await can_create_own_profile(update, context),
                ),
            )
            return

        await render_profile_card(q, p, page=page, context=context)
        return



    # ---------------- Рассмотрение номинаций администратором ----------------
    if data.startswith("help:nomination:admin:"):
        parts = data.split(":")
        action = parts[3] if len(parts) > 3 else ""
        try:
            nomination_id = int(parts[4])
        except (IndexError, ValueError):
            await q.answer("Номинация не найдена.", show_alert=True)
            return
        nomination = db_nomination_get(nomination_id)
        if not nomination:
            await q.answer("Номинация не найдена.", show_alert=True)
            return
        admin_user_id = update.effective_user.id if update.effective_user else 0
        if not admin_user_id or not await is_admin_in_chat(
            int(nomination["scope_chat_id"]), admin_user_id, context
        ):
            await q.answer("Доступно только администраторам чата.", show_alert=True)
            return
        context.user_data[HELP_SCOPE_CHAT_ID] = int(nomination["scope_chat_id"])
        category = nomination_category(nomination.get("category_key"))

        if action == "open":
            status_map = {"pending": "Ожидает решения", "approved": "Одобрена", "rejected": "Отклонена"}
            text = (
                f"🙌 <b>Номинация №{nomination_id}</b>\n\n"
                f"От: <b>{escape(nomination['nominator_name'])}</b>\n"
                f"Кого: <b>{escape(nomination['nominee_name'])}</b>\n"
                f"Категория: {escape(category['emoji'])} <b>{escape(category['title'])}</b>\n"
                f"Статус: <b>{status_map.get(nomination['status'], nomination['status'])}</b>\n\n"
                f"Причина:\n{escape(nomination['reason'])}"
            )
            markup = kb_nomination_admin_actions(nomination_id) if nomination["status"] == "pending" else InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ К номинациям", callback_data="help:settings:ach:nominations")]
            ])
            await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)
            return

        if nomination["status"] != "pending":
            await q.answer("Эта номинация уже рассмотрена.", show_alert=True)
            return

        if action == "approve":
            await q.edit_message_text(
                "⚠️ <b>Подтвердите одобрение номинации</b>\n\n"
                f"Сотрудник: <b>{escape(nomination['nominee_name'])}</b>\n"
                f"Категория: {escape(category['emoji'])} <b>{escape(category['title'])}</b>\n\n"
                "После подтверждения будет выдана ачивка и опубликована благодарность в командном чате.",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_danger_confirm(
                    f"help:nomination:admin:approve_confirm:{nomination_id}",
                    f"help:nomination:admin:open:{nomination_id}",
                    "✅ Да, одобрить и опубликовать",
                ),
            )
            return

        if action == "reject":
            await q.edit_message_text(
                "⚠️ <b>Подтвердите отклонение номинации</b>\n\n"
                f"Сотрудник: <b>{escape(nomination['nominee_name'])}</b>\n"
                f"Категория: {escape(category['emoji'])} <b>{escape(category['title'])}</b>\n\n"
                "Автор номинации получит уведомление о результате.",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_danger_confirm(
                    f"help:nomination:admin:reject_confirm:{nomination_id}",
                    f"help:nomination:admin:open:{nomination_id}",
                    "❌ Да, отклонить",
                ),
            )
            return

        if action == "approve_confirm":
            result = db_nomination_approve(nomination_id, admin_user_id)
            if not result:
                await q.answer("Номинация уже обработана другим администратором.", show_alert=True)
                return
            level_text = achievement_level_label(result["level"])
            progress = result["progress"]
            progress_text = progress["label"] if progress.get("next_threshold") else "максимальный уровень достигнут"
            approved_text = (
                f"✅ <b>Номинация одобрена</b>\n\n"
                f"{escape(nomination['nominee_name'])} получил(а) ачивку "
                f"{escape(result['emoji'])} <b>{escape(result['title'])} · уровень {level_text}</b>.\n\n"
                f"Причина:\n{escape(nomination['reason'])}\n\n"
                f"Прогресс: <b>{escape(progress_text)}</b>"
            )
            await q.edit_message_text(
                approved_text,
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📨 К номинациям", callback_data="help:settings:ach:nominations")]
                ]),
            )

            mention = normalize_tg_mention(nomination.get("nominee_tg_link") or "")
            who = mention if mention else f"<b>{escape(nomination['nominee_name'])}</b>"
            public_text = (
                f"🙌 <b>Спасибо, {who}!</b>\n\n"
                f"Коллега отметил вклад в команду, и номинация была одобрена.\n\n"
                f"{escape(result['emoji'])} <b>{escape(result['title'])} · уровень {level_text}</b>\n"
                f"За: «{escape(nomination['reason'])}»\n\n"
                f"📈 Прогресс: <b>{escape(progress_text)}</b>\n\n"
                "Поддержите коллегу реакцией 👇"
            )
            try:
                await context.bot.send_message(
                    chat_id=int(nomination["scope_chat_id"]),
                    text=public_text,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                    reply_markup=kb_achievement_reactions(int(result["award_id"])),
                )
            except Exception as exc:
                logger.exception("Cannot publish approved nomination: %s", exc)

            nominee_tg_user_id = nomination.get("nominee_tg_user_id")
            if nominee_tg_user_id:
                db_notification_add(
                    int(nominee_tg_user_id),
                    "achievement",
                    f"Новая ачивка: {result['emoji']} {result['title']} · {level_text}",
                    f"За: {nomination['reason']}\nПрогресс: {progress_text}",
                    callback_data="help:me:achievements",
                )
                try:
                    await context.bot.send_message(
                        chat_id=int(nominee_tg_user_id),
                        text=(
                            f"🎉 Вы получили ачивку: {result['emoji']} {result['title']} · уровень {level_text}\n\n"
                            f"За: {nomination['reason']}\n\n"
                            f"Прогресс: {progress_text}"
                        ),
                    )
                except Exception:
                    pass
            nominator_user_id = nomination.get("nominator_user_id")
            if nominator_user_id:
                db_notification_add(
                    int(nominator_user_id),
                    "nomination_approved",
                    f"Номинация для {nomination['nominee_name']} одобрена",
                    f"{result['emoji']} {result['title']} · уровень {level_text}\nСпасибо, что отмечаете вклад коллег!",
                    callback_data="help:notifications",
                )
                try:
                    await context.bot.send_message(
                        chat_id=int(nominator_user_id),
                        text=f"✅ Ваша номинация для {nomination['nominee_name']} одобрена. Спасибо, что отмечаете вклад коллег!",
                    )
                except Exception:
                    pass
            return

        if action == "reject_confirm":
            if not db_nomination_reject(nomination_id, admin_user_id):
                await q.answer("Номинация уже обработана другим администратором.", show_alert=True)
                return
            await q.edit_message_text(
                f"❌ <b>Номинация отклонена</b>\n\n"
                f"Кандидат: <b>{escape(nomination['nominee_name'])}</b>\n"
                f"Категория: {escape(category['emoji'])} <b>{escape(category['title'])}</b>\n\n"
                f"Причина номинации:\n{escape(nomination['reason'])}",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📨 К номинациям", callback_data="help:settings:ach:nominations")]
                ]),
            )
            nominator_user_id = nomination.get("nominator_user_id")
            if nominator_user_id:
                db_notification_add(
                    int(nominator_user_id),
                    "nomination_rejected",
                    f"Номинация для {nomination['nominee_name']} рассмотрена",
                    "В этот раз номинация не была одобрена. Спасибо за инициативу.",
                    callback_data="help:nomination",
                )
                try:
                    await context.bot.send_message(
                        chat_id=int(nominator_user_id),
                        text=(
                            f"Номинация для {nomination['nominee_name']} рассмотрена, "
                            "но в этот раз не была одобрена. Спасибо за инициативу."
                        ),
                    )
                except Exception:
                    pass
            return

    if data == "help:flow:cancel":
        clear_docs_flow(context)
        clear_faq_flow(context)
        clear_profile_wiz(context)
        clear_waiting_date(context)
        clear_csv_import(context)
        clear_zip_import(context)
        clear_restore_zip(context)
        clear_suggest_flow(context)
        clear_nomination_flow(context)
        clear_ach_wiz(context)
        clear_bcast_flow(context)
        profile = get_profile_for_user(update)
        unread_count = db_notifications_unread_count(update.effective_user.id if update.effective_user else None)
        bot_username = (context.bot.username or "blablabird_bot")
        await replace_callback_message_with_text(
            q,
            context,
            "✅ Действие отменено.\n\n" + help_text_main(
                bot_username,
                profile=profile,
                unread_count=unread_count,
                is_admin_user=is_adm,
                user_full_name=(update.effective_user.full_name if update.effective_user else None),
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=kb_help_main(is_admin_user=is_adm, unread_count=unread_count),
        )
        return

    if data == "help:settings":
        if not is_adm:
            try:
                await q.answer("⚠️ Кнопка доступна администраторам чата. Обратитесь к ним 🙂", show_alert=True)
            except (TimedOut, NetworkError):
                pass
            return
        text = (
            "⚙️ <b>Управление ботом</b>\n\n"
            "Настройки разделены по направлениям, чтобы нужное действие было проще найти."
        )
        await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb_help_settings())
        return

    if data == "help:settings:content":
        if not is_adm:
            await q.answer("Доступно администраторам.", show_alert=True)
            return
        await q.edit_message_text(
            "📄 <b>Контент</b>\n\nДокументы теперь управляются из единого раздела с поиском, тегами и подборками.",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_settings_content(),
        )
        return

    if data == "help:settings:people":
        if not is_adm:
            await q.answer("Доступно администраторам.", show_alert=True)
            return
        await q.edit_message_text(
            "👥 <b>Сотрудники</b>\n\nДобавление, редактирование и удаление анкет.",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_settings_people(),
        )
        return

    if data == "help:settings:communications":
        if not is_adm:
            await q.answer("Доступно администраторам.", show_alert=True)
            return
        await q.edit_message_text(
            "📣 <b>Коммуникации</b>\n\nВстречи, рассылки, теги и отложенная отправка.",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_settings_communications(),
        )
        return

    if data == "help:settings:system":
        if not is_adm:
            await q.answer("Доступно администраторам.", show_alert=True)
            return
        await q.edit_message_text(
            "🛠 <b>Система</b>\n\nРезервные копии, восстановление и обмен данными.",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_settings_system(),
        )
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
        if not is_adm:
            await q.answer("Доступно администраторам.", show_alert=True)
            return
        fid = int(data.split(":")[-1])
        item = db_faq_get(fid)
        if not item:
            await q.answer("Вопрос не найден.", show_alert=True)
            return
        await q.edit_message_text(
            "⚠️ <b>Подтверждение удаления FAQ</b>\n\n"
            f"Будет удалён вопрос:\n<b>{escape(item['question'])}</b>\n\n"
            "Действие нельзя отменить.",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_danger_confirm(
                f"help:settings:faq:del_confirm:{fid}",
                "help:settings:faq:del",
                "🗑 Да, удалить вопрос",
            ),
        )
        return

    if data.startswith("help:settings:faq:del_confirm:"):
        if not is_adm:
            await q.answer("Доступно администраторам.", show_alert=True)
            return
        fid = int(data.split(":")[-1])
        ok = db_faq_delete(fid)
        await q.edit_message_text(
            "✅ Вопрос удалён." if ok else "⚠️ Вопрос уже не найден.",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_settings_faq(),
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
            clear_nomination_flow(context)
            clear_ach_wiz(context)
            clear_bcast_flow(context)
            clear_comm_meeting_flow(context)
            clear_regular_meeting_flow(context)
            clear_bcast_tag_waiting(context)
            await q.edit_message_text("✅ Действие отменено.", reply_markup=kb_help_settings(), parse_mode=ParseMode.HTML)
            return


        # ---------------- COMMUNICATIONS: recurring meetings ----------------
        if data == "help:settings:regular_meetings":
            clear_regular_meeting_flow(context)
            clear_comm_meeting_flow(context)
            clear_bcast_flow(context)
            await q.edit_message_text(
                regular_meeting_week_text(),
                parse_mode=ParseMode.HTML,
                reply_markup=kb_regular_meetings_root(),
            )
            return

        if data.startswith("help:settings:regular_meeting:pick:"):
            parts = data.split(":")
            if len(parts) != 6:
                await q.answer("Не удалось определить встречу.", show_alert=True)
                return
            meeting_type = parts[4]
            try:
                original_d = date.fromisoformat(parts[5])
            except ValueError:
                await q.answer("Некорректная дата встречи.", show_alert=True)
                return

            today_d = datetime.now(MOSCOW_TZ).date()
            week_start, week_end = regular_meeting_week_bounds(today_d)
            if (
                meeting_type not in (MEETING_STANDUP, MEETING_INDUSTRY)
                or original_d < today_d
                or not (week_start <= original_d <= week_end)
                or not regular_meeting_is_due(meeting_type, original_d)
            ):
                await q.answer("Эта встреча уже недоступна для изменения.", show_alert=True)
                await q.edit_message_text(
                    regular_meeting_week_text(),
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb_regular_meetings_root(),
                )
                return

            clear_regular_meeting_flow(context)
            context.user_data[REGULAR_MEETING_DATA] = {
                "meeting_type": meeting_type,
                "original_date": format_regular_meeting_date(original_d),
            }
            state = db_get_state(meeting_type, original_d)
            status_lines = []
            if state.get("canceled") and state.get("reschedule_date"):
                try:
                    moved_to = date.fromisoformat(state["reschedule_date"])
                    moved_time = (
                        parse_regular_meeting_time(state.get("reschedule_time"))
                        or regular_meeting_default_time(meeting_type)
                    )
                    status_lines.append(
                        "Текущий статус: <b>перенесена на "
                        f"{format_regular_meeting_datetime(moved_to, moved_time)}</b>"
                    )
                except ValueError:
                    status_lines.append("Текущий статус: <b>перенесена</b>")
            elif state.get("canceled"):
                status_lines.append("Текущий статус: <b>отменена</b>")
            else:
                status_lines.append("Текущий статус: <b>запланирована</b>")

            await q.edit_message_text(
                f"🗓 <b>{escape(regular_meeting_title(meeting_type))}</b>\n\n"
                f"Дата: <b>{format_regular_meeting_date(original_d)}</b>\n"
                + "\n".join(status_lines)
                + "\n\nЧто нужно сделать с этой встречей?",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_regular_meeting_actions(meeting_type, original_d),
            )
            return

        if data.startswith("help:settings:regular_meeting:selected_action:"):
            action = data.rsplit(":", 1)[-1]
            d = context.user_data.get(REGULAR_MEETING_DATA) or {}
            meeting_type = d.get("meeting_type")
            original_d = parse_regular_meeting_date(d.get("original_date") or "")
            today_d = datetime.now(MOSCOW_TZ).date()
            week_start, week_end = regular_meeting_week_bounds(today_d)

            if (
                action not in ("cancel", "move")
                or meeting_type not in (MEETING_STANDUP, MEETING_INDUSTRY)
                or not original_d
                or original_d < today_d
                or not (week_start <= original_d <= week_end)
                or not regular_meeting_is_due(meeting_type, original_d)
            ):
                clear_regular_meeting_flow(context)
                await q.answer("Сначала выберите встречу из списка недели.", show_alert=True)
                await q.edit_message_text(
                    regular_meeting_week_text(),
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb_regular_meetings_root(),
                )
                return

            d["action"] = action
            d.pop("new_date", None)
            d.pop("new_time", None)
            context.user_data[REGULAR_MEETING_ACTIVE] = True
            context.user_data[REGULAR_MEETING_DATA] = d

            if action == "move":
                context.user_data[REGULAR_MEETING_STEP] = "new_date"
                await q.edit_message_text(
                    f"🔄 <b>Перенос: {escape(regular_meeting_title(meeting_type))}</b>\n\n"
                    f"Исходная дата: <b>{format_regular_meeting_date(original_d)}</b>\n\n"
                    "Отправьте новую дату в формате <code>ДД.ММ.ГГГГ</code>.\n"
                    "После даты бот предложит выбрать новое время уведомления.\n"
                    "Новая дата должна быть позже исходной.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("❌ Отмена", callback_data="help:settings:regular_meeting:cancel")]
                    ]),
                )
            else:
                context.user_data[REGULAR_MEETING_STEP] = "reason"
                await q.edit_message_text(
                    f"❌ <b>Отмена: {escape(regular_meeting_title(meeting_type))}</b>\n\n"
                    f"Дата: <b>{format_regular_meeting_date(original_d)}</b>\n\n"
                    "Укажите причину отмены одним сообщением.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("❌ Отмена", callback_data="help:settings:regular_meeting:cancel")]
                    ]),
                )
            return

        # Старые кнопки из ранее отправленных сообщений перенаправляем к новому списку недели.
        if data.startswith("help:settings:regular_meeting:type:") or data.startswith("help:settings:regular_meeting:action:"):
            clear_regular_meeting_flow(context)
            await q.edit_message_text(
                regular_meeting_week_text(),
                parse_mode=ParseMode.HTML,
                reply_markup=kb_regular_meetings_root(),
            )
            return

        if data.startswith("help:settings:regular_meeting:new_time:"):
            if not context.user_data.get(REGULAR_MEETING_ACTIVE):
                await q.answer("Мастер уже закрыт.", show_alert=True)
                return
            raw_time = data.rsplit(":", 1)[-1]
            new_time = parse_regular_meeting_time(raw_time)
            d = context.user_data.get(REGULAR_MEETING_DATA) or {}
            new_d = parse_regular_meeting_date(d.get("new_date") or "")
            if not new_time or not new_d or d.get("action") != "move":
                await q.answer("Сначала выберите новую дату.", show_alert=True)
                return
            d["new_time"] = new_time
            context.user_data[REGULAR_MEETING_DATA] = d
            context.user_data[REGULAR_MEETING_STEP] = "reason"
            await q.edit_message_text(
                "🔄 <b>Новое уведомление</b>\n\n"
                f"Дата и время: <b>{format_regular_meeting_datetime(new_d, new_time)}</b>\n\n"
                "Укажите <b>причину переноса</b> одним сообщением.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton(
                        "❌ Отмена",
                        callback_data="help:settings:regular_meeting:cancel",
                    )
                ]]),
            )
            return

        if data == "help:settings:regular_meeting:new_time_manual":
            if not context.user_data.get(REGULAR_MEETING_ACTIVE):
                await q.answer("Мастер уже закрыт.", show_alert=True)
                return
            d = context.user_data.get(REGULAR_MEETING_DATA) or {}
            if d.get("action") != "move" or not d.get("new_date"):
                await q.answer("Сначала выберите новую дату.", show_alert=True)
                return
            context.user_data[REGULAR_MEETING_STEP] = "new_time_manual"
            await q.edit_message_text(
                "🕒 <b>Другое время уведомления</b>\n\n"
                "Отправьте время по Москве в формате <code>ЧЧ:ММ</code>.\n"
                "Например: <code>14:45</code>.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton(
                        "❌ Отмена",
                        callback_data="help:settings:regular_meeting:cancel",
                    )
                ]]),
            )
            return

        if data.startswith("help:settings:regular_meeting:notify:"):
            if not context.user_data.get(REGULAR_MEETING_ACTIVE):
                await q.answer("Мастер уже закрыт.", show_alert=True)
                return
            notify = data.endswith(":yes")
            d = context.user_data.get(REGULAR_MEETING_DATA) or {}
            d["notify"] = notify
            context.user_data[REGULAR_MEETING_DATA] = d
            context.user_data[REGULAR_MEETING_STEP] = "confirm"
            await q.edit_message_text(
                regular_meeting_confirmation_html(d),
                parse_mode=ParseMode.HTML,
                reply_markup=kb_regular_meeting_confirm(),
            )
            return

        if data == "help:settings:regular_meeting:confirm":
            if not context.user_data.get(REGULAR_MEETING_ACTIVE):
                await q.answer("Мастер уже закрыт.", show_alert=True)
                return
            d = context.user_data.get(REGULAR_MEETING_DATA) or {}
            original_d = parse_regular_meeting_date(d.get("original_date") or "")
            new_d = parse_regular_meeting_date(d.get("new_date") or "") if d.get("new_date") else None
            new_time = parse_regular_meeting_time(d.get("new_time"))
            meeting_type = d.get("meeting_type")
            action = d.get("action")
            reason = (d.get("reason") or "").strip()
            if (
                not original_d
                or original_d < datetime.now(MOSCOW_TZ).date()
                or meeting_type not in (MEETING_STANDUP, MEETING_INDUSTRY)
                or not regular_meeting_is_due(meeting_type, original_d)
                or action not in ("cancel", "move")
                or not reason
            ):
                clear_regular_meeting_flow(context)
                await q.edit_message_text(
                    "❌ Не удалось применить изменение: данные мастера устарели.",
                    reply_markup=kb_settings_communications(),
                )
                return

            if action == "move":
                if not new_d or new_d <= original_d:
                    await q.answer("Новая дата должна быть позже исходной.", show_alert=True)
                    return
                if not new_time:
                    await q.answer("Выберите время нового уведомления.", show_alert=True)
                    return
                db_set_canceled(
                    meeting_type,
                    original_d,
                    reason,
                    reschedule_date=new_d.isoformat(),
                    reschedule_time=new_time,
                )
                db_upsert_reschedule(
                    meeting_type, original_d, new_d, new_time
                )
            else:
                db_set_canceled(meeting_type, original_d, reason)
                db_delete_reschedule(meeting_type, original_d)

            sent_ok = sent_fail = 0
            if d.get("notify"):
                sent_ok, sent_fail = await notify_regular_meeting_change(context, d)

            result_lines = [
                "✅ <b>Изменение сохранено</b>",
                "",
                f"{escape(regular_meeting_title(meeting_type))}: "
                + (
                    f"отменена на дату {format_regular_meeting_date(original_d)}"
                    if action == "cancel"
                    else f"перенесена с {format_regular_meeting_date(original_d)} "
                         f"на {format_regular_meeting_datetime(new_d, new_time)}"
                ),
            ]
            if d.get("notify"):
                result_lines.append(f"Уведомления в чатах: отправлено {sent_ok}, ошибок {sent_fail}.")
            else:
                result_lines.append("Сотрудники в чатах не уведомлялись.")
            clear_regular_meeting_flow(context)
            await q.edit_message_text(
                "\n".join(result_lines),
                parse_mode=ParseMode.HTML,
                reply_markup=kb_settings_communications(),
            )
            return

        if data == "help:settings:regular_meeting:cancel":
            clear_regular_meeting_flow(context)
            await q.edit_message_text(
                "✅ Управление встречей отменено.",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_settings_communications(),
            )
            return

        # ---------------- COMMUNICATIONS: saved broadcast tags ----------------
        if data == "help:settings:bcast_tags":
            clear_bcast_tag_waiting(context)
            await q.edit_message_text(
                "🏷 <b>Теги рассылок</b>\n\n"
                "Сохранённый тег можно выбрать вместо темы при создании рассылки. "
                "Нажатие на тег в этом списке удаляет его.",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_broadcast_tags_manage(),
            )
            return

        if data == "help:settings:bcast_tags:add":
            clear_bcast_tag_waiting(context)
            context.user_data[WAITING_BCAST_TAG_NAME] = True
            context.user_data[BCAST_TAG_MODE] = "manage"
            await q.edit_message_text(
                "➕ <b>Новый тег рассылки</b>\n\n"
                "Отправьте название одним сообщением. Символ <code>#</code> можно не вводить; "
                "пробелы будут заменены на подчёркивания.",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_cancel_wizard_settings(),
            )
            return

        if data.startswith("help:settings:bcast_tags:del:"):
            tag_id = int(data.split(":")[-1])
            ok = db_broadcast_tag_delete(tag_id)
            await q.answer("Тег удалён." if ok else "Тег уже не найден.")
            await q.edit_message_reply_markup(reply_markup=kb_broadcast_tags_manage())
            return

        # ---------------- COMMUNICATIONS: custom meeting ----------------
        if data == "help:settings:meeting":
            clear_comm_meeting_flow(context)
            clear_bcast_flow(context)
            context.user_data[COMM_MEETING_ACTIVE] = True
            context.user_data[COMM_MEETING_STEP] = "topic"
            context.user_data[COMM_MEETING_DATA] = {
                "topic": None,
                "description_html": None,
                "link": None,
                "recipient_mode": None,
                "profile_ids": [],
            }
            context.user_data[COMM_MEETING_SELECTED_PIDS] = []
            await q.edit_message_text(
                "📅 <b>Запланировать встречу</b>\n\n"
                "Шаг 1/5: отправьте <b>тему встречи</b>.\n"
                "В сообщении для получателей тема будет выделена жирным.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ Отмена", callback_data="help:settings:meeting:cancel")]
                ]),
            )
            return

        if data == "help:settings:meeting:cancel":
            clear_comm_meeting_flow(context)
            await q.edit_message_text(
                "✅ Создание встречи отменено.",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_settings_communications(),
            )
            return

        if data == "help:settings:meeting:recipients_back":
            if not context.user_data.get(COMM_MEETING_ACTIVE):
                await q.answer("Мастер встречи уже закрыт.", show_alert=True)
                return
            await q.edit_message_text(
                "Шаг 4/5: выберите, куда отправить встречу:",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_meeting_recipient_mode(),
            )
            return

        if data == "help:settings:meeting:recipients:chats":
            if not context.user_data.get(COMM_MEETING_ACTIVE):
                await q.answer("Мастер встречи уже закрыт.", show_alert=True)
                return
            d = _meeting_get_data(context)
            d["recipient_mode"] = "chats"
            d["profile_ids"] = []
            context.user_data[COMM_MEETING_DATA] = d
            await q.edit_message_text(
                "Шаг 5/5: отправить встречу сейчас или запланировать время?",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_send_timing("help:settings:meeting"),
            )
            return

        if data.startswith("help:settings:meeting:recipients:profiles:"):
            if not context.user_data.get(COMM_MEETING_ACTIVE):
                await q.answer("Мастер встречи уже закрыт.", show_alert=True)
                return
            page = int(data.split(":")[-1])
            selected = set(context.user_data.get(COMM_MEETING_SELECTED_PIDS) or [])
            await q.edit_message_text(
                "👥 <b>Выберите одного или нескольких сотрудников</b>\n\n"
                "⚠️ — сотрудник ещё не связал карточку со своим Telegram.",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_meeting_profile_picker(selected, page),
            )
            return

        if data.startswith("help:settings:meeting:profile_toggle:"):
            if not context.user_data.get(COMM_MEETING_ACTIVE):
                await q.answer("Мастер встречи уже закрыт.", show_alert=True)
                return
            parts = data.split(":")
            pid = int(parts[-2])
            page = int(parts[-1])
            selected = set(context.user_data.get(COMM_MEETING_SELECTED_PIDS) or [])
            if pid in selected:
                selected.remove(pid)
            else:
                selected.add(pid)
            context.user_data[COMM_MEETING_SELECTED_PIDS] = sorted(selected)
            await q.edit_message_reply_markup(reply_markup=kb_meeting_profile_picker(selected, page))
            await q.answer()
            return

        if data == "help:settings:meeting:profiles_done":
            if not context.user_data.get(COMM_MEETING_ACTIVE):
                await q.answer("Мастер встречи уже закрыт.", show_alert=True)
                return
            selected = sorted(set(context.user_data.get(COMM_MEETING_SELECTED_PIDS) or []))
            if not selected:
                await q.answer("Выберите хотя бы одного сотрудника.", show_alert=True)
                return
            d = _meeting_get_data(context)
            d["recipient_mode"] = "profiles"
            d["profile_ids"] = selected
            context.user_data[COMM_MEETING_DATA] = d
            await q.edit_message_text(
                "Шаг 5/5: отправить встречу сейчас или запланировать время?",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_send_timing("help:settings:meeting"),
            )
            return

        if data == "help:settings:meeting:timing:later":
            if not context.user_data.get(COMM_MEETING_ACTIVE):
                await q.answer("Мастер встречи уже закрыт.", show_alert=True)
                return
            context.user_data[COMM_MEETING_STEP] = "schedule_time"
            await q.edit_message_text(
                "🕒 <b>Время отправки встречи</b>\n\n"
                "Введите дату и время по Москве:\n"
                "<code>ДД.ММ.ГГГГ ЧЧ:ММ</code>\n"
                "Например: <code>24.07.2026 10:30</code>",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ Отмена", callback_data="help:settings:meeting:cancel")]
                ]),
            )
            return

        if data == "help:settings:meeting:timing:now":
            if not context.user_data.get(COMM_MEETING_ACTIVE):
                await q.answer("Мастер встречи уже закрыт.", show_alert=True)
                return
            d = _meeting_get_data(context)
            d["send_mode"] = "now"
            d.pop("send_at_utc", None)
            d.pop("send_at_display", None)
            context.user_data[COMM_MEETING_DATA] = d
            await q.edit_message_text(
                _meeting_confirmation_html(d),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Отправить встречу", callback_data="help:settings:meeting:confirm")],
                    [InlineKeyboardButton("❌ Отмена", callback_data="help:settings:meeting:cancel")],
                ]),
            )
            return

        if data == "help:settings:meeting:confirm":
            if not context.user_data.get(COMM_MEETING_ACTIVE):
                await q.answer("Данные встречи уже очищены.", show_alert=True)
                return
            d = _meeting_get_data(context)
            payload = _meeting_payload_from_data(d)
            if d.get("send_mode") == "schedule":
                item_id = db_scheduled_communication_add(
                    "meeting",
                    payload,
                    d["send_at_utc"],
                    update.effective_user.id if update.effective_user else None,
                )
                display = d.get("send_at_display") or "указанное время"
                clear_comm_meeting_flow(context)
                await q.edit_message_text(
                    f"✅ Встреча запланирована на <b>{escape(display)}</b>.\n"
                    f"Номер задания: <code>{item_id}</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb_settings_communications(),
                )
                return

            ok, fail = await send_custom_meeting(context, payload)
            clear_comm_meeting_flow(context)
            await q.edit_message_text(
                "✅ Встреча отправлена.\n\n"
                f"Успешно: <b>{ok}</b>\nОшибок: <b>{fail}</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_settings_communications(),
            )
            return

        # ---------------- COMMUNICATIONS: broadcast ----------------
        if data == "help:settings:bcast":
            clear_bcast_flow(context)
            clear_comm_meeting_flow(context)
            context.user_data[BCAST_ACTIVE] = True
            context.user_data[BCAST_STEP] = "heading_choice"
            context.user_data[BCAST_DATA] = {
                "topic": None,
                "tag": None,
                "text_html": None,
                "files": [],
            }
            await q.edit_message_text(
                "📣 <b>Рассылка</b>\n\n"
                "Шаг 1/4: выберите заголовок рассылки.",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_bcast_heading_choice(),
                disable_web_page_preview=True,
            )
            return

        if data == "help:settings:bcast:cancel":
            clear_bcast_flow(context)
            await q.edit_message_text("✅ Рассылка отменена.", parse_mode=ParseMode.HTML, reply_markup=kb_settings_communications())
            return

        if data == "help:settings:bcast:heading:topic":
            context.user_data[BCAST_STEP] = "topic"
            await q.edit_message_text(
                "Шаг 1/4: <b>Тема</b> будет выделена жирным.\n"
                "Отправьте тему одним сообщением.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ Отмена", callback_data="help:settings:bcast:cancel")]
                ]),
            )
            return

        if data == "help:settings:bcast:heading:tag":
            await q.edit_message_text(
                "🏷 <b>Выберите тег рассылки</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_bcast_tag_pick(),
            )
            return

        if data == "help:settings:bcast:heading:none":
            d = _bcast_get_data(context)
            d["topic"] = None
            d["tag"] = None
            context.user_data[BCAST_DATA] = d
            context.user_data[BCAST_STEP] = "text"
            await q.edit_message_text(
                "Шаг 2/4: <b>Текст рассылки</b> 📝\n"
                "Отправьте текст одним сообщением. Оформление Telegram сохранится.\n"
                "Если текст не нужен — отправьте <code>-</code>.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ Отмена", callback_data="help:settings:bcast:cancel")]
                ]),
            )
            return

        if data == "help:settings:bcast:tag_create":
            context.user_data[WAITING_BCAST_TAG_NAME] = True
            context.user_data[BCAST_TAG_MODE] = "wizard"
            await q.edit_message_text(
                "➕ <b>Новый тег рассылки</b>\n\nОтправьте название одним сообщением.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ Отмена", callback_data="help:settings:bcast:cancel")]
                ]),
            )
            return

        if data.startswith("help:settings:bcast:tag:"):
            tag = db_broadcast_tag_get(int(data.split(":")[-1]))
            if not tag:
                await q.answer("Тег не найден.", show_alert=True)
                return
            d = _bcast_get_data(context)
            d["topic"] = None
            d["tag"] = tag["name"]
            context.user_data[BCAST_DATA] = d
            context.user_data[BCAST_STEP] = "text"
            await q.edit_message_text(
                f"Выбран тег: <b>#{escape(tag['name'])}</b>\n\n"
                "Шаг 2/4: отправьте текст рассылки одним сообщением. "
                "Жирный, курсив, подчёркивание, зачёркивание и скрытый текст сохранятся.\n"
                "Если текст не нужен — отправьте <code>-</code>.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ Отмена", callback_data="help:settings:bcast:cancel")]
                ]),
            )
            return

        if data == "help:settings:bcast:clear_files":
            d = _bcast_get_data(context)
            d["files"] = []
            context.user_data[BCAST_DATA] = d
            await q.answer("Файлы очищены ✅")
            return

        if data == "help:settings:bcast:send":
            d = _bcast_get_data(context)
            message_html = _bcast_compose_message(d.get("topic"), d.get("text_html"), d.get("tag"))
            files = d.get("files") or []
            if not message_html and not files:
                await q.answer("Нечего отправлять: добавьте текст или файлы.", show_alert=True)
                return
            await q.edit_message_text(
                "Шаг 4/4: отправить рассылку сейчас или запланировать время?",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_send_timing("help:settings:bcast"),
            )
            return

        if data == "help:settings:bcast:timing:later":
            context.user_data[BCAST_STEP] = "schedule_time"
            await q.edit_message_text(
                "🕒 <b>Время отправки рассылки</b>\n\n"
                "Введите дату и время по Москве:\n"
                "<code>ДД.ММ.ГГГГ ЧЧ:ММ</code>\n"
                "Например: <code>24.07.2026 10:30</code>",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ Отмена", callback_data="help:settings:bcast:cancel")]
                ]),
            )
            return

        if data == "help:settings:bcast:timing:now":
            d = _bcast_get_data(context)
            d["send_mode"] = "now"
            d.pop("send_at_utc", None)
            d.pop("send_at_display", None)
            context.user_data[BCAST_DATA] = d
            await q.edit_message_text(
                _bcast_confirmation_html(d),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=kb_danger_confirm(
                    "help:settings:bcast:send_confirm",
                    "help:settings:bcast:cancel",
                    "📣 Да, отправить рассылку",
                ),
            )
            return

        if data == "help:settings:bcast:send_confirm":
            d = _bcast_get_data(context)
            message_html = _bcast_compose_message(d.get("topic"), d.get("text_html"), d.get("tag"))
            files = d.get("files") or []
            if not message_html and not files:
                await q.answer("Данные рассылки уже очищены.", show_alert=True)
                return

            if d.get("send_mode") == "schedule":
                payload = {
                    "topic": d.get("topic"),
                    "tag": d.get("tag"),
                    "text_html": d.get("text_html"),
                    "files": files,
                }
                item_id = db_scheduled_communication_add(
                    "broadcast",
                    payload,
                    d["send_at_utc"],
                    update.effective_user.id if update.effective_user else None,
                )
                display = d.get("send_at_display") or "указанное время"
                clear_bcast_flow(context)
                await q.edit_message_text(
                    f"✅ Рассылка запланирована на <b>{escape(display)}</b>.\n"
                    f"Номер задания: <code>{item_id}</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb_settings_communications(),
                )
                return

            ok, fail = await broadcast_to_chats(context, message_html, files)
            clear_bcast_flow(context)
            await q.edit_message_text(
                f"✅ Рассылка отправлена.\n\n"
                f"Успешно: <b>{ok}</b>\n"
                f"Ошибок: <b>{fail}</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_settings_communications(),
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
            await q.edit_message_text(
                "⚠️ <b>Опасная операция: восстановление базы</b>\n\n"
                "Данные из ZIP будут записаны в текущую базу: анкеты, документы, категории, "
                "чаты и ачивки могут быть добавлены или изменены.\n\n"
                "Перед продолжением рекомендуется скачать свежий бэкап.",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_danger_confirm(
                    "help:settings:restore_zip:confirm",
                    "help:settings:system",
                    "⚠️ Продолжить восстановление",
                ),
            )
            return

        if data == "help:settings:restore_zip:confirm":
            clear_restore_zip(context)
            context.chat_data[WAITING_RESTORE_ZIP] = True
            context.chat_data[WAITING_USER_ID] = update.effective_user.id
            context.chat_data[WAITING_SINCE_TS] = int(time.time())
            await q.edit_message_text(
                "📥 <b>Восстановление подтверждено</b>\n\n"
                "Теперь пришлите ZIP-файл бэкапа следующим сообщением.\n"
                "Ожидание будет отменено через 10 минут.",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_cancel_wizard_settings(),
            )
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
                            "Результаты теста покажут твою подкованность в данной тематике 💪\n"
                            f"{TEST_RETAKE_POLICY_TEXT}"
                        )
                        assignment_callback = f"help:testv2:myopen:{aid}"
                        await context.bot.send_message(
                            chat_id=tg_user_id,
                            text=notify_text,
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("▶️ Открыть назначение", callback_data=assignment_callback)]]),
                            disable_web_page_preview=True,
                        )
                        db_notification_add(
                            tg_user_id,
                            "test_assigned",
                            f"Назначен тест: {title}",
                            f"Длительность: {duration_text}. {TEST_RETAKE_POLICY_TEXT}",
                            callback_data=assignment_callback,
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
            assignment = db_test_get_assignment(aid)
            if not assignment:
                await q.answer("Результат не найден.", show_alert=True)
                return
            profile = db_profiles_get(int(assignment["profile_id"]))
            who = profile["full_name"] if profile else f"ID {assignment['profile_id']}"
            await q.edit_message_text(
                "⚠️ <b>Удалить результат теста?</b>\n\n"
                f"Сотрудник: <b>{escape(who)}</b>\n"
                f"Задание №<b>{aid}</b>\n\n"
                "Ответы и результат будут удалены без возможности восстановления.",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_danger_confirm(
                    f"help:settings:test:results:delete_confirm:{aid}",
                    "help:settings:test:results",
                    "🗑 Да, удалить результат",
                ),
            )
            return

        if data.startswith("help:settings:test:results:delete_confirm:"):
            aid = int(data.split(":")[-1])
            ok = db_test_delete_assignment_only(aid)
            items = db_test_list_recent_results(20)
            await q.edit_message_text(
                "✅ Результат удалён.\n\nВыберите тест:" if ok else "⚠️ Результат уже не найден.",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_test_results_list(items),
                disable_web_page_preview=True,
            )
            return

        if data == "help:settings:ach":
            clear_bcast_flow(context)
            clear_ach_wiz(context)
            await q.edit_message_text(
                "🏆 <b>Ачивки и номинации</b>\n\n"
                "Можно выдать ачивку вручную или рассмотреть номинации коллег.\n\n"
                "У каждой ачивки есть уровень: I, II или III. "
                "Для одобренных номинаций уровень «Командного вклада» повышается автоматически.",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_achievements_menu(),
            )
            return

        if data == "help:settings:ach:nominations":
            await q.edit_message_text(
                "📨 <b>Номинации на рассмотрении</b>\n\n"
                "Выберите номинацию, чтобы посмотреть причину и принять решение.",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_pending_nominations(),
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
                "Шаг 2/5: отправьте <b>эмодзи</b> (пример: 🏅)",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_cancel_wizard_settings(),
            )
            return

        if data.startswith("help:settings:ach:level:"):
            if not context.chat_data.get(ACH_WIZ_ACTIVE) or context.chat_data.get(ACH_WIZ_STEP) != "level":
                await q.answer("Сначала начните выдачу ачивки.", show_alert=True)
                return
            try:
                level = int(data.split(":")[-1])
            except ValueError:
                level = 1
            level = max(1, min(level, 3))
            achievement_data = context.chat_data.get(ACH_WIZ_DATA) or {}
            achievement_data["level"] = level
            achievement_data["achievement_key"] = normalize_achievement_key(achievement_data.get("title", "Ачивка"))
            context.chat_data[ACH_WIZ_DATA] = achievement_data
            context.chat_data[ACH_WIZ_STEP] = "description"
            await q.edit_message_text(
                f"🎁 Ачивка: <b>{escape(achievement_data.get('title', 'Ачивка'))}</b>\n"
                f"Уровень: <b>{achievement_level_label(level)}</b>\n\n"
                "Шаг 5/5: напишите <b>описание</b> — за что выдаётся ачивка 🙂",
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
                "• ✏️ Переименовать категорию — изменить название без переноса файлов\n"
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

        if data == "help:settings:cats:rename":
            clear_docs_flow(context)
            await q.edit_message_text(
                "✏️ <b>Переименование категории</b>\n\nВыберите категорию, которую нужно переименовать:",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_pick_category_to_rename(),
            )
            return

        if data.startswith("help:settings:cats:rename:"):
            cid = int(data.split(":")[-1])
            cat = db_docs_get_category(cid)
            if not cat:
                try:
                    await q.answer("Категория не найдена", show_alert=True)
                except (TimedOut, NetworkError):
                    pass
                await q.edit_message_text(
                    "✏️ <b>Переименование категории</b>\n\nКатегория не найдена.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb_pick_category_to_rename(),
                )
                return

            clear_docs_flow(context)
            context.chat_data[WAITING_EDIT_CATEGORY_ID] = cid
            context.chat_data[WAITING_USER_ID] = update.effective_user.id
            context.chat_data[WAITING_SINCE_TS] = int(time.time())
            await q.edit_message_text(
                "✏️ <b>Переименование категории</b>\n\n"
                f"Текущее название: <code>{escape(cat['title'])}</code>\n\n"
                "Отправьте новое название одним сообщением.",
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
            category = db_docs_get_category(cid)
            if not category:
                await q.answer("Категория не найдена.", show_alert=True)
                return
            await q.edit_message_text(
                "⚠️ <b>Удалить категорию?</b>\n\n"
                f"Категория: <b>{escape(category['title'])}</b>\n\n"
                "Удаление сработает только для пустой категории.",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_danger_confirm(
                    f"help:settings:cats:del_confirm:{cid}",
                    "help:settings:cats:del",
                    "🗑 Да, удалить категорию",
                ),
            )
            return

        if data.startswith("help:settings:cats:del_confirm:"):
            cid = int(data.split(":")[-1])
            ok = db_docs_delete_category_if_empty(cid)
            if ok:
                await q.edit_message_text("✅ Категория удалена.", reply_markup=kb_settings_categories(), parse_mode=ParseMode.HTML)
            else:
                await q.edit_message_text(
                    "⚠️ Категория не удалена: она содержит документы или уже отсутствует.",
                    reply_markup=kb_settings_categories(),
                    parse_mode=ParseMode.HTML,
                )
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
            document = db_docs_get(did)
            if not document:
                await q.answer("Файл не найден.", show_alert=True)
                return
            await q.edit_message_text(
                "⚠️ <b>Удалить документ?</b>\n\n"
                f"Документ: <b>{escape(document['title'])}</b>\n\n"
                "Запись исчезнет из каталога документов. Действие нельзя отменить.",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_danger_confirm(
                    f"help:settings:del_doc_confirm:{did}",
                    "help:settings:del_doc",
                    "🗑 Да, удалить документ",
                ),
            )
            return

        if data.startswith("help:settings:del_doc_confirm:"):
            did = int(data.split(":")[-1])
            ok = db_docs_delete_doc(did)
            await q.edit_message_text(
                "✅ Файл удалён." if ok else "⚠️ Файл уже не найден.",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_settings_content(),
            )
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
            new_doc_id = db_docs_add_doc(cid, pending["title"], pending.get("description"), pending["file_id"], pending["file_unique_id"], pending.get("mime"), pending.get("local_path"))
            clear_docs_flow(context)
            await q.edit_message_text(
                "✅ Документ добавлен. Теперь при необходимости назначьте ему теги или добавьте в подборку.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✏️ Настроить документ", callback_data=f"help:docs:admin:edit:{new_doc_id}")],
                    [InlineKeyboardButton("🏠 Документы", callback_data="help:docs")],
                ]),
            )
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
            start_profile_wizard(context, update.effective_user.id, mode="admin_add")
            await q.edit_message_text(
                "➕ <b>Добавление анкеты</b>\n\n"
                "Шаг 1/8: отправьте <b>Имя и Фамилию</b>.\n"
                "Пример: <code>Иван Петров</code>",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_cancel_wizard_settings(),
            )
            return

        if data == "help:settings:edit_profile":
            clear_profile_wiz(context)
            await q.edit_message_text(
                "✏️ <b>Редактирование анкеты</b>\n\nВыберите человека:",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_pick_profile_to_edit(),
            )
            return

        if data.startswith("help:settings:edit_profile:"):
            pid = int(data.split(":")[-1])
            p = db_profiles_get(pid)
            if not p:
                try:
                    await q.answer("Анкета не найдена", show_alert=True)
                except (TimedOut, NetworkError):
                    pass
                await q.edit_message_text(
                    "✏️ <b>Редактирование анкеты</b>\n\nАнкета не найдена.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb_pick_profile_to_edit(),
                )
                return

            start_profile_wizard(context, update.effective_user.id, mode="admin_edit", initial_data=p, edit_pid=pid)
            await q.edit_message_text(
                "✏️ <b>Редактирование анкеты</b>\n\n"
                f"Сейчас редактируем: <b>{html_lib.escape(p['full_name'])}</b>\n\n"
                "Шаг 1/8: отправьте <b>Имя и Фамилию</b>.\n"
                f"Текущее значение: <code>{html_lib.escape(p['full_name'])}</code>",
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
            profile = db_profiles_get(pid)
            if not profile:
                await q.answer("Анкета не найдена.", show_alert=True)
                return
            achievements_count = db_achievements_count(pid)
            test_summary = db_profile_test_summary(pid)
            await q.edit_message_text(
                "⚠️ <b>Удалить анкету сотрудника?</b>\n\n"
                f"Сотрудник: <b>{escape(profile['full_name'])}</b>\n"
                f"Ачивок: <b>{achievements_count}</b>\n"
                f"Тестов: <b>{test_summary['total']}</b>\n\n"
                "Связанные данные могут быть удалены каскадно. Действие нельзя отменить.",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_danger_confirm(
                    f"help:settings:del_profile_confirm:{pid}",
                    "help:settings:del_profile",
                    "🗑 Да, удалить анкету",
                ),
            )
            return

        if data.startswith("help:settings:del_profile_confirm:"):
            pid = int(data.split(":")[-1])
            ok = db_profiles_delete(pid)
            await q.edit_message_text(
                "✅ Анкета удалена." if ok else "⚠️ Анкета уже не найдена.",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_settings_people(),
            )
            return

    try:

        await q.answer()

    except (TimedOut, NetworkError):

        pass



# ---------------- HANDLERS: NEW MEMBERS ----------------

async def on_new_members(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat:
        return

    new_members = update.message.new_chat_members or []
    if not new_members:
        return

    bot_id = context.bot.id
    names: list[str] = []

    for member in new_members:
        # Добавление самого бота обрабатываем отдельно. При групповом добавлении
        # остальных участников цикл продолжается, чтобы их карточки не потерялись.
        if member.id == bot_id:
            await update.message.reply_text(
                "Привет! Я в чате ✅\n"
                "Чтобы включить уведомления, админ должен выполнить команду /setchat."
            )
            continue

        # Техническим ботам карточки сотрудников не создаём.
        if member.is_bot:
            continue

        name = (member.full_name or member.first_name or "коллега").strip()
        if name:
            names.append(name)

        # Автоматические карточки ведём только для основного рабочего чата.
        if update.effective_chat.id == ACCESS_CHAT_ID:
            try:
                profile_id, created = db_profiles_ensure_from_tg_user(member)
                logger.info(
                    "Employee profile %s: chat_id=%s user_id=%s profile_id=%s",
                    "created" if created else "activated",
                    update.effective_chat.id,
                    member.id,
                    profile_id,
                )
            except Exception:
                # Ошибка карточки не должна ломать приветственное сообщение.
                logger.exception(
                    "Could not create/activate employee profile: chat_id=%s user_id=%s",
                    update.effective_chat.id,
                    member.id,
                )

    if names:
        joined = ", ".join(names)
        text = WELCOME_TEXT.format(name=joined)
        await update.message.reply_text(text, disable_web_page_preview=True)


async def on_left_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Скрывает карточку сотрудника после выхода или удаления из рабочего чата."""
    if not update.message or not update.effective_chat:
        return
    if update.effective_chat.id != ACCESS_CHAT_ID:
        return

    member = update.message.left_chat_member
    if not member:
        return
    if member.id == context.bot.id or member.is_bot:
        return

    try:
        profile_id = db_profiles_deactivate_by_tg_user(member)
        if profile_id is None:
            logger.warning(
                "No employee profile found for departed member: chat_id=%s user_id=%s",
                update.effective_chat.id,
                member.id,
            )
            return

        logger.info(
            "Employee profile deactivated: chat_id=%s user_id=%s profile_id=%s",
            update.effective_chat.id,
            member.id,
            profile_id,
        )
    except Exception:
        logger.exception(
            "Could not deactivate employee profile: chat_id=%s user_id=%s",
            update.effective_chat.id,
            member.id,
        )

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
            await update.message.reply_text("✅ Документ добавлен. Можешь добавить ещё или нажмите «✅ Продолжить».", reply_markup=kb_bcast_files_menu())
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
                        achievement_key = (row.get("achievement_key") or normalize_achievement_key(title)).strip()
                        try:
                            level = max(1, int(row.get("level") or 1))
                        except (TypeError, ValueError):
                            level = 1
                        # не тащим awarded_at/awarded_by в точности — создаём новую запись восстановления
                        db_achievement_award_add(
                            int(pid), emoji, title, description, None,
                            level=level, achievement_key=achievement_key,
                        )
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

    # ---------------- DOC FILE REPLACEMENT ----------------
    replace_doc_id = context.chat_data.get(WAITING_DOC_REPLACE_ID)
    if replace_doc_id:
        waiting_user = context.chat_data.get(WAITING_USER_ID)
        current_user = update.effective_user.id if update.effective_user else None
        if waiting_user and current_user != waiting_user:
            return
        if not await is_admin_scoped(update, context):
            clear_docs_flow(context)
            await update.message.reply_text("❌ Только администраторы могут заменять документы.")
            return
        doc = update.message.document
        if not doc:
            return
        local_path = None
        try:
            tg_file = await context.bot.get_file(doc.file_id)
            safe_name = (doc.file_name or "document").replace("/", "_")
            local_path = str(Path(STORAGE_DIR) / "docs" / f"{doc.file_unique_id}_{safe_name}")
            await tg_file.download_to_drive(custom_path=local_path)
        except Exception as e:
            logger.exception("Failed to backup replacement doc locally: %s", e)
        ok = db_doc_replace_file(
            int(replace_doc_id),
            doc.file_id,
            doc.file_unique_id,
            doc.mime_type,
            local_path,
        )
        clear_docs_flow(context)
        await update.message.reply_text(
            "✅ Файл заменён. Название, описание, теги, избранное и подборки сохранены."
            if ok else "❌ Документ не найден.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📄 Открыть документ", callback_data=f"help:docs:open:{int(replace_doc_id)}")],
                [InlineKeyboardButton("🏠 Документы", callback_data="help:docs")],
            ]),
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

    # Фото сотрудника — последний шаг мастера анкеты.
    if context.chat_data.get(PROFILE_WIZ_ACTIVE) and context.chat_data.get(PROFILE_WIZ_STEP) == "photo":
        waiting_user = context.chat_data.get(WAITING_USER_ID)
        current_user = update.effective_user.id if update.effective_user else None
        if waiting_user and current_user != waiting_user:
            return
        photos = update.message.photo or []
        if not photos:
            return
        ph = photos[-1]
        data = context.chat_data.get(PROFILE_WIZ_DATA) or {}
        data["photo_file_id"] = ph.file_id
        data["photo_action"] = "replace"
        context.chat_data[PROFILE_WIZ_DATA] = data
        _ok, msg, markup = await finalize_profile_wizard(update, context)
        await update.message.reply_text(msg, reply_markup=markup)
        return

    if context.user_data.get(BCAST_ACTIVE) and context.user_data.get(BCAST_STEP) == "files":
        photos = update.message.photo or []
        if photos:
            # берём самый большой
            ph = photos[-1]
            d = _bcast_get_data(context)
            d["files"].append({"kind": "photo", "file_id": ph.file_id, "file_unique_id": ph.file_unique_id})
            context.user_data[BCAST_DATA] = d
            await update.message.reply_text("✅ Фото добавлено. Можешь добавить ещё или нажмите «✅ Продолжить».", reply_markup=kb_bcast_files_menu())
        return

async def on_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat:
        return
    if context.user_data.get(BCAST_ACTIVE) and context.user_data.get(BCAST_STEP) == "files":
        vid = update.message.video
        if vid:
            d = _bcast_get_data(context)
            d["files"].append({"kind": "video", "file_id": vid.file_id, "file_unique_id": vid.file_unique_id})
            context.user_data[BCAST_DATA] = d
            await update.message.reply_text("✅ Видео добавлено. Можешь добавить ещё или нажмите «✅ Продолжить».", reply_markup=kb_bcast_files_menu())



# ---------------- HANDLERS: TEXT INPUT (dates / categories / profiles) ----------------

async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat:
        return


    # ===================== TESTING: sync tg_user_id =============
    await sync_profile_user_id_from_update(update)

    user_id = update.effective_user.id if update.effective_user else None
    text = (update.message.text or "").strip()

    text_html = (message_to_html(update.message) or "").strip()

    # recurring stand-up / industry meeting management
    if context.user_data.get(REGULAR_MEETING_ACTIVE):
        if not await is_admin_scoped(update, context):
            clear_regular_meeting_flow(context)
            await update.message.reply_text("❌ Только администраторы могут управлять регулярными встречами.")
            return

        step = context.user_data.get(REGULAR_MEETING_STEP)
        d = context.user_data.get(REGULAR_MEETING_DATA) or {}
        meeting_type = d.get("meeting_type")

        if step == "original_date":
            original_d = parse_regular_meeting_date(text)
            if not original_d:
                await update.message.reply_text(
                    "❌ Не удалось распознать дату. Используйте формат ДД.ММ.ГГГГ, например 24.07.2026."
                )
                return
            today_d = datetime.now(MOSCOW_TZ).date()
            if original_d < today_d:
                await update.message.reply_text("❌ Нельзя изменить встречу задним числом.")
                return
            if not regular_meeting_is_due(meeting_type, original_d):
                schedule_hint = "понедельник, среду или пятницу" if meeting_type == MEETING_STANDUP else "среду"
                await update.message.reply_text(
                    f"❌ На эту дату регулярная встреча не запланирована. "
                    f"Для выбранного типа укажите {schedule_hint}."
                )
                return
            d["original_date"] = format_regular_meeting_date(original_d)
            context.user_data[REGULAR_MEETING_DATA] = d
            if d.get("action") == "move":
                context.user_data[REGULAR_MEETING_STEP] = "new_date"
                await update.message.reply_text(
                    "📅 Теперь отправьте <b>новую дату</b> в формате <code>ДД.ММ.ГГГГ</code>.\n"
                    "После даты бот предложит выбрать время уведомления.\n"
                    "Новая дата должна быть позже исходной.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("❌ Отмена", callback_data="help:settings:regular_meeting:cancel")]
                    ]),
                )
            else:
                context.user_data[REGULAR_MEETING_STEP] = "reason"
                await update.message.reply_text(
                    "📝 Укажите <b>причину отмены</b> одним сообщением.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("❌ Отмена", callback_data="help:settings:regular_meeting:cancel")]
                    ]),
                )
            return

        if step == "new_date":
            new_d = parse_regular_meeting_date(text)
            original_d = parse_regular_meeting_date(d.get("original_date") or "")
            if not new_d:
                await update.message.reply_text(
                    "❌ Не удалось распознать дату. Используйте формат ДД.ММ.ГГГГ."
                )
                return
            if not original_d or new_d <= original_d:
                await update.message.reply_text("❌ Новая дата должна быть позже исходной даты встречи.")
                return
            d["new_date"] = format_regular_meeting_date(new_d)
            d.pop("new_time", None)
            context.user_data[REGULAR_MEETING_DATA] = d
            context.user_data[REGULAR_MEETING_STEP] = "new_time"
            await update.message.reply_text(
                "🕒 <b>Выберите время нового уведомления</b>\n\n"
                f"Новая дата: <b>{format_regular_meeting_date(new_d)}</b>\n"
                "Время указывается по Москве.",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_regular_meeting_time_picker(meeting_type),
            )
            return

        if step == "new_time":
            await update.message.reply_text(
                "Выберите время кнопкой под сообщением или нажмите "
                "«Указать другое время».",
                reply_markup=kb_regular_meeting_time_picker(meeting_type),
            )
            return

        if step == "new_time_manual":
            new_time = parse_regular_meeting_time(text)
            new_d = parse_regular_meeting_date(d.get("new_date") or "")
            if not new_time:
                await update.message.reply_text(
                    "❌ Не удалось распознать время. Используйте формат ЧЧ:ММ, "
                    "например 14:45."
                )
                return
            if not new_d:
                clear_regular_meeting_flow(context)
                await update.message.reply_text(
                    "❌ Данные переноса устарели. Начните заново из раздела «Коммуникации»."
                )
                return
            d["new_time"] = new_time
            context.user_data[REGULAR_MEETING_DATA] = d
            context.user_data[REGULAR_MEETING_STEP] = "reason"
            await update.message.reply_text(
                "🔄 <b>Новое уведомление</b>\n\n"
                f"Дата и время: <b>{format_regular_meeting_datetime(new_d, new_time)}</b>\n\n"
                "Укажите <b>причину переноса</b> одним сообщением.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton(
                        "❌ Отмена",
                        callback_data="help:settings:regular_meeting:cancel",
                    )
                ]]),
            )
            return

        if step == "reason":
            reason = re.sub(r"\s+", " ", text).strip()
            if len(reason) < 2:
                await update.message.reply_text("❌ Причина слишком короткая.")
                return
            if len(reason) > 500:
                await update.message.reply_text("❌ Причина должна быть не длиннее 500 символов.")
                return
            d["reason"] = reason
            context.user_data[REGULAR_MEETING_DATA] = d
            context.user_data[REGULAR_MEETING_STEP] = "notify"
            await update.message.reply_text(
                "🔔 <b>Уведомить сотрудников об изменении в подключённых чатах?</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_regular_meeting_notify(),
            )
            return

        return


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
        clear_nomination_flow(context)
        clear_bcast_flow(context)
        await update.message.reply_text("⏳ Время ожидания истекло. Начните действие заново через /help.")
        return



    # номинация коллеги: ждём текст причины
    if context.user_data.get(NOMINATION_ACTIVE):
        nomination_step = context.user_data.get(NOMINATION_STEP)
        nomination_data = context.user_data.get(NOMINATION_DATA) or {}
        if nomination_step != "reason":
            await update.message.reply_text("Выберите категорию кнопкой в предыдущем сообщении.")
            return
        created_ts = int(nomination_data.get("created_ts") or 0)
        if created_ts and int(time.time()) - created_ts > 15 * 60:
            clear_nomination_flow(context)
            await update.message.reply_text("⏳ Время заполнения номинации истекло. Начните заново через /help.")
            return
        if len(text.strip()) < 25:
            await update.message.reply_text(
                "Напишите подробнее — минимум 25 символов. "
                "Укажите, что сделал коллега и почему это было важно."
            )
            return
        reason = text.strip()[:1000]
        nominee_profile_id = nomination_data.get("nominee_profile_id")
        nominator_profile_id = nomination_data.get("nominator_profile_id")
        category_key = nomination_data.get("category_key") or "team_help"
        scope_chat_id = int(nomination_data.get("scope_chat_id") or ACCESS_CHAT_ID)
        if not nominee_profile_id or not nominator_profile_id:
            clear_nomination_flow(context)
            await update.message.reply_text("Не удалось определить анкеты. Начните номинацию заново через /help.")
            return

        allowed, restriction_text = db_nomination_check_allowed(
            nominator_user_id=int(user_id),
            nominator_profile_id=int(nominator_profile_id),
            nominee_profile_id=int(nominee_profile_id),
            category_key=category_key,
        )
        if not allowed:
            clear_nomination_flow(context)
            await update.message.reply_text(
                f"⚠️ Номинацию нельзя отправить: {restriction_text}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🏠 Главное меню", callback_data="help:main")]
                ]),
            )
            return

        nomination_id = db_nomination_create(
            scope_chat_id=scope_chat_id,
            nominator_user_id=user_id,
            nominator_profile_id=int(nominator_profile_id),
            nominee_profile_id=int(nominee_profile_id),
            category_key=category_key,
            reason=reason,
        )
        sent_ok, sent_fail = await send_nomination_to_admins(nomination_id, context)
        nominee_name = nomination_data.get("nominee_name") or "коллеги"
        category = nomination_category(category_key)
        clear_nomination_flow(context)
        status_note = "Администраторы получили уведомление." if sent_ok else "Номинация сохранена и доступна администраторам в разделе ачивок."
        await update.message.reply_text(
            f"✅ Номинация для <b>{escape(nominee_name)}</b> отправлена.\n"
            f"Категория: {escape(category['emoji'])} <b>{escape(category['title'])}</b>\n\n"
            f"{status_note}\n"
            "Спасибо, что отмечаете вклад коллег 🙌",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🏠 Главное меню", callback_data="help:main")]
            ]),
        )
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

    # сохранение нового тега рассылки
    if context.user_data.get(WAITING_BCAST_TAG_NAME):
        tag = db_broadcast_tag_add(text)
        mode = context.user_data.get(BCAST_TAG_MODE) or "manage"
        clear_bcast_tag_waiting(context)
        if not tag:
            await update.message.reply_text(
                "❌ Не получилось создать тег. Используйте буквы, цифры или подчёркивание.",
                reply_markup=kb_settings_communications(),
            )
            return
        if mode == "wizard" and context.user_data.get(BCAST_ACTIVE):
            d = _bcast_get_data(context)
            d["topic"] = None
            d["tag"] = tag["name"]
            context.user_data[BCAST_DATA] = d
            context.user_data[BCAST_STEP] = "text"
            await update.message.reply_text(
                f"✅ Тег <b>#{escape(tag['name'])}</b> сохранён и выбран.\n\n"
                "Шаг 2/4: отправьте текст рассылки одним сообщением. "
                "Оформление Telegram сохранится. Если текст не нужен — отправьте <code>-</code>.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ Отмена", callback_data="help:settings:bcast:cancel")]
                ]),
            )
            return
        await update.message.reply_text(
            f"✅ Тег <b>#{escape(tag['name'])}</b> сохранён.",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_broadcast_tags_manage(),
        )
        return

    # запланированная встреча: тема / описание / ссылка / время
    if context.user_data.get(COMM_MEETING_ACTIVE):
        step = context.user_data.get(COMM_MEETING_STEP)
        d = _meeting_get_data(context)

        if step == "topic":
            topic = text.strip()
            if len(topic) < 2:
                await update.message.reply_text("❌ Тема встречи слишком короткая.")
                return
            if len(topic) > 200:
                await update.message.reply_text("❌ Тема встречи должна быть не длиннее 200 символов.")
                return
            d["topic"] = topic
            context.user_data[COMM_MEETING_DATA] = d
            context.user_data[COMM_MEETING_STEP] = "description"
            await update.message.reply_text(
                "Шаг 2/5: отправьте <b>описание встречи</b>.\n"
                "Оформление текста сохранится. Если описания нет — отправьте <code>-</code>.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ Отмена", callback_data="help:settings:meeting:cancel")]
                ]),
            )
            return

        if step == "description":
            if text == "-":
                d["description_html"] = None
            else:
                if len(text) > 3000:
                    await update.message.reply_text("❌ Описание слишком длинное. Максимум 3000 символов.")
                    return
                d["description_html"] = text_html
            context.user_data[COMM_MEETING_DATA] = d
            context.user_data[COMM_MEETING_STEP] = "link"
            await update.message.reply_text(
                "Шаг 3/5: отправьте <b>ссылку на встречу</b>.\n"
                "Если ссылки нет — отправьте <code>-</code>.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ Отмена", callback_data="help:settings:meeting:cancel")]
                ]),
            )
            return

        if step == "link":
            if text == "-":
                d["link"] = None
            else:
                link = text.strip()
                if not re.match(r"^https?://", link, flags=re.IGNORECASE):
                    await update.message.reply_text(
                        "❌ Ссылка должна начинаться с <code>http://</code> или <code>https://</code>.",
                        parse_mode=ParseMode.HTML,
                    )
                    return
                if len(link) > 1000:
                    await update.message.reply_text("❌ Ссылка слишком длинная.")
                    return
                d["link"] = link
            context.user_data[COMM_MEETING_DATA] = d
            context.user_data[COMM_MEETING_STEP] = "recipients"
            await update.message.reply_text(
                "Шаг 4/5: выберите, куда отправить встречу:",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_meeting_recipient_mode(),
            )
            return

        if step == "schedule_time":
            parsed = parse_moscow_send_time(text)
            if not parsed:
                await update.message.reply_text(
                    "❌ Не удалось распознать дату или время уже прошло.\n"
                    "Используйте формат <code>ДД.ММ.ГГГГ ЧЧ:ММ</code>, например "
                    "<code>24.07.2026 10:30</code>.",
                    parse_mode=ParseMode.HTML,
                )
                return
            send_at_utc, display = parsed
            d["send_mode"] = "schedule"
            d["send_at_utc"] = send_at_utc
            d["send_at_display"] = display
            context.user_data[COMM_MEETING_DATA] = d
            await update.message.reply_text(
                _meeting_confirmation_html(d),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Запланировать встречу", callback_data="help:settings:meeting:confirm")],
                    [InlineKeyboardButton("❌ Отмена", callback_data="help:settings:meeting:cancel")],
                ]),
            )
            return

        return

    # рассылка: тема/тег, форматированный текст, файлы и планирование
    if context.user_data.get(BCAST_ACTIVE):
        step = context.user_data.get(BCAST_STEP)
        d = _bcast_get_data(context)

        if step == "topic":
            topic = text.strip()
            if len(topic) < 2:
                await update.message.reply_text("❌ Тема слишком короткая.")
                return
            if len(topic) > 200:
                await update.message.reply_text("❌ Тема должна быть не длиннее 200 символов.")
                return
            d["topic"] = topic
            d["tag"] = None
            context.user_data[BCAST_DATA] = d
            context.user_data[BCAST_STEP] = "text"
            await update.message.reply_text(
                "Шаг 2/4: <b>Текст рассылки</b> 📝\n"
                "Отправьте текст одним сообщением. Жирный, курсив, подчёркивание, "
                "зачёркивание и скрытый текст сохранятся.\n"
                "Если текст не нужен — отправьте <code>-</code>.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ Отмена", callback_data="help:settings:bcast:cancel")]
                ]),
            )
            return

        if step == "text":
            if text == "-":
                d["text_html"] = None
            else:
                # Telegram counts visible text, while text_html contains tags.
                if len(text) > 3500:
                    await update.message.reply_text("❌ Текст слишком длинный. Максимум 3500 символов.")
                    return
                d["text_html"] = text_html
            context.user_data[BCAST_DATA] = d
            context.user_data[BCAST_STEP] = "files"
            await update.message.reply_text(
                "Шаг 3/4: <b>Файлы</b> 📎\n\n"
                "Можно прикрепить <b>документы, фото или видео</b>.\n"
                "Когда закончите — нажмите <b>✅ Продолжить</b>.\n"
                "Файлы необязательны.",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_bcast_files_menu(),
            )
            return

        if step == "schedule_time":
            parsed = parse_moscow_send_time(text)
            if not parsed:
                await update.message.reply_text(
                    "❌ Не удалось распознать дату или время уже прошло.\n"
                    "Используйте формат <code>ДД.ММ.ГГГГ ЧЧ:ММ</code>, например "
                    "<code>24.07.2026 10:30</code>.",
                    parse_mode=ParseMode.HTML,
                )
                return
            send_at_utc, display = parsed
            d["send_mode"] = "schedule"
            d["send_at_utc"] = send_at_utc
            d["send_at_display"] = display
            context.user_data[BCAST_DATA] = d
            await update.message.reply_text(
                _bcast_confirmation_html(d),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=kb_danger_confirm(
                    "help:settings:bcast:send_confirm",
                    "help:settings:bcast:cancel",
                    "🕒 Да, запланировать рассылку",
                ),
            )
            return

        # files and heading_choice wait for buttons/media.
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
                "Шаг 3/5: отправьте <b>название ачивки</b> (будет жирным).",
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
            d["achievement_key"] = normalize_achievement_key(d["title"])
            context.chat_data[ACH_WIZ_DATA] = d
            context.chat_data[ACH_WIZ_STEP] = "level"
            await update.message.reply_text(
                "Шаг 4/5: выберите <b>уровень ачивки</b>.\n\n"
                "I — первое достижение, II — уверенное развитие, III — высокий уровень вклада.",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_achievement_level_select(),
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
            level = max(1, min(int(d.get("level") or 1), 3))
            achievement_key = d.get("achievement_key") or normalize_achievement_key(d.get("title", "Ачивка"))
            award_id = db_achievement_award_add(
                int(pid),
                d.get("emoji", "🏆"),
                d.get("title", "Ачивка"),
                d.get("description", ""),
                admin_id,
                level=level,
                achievement_key=achievement_key,
            )
            progress = db_achievement_progress(int(pid), achievement_key)
            progress_text = progress["label"] if progress.get("next_threshold") else "максимальный уровень достигнут"

            scope_chat_id = get_scope_chat_id(update, context)
            mention = normalize_tg_mention(d.get("tg_link", "") or "")
            who = mention if mention else f"<b>{escape(d.get('full_name', 'Сотрудник'))}</b>"
            msg = (
                f"🎉 <b>Поздравляем, {who}!</b>\n\n"
                f"В твой профиль добавлена новая ачивка: <b>{escape(d.get('emoji', '🏆'))} {escape(d.get('title', 'Ачивка'))} · уровень {achievement_level_label(level)}</b>\n\n"
                f"Достижение получено за: «{escape(d.get('description', ''))}»\n\n"
                f"📈 Прогресс: <b>{escape(progress_text)}</b>\n\n"
                f"Так держать! 🚀🔥\n\n"
                f"Поддержите коллегу реакцией 👇"
            )

            sent = False
            if scope_chat_id:
                try:
                    await context.bot.send_message(
                        chat_id=scope_chat_id,
                        text=msg,
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True,
                        reply_markup=kb_achievement_reactions(award_id),
                    )
                    sent = True
                except Exception as e:
                    logger.exception("Cannot send achievement notify to scope chat: %s", e)

            if not sent:
                for chat_id in db_list_chats():
                    try:
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text=msg,
                            parse_mode=ParseMode.HTML,
                            disable_web_page_preview=True,
                            reply_markup=kb_achievement_reactions(award_id),
                        )
                        sent = True
                        break
                    except Exception:
                        pass

            prof = db_profiles_get(int(pid))
            if prof and prof.get("tg_user_id"):
                db_notification_add(
                    int(prof["tg_user_id"]),
                    "achievement",
                    f"Новая ачивка: {d.get('emoji', '🏆')} {d.get('title', 'Ачивка')} · {achievement_level_label(level)}",
                    f"За: {d.get('description', '')}\nПрогресс: {progress_text}",
                    callback_data="help:me:achievements",
                )
            clear_ach_wiz(context)
            await update.message.reply_text("✅ Ачивка выдана и опубликована в чате.", reply_markup=kb_help_settings())
            return

    # ---------------- DOCUMENTS KNOWLEDGE-BASE TEXT FLOWS ----------------
    if context.chat_data.get(WAITING_DOC_SEARCH):
        context.chat_data[WAITING_DOC_SEARCH] = False
        context.chat_data.pop(WAITING_USER_ID, None)
        context.chat_data.pop(WAITING_SINCE_TS, None)
        items = db_docs_search(text)
        context.user_data[DOCS_RETURN_CB] = "help:docs"
        await update.message.reply_text(
            f"🔎 <b>Результаты поиска</b>\n\nЗапрос: <code>{escape(text[:80])}</code>\nНайдено: <b>{len(items)}</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_docs_result_list(items, "— ничего не найдено —"),
        )
        return

    edit_title_id = context.chat_data.get(WAITING_DOC_EDIT_TITLE_ID)
    if edit_title_id:
        if not await is_admin_scoped(update, context):
            clear_docs_flow(context)
            await update.message.reply_text("❌ Только администраторы могут редактировать документы.")
            return
        if len(text.strip()) < 2:
            await update.message.reply_text("Название слишком короткое. Отправьте другое.")
            return
        ok = db_doc_update_title(int(edit_title_id), text)
        clear_docs_flow(context)
        await update.message.reply_text(
            "✅ Название обновлено." if ok else "❌ Документ не найден.",
            reply_markup=kb_doc_edit_menu(int(edit_title_id)),
        )
        return

    edit_desc_id = context.chat_data.get(WAITING_DOC_EDIT_DESC_ID)
    if edit_desc_id:
        if not await is_admin_scoped(update, context):
            clear_docs_flow(context)
            await update.message.reply_text("❌ Только администраторы могут редактировать документы.")
            return
        description = None if text.strip() == "-" else text.strip()
        ok = db_doc_update_description(int(edit_desc_id), description)
        clear_docs_flow(context)
        await update.message.reply_text(
            "✅ Описание обновлено." if ok else "❌ Документ не найден.",
            reply_markup=kb_doc_edit_menu(int(edit_desc_id)),
        )
        return

    if context.chat_data.get(WAITING_DOC_TAG_NAME):
        if not await is_admin_scoped(update, context):
            clear_docs_flow(context)
            await update.message.reply_text("❌ Только администраторы могут создавать теги.")
            return
        try:
            db_doc_tag_add(text)
        except sqlite3.IntegrityError:
            await update.message.reply_text("❌ Такой тег уже существует. Отправьте другое название.")
            return
        except ValueError as e:
            await update.message.reply_text(f"❌ {e}")
            return
        clear_docs_flow(context)
        await update.message.reply_text("✅ Тег создан.", reply_markup=kb_doc_tags_manage())
        return

    if context.chat_data.get(WAITING_DOC_COLLECTION_NAME):
        if not await is_admin_scoped(update, context):
            clear_docs_flow(context)
            await update.message.reply_text("❌ Только администраторы могут создавать подборки.")
            return
        try:
            collection_id = db_doc_collection_add(text)
        except sqlite3.IntegrityError:
            await update.message.reply_text("❌ Подборка с таким названием уже существует.")
            return
        except ValueError as e:
            await update.message.reply_text(f"❌ {e}")
            return
        clear_docs_flow(context)
        await update.message.reply_text(
            "✅ Подборка создана. Теперь добавьте в неё документы.",
            reply_markup=kb_doc_collection_manage(collection_id),
        )
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


    # ---------------- FAQ SEARCH FLOW ----------------
    if context.chat_data.get(WAITING_FAQ_SEARCH):
        query_text = (text or "").strip()
        if len(query_text) < 2:
            await update.message.reply_text(
                "Введите минимум 2 символа для поиска."
            )
            return

        context.chat_data[WAITING_FAQ_SEARCH] = False
        context.chat_data[FAQ_SEARCH_QUERY] = query_text
        result_text, result_keyboard = build_help_faq_search_page(query_text, 0)
        await update.message.reply_text(
            result_text,
            parse_mode=ParseMode.HTML,
            reply_markup=result_keyboard,
            disable_web_page_preview=True,
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
            "✅ Вопрос добавлен в FAQ и появился в разделе «Ответы на вопросы».",
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
        notice = await update.message.reply_text(
            f"{title}\nНовая дата: {text} 📌\nСледите за расписанием или чатом"
        )
        schedule_message_delete(context, notice)
        return

    # переименование категории
    if context.chat_data.get(WAITING_EDIT_CATEGORY_ID):
        if not await is_admin_scoped(update, context):
            clear_docs_flow(context)
            await update.message.reply_text("❌ Только администраторы могут управлять категориями.")
            return

        cid = int(context.chat_data.get(WAITING_EDIT_CATEGORY_ID))
        new_title = text.strip()
        if len(new_title) < 2:
            await update.message.reply_text("❌ Слишком коротко. Отправьте нормальное название категории.")
            return

        cat = db_docs_get_category(cid)
        if not cat:
            clear_docs_flow(context)
            await update.message.reply_text("❌ Категория не найдена. Начните заново через /help.", reply_markup=kb_help_settings())
            return

        old_title = cat["title"]
        if old_title.strip() == new_title:
            clear_docs_flow(context)
            await update.message.reply_text("ℹ️ Название не изменилось.", reply_markup=kb_settings_categories())
            return

        try:
            ok = db_docs_rename_category(cid, new_title)
        except sqlite3.IntegrityError:
            await update.message.reply_text("❌ Такая категория уже существует. Отправьте другое название.")
            return

        clear_docs_flow(context)
        if ok:
            await update.message.reply_text(
                f"✅ Категория переименована:\n{old_title} → {new_title}",
                reply_markup=kb_settings_categories(),
            )
        else:
            await update.message.reply_text("❌ Не удалось переименовать категорию.", reply_markup=kb_help_settings())
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
            new_doc_id = db_docs_add_doc(cid, pending["title"], pending.get("description"), pending["file_id"], pending["file_unique_id"], pending.get("mime"), pending.get("local_path"))
            clear_docs_flow(context)
            await update.message.reply_text(
                "✅ Категория создана и документ добавлен.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✏️ Настроить документ", callback_data=f"help:docs:admin:edit:{new_doc_id}")],
                    [InlineKeyboardButton("🏠 Документы", callback_data="help:docs")],
                ]),
            )
            return

        clear_docs_flow(context)
        await update.message.reply_text("✅ Категория добавлена.", reply_markup=kb_help_settings())
        return

    # анкета — шаги
    if context.chat_data.get(PROFILE_WIZ_ACTIVE):
        mode = context.chat_data.get(PROFILE_WIZ_MODE) or "admin_add"
        is_admin_here = await is_admin_scoped(update, context)
        if mode in ("admin_add", "admin_edit") and not is_admin_here:
            clear_profile_wiz(context)
            await update.message.reply_text("❌ Только администраторы могут управлять анкетами.")
            return
        if mode == "self_create" and is_admin_here:
            clear_profile_wiz(context)
            await update.message.reply_text("❌ Для этого сценария используйте раздел настроек администратора.")
            return
        if mode == "self_edit":
            own_profile = get_profile_for_user(update)
            edit_pid = context.chat_data.get(PROFILE_WIZ_EDIT_PID)
            if not own_profile or not edit_pid or int(own_profile["id"]) != int(edit_pid):
                clear_profile_wiz(context)
                await update.message.reply_text(
                    "❌ Не удалось подтвердить владельца анкеты. Редактирование отменено.",
                    reply_markup=kb_help_main(is_admin_user=is_admin_here),
                )
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
            await update.message.reply_text("Шаг 2/8: с какого года работает? Пример: 2022", reply_markup=kb_cancel_wizard_settings())
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
            await update.message.reply_text("Шаг 3/8: город проживания. Пример: Москва", reply_markup=kb_cancel_wizard_settings())
            return

        if step == "city":
            if len(text) < 2:
                await update.message.reply_text("❌ Укажите город.")
                return
            data["city"] = text
            context.chat_data[PROFILE_WIZ_DATA] = data
            context.chat_data[PROFILE_WIZ_STEP] = "birthday"
            await update.message.reply_text(
                "Шаг 4/8: день рождения (формат <b>ДД.ММ</b>)\n"
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
            await update.message.reply_text("Шаг 5/8: кратко о себе (1–3 предложения)", reply_markup=kb_cancel_wizard_settings())
            return

        if step == "about":
            if len(text) < 5:
                await update.message.reply_text("❌ Напишите чуть подробнее 🙂")
                return
            data["about"] = text
            context.chat_data[PROFILE_WIZ_DATA] = data
            context.chat_data[PROFILE_WIZ_STEP] = "topics"
            await update.message.reply_text("Шаг 6/8: по каким вопросам обращаться?", reply_markup=kb_cancel_wizard_settings())
            return

        if step == "topics":
            if len(text) < 3:
                await update.message.reply_text("❌ Укажите темы/вопросы.")
                return
            data["topics"] = text
            context.chat_data[PROFILE_WIZ_DATA] = data
            context.chat_data[PROFILE_WIZ_STEP] = "tg_link"
            await update.message.reply_text("Шаг 7/8: Telegram (@username или https://t.me/username)", reply_markup=kb_cancel_wizard_settings())
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
            context.chat_data[PROFILE_WIZ_DATA] = data
            context.chat_data[PROFILE_WIZ_STEP] = "photo"
            has_current_photo = bool(data.get("photo_file_id"))
            text_photo = (
                "Шаг 8/8: отправьте <b>фотографию сотрудника</b>.\n\n"
                "Фото будет показано в карточке команды. "
                "Можно отправить новое фото или воспользоваться кнопкой ниже."
            )
            await update.message.reply_text(
                text_photo,
                parse_mode=ParseMode.HTML,
                reply_markup=kb_profile_photo_step(has_current_photo=has_current_photo),
            )
            return

        if step == "photo":
            await update.message.reply_text(
                "📷 На этом шаге отправьте фотографию как фото либо нажмите кнопку под предыдущим сообщением.",
                reply_markup=kb_profile_photo_step(has_current_photo=bool(data.get("photo_file_id"))),
            )
            return



async def send_nomination_to_admins(
    nomination_id: int,
    context: ContextTypes.DEFAULT_TYPE,
) -> tuple[int, int]:
    """Отправляет новую номинацию администраторам исходного чата и в их центр уведомлений."""
    nomination = db_nomination_get(nomination_id)
    if not nomination:
        return (0, 0)

    scope_chat_id = int(nomination["scope_chat_id"])
    category = nomination_category(nomination.get("category_key"))
    try:
        chat = await context.bot.get_chat(scope_chat_id)
        chat_title = chat.title or str(scope_chat_id)
    except Exception:
        chat_title = str(scope_chat_id)

    text = (
        f"🙌 <b>Новая номинация №{nomination_id}</b>\n\n"
        f"Чат: <b>{escape(chat_title)}</b>\n"
        f"От: <b>{escape(nomination['nominator_name'])}</b>\n"
        f"Кого номинируют: <b>{escape(nomination['nominee_name'])}</b>\n"
        f"Категория: {escape(category['emoji'])} <b>{escape(category['title'])}</b>\n\n"
        f"Причина:\n{escape(nomination['reason'])}\n\n"
        "Уровень будет рассчитан автоматически по числу одобренных номинаций: 1 / 3 / 7."
    )

    try:
        admins = await context.bot.get_chat_administrators(scope_chat_id)
    except Exception as exc:
        logger.exception("Cannot get admins for nomination: %s", exc)
        return (0, 0)

    sent_ok = 0
    sent_fail = 0
    for member in admins:
        if getattr(member.user, "is_bot", False):
            continue
        db_notification_add(
            member.user.id,
            "nomination_pending",
            f"Новая номинация: {nomination['nominee_name']}",
            f"{category['emoji']} {category['title']}\nОт: {nomination['nominator_name']}\n{nomination['reason']}",
            callback_data=f"help:nomination:admin:open:{nomination_id}",
        )
        try:
            await context.bot.send_message(
                chat_id=member.user.id,
                text=text,
                parse_mode=ParseMode.HTML,
                reply_markup=kb_nomination_admin_actions(nomination_id),
                disable_web_page_preview=True,
            )
            sent_ok += 1
        except Exception:
            sent_fail += 1
    return sent_ok, sent_fail


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



# ---------------- COMMUNICATIONS / BROADCAST ----------------

def parse_moscow_send_time(value: str) -> tuple[str, str] | None:
    """Parses admin-entered Moscow time and returns (naive UTC ISO, display)."""
    raw = (value or "").strip()
    naive = None
    for fmt in ("%d.%m.%Y %H:%M", "%d.%m.%y %H:%M"):
        try:
            naive = datetime.strptime(raw, fmt)
            break
        except ValueError:
            continue
    if naive is None:
        return None
    try:
        aware_msk = MOSCOW_TZ.localize(naive)
    except Exception:
        return None
    if aware_msk <= datetime.now(MOSCOW_TZ) + timedelta(seconds=20):
        return None
    utc_naive = aware_msk.astimezone(pytz.utc).replace(tzinfo=None)
    return utc_naive.isoformat(), aware_msk.strftime("%d.%m.%Y %H:%M МСК")


def _html_plain_text(value: str | None) -> str:
    if not value:
        return ""
    return html_lib.unescape(re.sub(r"<[^>]+>", "", value))


def _communication_preview_html(message_html: str, max_visible: int = 800) -> str:
    plain = _html_plain_text(message_html).strip()
    if len(plain) > max_visible:
        plain = plain[:max_visible].rstrip() + "…"
    return escape(plain or "Только вложения")


def _meeting_get_data(context: ContextTypes.DEFAULT_TYPE) -> dict:
    data = context.user_data.get(COMM_MEETING_DATA)
    if not isinstance(data, dict):
        data = {
            "topic": None,
            "description_html": None,
            "link": None,
            "recipient_mode": None,
            "profile_ids": [],
        }
        context.user_data[COMM_MEETING_DATA] = data
    return data


def _meeting_compose_message(topic: str, description_html: str | None, link: str | None) -> str:
    parts = [f"<b>{escape((topic or '').strip())}</b>"]
    if description_html:
        parts.append(description_html.strip())
    if link:
        safe_link = html_lib.escape(link.strip(), quote=True)
        parts.append(f'<a href="{safe_link}">🔗 Ссылка на встречу</a>')
    return "\n\n".join(part for part in parts if part)


def _meeting_payload_from_data(data: dict) -> dict:
    return {
        "topic": data.get("topic"),
        "description_html": data.get("description_html"),
        "link": data.get("link"),
        "recipient_mode": data.get("recipient_mode"),
        "profile_ids": [int(x) for x in (data.get("profile_ids") or [])],
    }


def _meeting_recipient_summary(data: dict) -> str:
    if data.get("recipient_mode") == "chats":
        return f"Общий чат ({len(db_list_chats())})"
    selected = [int(x) for x in (data.get("profile_ids") or [])]
    names = []
    for pid in selected[:8]:
        profile = db_profiles_get(pid)
        names.append(profile["full_name"] if profile else f"id={pid}")
    text = ", ".join(names)
    if len(selected) > 8:
        text += f" и ещё {len(selected) - 8}"
    return text or "Сотрудники не выбраны"


def _meeting_confirmation_html(data: dict) -> str:
    message_html = _meeting_compose_message(
        data.get("topic") or "",
        data.get("description_html"),
        data.get("link"),
    )
    timing = "сразу" if data.get("send_mode") != "schedule" else data.get("send_at_display", "—")
    return (
        "📅 <b>Проверьте встречу</b>\n\n"
        f"Получатели: <b>{escape(_meeting_recipient_summary(data))}</b>\n"
        f"Отправка: <b>{escape(timing)}</b>\n\n"
        "Предпросмотр:\n\n"
        f"{message_html}"
    )


async def send_custom_meeting(context: ContextTypes.DEFAULT_TYPE, payload: dict) -> tuple[int, int]:
    message_html = _meeting_compose_message(
        payload.get("topic") or "",
        payload.get("description_html"),
        payload.get("link"),
    )
    ok = 0
    fail = 0
    if payload.get("recipient_mode") == "chats":
        for chat_id in db_list_chats():
            try:
                sent = await context.bot.send_message(
                    chat_id=int(chat_id),
                    text=message_html,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
                # Custom meeting notices follow the same clean-chat rule as regular meetings.
                schedule_message_delete(context, sent)
                ok += 1
            except Exception as exc:
                logger.exception("Custom meeting failed to chat %s: %s", chat_id, exc)
                fail += 1
        return ok, fail

    seen_user_ids = set()
    for pid in payload.get("profile_ids") or []:
        profile = db_profiles_get(int(pid))
        user_id = profile.get("tg_user_id") if profile else None
        if not user_id or int(user_id) in seen_user_ids:
            fail += 1
            continue
        seen_user_ids.add(int(user_id))
        try:
            await context.bot.send_message(
                chat_id=int(user_id),
                text=message_html,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            ok += 1
        except Exception as exc:
            logger.exception("Custom meeting failed to profile %s: %s", pid, exc)
            fail += 1
    return ok, fail


def _bcast_get_data(context: ContextTypes.DEFAULT_TYPE) -> dict:
    data = context.user_data.get(BCAST_DATA)
    if not isinstance(data, dict):
        data = {"topic": None, "tag": None, "text_html": None, "files": []}
        context.user_data[BCAST_DATA] = data
    if "files" not in data or not isinstance(data.get("files"), list):
        data["files"] = []
    # Migration of an unfinished wizard from the previous build.
    if data.get("text_html") is None and data.get("text"):
        data["text_html"] = escape(str(data.get("text")))
    return data


def _bcast_compose_message(
    topic: str | None,
    body_html: str | None,
    tag: str | None = None,
) -> str:
    heading = ""
    if topic and str(topic).strip():
        heading = f"<b>{escape(str(topic).strip())}</b>"
    elif tag and str(tag).strip():
        clean_tag = normalize_broadcast_tag_name(str(tag))
        if clean_tag:
            heading = f"<b>#{escape(clean_tag)}</b>"
    body = (body_html or "").strip()
    if heading and body:
        return f"{heading}\n\n{body}"
    return heading or body


def _bcast_confirmation_html(data: dict) -> str:
    message_html = _bcast_compose_message(
        data.get("topic"), data.get("text_html"), data.get("tag")
    )
    timing = "сразу" if data.get("send_mode") != "schedule" else data.get("send_at_display", "—")
    return (
        "⚠️ <b>Подтверждение рассылки</b>\n\n"
        f"Получателей-чатов: <b>{len(db_list_chats())}</b>\n"
        f"Вложений: <b>{len(data.get('files') or [])}</b>\n"
        f"Отправка: <b>{escape(timing)}</b>\n\n"
        "Предпросмотр:\n"
        f"{_communication_preview_html(message_html)}"
    )


async def broadcast_to_chats(
    context: ContextTypes.DEFAULT_TYPE,
    message_html: str,
    files: list[dict],
) -> tuple[int, int]:
    """Broadcasts to notify_chats while preserving complete HTML entities."""
    ok = 0
    fail = 0
    chat_ids = db_list_chats()
    files = files or []

    # Never cut HTML in the middle of an entity/tag. If the formatted text is too
    # long for a caption, send it as a separate text message and keep files clean.
    can_use_caption = len(_html_plain_text(message_html)) <= 900

    for cid in chat_ids:
        try:
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

            if len(files) == 1:
                f0 = files[0]
                kind = f0.get("kind")
                file_id = f0.get("file_id")
                caption = message_html if message_html and can_use_caption else None
                if message_html and not caption:
                    await context.bot.send_message(
                        chat_id=cid,
                        text=message_html,
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True,
                    )
                if kind == "document":
                    await context.bot.send_document(
                        chat_id=cid, document=file_id, caption=caption,
                        parse_mode=ParseMode.HTML if caption else None,
                    )
                elif kind == "photo":
                    await context.bot.send_photo(
                        chat_id=cid, photo=file_id, caption=caption,
                        parse_mode=ParseMode.HTML if caption else None,
                    )
                elif kind == "video":
                    await context.bot.send_video(
                        chat_id=cid, video=file_id, caption=caption,
                        parse_mode=ParseMode.HTML if caption else None,
                    )
                else:
                    if file_id:
                        await context.bot.send_document(chat_id=cid, document=file_id)
                ok += 1
                continue

            all_media = all((x.get("kind") in ("photo", "video")) for x in files)
            if all_media:
                caption = message_html if message_html and can_use_caption else None
                if message_html and not caption:
                    await context.bot.send_message(
                        chat_id=cid,
                        text=message_html,
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True,
                    )
                media = []
                for i, f0 in enumerate(files[:10]):
                    kind = f0.get("kind")
                    file_id = f0.get("file_id")
                    if not file_id:
                        continue
                    common = {
                        "media": file_id,
                        "caption": caption if i == 0 and caption else None,
                        "parse_mode": ParseMode.HTML if i == 0 and caption else None,
                    }
                    media.append(InputMediaPhoto(**common) if kind == "photo" else InputMediaVideo(**common))
                if media:
                    await context.bot.send_media_group(chat_id=cid, media=media)
                    ok += 1
                    continue

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
        except Exception as exc:
            logger.exception("Broadcast failed to %s: %s", cid, exc)
            fail += 1
    return ok, fail


async def process_due_communications(context: ContextTypes.DEFAULT_TYPE):
    """Sends durable scheduled meetings and broadcasts that have become due."""
    for item in db_scheduled_communications_due(limit=20):
        item_id = int(item["id"])
        if not db_scheduled_communication_reserve(item_id):
            continue
        try:
            payload = json.loads(item.get("payload_json") or "{}")
            if item.get("kind") == "meeting":
                ok, fail = await send_custom_meeting(context, payload)
            elif item.get("kind") == "broadcast":
                message_html = _bcast_compose_message(
                    payload.get("topic"), payload.get("text_html"), payload.get("tag")
                )
                ok, fail = await broadcast_to_chats(context, message_html, payload.get("files") or [])
            else:
                raise RuntimeError(f"Unknown scheduled communication kind: {item.get('kind')}")

            status = "sent" if ok > 0 or fail == 0 else "failed"
            db_scheduled_communication_finish(
                item_id,
                status,
                result={"ok": int(ok), "fail": int(fail)},
                error=("No deliveries succeeded" if status == "failed" else None),
            )
        except Exception as exc:
            logger.exception("Scheduled communication %s failed: %s", item_id, exc)
            db_scheduled_communication_finish(item_id, "failed", error=str(exc)[:1000])


# ---------------- ERROR HANDLER ----------------

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Логируем любые необработанные ошибки, чтобы бот не падал молча."""
    try:
        logger.exception("Unhandled exception while processing update: %s", context.error)
    except Exception:
        pass

# ===================== TESTING V2: COMPLETE SUBSYSTEM =====================
# This block intentionally overrides the legacy testing UI while preserving
# compatibility with already-created templates, assignments and answers.

TEST_V2_BUILD = "TESTING-V2-ALL-FEATURES-2026-07-20"
TV2_STATE = "tv2_state"
TV2_DATA = "tv2_data"
TV2_MULTI = "tv2_multi"
TV2_ACTIVE_ASSIGNMENT = "tv2_active_assignment"
TV2_ADMIN_PAGE_SIZE = 8
TV2_MY_PAGE_SIZE = 6

_tv2_legacy_db_init = db_init
_tv2_legacy_cb_help = cb_help
_tv2_legacy_cb_test = cb_test
_tv2_legacy_on_text = on_text
_tv2_legacy_check_and_send_jobs = check_and_send_jobs


def _tv2_add_column(cur, table: str, definition: str):
    try:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {definition}")
    except sqlite3.OperationalError:
        pass


def db_init():
    _tv2_legacy_db_init()
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    # Optional organizational field used for group test assignment.
    _tv2_add_column(cur, "profiles", "department TEXT")

    # Template settings, versioning and publication.
    for definition in (
        "passing_score INTEGER NOT NULL DEFAULT 70",
        "max_attempts INTEGER NOT NULL DEFAULT 1",
        "scoring_policy TEXT NOT NULL DEFAULT 'best'",
        "result_mode TEXT NOT NULL DEFAULT 'errors'",
        "test_mode TEXT NOT NULL DEFAULT 'exam'",
        "shuffle_questions INTEGER NOT NULL DEFAULT 0",
        "shuffle_options INTEGER NOT NULL DEFAULT 0",
        "allow_back INTEGER NOT NULL DEFAULT 0",
        "allow_skip INTEGER NOT NULL DEFAULT 1",
        "immediate_feedback INTEGER NOT NULL DEFAULT 0",
        "default_time_limit_sec INTEGER",
        "version INTEGER NOT NULL DEFAULT 1",
        "parent_template_id INTEGER",
        "is_published INTEGER NOT NULL DEFAULT 0",
        "published_at TEXT",
        "updated_at TEXT",
    ):
        _tv2_add_column(cur, "test_templates", definition)

    # Rich question metadata.
    for definition in (
        "points REAL NOT NULL DEFAULT 1",
        "explanation TEXT",
        "category TEXT",
        "difficulty INTEGER NOT NULL DEFAULT 1",
        "tags TEXT",
    ):
        _tv2_add_column(cur, "test_questions", definition)

    # Assignment lifecycle, attempts, scoring, navigation and reminders.
    for definition in (
        "due_at TEXT",
        "attempt_no INTEGER NOT NULL DEFAULT 1",
        "attempt_group TEXT",
        "parent_assignment_id INTEGER",
        "score_percent REAL",
        "points_earned REAL",
        "points_total REAL",
        "passed INTEGER",
        "review_status TEXT NOT NULL DEFAULT 'none'",
        "reviewer_comment TEXT",
        "question_order_json TEXT",
        "option_order_json TEXT",
        "flagged_json TEXT",
        "reminder_24_sent INTEGER NOT NULL DEFAULT 0",
        "reminder_2_sent INTEGER NOT NULL DEFAULT 0",
        "overdue_notice_sent INTEGER NOT NULL DEFAULT 0",
    ):
        _tv2_add_column(cur, "test_assignments", definition)

    # Per-answer manual review and partial scoring.
    for definition in (
        "awarded_points REAL",
        "review_status TEXT NOT NULL DEFAULT 'auto'",
        "reviewer_comment TEXT",
        "is_flagged INTEGER NOT NULL DEFAULT 0",
    ):
        _tv2_add_column(cur, "test_answers", definition)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS test_question_bank (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            q_type TEXT NOT NULL,
            question_text TEXT NOT NULL,
            options_json TEXT,
            correct_json TEXT,
            points REAL NOT NULL DEFAULT 1,
            explanation TEXT,
            category TEXT,
            difficulty INTEGER NOT NULL DEFAULT 1,
            tags TEXT,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_by INTEGER,
            created_at TEXT NOT NULL,
            updated_at TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS test_attempt_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            assignment_id INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            payload_json TEXT,
            created_at TEXT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS test_admin_comments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            assignment_id INTEGER NOT NULL,
            admin_user_id INTEGER,
            comment TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_test_assign_profile_status ON test_assignments(profile_id, status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_test_assign_due ON test_assignments(due_at, status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_test_answers_assignment ON test_answers(assignment_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_test_bank_category ON test_question_bank(category, is_active)")

    # Existing legacy templates are drafts; assignments keep working as-is.
    cur.execute("UPDATE test_templates SET updated_at=COALESCE(updated_at, created_at)")
    cur.execute("UPDATE test_questions SET points=1 WHERE points IS NULL OR points<=0")
    cur.execute("UPDATE test_assignments SET attempt_group=COALESCE(attempt_group, 'legacy-' || id)")
    con.commit()
    con.close()
    logger.warning("=== %s | FILE=%s | DB=%s ===", TEST_V2_BUILD, os.path.abspath(__file__), os.path.abspath(DB_PATH))


def tv2_connect():
    con = sqlite3.connect(DB_PATH, timeout=20)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON")
    con.execute("PRAGMA busy_timeout=20000")
    return con


def tv2_clear(context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop(TV2_STATE, None)
    context.user_data.pop(TV2_DATA, None)
    context.user_data.pop(TV2_MULTI, None)


def tv2_set_state(context: ContextTypes.DEFAULT_TYPE, state: str, **data):
    context.user_data[TV2_STATE] = state
    context.user_data[TV2_DATA] = dict(data)


def tv2_parse_dt(value: str) -> str | None:
    value = (value or "").strip().lower()
    if value in ("нет", "без срока", "-", "none"):
        return None
    for fmt in ("%d.%m.%Y %H:%M", "%d.%m.%Y", "%d.%m.%y %H:%M", "%d.%m.%y"):
        try:
            dt = datetime.strptime(value, fmt)
            if "%H" not in fmt:
                dt = dt.replace(hour=23, minute=59)
            localized = MOSCOW_TZ.localize(dt)
            return localized.astimezone(pytz.UTC).replace(tzinfo=None).isoformat()
        except Exception:
            continue
    return "INVALID"


def tv2_fmt_dt(value: str | None) -> str:
    if not value:
        return "без срока"
    try:
        dt = datetime.fromisoformat(value).replace(tzinfo=pytz.UTC).astimezone(MOSCOW_TZ)
        return dt.strftime("%d.%m.%Y %H:%M МСК")
    except Exception:
        return str(value)


def tv2_template_defaults(mode: str) -> dict:
    if mode == "learning":
        return {
            "passing_score": 70, "max_attempts": TEST_MAX_ATTEMPTS, "scoring_policy": "best",
            "result_mode": "all", "test_mode": "learning", "shuffle_questions": 0,
            "shuffle_options": 0, "allow_back": 1, "allow_skip": 1,
            "immediate_feedback": 1,
        }
    return {
        "passing_score": 80, "max_attempts": TEST_MAX_ATTEMPTS, "scoring_policy": "last",
        "result_mode": "score", "test_mode": "exam", "shuffle_questions": 1,
        "shuffle_options": 1, "allow_back": 0, "allow_skip": 0,
        "immediate_feedback": 0,
    }


def tv2_create_template(title: str, created_by: int | None, mode: str = "exam") -> int:
    cfg = tv2_template_defaults(mode)
    now = datetime.utcnow().isoformat()
    with tv2_connect() as con:
        cur = con.execute(
            """INSERT INTO test_templates(
                   title, created_by, created_at, is_draft_visible,
                   passing_score, max_attempts, scoring_policy, result_mode,
                   test_mode, shuffle_questions, shuffle_options, allow_back,
                   allow_skip, immediate_feedback, version, is_published, updated_at
               ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (title.strip(), created_by, now, 1, cfg["passing_score"], cfg["max_attempts"],
             cfg["scoring_policy"], cfg["result_mode"], cfg["test_mode"],
             cfg["shuffle_questions"], cfg["shuffle_options"], cfg["allow_back"],
             cfg["allow_skip"], cfg["immediate_feedback"], 1, 0, now),
        )
        return int(cur.lastrowid)


def tv2_get_template(tid: int) -> dict | None:
    with tv2_connect() as con:
        row = con.execute("SELECT * FROM test_templates WHERE id=?", (int(tid),)).fetchone()
    return dict(row) if row else None


def tv2_list_templates(published: int | None = None, limit: int = 100) -> list[dict]:
    sql = "SELECT * FROM test_templates WHERE is_draft_visible=1"
    args = []
    if published is not None:
        sql += " AND is_published=?"
        args.append(int(published))
    sql += " ORDER BY COALESCE(updated_at, created_at) DESC LIMIT ?"
    args.append(int(limit))
    with tv2_connect() as con:
        return [dict(r) for r in con.execute(sql, args).fetchall()]


def tv2_questions(tid: int) -> list[dict]:
    with tv2_connect() as con:
        rows = con.execute("SELECT * FROM test_questions WHERE template_id=? ORDER BY idx", (int(tid),)).fetchall()
    out = []
    for row in rows:
        d = dict(row)
        d["options"] = _safe_json_loads(d.get("options_json"), [])
        d["correct"] = _safe_json_loads(d.get("correct_json"), [])
        out.append(d)
    return out


def tv2_add_question(tid: int, q_type: str, text: str, options: list[str] | None,
                     correct: list[int] | None, points: float = 1, explanation: str = "",
                     category: str = "", difficulty: int = 1, tags: str = "") -> int:
    with tv2_connect() as con:
        idx = int(con.execute("SELECT COALESCE(MAX(idx),0)+1 FROM test_questions WHERE template_id=?", (int(tid),)).fetchone()[0])
        cur = con.execute(
            """INSERT INTO test_questions(
                   template_id, idx, q_type, question_text, options_json, correct_json,
                   created_at, points, explanation, category, difficulty, tags
               ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
            (int(tid), idx, q_type, text.strip(), _safe_json_dumps(options or []),
             _safe_json_dumps(correct or []), datetime.utcnow().isoformat(), float(points),
             (explanation or "").strip() or None, (category or "").strip() or None,
             max(1, min(int(difficulty or 1), 5)), (tags or "").strip() or None),
        )
        con.execute("UPDATE test_templates SET updated_at=? WHERE id=?", (datetime.utcnow().isoformat(), int(tid)))
        return int(cur.lastrowid)


def tv2_update_question(qid: int, field: str, value):
    allowed = {"question_text", "options_json", "correct_json", "points", "explanation", "category", "difficulty", "tags"}
    if field not in allowed:
        raise ValueError("unsupported field")
    with tv2_connect() as con:
        con.execute(f"UPDATE test_questions SET {field}=? WHERE id=?", (value, int(qid)))


def tv2_delete_question(qid: int):
    with tv2_connect() as con:
        row = con.execute("SELECT template_id, idx FROM test_questions WHERE id=?", (int(qid),)).fetchone()
        if not row:
            return False
        con.execute("DELETE FROM test_questions WHERE id=?", (int(qid),))
        con.execute("UPDATE test_questions SET idx=idx-1 WHERE template_id=? AND idx>?", (int(row[0]), int(row[1])))
        return True


def tv2_move_question(qid: int, delta: int):
    with tv2_connect() as con:
        row = con.execute("SELECT template_id, idx FROM test_questions WHERE id=?", (int(qid),)).fetchone()
        if not row:
            return False
        tid, idx = int(row[0]), int(row[1])
        other = con.execute("SELECT id, idx FROM test_questions WHERE template_id=? AND idx=?", (tid, idx + int(delta))).fetchone()
        if not other:
            return False
        con.execute("UPDATE test_questions SET idx=-1 WHERE id=?", (int(qid),))
        con.execute("UPDATE test_questions SET idx=? WHERE id=?", (idx, int(other[0])))
        con.execute("UPDATE test_questions SET idx=? WHERE id=?", (idx + int(delta), int(qid)))
        return True


def tv2_publish_template(tid: int, user_id: int | None = None) -> int:
    src = tv2_get_template(tid)
    if not src:
        raise ValueError("template not found")
    if int(src.get("is_published") or 0) == 1:
        return int(tid)
    root_id = int(src.get("parent_template_id") or src["id"])
    with tv2_connect() as con:
        max_ver = int(con.execute(
            "SELECT COALESCE(MAX(version),0) FROM test_templates WHERE id=? OR parent_template_id=?",
            (root_id, root_id),
        ).fetchone()[0] or 0)
        version = max(1, max_ver + 1)
        fields = [
            "title", "created_by", "created_at", "is_draft_visible", "passing_score",
            "max_attempts", "scoring_policy", "result_mode", "test_mode",
            "shuffle_questions", "shuffle_options", "allow_back", "allow_skip",
            "immediate_feedback", "default_time_limit_sec", "version",
            "parent_template_id", "is_published", "published_at", "updated_at",
        ]
        now = datetime.utcnow().isoformat()
        vals = [src.get("title"), user_id or src.get("created_by"), now, 1,
                src.get("passing_score", 70), src.get("max_attempts", 1),
                src.get("scoring_policy", "best"), src.get("result_mode", "errors"),
                src.get("test_mode", "exam"), src.get("shuffle_questions", 0),
                src.get("shuffle_options", 0), src.get("allow_back", 0),
                src.get("allow_skip", 1), src.get("immediate_feedback", 0),
                src.get("default_time_limit_sec"), version, root_id, 1, now, now]
        placeholders = ",".join("?" for _ in fields)
        cur = con.execute(f"INSERT INTO test_templates({','.join(fields)}) VALUES({placeholders})", vals)
        pub_id = int(cur.lastrowid)
        for q in tv2_questions(tid):
            con.execute(
                """INSERT INTO test_questions(
                       template_id, idx, q_type, question_text, options_json, correct_json,
                       created_at, points, explanation, category, difficulty, tags
                   ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                (pub_id, q["idx"], q["q_type"], q["question_text"], q.get("options_json"),
                 q.get("correct_json"), now, q.get("points", 1), q.get("explanation"),
                 q.get("category"), q.get("difficulty", 1), q.get("tags")),
            )
        return pub_id


def tv2_bank_add(q_type: str, text: str, options: list[str], correct: list[int],
                 points: float, explanation: str, category: str, difficulty: int,
                 tags: str, created_by: int | None) -> int:
    with tv2_connect() as con:
        cur = con.execute(
            """INSERT INTO test_question_bank(
                   q_type, question_text, options_json, correct_json, points,
                   explanation, category, difficulty, tags, created_by, created_at
               ) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
            (q_type, text.strip(), _safe_json_dumps(options), _safe_json_dumps(correct),
             float(points), explanation.strip() or None, category.strip() or "Без категории",
             max(1, min(int(difficulty), 5)), tags.strip() or None, created_by,
             datetime.utcnow().isoformat()),
        )
        return int(cur.lastrowid)


def tv2_bank_list(category: str | None = None, limit: int = 100) -> list[dict]:
    sql = "SELECT * FROM test_question_bank WHERE is_active=1"
    args = []
    if category:
        sql += " AND category=?"
        args.append(category)
    sql += " ORDER BY id DESC LIMIT ?"
    args.append(int(limit))
    with tv2_connect() as con:
        rows = con.execute(sql, args).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        d["options"] = _safe_json_loads(d.get("options_json"), [])
        d["correct"] = _safe_json_loads(d.get("correct_json"), [])
        out.append(d)
    return out


def tv2_bank_categories() -> list[str]:
    with tv2_connect() as con:
        return [str(r[0]) for r in con.execute(
            "SELECT DISTINCT COALESCE(category,'Без категории') FROM test_question_bank WHERE is_active=1 ORDER BY 1"
        ).fetchall()]


def tv2_copy_bank_question(bank_id: int, tid: int) -> bool:
    with tv2_connect() as con:
        row = con.execute("SELECT * FROM test_question_bank WHERE id=? AND is_active=1", (int(bank_id),)).fetchone()
    if not row:
        return False
    d = dict(row)
    tv2_add_question(tid, d["q_type"], d["question_text"],
                     _safe_json_loads(d.get("options_json"), []),
                     _safe_json_loads(d.get("correct_json"), []),
                     d.get("points") or 1, d.get("explanation") or "",
                     d.get("category") or "", d.get("difficulty") or 1,
                     d.get("tags") or "")
    return True


def tv2_profile_ids_for_rule(rule: str, value: str | None = None, template_id: int | None = None) -> list[int]:
    with tv2_connect() as con:
        if rule == "all":
            rows = con.execute(
                "SELECT id FROM profiles WHERE COALESCE(is_active, 1)=1 ORDER BY full_name"
            ).fetchall()
        elif rule == "city":
            rows = con.execute(
                "SELECT id FROM profiles WHERE city=? AND COALESCE(is_active, 1)=1 ORDER BY full_name",
                (value or "",),
            ).fetchall()
        elif rule == "department":
            rows = con.execute(
                "SELECT id FROM profiles WHERE COALESCE(department, '')=? "
                "AND COALESCE(is_active, 1)=1 ORDER BY full_name",
                (value or "",),
            ).fetchall()
        elif rule == "failed" and template_id:
            root = tv2_get_template(template_id)
            root_id = int((root or {}).get("parent_template_id") or template_id)
            rows = con.execute(
                """SELECT DISTINCT p.id FROM profiles p
                   JOIN test_assignments a ON a.profile_id=p.id
                   JOIN test_templates t ON t.id=a.template_id
                   WHERE (t.id=? OR t.parent_template_id=?)
                     AND COALESCE(a.passed,0)=0
                     AND COALESCE(p.is_active,1)=1""",
                (root_id, root_id),
            ).fetchall()
        else:
            return []
    return [int(r[0]) for r in rows]


def tv2_create_assignment(template_id: int, profile_id: int, assigned_by: int | None,
                          due_at: str | None, time_limit_sec: int | None,
                          attempt_no: int = 1, attempt_group: str | None = None,
                          parent_assignment_id: int | None = None) -> int:
    template = tv2_get_template(template_id) or {}
    questions = tv2_questions(template_id)
    order = [int(q["id"]) for q in questions]
    if int(template.get("shuffle_questions") or 0):
        random.shuffle(order)
    option_orders = {}
    if int(template.get("shuffle_options") or 0):
        for q in questions:
            indexes = list(range(len(q.get("options") or [])))
            random.shuffle(indexes)
            option_orders[str(q["id"])] = indexes
    # Ретрай не создаётся из пользовательского потока. Новый assignment
    # всегда является отдельным администраторским назначением с одной попыткой.
    attempt_no = TEST_MAX_ATTEMPTS
    attempt_group = None
    parent_assignment_id = None
    group = f"{profile_id}-{template_id}-{int(time.time())}-{random.randint(1000,9999)}"
    now = datetime.utcnow().isoformat()
    with tv2_connect() as con:
        cur = con.execute(
            """INSERT INTO test_assignments(
                   template_id, profile_id, assigned_by, assigned_at, time_limit_sec,
                   deadline_at, status, current_idx, due_at, attempt_no, attempt_group,
                   parent_assignment_id, review_status, question_order_json,
                   option_order_json, flagged_json
               ) VALUES(?,?,?,?,?,NULL,'assigned',0,?,?,?,?,?,?,?,?)""",
            (int(template_id), int(profile_id), assigned_by, now, time_limit_sec,
             due_at, TEST_MAX_ATTEMPTS, group, None, "none",
             _safe_json_dumps(order), _safe_json_dumps(option_orders), _safe_json_dumps([])),
        )
        return int(cur.lastrowid)


def tv2_get_assignment(aid: int) -> dict | None:
    with tv2_connect() as con:
        row = con.execute(
            """SELECT a.*, t.title, t.passing_score, t.max_attempts, t.scoring_policy,
                      t.result_mode, t.test_mode, t.shuffle_questions, t.shuffle_options,
                      t.allow_back, t.allow_skip, t.immediate_feedback, t.version,
                      p.full_name, p.tg_user_id, p.tg_link
               FROM test_assignments a
               JOIN test_templates t ON t.id=a.template_id
               JOIN profiles p ON p.id=a.profile_id
               WHERE a.id=?""",
            (int(aid),),
        ).fetchone()
    return dict(row) if row else None


def tv2_assignment_order(a: dict) -> list[int]:
    order = _safe_json_loads(a.get("question_order_json"), [])
    if order:
        return [int(x) for x in order]
    return [int(q["id"]) for q in tv2_questions(int(a["template_id"]))]


def tv2_question_by_id(qid: int) -> dict | None:
    with tv2_connect() as con:
        row = con.execute("SELECT * FROM test_questions WHERE id=?", (int(qid),)).fetchone()
    if not row:
        return None
    d = dict(row)
    d["options"] = _safe_json_loads(d.get("options_json"), [])
    d["correct"] = _safe_json_loads(d.get("correct_json"), [])
    return d


def tv2_answer(aid: int, qid: int) -> dict | None:
    with tv2_connect() as con:
        row = con.execute("SELECT * FROM test_answers WHERE assignment_id=? AND question_id=?", (int(aid), int(qid))).fetchone()
    if not row:
        return None
    d = dict(row)
    d["answer"] = _safe_json_loads(d.get("answer_json"), {})
    return d


def tv2_save_answer(aid: int, qid: int, answer: dict, is_correct: int | None,
                    awarded_points: float | None, review_status: str = "auto"):
    now = datetime.utcnow().isoformat()
    with tv2_connect() as con:
        status_row = con.execute(
            "SELECT status FROM test_assignments WHERE id=?",
            (int(aid),),
        ).fetchone()
        if not status_row or status_row[0] not in ("assigned", "in_progress", "saved"):
            return False
        con.execute(
            """INSERT INTO test_answers(
                   assignment_id, question_id, answer_json, is_correct, answered_at,
                   awarded_points, review_status
               ) VALUES(?,?,?,?,?,?,?)
               ON CONFLICT(assignment_id, question_id) DO UPDATE SET
                   answer_json=excluded.answer_json,
                   is_correct=excluded.is_correct,
                   answered_at=excluded.answered_at,
                   awarded_points=excluded.awarded_points,
                   review_status=excluded.review_status""",
            (int(aid), int(qid), _safe_json_dumps(answer), is_correct, now,
             awarded_points, review_status),
        )
        con.execute("INSERT INTO test_attempt_events(assignment_id,event_type,payload_json,created_at) VALUES(?,?,?,?)",
                    (int(aid), "answer_saved", _safe_json_dumps({"question_id": qid}), now))
    return True


def tv2_set_current(aid: int, idx: int):
    with tv2_connect() as con:
        con.execute(
            "UPDATE test_assignments SET current_idx=? "
            "WHERE id=? AND status IN ('assigned','in_progress','saved')",
            (max(0, int(idx)), int(aid)),
        )


def tv2_toggle_flag(aid: int, qid: int) -> bool:
    a = tv2_get_assignment(aid)
    if not a or a.get("status") not in ("assigned", "in_progress", "saved"):
        return False
    flags = set(int(x) for x in _safe_json_loads((a or {}).get("flagged_json"), []))
    if int(qid) in flags:
        flags.remove(int(qid)); active = False
    else:
        flags.add(int(qid)); active = True
    with tv2_connect() as con:
        con.execute(
            "UPDATE test_assignments SET flagged_json=? "
            "WHERE id=? AND status IN ('assigned','in_progress','saved')",
            (_safe_json_dumps(sorted(flags)), int(aid)),
        )
    return active


def tv2_start_assignment(aid: int):
    a = tv2_get_assignment(aid)
    if not a:
        return False
    if a.get("status") not in ("assigned", "in_progress", "saved"):
        return False
    deadline = None
    if a.get("time_limit_sec"):
        deadline = (datetime.utcnow() + timedelta(seconds=int(a["time_limit_sec"]))).isoformat()
    with tv2_connect() as con:
        cur = con.execute(
            """UPDATE test_assignments SET status='in_progress',
                   started_at=COALESCE(started_at,?), deadline_at=COALESCE(deadline_at,?)
               WHERE id=? AND status IN ('assigned','saved','in_progress')""",
            (datetime.utcnow().isoformat(), deadline, int(aid)),
        )
        return cur.rowcount > 0


def tv2_is_expired(a: dict) -> bool:
    now = datetime.utcnow()
    for key in ("due_at", "deadline_at"):
        value = a.get(key)
        if value:
            try:
                if now >= datetime.fromisoformat(value):
                    return True
            except Exception:
                pass
    return False


def tv2_mark_expired(aid: int):
    with tv2_connect() as con:
        con.execute("UPDATE test_assignments SET status='expired', finished_at=? WHERE id=? AND status NOT IN ('finished','reviewed','expired','canceled')",
                    (datetime.utcnow().isoformat(), int(aid)))


def tv2_calculate(aid: int, finalize: bool = True) -> dict:
    a = tv2_get_assignment(aid)
    if not a:
        return {"percent": 0, "earned": 0, "total": 0, "pending": 0, "passed": False}
    qs = tv2_questions(int(a["template_id"]))
    total = float(sum(float(q.get("points") or 1) for q in qs))
    with tv2_connect() as con:
        rows = con.execute("SELECT question_id, awarded_points, review_status FROM test_answers WHERE assignment_id=?", (int(aid),)).fetchall()
    earned = 0.0
    pending = 0
    amap = {int(r[0]): r for r in rows}
    for q in qs:
        r = amap.get(int(q["id"]))
        if q["q_type"] == "open":
            if r and str(r[2]) in ("pending", ""):
                pending += 1
            elif r:
                earned += float(r[1] or 0)
        elif r:
            earned += float(r[1] or 0)
    percent = round((earned / total * 100) if total > 0 else 0, 2)
    passed = percent >= float(a.get("passing_score") or 70)
    if finalize:
        status = "needs_review" if pending else "finished"
        review_status = "pending" if pending else "reviewed"
        with tv2_connect() as con:
            con.execute(
                """UPDATE test_assignments SET status=?, review_status=?, finished_at=?,
                       score_percent=?, points_earned=?, points_total=?, passed=?
                   WHERE id=?""",
                (status, review_status, datetime.utcnow().isoformat(), percent, earned, total,
                 1 if passed and not pending else 0, int(aid)),
            )
        if not pending:
            tv2_update_profile_average(int(a["profile_id"]))
            tv2_award_test_achievements(int(a["profile_id"]), aid, percent, passed)
    return {"percent": percent, "earned": earned, "total": total, "pending": pending, "passed": passed and not pending}


def tv2_update_profile_average(profile_id: int):
    with tv2_connect() as con:
        row = con.execute(
            """SELECT AVG(score_percent) FROM test_assignments
               WHERE profile_id=? AND status='finished' AND score_percent IS NOT NULL""",
            (int(profile_id),),
        ).fetchone()
        avg = int(round(float(row[0]))) if row and row[0] is not None else None
        con.execute("UPDATE profiles SET avg_test_score=? WHERE id=?", (avg, int(profile_id)))


def tv2_has_achievement(profile_id: int, key: str, level: int | None = None) -> bool:
    with tv2_connect() as con:
        if level is None:
            row = con.execute("SELECT 1 FROM achievement_awards WHERE profile_id=? AND achievement_key=? LIMIT 1", (int(profile_id), key)).fetchone()
        else:
            row = con.execute("SELECT 1 FROM achievement_awards WHERE profile_id=? AND achievement_key=? AND level=? LIMIT 1", (int(profile_id), key, int(level))).fetchone()
    return bool(row)


def tv2_award_test_achievements(profile_id: int, aid: int, percent: float, passed: bool):
    if percent >= 100 and not tv2_has_achievement(profile_id, "test_perfect"):
        db_achievement_award_add(profile_id, "💯", "Без единой ошибки",
                                 "Первый тест, завершённый с результатом 100%.",
                                 awarded_by=None, level=1, achievement_key="test_perfect")
    if passed:
        with tv2_connect() as con:
            count = int(con.execute("SELECT COUNT(*) FROM test_assignments WHERE profile_id=? AND status='finished' AND passed=1", (int(profile_id),)).fetchone()[0])
        for threshold, level in ((3, 1), (7, 2), (15, 3)):
            if count >= threshold and not tv2_has_achievement(profile_id, "test_growth", level):
                db_achievement_award_add(profile_id, "📚", "Стабильное развитие",
                                         f"Успешно пройдено не менее {threshold} тестов.",
                                         awarded_by=None, level=level, achievement_key="test_growth")


def tv2_attempts_summary(a: dict) -> dict:
    with tv2_connect() as con:
        rows = con.execute(
            "SELECT attempt_no, score_percent, status, passed FROM test_assignments WHERE attempt_group=? ORDER BY attempt_no",
            (a.get("attempt_group"),),
        ).fetchall()
    scores = [float(r[1]) for r in rows if r[1] is not None and r[2] == "finished"]
    policy = a.get("scoring_policy") or "best"
    final = None
    if scores:
        final = max(scores) if policy == "best" else (scores[-1] if policy == "last" else sum(scores) / len(scores))
    return {"count": len(rows), "scores": scores, "final": final}


def tv2_can_retry(a: dict) -> bool:
    # Повторное прохождение возможно только через новое назначение администратором.
    return False


def tv2_create_retry(aid: int, user_id: int | None) -> int | None:
    # Оставляем функцию для совместимости со старыми callback_data, но никогда
    # не создаём пользовательский retry.
    return None


def tv2_my_tests(profile_id: int, status_filter: str = "all", page: int = 0):
    where = "a.profile_id=?"
    args = [int(profile_id)]
    if status_filter == "new":
        where += " AND a.status='assigned'"
    elif status_filter == "progress":
        where += " AND a.status IN ('in_progress','saved')"
    elif status_filter == "done":
        where += " AND a.status IN ('finished','needs_review')"
    elif status_filter == "expired":
        where += " AND a.status='expired'"
    with tv2_connect() as con:
        total = int(con.execute(f"SELECT COUNT(*) FROM test_assignments a WHERE {where}", args).fetchone()[0])
        rows = con.execute(
            f"""SELECT a.*, t.title, t.passing_score, t.max_attempts, t.test_mode
                FROM test_assignments a JOIN test_templates t ON t.id=a.template_id
                WHERE {where} ORDER BY a.assigned_at DESC LIMIT ? OFFSET ?""",
            args + [TV2_MY_PAGE_SIZE, max(0, int(page)) * TV2_MY_PAGE_SIZE],
        ).fetchall()
    return [dict(r) for r in rows], total


def tv2_result_text(aid: int) -> str:
    a = tv2_get_assignment(aid)
    if not a:
        return "Тест не найден."
    calc = tv2_calculate(aid, finalize=False)
    lines = [f"📝 <b>{escape(a['title'])}</b>", ""]
    if a.get("status") == "needs_review":
        lines.append("⏳ <b>Ожидает проверки открытых ответов</b>")
    else:
        lines.append(f"Результат: <b>{calc['percent']:.0f}%</b>")
        lines.append(f"Баллы: <b>{calc['earned']:.1f} из {calc['total']:.1f}</b>")
        lines.append(f"Проходной балл: <b>{int(a.get('passing_score') or 70)}%</b>")
        lines.append("Статус: " + ("✅ успешно пройден" if calc["passed"] else "❌ не пройден"))
    summary = tv2_attempts_summary(a)
    lines.append(f"Попытка: <b>{TEST_MAX_ATTEMPTS} из {TEST_MAX_ATTEMPTS}</b>")
    if summary.get("final") is not None:
        policy_names = {"best": "лучший", "last": "последний", "average": "средний"}
        lines.append(f"Зачётный результат ({policy_names.get(a.get('scoring_policy'),'лучший')}): <b>{summary['final']:.0f}%</b>")
    if a.get("reviewer_comment"):
        lines.extend(["", "💬 <b>Комментарий руководителя</b>", escape(a["reviewer_comment"])])
    return "\n".join(lines)


def tv2_render_result_details(aid: int) -> str:
    a = tv2_get_assignment(aid)
    if not a:
        return "Тест не найден."
    mode = a.get("result_mode") or "errors"
    base = tv2_result_text(aid)
    if mode in ("score", "hidden") or a.get("status") == "needs_review":
        return base
    lines = [base, "", "<b>Разбор ответов</b>"]
    for q in tv2_questions(int(a["template_id"])):
        ans = tv2_answer(aid, int(q["id"]))
        is_corr = ans.get("is_correct") if ans else None
        if mode == "errors" and is_corr == 1:
            continue
        marker = "✅" if is_corr == 1 else ("❌" if is_corr == 0 else "⏳")
        lines.append(f"\n{marker} <b>{int(q['idx'])}. {escape(q['question_text'])}</b>")
        if ans:
            payload = ans.get("answer") or {}
            if q["q_type"] == "open":
                lines.append(escape(str(payload.get("text") or "—")))
            else:
                selected = payload.get("selected") or []
                names = [q.get("options", [])[i] for i in selected if 0 <= int(i) < len(q.get("options") or [])]
                lines.append("Ответ: " + escape(", ".join(names) or "—"))
        if q.get("explanation"):
            lines.append("💡 " + escape(q["explanation"]))
    return "\n".join(lines)[:4000]


def tv2_kb_admin_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Создать тест", callback_data="help:testv2:create")],
        [InlineKeyboardButton("🗂 Черновики и версии", callback_data="help:testv2:drafts:0")],
        [InlineKeyboardButton("📚 Банк вопросов", callback_data="help:testv2:bank")],
        [InlineKeyboardButton("👥 Назначить тест", callback_data="help:testv2:assign")],
        [InlineKeyboardButton("🧑‍🏫 Проверить открытые ответы", callback_data="help:testv2:review")],
        [InlineKeyboardButton("📊 Результаты и аналитика", callback_data="help:testv2:analytics")],
        [InlineKeyboardButton("⌛ Просроченные", callback_data="help:testv2:overdue")],
        [InlineKeyboardButton("🏢 Отделы сотрудников", callback_data="help:testv2:departments:0")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="help:settings")],
    ])


def tv2_kb_cancel(back: str = "help:testv2:admin"):
    return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data=back)]])


def tv2_kb_drafts(page: int = 0):
    items = tv2_list_templates(published=None, limit=200)
    total_pages = max(1, (len(items) + TV2_ADMIN_PAGE_SIZE - 1) // TV2_ADMIN_PAGE_SIZE)
    page = max(0, min(int(page), total_pages - 1))
    chunk = items[page * TV2_ADMIN_PAGE_SIZE:(page + 1) * TV2_ADMIN_PAGE_SIZE]
    rows = []
    for t in chunk:
        icon = "🔒" if int(t.get("is_published") or 0) else "✏️"
        label = f"{icon} {t['title']} · v{int(t.get('version') or 1)}"
        rows.append([InlineKeyboardButton(label[:60], callback_data=f"help:testv2:template:{int(t['id'])}")])
    nav = []
    if page > 0: nav.append(InlineKeyboardButton("◀️", callback_data=f"help:testv2:drafts:{page-1}"))
    nav.append(InlineKeyboardButton(f"{page+1}/{total_pages}", callback_data="noop"))
    if page + 1 < total_pages: nav.append(InlineKeyboardButton("▶️", callback_data=f"help:testv2:drafts:{page+1}"))
    rows.append(nav)
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="help:testv2:admin")])
    return InlineKeyboardMarkup(rows)


def tv2_template_text(tid: int) -> str:
    t = tv2_get_template(tid)
    if not t: return "Шаблон не найден."
    qs = tv2_questions(tid)
    mode = {"learning": "📚 Обучение", "exam": "🎓 Аттестация", "custom": "⚙️ Свой"}.get(t.get("test_mode"), t.get("test_mode"))
    result_mode = {"score": "только балл", "errors": "ошибки", "all": "все ответы", "hidden": "скрыто"}.get(t.get("result_mode"), t.get("result_mode"))
    return (
        f"📝 <b>{escape(t['title'])}</b>\n"
        f"Версия: <b>{int(t.get('version') or 1)}</b> · {'опубликована' if int(t.get('is_published') or 0) else 'черновик'}\n\n"
        f"Режим: <b>{escape(str(mode))}</b>\n"
        f"Вопросов: <b>{len(qs)}</b>\n"
        f"Проходной балл: <b>{int(t.get('passing_score') or 70)}%</b>\n"
        f"Попыток: <b>{TEST_MAX_ATTEMPTS}</b>\n"
        f"Зачёт: <b>{escape(str(t.get('scoring_policy') or 'best'))}</b>\n"
        f"Показывать результат: <b>{escape(str(result_mode))}</b>\n"
        f"Перемешивать вопросы: <b>{'да' if int(t.get('shuffle_questions') or 0) else 'нет'}</b>\n"
        f"Перемешивать варианты: <b>{'да' if int(t.get('shuffle_options') or 0) else 'нет'}</b>\n"
        f"Назад/пропуск: <b>{'да' if int(t.get('allow_back') or 0) else 'нет'} / {'да' if int(t.get('allow_skip') or 0) else 'нет'}</b>"
    )


def tv2_kb_template(tid: int):
    t = tv2_get_template(tid) or {}
    rows = [[InlineKeyboardButton("👁 Предпросмотр", callback_data=f"help:testv2:preview:{tid}")]]
    if not int(t.get("is_published") or 0):
        rows += [
            [InlineKeyboardButton("➕ Добавить вопрос", callback_data=f"help:testv2:qadd:{tid}")],
            [InlineKeyboardButton("📚 Добавить из банка", callback_data=f"help:testv2:bankpick:{tid}:0")],
            [InlineKeyboardButton("🎲 Добавить 10 случайных", callback_data=f"help:testv2:bankrandom:{tid}")],
            [InlineKeyboardButton("✏️ Редактор вопросов", callback_data=f"help:testv2:qeditlist:{tid}:0")],
            [InlineKeyboardButton("⚙️ Настройки теста", callback_data=f"help:testv2:settings:{tid}")],
            [InlineKeyboardButton("🔒 Опубликовать версию", callback_data=f"help:testv2:publishconfirm:{tid}")],
            [InlineKeyboardButton("🗑 Удалить черновик", callback_data=f"help:testv2:templatedeleteconfirm:{tid}")],
        ]
    rows += [
        [InlineKeyboardButton("👥 Назначить", callback_data=f"help:testv2:assign_template:{tid}")],
        [InlineKeyboardButton("📊 Аналитика", callback_data=f"help:testv2:analytic:{tid}")],
        [InlineKeyboardButton("⬅️ К шаблонам", callback_data="help:testv2:drafts:0")],
    ]
    return InlineKeyboardMarkup(rows)


def tv2_kb_settings(tid: int):
    t = tv2_get_template(tid) or {}
    def yn(v): return "✅" if int(v or 0) else "❌"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"Режим: {t.get('test_mode','exam')}", callback_data=f"help:testv2:set:mode:{tid}")],
        [InlineKeyboardButton(f"Проходной: {int(t.get('passing_score') or 70)}%", callback_data=f"help:testv2:set:passing:{tid}")],
        [InlineKeyboardButton(f"Зачёт: {t.get('scoring_policy','best')}", callback_data=f"help:testv2:set:policy:{tid}")],
        [InlineKeyboardButton(f"Результаты: {t.get('result_mode','errors')}", callback_data=f"help:testv2:set:result:{tid}")],
        [InlineKeyboardButton(f"{yn(t.get('shuffle_questions'))} Перемешивать вопросы", callback_data=f"help:testv2:toggle:shuffleq:{tid}")],
        [InlineKeyboardButton(f"{yn(t.get('shuffle_options'))} Перемешивать варианты", callback_data=f"help:testv2:toggle:shuffleo:{tid}")],
        [InlineKeyboardButton(f"{yn(t.get('allow_back'))} Разрешить назад", callback_data=f"help:testv2:toggle:back:{tid}")],
        [InlineKeyboardButton(f"{yn(t.get('allow_skip'))} Разрешить пропуск", callback_data=f"help:testv2:toggle:skip:{tid}")],
        [InlineKeyboardButton(f"{yn(t.get('immediate_feedback'))} Мгновенная обратная связь", callback_data=f"help:testv2:toggle:feedback:{tid}")],
        [InlineKeyboardButton("⏱ Время на сам тест", callback_data=f"help:testv2:set:time:{tid}")],
        [InlineKeyboardButton("⬅️ Назад", callback_data=f"help:testv2:template:{tid}")],
    ])


def tv2_kb_question_list(tid: int, page: int = 0):
    qs = tv2_questions(tid)
    total_pages = max(1, (len(qs) + TV2_ADMIN_PAGE_SIZE - 1)//TV2_ADMIN_PAGE_SIZE)
    page = max(0, min(page, total_pages-1))
    rows=[]
    for q in qs[page*TV2_ADMIN_PAGE_SIZE:(page+1)*TV2_ADMIN_PAGE_SIZE]:
        rows.append([InlineKeyboardButton(f"{q['idx']}. {q['question_text'][:45]}", callback_data=f"help:testv2:qedit:{int(q['id'])}")])
    nav=[]
    if page>0: nav.append(InlineKeyboardButton("◀️", callback_data=f"help:testv2:qeditlist:{tid}:{page-1}"))
    nav.append(InlineKeyboardButton(f"{page+1}/{total_pages}", callback_data="noop"))
    if page+1<total_pages: nav.append(InlineKeyboardButton("▶️", callback_data=f"help:testv2:qeditlist:{tid}:{page+1}"))
    rows.append(nav)
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"help:testv2:template:{tid}")])
    return InlineKeyboardMarkup(rows)


def tv2_question_text(q: dict) -> str:
    opts = q.get("options") or []
    correct = set(int(x) for x in (q.get("correct") or []))
    lines=[f"❓ <b>{int(q['idx'])}. {escape(q['question_text'])}</b>",
           f"Тип: <b>{escape(q['q_type'])}</b> · Баллы: <b>{float(q.get('points') or 1):g}</b>"]
    for i,opt in enumerate(opts):
        lines.append(f"{'✅' if i in correct else '▫️'} {i+1}. {escape(opt)}")
    if q.get("explanation"): lines.extend(["", "💡 "+escape(q["explanation"])])
    return "\n".join(lines)


def tv2_kb_question_edit(q: dict):
    qid=int(q["id"]); tid=int(q["template_id"])
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✏️ Текст", callback_data=f"help:testv2:qfield:text:{qid}"), InlineKeyboardButton("⭐ Баллы", callback_data=f"help:testv2:qfield:points:{qid}")],
        [InlineKeyboardButton("📋 Варианты", callback_data=f"help:testv2:qfield:options:{qid}"), InlineKeyboardButton("✅ Правильный", callback_data=f"help:testv2:qfield:correct:{qid}")],
        [InlineKeyboardButton("💡 Пояснение", callback_data=f"help:testv2:qfield:explanation:{qid}")],
        [InlineKeyboardButton("⬆️", callback_data=f"help:testv2:qmove:{qid}:-1"), InlineKeyboardButton("⬇️", callback_data=f"help:testv2:qmove:{qid}:1")],
        [InlineKeyboardButton("🗑 Удалить", callback_data=f"help:testv2:qdeleteconfirm:{qid}")],
        [InlineKeyboardButton("⬅️ К вопросам", callback_data=f"help:testv2:qeditlist:{tid}:0")],
    ])


def tv2_kb_my(profile_id: int, status_filter: str = "all", page: int = 0):
    items,total=tv2_my_tests(profile_id,status_filter,page)
    rows=[
        [InlineKeyboardButton("🔴 Новые", callback_data="help:testv2:my:new:0"), InlineKeyboardButton("🟡 В процессе", callback_data="help:testv2:my:progress:0")],
        [InlineKeyboardButton("🟢 История", callback_data="help:testv2:my:done:0"), InlineKeyboardButton("⌛ Истекли", callback_data="help:testv2:my:expired:0")],
    ]
    labels={"assigned":"▶️","in_progress":"⏳","saved":"💾","finished":"✅","needs_review":"🧑‍🏫","expired":"⌛","canceled":"❌"}
    for a in items:
        score = f" · {float(a['score_percent']):.0f}%" if a.get("score_percent") is not None else ""
        rows.append([InlineKeyboardButton(f"{labels.get(a['status'],'📝')} {a['title']}{score}"[:60], callback_data=f"help:testv2:myopen:{int(a['id'])}")])
    pages=max(1,(total+TV2_MY_PAGE_SIZE-1)//TV2_MY_PAGE_SIZE)
    nav=[]
    if page>0: nav.append(InlineKeyboardButton("◀️",callback_data=f"help:testv2:my:{status_filter}:{page-1}"))
    nav.append(InlineKeyboardButton(f"{page+1}/{pages}",callback_data="noop"))
    if page+1<pages: nav.append(InlineKeyboardButton("▶️",callback_data=f"help:testv2:my:{status_filter}:{page+1}"))
    rows.append(nav)
    rows.append([InlineKeyboardButton("⬅️ В мой кабинет",callback_data="help:me")])
    return InlineKeyboardMarkup(rows)


def tv2_my_open_text(a: dict) -> str:
    status_names={"assigned":"не начат","in_progress":"в процессе","saved":"сохранён","finished":"завершён","needs_review":"ожидает проверки","expired":"просрочен","canceled":"отменён"}
    qcount=len(tv2_questions(int(a["template_id"])))
    duration=f"{int(a['time_limit_sec'])//60} мин." if a.get("time_limit_sec") else "без ограничения"
    text=(f"📝 <b>{escape(a['title'])}</b>\n\n"
          f"Статус: <b>{status_names.get(a['status'],a['status'])}</b>\n"
          f"Вопросов: <b>{qcount}</b>\n"
          f"Время на сам тест: <b>{duration}</b>\n"
          f"Пройти до: <b>{escape(tv2_fmt_dt(a.get('due_at')))}</b>\n"
          f"Проходной балл: <b>{int(a.get('passing_score') or 70)}%</b>\n"
          f"Попытка: <b>{TEST_MAX_ATTEMPTS} из {TEST_MAX_ATTEMPTS}</b>\n"
          f"ℹ️ {escape(TEST_RETAKE_POLICY_TEXT)}")
    if a.get("status") in ("finished","needs_review"):
        text += "\n\n" + tv2_result_text(int(a["id"]))
    return text


def tv2_kb_my_open(a: dict):
    aid=int(a["id"]); rows=[]
    if a["status"]=="assigned": rows.append([InlineKeyboardButton("▶️ Начать",callback_data=f"test:v2:start:{aid}")])
    elif a["status"] in ("in_progress","saved"): rows.append([InlineKeyboardButton("⏳ Продолжить",callback_data=f"test:v2:continue:{aid}")])
    if a["status"] in ("finished","needs_review"):
        rows.append([InlineKeyboardButton("📊 Результат",callback_data=f"help:testv2:result:{aid}")])
    rows.append([InlineKeyboardButton("⬅️ К моим тестам",callback_data="help:testv2:my:all:0")])
    return InlineKeyboardMarkup(rows)


def tv2_question_display(a: dict, q: dict, position: int) -> tuple[str, InlineKeyboardMarkup | None]:
    order=tv2_assignment_order(a); aid=int(a["id"]); qid=int(q["id"])
    remaining=""
    if a.get("deadline_at"):
        try:
            sec=max(0,int((datetime.fromisoformat(a["deadline_at"])-datetime.utcnow()).total_seconds()))
            remaining=f" · ⏱ {sec//60:02d}:{sec%60:02d}"
        except Exception: pass
    flags=set(int(x) for x in _safe_json_loads(a.get("flagged_json"),[]))
    text=f"📝 <b>{escape(a['title'])}</b>\nВопрос <b>{position+1} из {len(order)}</b>{remaining}\n\n<b>{escape(q['question_text'])}</b>"
    if q["q_type"]=="open":
        text += "\n\nОтправьте ответ следующим сообщением."
        rows=[]
    else:
        rows=[]
        option_order=_safe_json_loads(a.get("option_order_json"),{}).get(str(qid),list(range(len(q.get("options") or []))))
        selected=set((context_selected := []))
        # Current multi selection is read in callback renderer; default empty here.
        for original_idx in option_order:
            if 0 <= int(original_idx) < len(q.get("options") or []):
                label=q["options"][int(original_idx)]
                if q["q_type"]=="single":
                    rows.append([InlineKeyboardButton(label[:60],callback_data=f"test:v2:single:{aid}:{qid}:{int(original_idx)}")])
                else:
                    rows.append([InlineKeyboardButton("▫️ "+label[:55],callback_data=f"test:v2:toggle:{aid}:{qid}:{int(original_idx)}")])
        if q["q_type"]=="multi": rows.append([InlineKeyboardButton("✅ Сохранить ответ",callback_data=f"test:v2:multisubmit:{aid}:{qid}")])
    nav=[]
    if int(a.get("allow_back") or 0) and position>0: nav.append(InlineKeyboardButton("◀️ Назад",callback_data=f"test:v2:goto:{aid}:{position-1}"))
    nav.append(InlineKeyboardButton("🚩" if qid in flags else "🏳️",callback_data=f"test:v2:flag:{aid}:{qid}"))
    if int(a.get("allow_skip") or 0) and position+1<len(order): nav.append(InlineKeyboardButton("Пропустить ▶️",callback_data=f"test:v2:goto:{aid}:{position+1}"))
    if nav: rows.append(nav)
    rows.append([InlineKeyboardButton("📋 Проверить ответы",callback_data=f"test:v2:reviewpage:{aid}")])
    return text, InlineKeyboardMarkup(rows)


async def tv2_send_question(update: Update, context: ContextTypes.DEFAULT_TYPE, aid: int, position: int | None = None):
    a=tv2_get_assignment(aid)
    if not a: return
    if a.get("status") not in ("assigned", "in_progress", "saved"):
        await context.bot.send_message(
            update.effective_user.id,
            "Этот тест уже завершён или недоступен. Новая попытка возможна "
            "только после повторного назначения администратором.",
        )
        return
    if tv2_is_expired(a):
        tv2_mark_expired(aid)
        await context.bot.send_message(update.effective_user.id,"⌛ Время или срок прохождения теста истёк.")
        return
    order=tv2_assignment_order(a)
    if position is None: position=int(a.get("current_idx") or 0)
    position=max(0,min(int(position),max(0,len(order)-1)))
    tv2_set_current(aid,position)
    q=tv2_question_by_id(order[position]) if order else None
    if not q:
        await context.bot.send_message(update.effective_user.id,"В тесте нет вопросов.")
        return
    context.user_data[TV2_ACTIVE_ASSIGNMENT]=aid
    if q["q_type"]=="open":
        tv2_set_state(context,"open_answer",assignment_id=aid,question_id=int(q["id"]),position=position)
    else:
        context.user_data.pop(TV2_STATE, None)
        context.user_data.pop(TV2_DATA, None)
    text,kb=tv2_question_display(tv2_get_assignment(aid),q,position)
    cq=update.callback_query
    if cq:
        try:
            await cq.edit_message_text(text,parse_mode=ParseMode.HTML,reply_markup=kb)
            return
        except Exception:
            pass
    await context.bot.send_message(update.effective_user.id,text,parse_mode=ParseMode.HTML,reply_markup=kb)


def tv2_review_page_text(aid: int) -> tuple[str, InlineKeyboardMarkup]:
    a=tv2_get_assignment(aid); order=tv2_assignment_order(a or {})
    flags=set(int(x) for x in _safe_json_loads((a or {}).get("flagged_json"),[]))
    answered=[]; missing=[]
    rows=[]
    for pos,qid in enumerate(order):
        ans=tv2_answer(aid,qid)
        if ans: answered.append(qid)
        else: missing.append(qid)
        marker="🚩" if qid in flags else ("✅" if ans else "⚪")
        rows.append([InlineKeyboardButton(f"{marker} Вопрос {pos+1}",callback_data=f"test:v2:goto:{aid}:{pos}")])
    text=(f"📋 <b>Проверка ответов</b>\n\n"
          f"✅ Отвечено: <b>{len(answered)}</b>\n"
          f"⚪ Без ответа: <b>{len(missing)}</b>\n"
          f"🚩 Отмечено: <b>{len(flags)}</b>")
    rows.append([InlineKeyboardButton("✅ Завершить тест",callback_data=f"test:v2:finishconfirm:{aid}")])
    return text,InlineKeyboardMarkup(rows)


def tv2_admin_review_list():
    with tv2_connect() as con:
        rows=con.execute("""SELECT a.id,t.title,p.full_name,a.finished_at
                            FROM test_assignments a JOIN test_templates t ON t.id=a.template_id
                            JOIN profiles p ON p.id=a.profile_id
                            WHERE a.status='needs_review' ORDER BY a.finished_at""").fetchall()
    return [dict(r) for r in rows]


def tv2_analytics(tid: int) -> dict:
    t=tv2_get_template(tid) or {}
    root=int(t.get("parent_template_id") or tid)
    with tv2_connect() as con:
        row=con.execute("""SELECT COUNT(*),
                            SUM(CASE WHEN a.status!='assigned' THEN 1 ELSE 0 END),
                            SUM(CASE WHEN a.status IN ('finished','needs_review') THEN 1 ELSE 0 END),
                            SUM(CASE WHEN a.status='expired' THEN 1 ELSE 0 END),
                            AVG(CASE WHEN a.status='finished' THEN a.score_percent END),
                            SUM(CASE WHEN a.passed=1 THEN 1 ELSE 0 END)
                            FROM test_assignments a JOIN test_templates t ON t.id=a.template_id
                            WHERE t.id=? OR t.parent_template_id=?""",(root,root)).fetchone()
        hard=con.execute("""SELECT q.question_text,
                            AVG(CASE WHEN ans.is_correct=1 THEN 1.0 ELSE 0.0 END) rate,
                            COUNT(ans.id) cnt
                            FROM test_questions q JOIN test_templates t ON t.id=q.template_id
                            LEFT JOIN test_answers ans ON ans.question_id=q.id
                            WHERE (t.id=? OR t.parent_template_id=?) AND q.q_type!='open'
                            GROUP BY q.id HAVING cnt>0 ORDER BY rate ASC LIMIT 5""",(root,root)).fetchall()
    return {"assigned":int(row[0] or 0),"started":int(row[1] or 0),"completed":int(row[2] or 0),
            "expired":int(row[3] or 0),"avg":float(row[4] or 0),"passed":int(row[5] or 0),
            "hard":[(str(r[0]),float(r[1] or 0),int(r[2] or 0)) for r in hard]}


async def tv2_notify_assignment(context, aid: int):
    assignment = tv2_get_assignment(aid)
    if not assignment or not assignment.get("tg_user_id"):
        return False

    user_id = int(assignment["tg_user_id"])
    callback = f"help:testv2:myopen:{aid}"
    duration = (
        f"{int(assignment['time_limit_sec']) // 60} минут"
        if assignment.get("time_limit_sec")
        else "без ограничения"
    )
    due_text = tv2_fmt_dt(assignment.get("due_at"))

    # Назначение сразу появляется в личном центре уведомлений.
    db_notification_add_once(
        user_id,
        "test_assigned_v2",
        f"Назначен новый тест: {assignment['title']}",
        (
            f"Пройти до: {due_text}. "
            f"Время после запуска: {duration}. "
            "Тест ещё не начат. "
            f"{TEST_RETAKE_POLICY_TEXT}"
        ),
        callback_data=callback,
    )

    text = (
        f"📝 Вам назначен новый тест: <b>{escape(assignment['title'])}</b>\n"
        f"⏱ После запуска: <b>{escape(duration)}</b>\n"
        f"📅 Пройти до: <b>{escape(due_text)}</b>\n"
        f"🎯 Проходной балл: <b>{int(assignment.get('passing_score') or 70)}%</b>\n"
        f"ℹ️ {escape(TEST_RETAKE_POLICY_TEXT)}"
    )
    try:
        await context.bot.send_message(
            user_id,
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("▶️ Открыть тест", callback_data=callback)
            ]]),
        )
        return True
    except Exception as exc:
        logger.warning("Cannot notify employee about test assignment %s: %s", aid, exc)
        return False


async def tv2_send_reminders(context):
    """Напоминает о сроке и отдельно сигнализирует, если тест ещё не начат."""
    now = datetime.utcnow()
    with tv2_connect() as con:
        rows = con.execute(
            """
            SELECT a.id, a.due_at, a.reminder_24_sent, a.reminder_2_sent,
                   a.overdue_notice_sent, p.tg_user_id, t.title, a.status
            FROM test_assignments a
            JOIN profiles p ON p.id=a.profile_id
            JOIN test_templates t ON t.id=a.template_id
            WHERE a.status IN ('assigned','in_progress','saved')
              AND a.due_at IS NOT NULL
            """
        ).fetchall()

    for row in rows:
        aid = int(row[0])
        due_iso = row[1]
        user_id = row[5]
        title = str(row[6] or "Тест")
        status = str(row[7] or "assigned")
        if not user_id:
            continue

        try:
            due_dt = datetime.fromisoformat(due_iso)
        except Exception:
            continue

        remaining = (due_dt - now).total_seconds()
        callback = f"help:testv2:myopen:{aid}"
        due_text = tv2_fmt_dt(due_iso)
        flag = None
        extra_flag = None
        message_text = None
        notification_type = None
        notification_title = None
        notification_body = None
        button_text = "Открыть тест"

        if remaining <= 0 and not int(row[4] or 0):
            was_not_started = status == "assigned"
            tv2_mark_expired(aid)
            flag = "overdue_notice_sent"
            notification_type = "test_not_started_expired" if was_not_started else "test_expired"
            if was_not_started:
                message_text = (
                    f"⌛ Срок теста «{escape(title)}» истёк. "
                    "Вы не успели приступить к тестированию."
                )
                notification_title = f"Срок теста истёк: {title}"
                notification_body = (
                    f"Тест не был начат. Предельный срок: {due_text}."
                )
            else:
                message_text = f"⌛ Срок теста «{escape(title)}» истёк."
                notification_title = f"Срок теста истёк: {title}"
                notification_body = f"Предельный срок: {due_text}."

        elif remaining <= 7200 and not int(row[3] or 0):
            flag = "reminder_2_sent"
            # Не отправляем вслед за срочным alarm более слабое уведомление «за сутки».
            extra_flag = "reminder_24_sent"
            if status == "assigned":
                notification_type = "test_not_started_2h"
                notification_title = f"Срочно начните тест: {title}"
                notification_body = (
                    f"До предельного срока осталось менее 2 часов. "
                    f"Пройти до: {due_text}. Тест ещё не начат. "
                    f"{TEST_RETAKE_POLICY_TEXT}"
                )
                message_text = (
                    f"🚨 <b>Вы ещё не приступили к тесту</b>\n\n"
                    f"Тест: <b>{escape(title)}</b>\n"
                    f"До предельного срока осталось <b>менее 2 часов</b>.\n"
                    f"Пройти до: <b>{escape(due_text)}</b>"
                )
                button_text = "▶️ Начать тест"
            else:
                notification_type = "test_due_2h"
                notification_title = f"До срока теста менее 2 часов: {title}"
                notification_body = f"Пройти до: {due_text}."
                message_text = (
                    f"⏰ До срока теста «{escape(title)}» осталось менее 2 часов."
                )

        elif 7200 < remaining <= 86400 and not int(row[2] or 0):
            flag = "reminder_24_sent"
            if status == "assigned":
                notification_type = "test_not_started_24h"
                notification_title = f"Пора начать тест: {title}"
                notification_body = (
                    f"До предельного срока осталось менее суток. "
                    f"Пройти до: {due_text}. Тест ещё не начат. "
                    f"{TEST_RETAKE_POLICY_TEXT}"
                )
                message_text = (
                    f"⏰ <b>Вы ещё не приступили к тесту</b>\n\n"
                    f"Тест: <b>{escape(title)}</b>\n"
                    f"До предельного срока осталось <b>менее суток</b>.\n"
                    f"Пройти до: <b>{escape(due_text)}</b>"
                )
                button_text = "▶️ Начать тест"
            else:
                notification_type = "test_due_24h"
                notification_title = f"До срока теста менее суток: {title}"
                notification_body = f"Пройти до: {due_text}."
                message_text = (
                    f"⏰ До срока теста «{escape(title)}» осталось менее суток."
                )

        if not flag or not message_text:
            continue

        db_notification_add_once(
            int(user_id),
            notification_type or "test_reminder",
            notification_title or "Напоминание о тесте",
            notification_body or "",
            callback_data=callback,
        )

        try:
            await context.bot.send_message(
                int(user_id),
                message_text,
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton(button_text, callback_data=callback)
                ]]),
            )
        except Exception as exc:
            logger.warning("Cannot send test reminder %s to %s: %s", aid, user_id, exc)

        # Фиксируем событие один раз, даже если Telegram временно не доставил push:
        # запись остаётся доступной в разделе «Уведомления».
        with tv2_connect() as con:
            if extra_flag:
                con.execute(
                    f"UPDATE test_assignments SET {flag}=1, {extra_flag}=1 WHERE id=?",
                    (aid,),
                )
            else:
                con.execute(
                    f"UPDATE test_assignments SET {flag}=1 WHERE id=?",
                    (aid,),
                )


async def check_and_send_jobs(context: ContextTypes.DEFAULT_TYPE):
    await _tv2_legacy_check_and_send_jobs(context)
    try:
        await tv2_send_reminders(context)
    except Exception as e:
        logger.exception("TEST V2 reminder error: %s",e)


async def tv2_admin_guard(update, context) -> bool:
    if not await is_admin_scoped(update,context):
        try: await update.callback_query.answer("Недостаточно прав.",show_alert=True)
        except Exception: pass
        return False
    return True


async def cb_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data=(update.callback_query.data or "") if update.callback_query else ""
    if data=="help:settings:test": data="help:testv2:admin"
    if data=="help:me:tests": data="help:testv2:my:all:0"
    if not data.startswith("help:testv2:"):
        return await _tv2_legacy_cb_help(update,context)
    q=update.callback_query
    try: await q.answer()
    except Exception: pass
    await sync_profile_user_id_from_update(update)

    if data=="help:testv2:admin":
        if not await tv2_admin_guard(update,context): return
        tv2_clear(context)
        await q.edit_message_text("📝 <b>Тестирование 2.0</b>\n\nСоздание, версии, банк вопросов, проверки, аналитика и напоминания.",parse_mode=ParseMode.HTML,reply_markup=tv2_kb_admin_menu())
        return

    if data.startswith("help:testv2:departments:"):
        if not await tv2_admin_guard(update,context): return
        page=int(data.rsplit(":",1)[-1]); people=db_profiles_list(); pages=max(1,(len(people)+7)//8); page=max(0,min(page,pages-1)); rows=[]
        with tv2_connect() as con:
            depmap={int(r[0]):(r[1] or "—") for r in con.execute("SELECT id,department FROM profiles").fetchall()}
        for pid,name in people[page*8:(page+1)*8]:
            rows.append([InlineKeyboardButton(f"{name[:38]} · {str(depmap.get(int(pid),'—'))[:18]}",callback_data=f"help:testv2:departmentprofile:{int(pid)}:{page}")])
        nav=[]
        if page>0: nav.append(InlineKeyboardButton("◀️",callback_data=f"help:testv2:departments:{page-1}"))
        nav.append(InlineKeyboardButton(f"{page+1}/{pages}",callback_data="noop"))
        if page+1<pages: nav.append(InlineKeyboardButton("▶️",callback_data=f"help:testv2:departments:{page+1}"))
        rows.append(nav); rows.append([InlineKeyboardButton("⬅️ Назад",callback_data="help:testv2:admin")])
        await q.edit_message_text("🏢 <b>Отделы сотрудников</b>\n\nВыберите сотрудника, чтобы указать отдел.",parse_mode=ParseMode.HTML,reply_markup=InlineKeyboardMarkup(rows)); return

    if data.startswith("help:testv2:departmentprofile:"):
        parts=data.split(":"); pid=int(parts[-2]); page=int(parts[-1]); p=db_profiles_get(pid)
        if not p: await q.answer("Анкета не найдена",show_alert=True); return
        tv2_set_state(context,"profile_department",profile_id=pid,page=page)
        with tv2_connect() as con:
            row=con.execute("SELECT COALESCE(department,'') FROM profiles WHERE id=?",(pid,)).fetchone()
        await q.edit_message_text(f"🏢 <b>{escape(p['full_name'])}</b>\nТекущий отдел: <b>{escape((row[0] if row else '') or '—')}</b>\n\nВведите название отдела или '-' чтобы очистить.",parse_mode=ParseMode.HTML,reply_markup=tv2_kb_cancel(f"help:testv2:departments:{page}")); return

    if data.startswith("help:testv2:my:"):
        profile=get_profile_for_user(update)
        if not profile:
            await q.answer("Анкета не найдена",show_alert=True); return
        parts=data.split(":"); filt=parts[3] if len(parts)>3 else "all"; page=int(parts[4]) if len(parts)>4 else 0
        items,total=tv2_my_tests(int(profile["id"]),filt,page)
        counts={}
        for key in ("new","progress","done","expired"):
            counts[key]=tv2_my_tests(int(profile["id"]),key,0)[1]
        text=("📝 <b>Мои тесты</b>\n\n"
              f"🔴 Новые: <b>{counts['new']}</b>\n🟡 В процессе: <b>{counts['progress']}</b>\n"
              f"🟢 Завершены: <b>{counts['done']}</b>\n⌛ Истекли: <b>{counts['expired']}</b>")
        await q.edit_message_text(text,parse_mode=ParseMode.HTML,reply_markup=tv2_kb_my(int(profile["id"]),filt,page))
        return

    if data.startswith("help:testv2:myopen:"):
        aid=int(data.rsplit(":",1)[-1]); a=tv2_get_assignment(aid); p=get_profile_for_user(update)
        if not a or not p or int(a["profile_id"])!=int(p["id"]): await q.answer("Тест не найден",show_alert=True); return
        await q.edit_message_text(tv2_my_open_text(a),parse_mode=ParseMode.HTML,reply_markup=tv2_kb_my_open(a))
        return

    if data.startswith("help:testv2:result:"):
        aid=int(data.rsplit(":",1)[-1]); a=tv2_get_assignment(aid); p=get_profile_for_user(update)
        if not a or (not await is_admin_scoped(update,context) and (not p or int(a["profile_id"])!=int(p["id"]))): return
        is_admin = await is_admin_scoped(update, context)
        rows=[]
        if is_admin:
            rows.append([InlineKeyboardButton("🗑 Удалить результат",callback_data=f"help:testv2:resultdeleteconfirm:{aid}")])
            rows.append([InlineKeyboardButton("⬅️ К аналитике",callback_data="help:testv2:analytics")])
        else:
            rows.append([InlineKeyboardButton("⬅️ Назад",callback_data=f"help:testv2:myopen:{aid}")])
        await q.edit_message_text(tv2_render_result_details(aid),parse_mode=ParseMode.HTML,reply_markup=InlineKeyboardMarkup(rows))
        return

    if not await tv2_admin_guard(update,context): return

    if data=="help:testv2:create":
        tv2_set_state(context,"create_title")
        await q.edit_message_text("➕ <b>Новый тест</b>\n\nВведите название теста.",parse_mode=ParseMode.HTML,reply_markup=tv2_kb_cancel())
        return

    if data.startswith("help:testv2:createmode:"):
        mode=data.rsplit(":",1)[-1]; d=context.user_data.get(TV2_DATA) or {}; title=d.get("title")
        tid=tv2_create_template(title,update.effective_user.id,mode); tv2_clear(context)
        await q.edit_message_text(tv2_template_text(tid),parse_mode=ParseMode.HTML,reply_markup=tv2_kb_template(tid))
        return

    if data.startswith("help:testv2:drafts:"):
        page=int(data.rsplit(":",1)[-1]); await q.edit_message_text("🗂 <b>Шаблоны и опубликованные версии</b>",parse_mode=ParseMode.HTML,reply_markup=tv2_kb_drafts(page)); return

    if data.startswith("help:testv2:template:"):
        tid=int(data.rsplit(":",1)[-1]); await q.edit_message_text(tv2_template_text(tid),parse_mode=ParseMode.HTML,reply_markup=tv2_kb_template(tid)); return

    if data.startswith("help:testv2:preview:"):
        tid=int(data.rsplit(":",1)[-1]); qs=tv2_questions(tid); lines=[tv2_template_text(tid),"","<b>Предпросмотр вопросов</b>"]
        for x in qs[:20]: lines.append(f"\n{x['idx']}. {escape(x['question_text'])} · {float(x.get('points') or 1):g} б.")
        await q.edit_message_text("\n".join(lines)[:4000],parse_mode=ParseMode.HTML,reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад",callback_data=f"help:testv2:template:{tid}")]])); return

    if data.startswith("help:testv2:qadd:"):
        tid=int(data.rsplit(":",1)[-1]); context.user_data[TV2_DATA]={"template_id":tid,"target":"template"}
        await q.edit_message_text("Выберите тип вопроса:",reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📝 Открытый",callback_data="help:testv2:qtype:open")],
            [InlineKeyboardButton("🔘 Один вариант",callback_data="help:testv2:qtype:single")],
            [InlineKeyboardButton("☑️ Несколько вариантов",callback_data="help:testv2:qtype:multi")],
            [InlineKeyboardButton("⬅️ Назад",callback_data=f"help:testv2:template:{tid}")],])); return

    if data.startswith("help:testv2:qtype:"):
        qtype=data.rsplit(":",1)[-1]; d=context.user_data.get(TV2_DATA) or {}; d["q_type"]=qtype; context.user_data[TV2_DATA]=d; context.user_data[TV2_STATE]="q_text"
        await q.edit_message_text("Введите текст вопроса:",reply_markup=tv2_kb_cancel(f"help:testv2:template:{d.get('template_id',0)}")); return

    if data.startswith("help:testv2:qeditlist:"):
        parts=data.split(":"); tid=int(parts[-2]); page=int(parts[-1]); await q.edit_message_text("✏️ <b>Редактор вопросов</b>",parse_mode=ParseMode.HTML,reply_markup=tv2_kb_question_list(tid,page)); return

    if data.startswith("help:testv2:qedit:"):
        qid=int(data.rsplit(":",1)[-1]); qq=tv2_question_by_id(qid)
        if qq: await q.edit_message_text(tv2_question_text(qq),parse_mode=ParseMode.HTML,reply_markup=tv2_kb_question_edit(qq))
        return

    if data.startswith("help:testv2:qfield:"):
        parts=data.split(":"); field=parts[-2]; qid=int(parts[-1]); qq=tv2_question_by_id(qid)
        state_map={"text":"edit_q_text","points":"edit_q_points","options":"edit_q_options","correct":"edit_q_correct","explanation":"edit_q_explanation"}
        tv2_set_state(context,state_map[field],question_id=qid,template_id=int(qq["template_id"]))
        prompts={"text":"Введите новый текст вопроса:","points":"Введите количество баллов, например 1 или 2.5:","options":"Введите варианты, каждый с новой строки:","correct":"Введите номера правильных вариантов через запятую:","explanation":"Введите пояснение или '-' для очистки:"}
        await q.edit_message_text(prompts[field],reply_markup=tv2_kb_cancel(f"help:testv2:qedit:{qid}")); return

    if data.startswith("help:testv2:qmove:"):
        parts=data.split(":"); qid=int(parts[-2]); delta=int(parts[-1]); tv2_move_question(qid,delta); qq=tv2_question_by_id(qid)
        await q.edit_message_text(tv2_question_text(qq),parse_mode=ParseMode.HTML,reply_markup=tv2_kb_question_edit(qq)); return

    if data.startswith("help:testv2:qdeleteconfirm:"):
        qid=int(data.rsplit(":",1)[-1]); qq=tv2_question_by_id(qid)
        await q.edit_message_text("⚠️ Удалить вопрос? Это действие нельзя отменить.",reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🗑 Да, удалить",callback_data=f"help:testv2:qdelete:{qid}")],
            [InlineKeyboardButton("Отмена",callback_data=f"help:testv2:qedit:{qid}")],])); return

    if data.startswith("help:testv2:qdelete:"):
        qid=int(data.rsplit(":",1)[-1]); qq=tv2_question_by_id(qid); tid=int(qq["template_id"]); tv2_delete_question(qid)
        await q.edit_message_text("Вопрос удалён.",reply_markup=tv2_kb_question_list(tid,0)); return

    if data.startswith("help:testv2:settings:"):
        tid=int(data.rsplit(":",1)[-1]); await q.edit_message_text("⚙️ <b>Настройки теста</b>",parse_mode=ParseMode.HTML,reply_markup=tv2_kb_settings(tid)); return

    if data.startswith("help:testv2:toggle:"):
        parts=data.split(":"); key=parts[-2]; tid=int(parts[-1]); col={"shuffleq":"shuffle_questions","shuffleo":"shuffle_options","back":"allow_back","skip":"allow_skip","feedback":"immediate_feedback"}[key]
        with tv2_connect() as con: con.execute(f"UPDATE test_templates SET {col}=CASE WHEN COALESCE({col},0)=1 THEN 0 ELSE 1 END, updated_at=? WHERE id=?",(datetime.utcnow().isoformat(),tid))
        await q.edit_message_text("⚙️ <b>Настройки теста</b>",parse_mode=ParseMode.HTML,reply_markup=tv2_kb_settings(tid)); return

    if data.startswith("help:testv2:set:"):
        parts=data.split(":"); setting=parts[-2]; tid=int(parts[-1])
        choices={
            "mode":[("📚 Обучение","learning"),("🎓 Аттестация","exam"),("⚙️ Свой","custom")],
            "passing":[("60%","60"),("70%","70"),("80%","80"),("90%","90"),("✍️ Ввести","custom")],
            "attempts":[("1","1")],
            "policy":[("Лучший","best"),("Последний","last"),("Средний","average")],
            "result":[("Только балл","score"),("Ошибки","errors"),("Все ответы","all"),("Скрыть","hidden")],
            "time":[("Без ограничения","0"),("10 минут","600"),("20 минут","1200"),("30 минут","1800"),("✍️ Ввести","custom")],
        }
        rows=[]
        for label,val in choices[setting]: rows.append([InlineKeyboardButton(label,callback_data=f"help:testv2:setvalue:{setting}:{tid}:{val}")])
        rows.append([InlineKeyboardButton("⬅️ Назад",callback_data=f"help:testv2:settings:{tid}")])
        await q.edit_message_text("Выберите значение:",reply_markup=InlineKeyboardMarkup(rows)); return

    if data.startswith("help:testv2:setvalue:"):
        parts=data.split(":"); setting=parts[-3]; tid=int(parts[-2]); val=parts[-1]
        if val=="custom":
            tv2_set_state(context,f"custom_{setting}",template_id=tid)
            prompt="Введите число:" if setting!="time" else "Введите длительность в минутах:"
            await q.edit_message_text(prompt,reply_markup=tv2_kb_cancel(f"help:testv2:settings:{tid}")); return
        col={"mode":"test_mode","passing":"passing_score","attempts":"max_attempts","policy":"scoring_policy","result":"result_mode","time":"default_time_limit_sec"}[setting]
        # Даже старые кнопки/ссылки с values 2, 3 или 99 не могут включить
        # повторное прохождение.
        value=(TEST_MAX_ATTEMPTS if setting == "attempts" else int(val)) if setting in ("passing","attempts","time") else val
        with tv2_connect() as con:
            con.execute(f"UPDATE test_templates SET {col}=?, updated_at=? WHERE id=?",(value,datetime.utcnow().isoformat(),tid))
            if setting=="mode" and val in ("learning","exam"):
                cfg=tv2_template_defaults(val)
                con.execute("""UPDATE test_templates SET passing_score=?,max_attempts=?,scoring_policy=?,result_mode=?,
                               shuffle_questions=?,shuffle_options=?,allow_back=?,allow_skip=?,immediate_feedback=? WHERE id=?""",
                            (cfg["passing_score"],cfg["max_attempts"],cfg["scoring_policy"],cfg["result_mode"],cfg["shuffle_questions"],cfg["shuffle_options"],cfg["allow_back"],cfg["allow_skip"],cfg["immediate_feedback"],tid))
        await q.edit_message_text("⚙️ <b>Настройки теста</b>",parse_mode=ParseMode.HTML,reply_markup=tv2_kb_settings(tid)); return

    if data.startswith("help:testv2:publishconfirm:"):
        tid=int(data.rsplit(":",1)[-1]); await q.edit_message_text("🔒 Опубликовать неизменяемую версию теста?",reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Опубликовать",callback_data=f"help:testv2:publish:{tid}")],
            [InlineKeyboardButton("Отмена",callback_data=f"help:testv2:template:{tid}")],])); return

    if data.startswith("help:testv2:publish:"):
        tid=int(data.rsplit(":",1)[-1]); pub=tv2_publish_template(tid,update.effective_user.id)
        await q.edit_message_text("✅ Версия опубликована. Назначения будут ссылаться на неизменяемую копию.",reply_markup=tv2_kb_template(pub)); return

    if data.startswith("help:testv2:templatedeleteconfirm:"):
        tid=int(data.rsplit(":",1)[-1])
        await q.edit_message_text("⚠️ Удалить черновик и все его вопросы? Это действие нельзя отменить.",reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🗑 Да, удалить",callback_data=f"help:testv2:templatedelete:{tid}")],
            [InlineKeyboardButton("Отмена",callback_data=f"help:testv2:template:{tid}")],
        ])); return

    if data.startswith("help:testv2:templatedelete:"):
        tid=int(data.rsplit(":",1)[-1])
        with tv2_connect() as con:
            has_assign=con.execute("SELECT 1 FROM test_assignments WHERE template_id=? LIMIT 1",(tid,)).fetchone()
            if has_assign:
                con.execute("UPDATE test_templates SET is_draft_visible=0 WHERE id=?",(tid,))
            else:
                con.execute("DELETE FROM test_questions WHERE template_id=?",(tid,)); con.execute("DELETE FROM test_templates WHERE id=?",(tid,))
        await q.edit_message_text("✅ Черновик удалён из списка.",reply_markup=tv2_kb_drafts(0)); return

    if data.startswith("help:testv2:resultdeleteconfirm:"):
        aid=int(data.rsplit(":",1)[-1])
        await q.edit_message_text("⚠️ Удалить результат теста и все ответы сотрудника?",reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🗑 Да, удалить",callback_data=f"help:testv2:resultdelete:{aid}")],
            [InlineKeyboardButton("Отмена",callback_data=f"help:testv2:result:{aid}")],
        ])); return

    if data.startswith("help:testv2:resultdelete:"):
        aid=int(data.rsplit(":",1)[-1]); a=tv2_get_assignment(aid)
        with tv2_connect() as con:
            con.execute("DELETE FROM test_answers WHERE assignment_id=?",(aid,)); con.execute("DELETE FROM test_attempt_events WHERE assignment_id=?",(aid,)); con.execute("DELETE FROM test_admin_comments WHERE assignment_id=?",(aid,)); con.execute("DELETE FROM test_assignments WHERE id=?",(aid,))
        if a: tv2_update_profile_average(int(a["profile_id"]))
        await q.edit_message_text("✅ Результат удалён.",reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ К аналитике",callback_data="help:testv2:analytics")]])); return

    if data=="help:testv2:bank":
        cats=tv2_bank_categories(); rows=[[InlineKeyboardButton(c,callback_data=f"help:testv2:bankcat:{c}")] for c in cats[:30]]
        rows.append([InlineKeyboardButton("➕ Добавить вопрос в банк",callback_data="help:testv2:bankadd")]); rows.append([InlineKeyboardButton("⬅️ Назад",callback_data="help:testv2:admin")])
        await q.edit_message_text("📚 <b>Банк вопросов</b>\n\nВыберите категорию.",parse_mode=ParseMode.HTML,reply_markup=InlineKeyboardMarkup(rows)); return

    if data=="help:testv2:bankadd":
        context.user_data[TV2_DATA]={"target":"bank"}
        await q.edit_message_text("Выберите тип вопроса:",reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📝 Открытый",callback_data="help:testv2:qtype:open")],[InlineKeyboardButton("🔘 Один вариант",callback_data="help:testv2:qtype:single")],[InlineKeyboardButton("☑️ Несколько",callback_data="help:testv2:qtype:multi")],[InlineKeyboardButton("⬅️ Назад",callback_data="help:testv2:bank")],])); return

    if data.startswith("help:testv2:bankcat:"):
        cat=data.split(":",3)[-1]; items=tv2_bank_list(cat); rows=[[InlineKeyboardButton(x["question_text"][:55],callback_data="noop")] for x in items[:30]]
        rows.append([InlineKeyboardButton("⬅️ Назад",callback_data="help:testv2:bank")]); await q.edit_message_text(f"📚 <b>{escape(cat)}</b>\nВопросов: {len(items)}",parse_mode=ParseMode.HTML,reply_markup=InlineKeyboardMarkup(rows)); return

    if data.startswith("help:testv2:bankpick:"):
        parts=data.split(":"); tid=int(parts[-2]); page=int(parts[-1]); items=tv2_bank_list(limit=200); pages=max(1,(len(items)+7)//8); page=max(0,min(page,pages-1)); rows=[]
        for x in items[page*8:(page+1)*8]: rows.append([InlineKeyboardButton(f"➕ {x['question_text'][:50]}",callback_data=f"help:testv2:bankcopy:{tid}:{int(x['id'])}")])
        nav=[]
        if page>0: nav.append(InlineKeyboardButton("◀️",callback_data=f"help:testv2:bankpick:{tid}:{page-1}"))
        nav.append(InlineKeyboardButton(f"{page+1}/{pages}",callback_data="noop"))
        if page+1<pages: nav.append(InlineKeyboardButton("▶️",callback_data=f"help:testv2:bankpick:{tid}:{page+1}"))
        rows.append(nav); rows.append([InlineKeyboardButton("⬅️ Назад",callback_data=f"help:testv2:template:{tid}")])
        await q.edit_message_text("📚 Выберите вопрос для добавления:",reply_markup=InlineKeyboardMarkup(rows)); return

    if data.startswith("help:testv2:bankcopy:"):
        parts=data.split(":"); tid=int(parts[-2]); bid=int(parts[-1]); tv2_copy_bank_question(bid,tid)
        await q.answer("Добавлено",show_alert=False); await q.edit_message_text(tv2_template_text(tid),parse_mode=ParseMode.HTML,reply_markup=tv2_kb_template(tid)); return

    if data.startswith("help:testv2:bankrandom:"):
        tid=int(data.rsplit(":",1)[-1])
        with tv2_connect() as con:
            ids=[int(r[0]) for r in con.execute("SELECT id FROM test_question_bank WHERE is_active=1 ORDER BY RANDOM() LIMIT 10").fetchall()]
        for bid in ids: tv2_copy_bank_question(bid,tid)
        await q.edit_message_text(f"✅ Добавлено случайных вопросов: {len(ids)}\n\n"+tv2_template_text(tid),parse_mode=ParseMode.HTML,reply_markup=tv2_kb_template(tid)); return

    if data=="help:testv2:assign":
        items=tv2_list_templates(limit=100); rows=[[InlineKeyboardButton(f"{x['title']} · v{x.get('version',1)}",callback_data=f"help:testv2:assign_template:{int(x['id'])}")] for x in items[:40]]; rows.append([InlineKeyboardButton("⬅️ Назад",callback_data="help:testv2:admin")])
        await q.edit_message_text("👥 Выберите тест для назначения:",reply_markup=InlineKeyboardMarkup(rows)); return

    if data.startswith("help:testv2:assign_template:"):
        tid=int(data.rsplit(":",1)[-1]); t=tv2_get_template(tid); pub=tid if int(t.get("is_published") or 0) else tv2_publish_template(tid,update.effective_user.id)
        context.user_data[TV2_DATA]={"template_id":pub,"source_template_id":tid,"selected":[]}
        await q.edit_message_text("Кому назначить тест?",reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("👥 Всем",callback_data="help:testv2:recipients:all")],
            [InlineKeyboardButton("🏢 По отделу",callback_data="help:testv2:recipients:department")],
            [InlineKeyboardButton("🏙 По городу",callback_data="help:testv2:recipients:city")],
            [InlineKeyboardButton("👤 Выбрать вручную",callback_data="help:testv2:recipients:manual:0")],
            [InlineKeyboardButton("🔁 Только не прошедшим",callback_data="help:testv2:recipients:failed")],
            [InlineKeyboardButton("⬅️ Назад",callback_data=f"help:testv2:template:{tid}")],])); return

    if data.startswith("help:testv2:recipients:"):
        parts=data.split(":"); rule=parts[3]; d=context.user_data.get(TV2_DATA) or {}; tid=int(d.get("template_id"))
        if rule=="all":
            d["selected"] = tv2_profile_ids_for_rule("all")
            context.user_data[TV2_DATA] = d
            context.user_data[TV2_STATE] = "assign_due_buttons"
            await q.edit_message_text(
                tv3_due_selection_text(f"Выбрано сотрудников: {len(d['selected'])}"),
                reply_markup=tv3_assignment_due_main_keyboard(),
            )
            return
        if rule=="failed":
            d["selected"] = tv2_profile_ids_for_rule("failed", template_id=tid)
            context.user_data[TV2_DATA] = d
            context.user_data[TV2_STATE] = "assign_due_buttons"
            await q.edit_message_text(
                tv3_due_selection_text(f"Выбрано сотрудников: {len(d['selected'])}"),
                reply_markup=tv3_assignment_due_main_keyboard(),
            )
            return
        if rule=="department":
            with tv2_connect() as con: deps=[str(r[0]) for r in con.execute("SELECT DISTINCT department FROM profiles WHERE COALESCE(department,'')!='' AND COALESCE(is_active,1)=1 ORDER BY department").fetchall()]
            context.user_data["tv2_department_options"]=deps
            rows=[[InlineKeyboardButton(dep[:55],callback_data=f"help:testv2:deptpick:{i}")] for i,dep in enumerate(deps[:40])]; rows.append([InlineKeyboardButton("⬅️ Назад",callback_data=f"help:testv2:assign_template:{d.get('source_template_id',tid)}")]); await q.edit_message_text("Выберите отдел:",reply_markup=InlineKeyboardMarkup(rows)); return
        if rule=="city":
            with tv2_connect() as con: cities=[str(r[0]) for r in con.execute("SELECT DISTINCT city FROM profiles WHERE city!='' AND COALESCE(is_active,1)=1 ORDER BY city").fetchall()]
            rows=[[InlineKeyboardButton(c,callback_data=f"help:testv2:citypick:{c}")] for c in cities[:40]]; rows.append([InlineKeyboardButton("⬅️ Назад",callback_data=f"help:testv2:assign_template:{d.get('source_template_id',tid)}")]); await q.edit_message_text("Выберите город:",reply_markup=InlineKeyboardMarkup(rows)); return
        if rule=="manual":
            page=int(parts[4]) if len(parts)>4 else 0; people=db_profiles_list(); selected=set(d.get("selected") or []); pages=max(1,(len(people)+7)//8); page=max(0,min(page,pages-1)); rows=[]
            for pid,name in people[page*8:(page+1)*8]: rows.append([InlineKeyboardButton(("✅ " if int(pid) in selected else "▫️ ")+name[:50],callback_data=f"help:testv2:manualtoggle:{int(pid)}:{page}")])
            nav=[]
            if page>0: nav.append(InlineKeyboardButton("◀️",callback_data=f"help:testv2:recipients:manual:{page-1}"))
            nav.append(InlineKeyboardButton(f"{page+1}/{pages}",callback_data="noop"))
            if page+1<pages: nav.append(InlineKeyboardButton("▶️",callback_data=f"help:testv2:recipients:manual:{page+1}"))
            rows.append(nav); rows.append([InlineKeyboardButton(f"Готово ({len(selected)})",callback_data="help:testv2:manualdone")]); await q.edit_message_text("Выберите сотрудников:",reply_markup=InlineKeyboardMarkup(rows)); return

    if data.startswith("help:testv2:deptpick:"):
        idx = int(data.rsplit(":", 1)[-1])
        deps = context.user_data.get("tv2_department_options") or []
        dep = deps[idx] if 0 <= idx < len(deps) else ""
        d = context.user_data.get(TV2_DATA) or {}
        d["selected"] = tv2_profile_ids_for_rule("department", dep)
        context.user_data[TV2_DATA] = d
        context.user_data[TV2_STATE] = "assign_due_buttons"
        await q.edit_message_text(
            tv3_due_selection_text(
                f"Отдел: {escape(dep)}\nВыбрано: {len(d['selected'])}"
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=tv3_assignment_due_main_keyboard(),
        )
        return

    if data.startswith("help:testv2:citypick:"):
        city = data.split(":", 3)[-1]
        d = context.user_data.get(TV2_DATA) or {}
        d["selected"] = tv2_profile_ids_for_rule("city", city)
        context.user_data[TV2_DATA] = d
        context.user_data[TV2_STATE] = "assign_due_buttons"
        await q.edit_message_text(
            tv3_due_selection_text(
                f"Город: {escape(city)}\nВыбрано: {len(d['selected'])}"
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=tv3_assignment_due_main_keyboard(),
        )
        return

    if data.startswith("help:testv2:manualtoggle:"):
        parts=data.split(":"); pid=int(parts[-2]); page=int(parts[-1]); d=context.user_data.get(TV2_DATA) or {}; s=set(d.get("selected") or [])
        if pid in s: s.remove(pid)
        else: s.add(pid)
        d["selected"]=sorted(s); context.user_data[TV2_DATA]=d
        people=db_profiles_list(); pages=max(1,(len(people)+7)//8); page=max(0,min(page,pages-1)); rows=[]
        for person_id,name in people[page*8:(page+1)*8]:
            rows.append([InlineKeyboardButton(("✅ " if int(person_id) in s else "▫️ ")+name[:50],callback_data=f"help:testv2:manualtoggle:{int(person_id)}:{page}")])
        nav=[]
        if page>0: nav.append(InlineKeyboardButton("◀️",callback_data=f"help:testv2:recipients:manual:{page-1}"))
        nav.append(InlineKeyboardButton(f"{page+1}/{pages}",callback_data="noop"))
        if page+1<pages: nav.append(InlineKeyboardButton("▶️",callback_data=f"help:testv2:recipients:manual:{page+1}"))
        rows.append(nav); rows.append([InlineKeyboardButton(f"Готово ({len(s)})",callback_data="help:testv2:manualdone")])
        await q.edit_message_text("Выберите сотрудников:",reply_markup=InlineKeyboardMarkup(rows)); return

    if data=="help:testv2:manualdone":
        d = context.user_data.get(TV2_DATA) or {}
        context.user_data[TV2_STATE] = "assign_due_buttons"
        await q.edit_message_text(
            tv3_due_selection_text(
                f"Выбрано сотрудников: {len(d.get('selected') or [])}"
            ),
            reply_markup=tv3_assignment_due_main_keyboard(),
        )
        return

    if data=="help:testv2:assignconfirm":
        d=context.user_data.get(TV2_DATA) or {}; tid=int(d["template_id"]); selected=d.get("selected") or []; t=tv2_get_template(tid) or {}; duration=d.get("time_limit_sec",t.get("default_time_limit_sec"))
        lines=["📋 <b>Проверка назначения</b>","",f"Тест: <b>{escape(t.get('title',''))}</b>",f"Получателей: <b>{len(selected)}</b>",f"Срок: <b>{escape(tv2_fmt_dt(d.get('due_at')))}</b>",f"Время на сам тест: <b>{int(duration)//60 if duration else 'без ограничения'}</b>"]
        await q.edit_message_text("\n".join(lines),parse_mode=ParseMode.HTML,reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✅ Назначить",callback_data="help:testv2:assignsend")],[InlineKeyboardButton("❌ Отмена",callback_data="help:testv2:admin")]])); return

    if data=="help:testv2:assignsend":
        d=context.user_data.get(TV2_DATA) or {}; tid=int(d["template_id"]); selected=d.get("selected") or []; t=tv2_get_template(tid) or {}; duration=d.get("time_limit_sec",t.get("default_time_limit_sec")); aids=[]
        for pid in selected:
            aid=tv2_create_assignment(tid,int(pid),update.effective_user.id,d.get("due_at"),duration); aids.append(aid); await tv2_notify_assignment(context,aid)
        tv2_clear(context); await q.edit_message_text(f"✅ Назначено сотрудникам: {len(aids)}",reply_markup=tv2_kb_admin_menu()); return

    if data=="help:testv2:review":
        items=tv2_admin_review_list(); rows=[[InlineKeyboardButton(f"{x['full_name']} · {x['title']}",callback_data=f"help:testv2:reviewopen:{int(x['id'])}")] for x in items[:40]]; rows.append([InlineKeyboardButton("⬅️ Назад",callback_data="help:testv2:admin")]); await q.edit_message_text(f"🧑‍🏫 <b>Ожидают проверки: {len(items)}</b>",parse_mode=ParseMode.HTML,reply_markup=InlineKeyboardMarkup(rows)); return

    if data.startswith("help:testv2:reviewopen:"):
        aid=int(data.rsplit(":",1)[-1]); a=tv2_get_assignment(aid); rows=[]
        for qq in tv2_questions(int(a["template_id"])):
            if qq["q_type"]=="open":
                ans=tv2_answer(aid,int(qq["id"])); marker="✅" if ans and ans.get("review_status") not in ("pending",None) else "⏳"; rows.append([InlineKeyboardButton(f"{marker} {qq['idx']}. {qq['question_text'][:45]}",callback_data=f"help:testv2:reviewanswer:{aid}:{int(qq['id'])}")])
        rows.append([InlineKeyboardButton("💬 Общий комментарий",callback_data=f"help:testv2:reviewcomment:{aid}")]); rows.append([InlineKeyboardButton("⬅️ К списку",callback_data="help:testv2:review")]); await q.edit_message_text(f"🧑‍🏫 <b>{escape(a['full_name'])}</b> · {escape(a['title'])}",parse_mode=ParseMode.HTML,reply_markup=InlineKeyboardMarkup(rows)); return

    if data.startswith("help:testv2:reviewanswer:"):
        parts=data.split(":"); aid=int(parts[-2]); qid=int(parts[-1]); qq=tv2_question_by_id(qid); ans=tv2_answer(aid,qid); answer_text=((ans or {}).get("answer") or {}).get("text") or "—"; pts=float(qq.get("points") or 1)
        text=f"❓ <b>{escape(qq['question_text'])}</b>\n\nОтвет:\n{escape(answer_text)}\n\nМаксимум: {pts:g} балла"
        kb=InlineKeyboardMarkup([[InlineKeyboardButton("✅ Верно",callback_data=f"help:testv2:grade:{aid}:{qid}:100"),InlineKeyboardButton("🟡 Частично",callback_data=f"help:testv2:grade:{aid}:{qid}:50"),InlineKeyboardButton("❌ Неверно",callback_data=f"help:testv2:grade:{aid}:{qid}:0")],[InlineKeyboardButton("💬 Комментарий к ответу",callback_data=f"help:testv2:answercomment:{aid}:{qid}")],[InlineKeyboardButton("⬅️ Назад",callback_data=f"help:testv2:reviewopen:{aid}")]])
        await q.edit_message_text(text,parse_mode=ParseMode.HTML,reply_markup=kb); return

    if data.startswith("help:testv2:grade:"):
        parts=data.split(":"); aid=int(parts[-3]); qid=int(parts[-2]); pct=int(parts[-1]); qq=tv2_question_by_id(qid); awarded=float(qq.get("points") or 1)*pct/100; status={100:"full",50:"partial",0:"wrong"}[pct]
        with tv2_connect() as con: con.execute("UPDATE test_answers SET awarded_points=?,is_correct=?,review_status=? WHERE assignment_id=? AND question_id=?",(awarded,1 if pct==100 else 0,status,aid,qid))
        calc=tv2_calculate(aid,finalize=True); await q.answer("Оценка сохранена",show_alert=False)
        if calc["pending"]==0:
            a=tv2_get_assignment(aid)
            try: await context.bot.send_message(int(a["tg_user_id"]),"✅ Проверка теста завершена. Результат доступен в «Моих тестах».",reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Открыть результат",callback_data=f"help:testv2:result:{aid}")]]))
            except Exception: pass
            await q.edit_message_text("✅ Все открытые ответы проверены.",reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📊 Результат",callback_data=f"help:testv2:result:{aid}")],[InlineKeyboardButton("⬅️ К проверкам",callback_data="help:testv2:review")]])); return
        a=tv2_get_assignment(aid); rows=[]
        for open_q in tv2_questions(int(a["template_id"])):
            if open_q["q_type"]=="open":
                open_ans=tv2_answer(aid,int(open_q["id"])); marker="✅" if open_ans and open_ans.get("review_status") not in ("pending",None) else "⏳"
                rows.append([InlineKeyboardButton(f"{marker} {open_q['idx']}. {open_q['question_text'][:45]}",callback_data=f"help:testv2:reviewanswer:{aid}:{int(open_q['id'])}")])
        rows.append([InlineKeyboardButton("💬 Общий комментарий",callback_data=f"help:testv2:reviewcomment:{aid}")])
        rows.append([InlineKeyboardButton("⬅️ К списку",callback_data="help:testv2:review")])
        await q.edit_message_text(f"🧑‍🏫 <b>{escape(a['full_name'])}</b> · {escape(a['title'])}",parse_mode=ParseMode.HTML,reply_markup=InlineKeyboardMarkup(rows)); return

    if data.startswith("help:testv2:reviewcomment:"):
        aid=int(data.rsplit(":",1)[-1]); tv2_set_state(context,"review_comment",assignment_id=aid); await q.edit_message_text("Введите общий комментарий руководителя:",reply_markup=tv2_kb_cancel(f"help:testv2:reviewopen:{aid}")); return

    if data.startswith("help:testv2:answercomment:"):
        parts=data.split(":"); aid=int(parts[-2]); qid=int(parts[-1]); tv2_set_state(context,"answer_comment",assignment_id=aid,question_id=qid); await q.edit_message_text("Введите комментарий к ответу:",reply_markup=tv2_kb_cancel(f"help:testv2:reviewanswer:{aid}:{qid}")); return

    if data=="help:testv2:analytics":
        items=tv2_list_templates(limit=100); rows=[[InlineKeyboardButton(x["title"][:55],callback_data=f"help:testv2:analytic:{int(x['id'])}")] for x in items[:50]]; rows.append([InlineKeyboardButton("📋 Фильтры результатов",callback_data="help:testv2:resultsfilters")]); rows.append([InlineKeyboardButton("⬅️ Назад",callback_data="help:testv2:admin")]); await q.edit_message_text("📊 Выберите тест:",reply_markup=InlineKeyboardMarkup(rows)); return

    if data.startswith("help:testv2:analytic:"):
        tid=int(data.rsplit(":",1)[-1]); t=tv2_get_template(tid); s=tv2_analytics(tid); lines=[f"📊 <b>{escape(t['title'])}</b>","",f"Назначено: <b>{s['assigned']}</b>",f"Начали: <b>{s['started']}</b>",f"Завершили: <b>{s['completed']}</b>",f"Просрочили: <b>{s['expired']}</b>",f"Средний результат: <b>{s['avg']:.0f}%</b>",f"Успешно прошли: <b>{s['passed']}</b>","","<b>Самые сложные вопросы</b>"]
        for i,(text,rate,cnt) in enumerate(s["hard"],1): lines.append(f"{i}. {escape(text[:80])} — {rate*100:.0f}% правильных ({cnt})")
        await q.edit_message_text("\n".join(lines),parse_mode=ParseMode.HTML,reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад",callback_data="help:testv2:analytics")]])); return

    if data=="help:testv2:resultsfilters":
        rows=[[InlineKeyboardButton("❌ Не прошли",callback_data="help:testv2:results:failed")],[InlineKeyboardButton("🧑‍🏫 Ожидают проверки",callback_data="help:testv2:results:review")],[InlineKeyboardButton("⌛ Просрочены",callback_data="help:testv2:results:expired")],[InlineKeyboardButton("✅ Успешные",callback_data="help:testv2:results:passed")],[InlineKeyboardButton("👤 По сотрудникам",callback_data="help:testv2:resultspeople")],[InlineKeyboardButton("📅 За 7 дней",callback_data="help:testv2:resultsperiod:7"),InlineKeyboardButton("📅 За 30 дней",callback_data="help:testv2:resultsperiod:30")],[InlineKeyboardButton("⬅️ Назад",callback_data="help:testv2:analytics")]]; await q.edit_message_text("📋 Фильтр результатов:",reply_markup=InlineKeyboardMarkup(rows)); return

    if data=="help:testv2:resultspeople":
        with tv2_connect() as con:
            people=con.execute("""SELECT p.id,p.full_name,COUNT(a.id) cnt FROM profiles p JOIN test_assignments a ON a.profile_id=p.id GROUP BY p.id ORDER BY p.full_name""").fetchall()
        rows=[[InlineKeyboardButton(f"{r[1]} · {int(r[2])}",callback_data=f"help:testv2:resultsperson:{int(r[0])}")] for r in people[:60]]
        rows.append([InlineKeyboardButton("⬅️ Назад",callback_data="help:testv2:resultsfilters")])
        await q.edit_message_text("👤 Выберите сотрудника:",reply_markup=InlineKeyboardMarkup(rows)); return

    if data.startswith("help:testv2:resultsperson:"):
        pid=int(data.rsplit(":",1)[-1])
        with tv2_connect() as con:
            rows=con.execute("""SELECT a.id,t.title,a.status,a.score_percent FROM test_assignments a JOIN test_templates t ON t.id=a.template_id WHERE a.profile_id=? ORDER BY a.assigned_at DESC LIMIT 50""",(pid,)).fetchall()
            person=con.execute("SELECT full_name FROM profiles WHERE id=?",(pid,)).fetchone()
        kb=[[InlineKeyboardButton(f"{r[1]} · {r[2]}{(' · '+str(round(r[3]))+'%') if r[3] is not None else ''}"[:60],callback_data=f"help:testv2:result:{int(r[0])}")] for r in rows]
        kb.append([InlineKeyboardButton("⬅️ Назад",callback_data="help:testv2:resultspeople")])
        await q.edit_message_text(f"Результаты: {escape(person[0] if person else str(pid))}",parse_mode=ParseMode.HTML,reply_markup=InlineKeyboardMarkup(kb)); return

    if data.startswith("help:testv2:resultsperiod:"):
        days=int(data.rsplit(":",1)[-1]); cutoff=(datetime.utcnow()-timedelta(days=days)).isoformat()
        with tv2_connect() as con:
            rows=con.execute("""SELECT a.id,p.full_name,t.title,a.status,a.score_percent FROM test_assignments a JOIN profiles p ON p.id=a.profile_id JOIN test_templates t ON t.id=a.template_id WHERE COALESCE(a.finished_at,a.assigned_at)>=? ORDER BY COALESCE(a.finished_at,a.assigned_at) DESC LIMIT 60""",(cutoff,)).fetchall()
        kb=[[InlineKeyboardButton(f"{r[1]} · {r[2]}{(' · '+str(round(r[4]))+'%') if r[4] is not None else ''}"[:60],callback_data=f"help:testv2:result:{int(r[0])}")] for r in rows]
        kb.append([InlineKeyboardButton("⬅️ Назад",callback_data="help:testv2:resultsfilters")])
        await q.edit_message_text(f"📅 Результаты за {days} дней: {len(rows)}",reply_markup=InlineKeyboardMarkup(kb)); return

    if data.startswith("help:testv2:results:"):
        filt=data.rsplit(":",1)[-1]; conditions={"failed":"a.status='finished' AND COALESCE(a.passed,0)=0","review":"a.status='needs_review'","expired":"a.status='expired'","passed":"a.status='finished' AND a.passed=1"};
        with tv2_connect() as con: rows=con.execute(f"""SELECT a.id,p.full_name,t.title,a.score_percent FROM test_assignments a JOIN profiles p ON p.id=a.profile_id JOIN test_templates t ON t.id=a.template_id WHERE {conditions[filt]} ORDER BY COALESCE(a.finished_at,a.assigned_at) DESC LIMIT 50""").fetchall()
        kb=[[InlineKeyboardButton(f"{r[1]} · {r[2]}{(' · '+str(round(r[3]))+'%') if r[3] is not None else ''}"[:60],callback_data=f"help:testv2:result:{int(r[0])}")] for r in rows]; kb.append([InlineKeyboardButton("⬅️ Назад",callback_data="help:testv2:resultsfilters")]); await q.edit_message_text(f"Найдено: {len(rows)}",reply_markup=InlineKeyboardMarkup(kb)); return

    if data=="help:testv2:overdue":
        with tv2_connect() as con: rows=con.execute("""SELECT a.id,p.full_name,t.title,a.due_at FROM test_assignments a JOIN profiles p ON p.id=a.profile_id JOIN test_templates t ON t.id=a.template_id WHERE a.status='expired' ORDER BY a.due_at DESC LIMIT 50""").fetchall()
        kb=[[InlineKeyboardButton(f"⌛ {r[1]} · {r[2]}"[:60],callback_data=f"help:testv2:result:{int(r[0])}")] for r in rows]; kb.append([InlineKeyboardButton("⬅️ Назад",callback_data="help:testv2:admin")]); await q.edit_message_text(f"⌛ Просроченные тесты: {len(rows)}",reply_markup=InlineKeyboardMarkup(kb)); return


async def cb_test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data=(update.callback_query.data or "") if update.callback_query else ""
    if not data.startswith("test:v2:"):
        return await _tv2_legacy_cb_test(update,context)
    q=update.callback_query
    try: await q.answer()
    except Exception: pass
    parts=data.split(":"); action=parts[2]; aid=int(parts[3]) if len(parts)>3 else 0
    a=tv2_get_assignment(aid); p=get_profile_for_user(update)
    if not a or not p or int(a["profile_id"])!=int(p["id"]): await q.answer("Тест назначен другому сотруднику",show_alert=True); return

    if action in ("start","continue"):
        if a.get("status") not in ("assigned", "in_progress", "saved"):
            await q.answer(
                "Этот тест уже завершён или недоступен. Новая попытка возможна "
                "только после повторного назначения администратором.",
                show_alert=True,
            )
            return
        if tv2_is_expired(a): tv2_mark_expired(aid); await q.edit_message_text("⌛ Срок теста истёк."); return
        if not tv2_start_assignment(aid):
            await q.answer("Этот тест уже завершён или недоступен.", show_alert=True)
            return
        tv2_clear(context); await tv2_send_question(update,context,aid); return

    if action=="retry":
        await q.answer(
            "Повторное прохождение недоступно. Новая попытка появится только "
            "после повторного назначения администратором.",
            show_alert=True,
        )
        return

    if tv2_is_expired(a): tv2_mark_expired(aid); await q.edit_message_text("⌛ Время теста истекло."); return
    order=tv2_assignment_order(a)

    if action=="single":
        qid=int(parts[4]); opt=int(parts[5]); qq=tv2_question_by_id(qid); correct=set(int(x) for x in qq.get("correct") or []); ok=1 if {opt}==correct else 0; pts=float(qq.get("points") or 1) if ok else 0
        tv2_save_answer(aid,qid,{"selected":[opt]},ok,pts,"auto")
        if int(a.get("immediate_feedback") or 0):
            text=("✅ Верно" if ok else "❌ Неверно")+(f"\n\n💡 {qq.get('explanation')}" if qq.get("explanation") else "")
            await q.edit_message_text(text,reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Далее ▶️",callback_data=f"test:v2:next:{aid}")]])); return
        return await cb_test_goto_next(update,context,aid)

    if action=="toggle":
        qid=int(parts[4]); opt=int(parts[5]); selmap=context.user_data.get(TV2_MULTI) or {}; cur=set(selmap.get(str(qid),[])); cur.discard(opt) if opt in cur else cur.add(opt); selmap[str(qid)]=sorted(cur); context.user_data[TV2_MULTI]=selmap; qq=tv2_question_by_id(qid); a=tv2_get_assignment(aid); pos=tv2_assignment_order(a).index(qid); text,kb=tv2_question_display(a,qq,pos)
        rows=[]
        for row in kb.inline_keyboard:
            new=[]
            for b in row:
                if b.callback_data and b.callback_data.startswith(f"test:v2:toggle:{aid}:{qid}:"):
                    oi=int(b.callback_data.rsplit(":",1)[-1]); label=("✅ " if oi in cur else "▫️ ")+qq["options"][oi][:55]; new.append(InlineKeyboardButton(label,callback_data=b.callback_data))
                else:new.append(b)
            rows.append(new)
        await q.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(rows)); return

    if action=="multisubmit":
        qid=int(parts[4]); qq=tv2_question_by_id(qid); selected=set((context.user_data.get(TV2_MULTI) or {}).get(str(qid),[])); correct=set(int(x) for x in qq.get("correct") or []); ok=1 if selected==correct else 0; pts=float(qq.get("points") or 1) if ok else 0; tv2_save_answer(aid,qid,{"selected":sorted(selected)},ok,pts,"auto")
        if int(a.get("immediate_feedback") or 0):
            text=("✅ Верно" if ok else "❌ Неверно")+(f"\n\n💡 {qq.get('explanation')}" if qq.get("explanation") else ""); await q.edit_message_text(text,reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Далее ▶️",callback_data=f"test:v2:next:{aid}")]])); return
        return await cb_test_goto_next(update,context,aid)

    if action=="next": return await cb_test_goto_next(update,context,aid)
    if action=="goto": return await tv2_send_question(update,context,aid,int(parts[4]))
    if action=="flag":
        qid=int(parts[4]); tv2_toggle_flag(aid,qid); pos=tv2_assignment_order(tv2_get_assignment(aid)).index(qid); return await tv2_send_question(update,context,aid,pos)
    if action=="reviewpage":
        text,kb=tv2_review_page_text(aid); await q.edit_message_text(text,parse_mode=ParseMode.HTML,reply_markup=kb); return
    if action=="finishconfirm":
        unanswered=sum(1 for qid in order if not tv2_answer(aid,qid)); await q.edit_message_text(f"Завершить тест?\n\nБез ответа: {unanswered}\nПосле завершения изменить ответы будет нельзя.",reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✅ Да, завершить",callback_data=f"test:v2:finish:{aid}")],[InlineKeyboardButton("◀️ Вернуться",callback_data=f"test:v2:reviewpage:{aid}")]])); return
    if action=="finish":
        calc=tv2_calculate(aid,finalize=True); tv2_clear(context); await q.edit_message_text(tv2_result_text(aid),parse_mode=ParseMode.HTML,reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📊 Подробнее",callback_data=f"help:testv2:result:{aid}")],[InlineKeyboardButton("⬅️ Мои тесты",callback_data="help:testv2:my:all:0")]])); return


async def cb_test_goto_next(update,context,aid:int):
    a=tv2_get_assignment(aid); order=tv2_assignment_order(a); pos=int(a.get("current_idx") or 0)+1
    if pos>=len(order):
        text,kb=tv2_review_page_text(aid); await update.callback_query.edit_message_text(text,parse_mode=ParseMode.HTML,reply_markup=kb); return
    await tv2_send_question(update,context,aid,pos)


async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state=context.user_data.get(TV2_STATE)
    if not state:
        return await _tv2_legacy_on_text(update,context)
    if await deny_no_access(update,context): return
    await sync_profile_user_id_from_update(update)
    text=(update.message.text or "").strip(); d=context.user_data.get(TV2_DATA) or {}

    if state=="profile_department":
        pid=int(d["profile_id"]); page=int(d.get("page") or 0); value=None if text=="-" else text[:120]
        with tv2_connect() as con: con.execute("UPDATE profiles SET department=? WHERE id=?",(value,pid))
        tv2_clear(context); await update.message.reply_text("✅ Отдел сохранён.",reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ К сотрудникам",callback_data=f"help:testv2:departments:{page}")]])); return

    if state=="create_title":
        if len(text)<3: await update.message.reply_text("Название слишком короткое."); return
        context.user_data[TV2_DATA]={"title":text}; context.user_data[TV2_STATE]="create_mode"
        await update.message.reply_text("Выберите режим:",reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📚 Обучение",callback_data="help:testv2:createmode:learning")],[InlineKeyboardButton("🎓 Аттестация",callback_data="help:testv2:createmode:exam")],[InlineKeyboardButton("⚙️ Свой",callback_data="help:testv2:createmode:custom")]])); return

    if state=="q_text":
        d["question_text"]=text; context.user_data[TV2_DATA]=d
        if d.get("q_type")=="open": context.user_data[TV2_STATE]="q_points"; await update.message.reply_text("Сколько баллов даёт вопрос?"); return
        context.user_data[TV2_STATE]="q_options"; await update.message.reply_text("Введите варианты ответа, каждый с новой строки (минимум 2):"); return

    if state=="q_options":
        opts=[x.strip() for x in text.splitlines() if x.strip()]
        if len(opts)<2: await update.message.reply_text("Нужно минимум два варианта."); return
        d["options"]=opts; context.user_data[TV2_DATA]=d; context.user_data[TV2_STATE]="q_correct"; await update.message.reply_text("Введите номера правильных вариантов через запятую, например 1 или 1,3:"); return

    if state=="q_correct":
        try: indexes=sorted(set(int(x.strip())-1 for x in re.split(r"[,; ]+",text) if x.strip()))
        except Exception: await update.message.reply_text("Не удалось разобрать номера."); return
        if not indexes or any(i<0 or i>=len(d.get("options") or []) for i in indexes): await update.message.reply_text("Проверьте номера вариантов."); return
        if d.get("q_type")=="single" and len(indexes)!=1: await update.message.reply_text("Для одиночного выбора нужен один номер."); return
        d["correct"]=indexes; context.user_data[TV2_DATA]=d; context.user_data[TV2_STATE]="q_points"; await update.message.reply_text("Сколько баллов даёт вопрос?"); return

    if state=="q_points":
        try: pts=float(text.replace(",",".")); assert pts>0
        except Exception: await update.message.reply_text("Введите положительное число."); return
        d["points"]=pts; context.user_data[TV2_DATA]=d; context.user_data[TV2_STATE]="q_explanation"; await update.message.reply_text("Введите пояснение к правильному ответу или '-' если оно не нужно:"); return

    if state=="q_explanation":
        d["explanation"]="" if text=="-" else text
        if d.get("target")=="bank": context.user_data[TV2_DATA]=d; context.user_data[TV2_STATE]="q_bank_meta"; await update.message.reply_text("Введите: категория | сложность 1-5 | теги через запятую\nНапример: CRM | 2 | лиды, синхронизация"); return
        tv2_add_question(int(d["template_id"]),d["q_type"],d["question_text"],d.get("options",[]),d.get("correct",[]),d["points"],d["explanation"]); tid=int(d["template_id"]); tv2_clear(context); await update.message.reply_text("✅ Вопрос добавлен.",reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Открыть тест",callback_data=f"help:testv2:template:{tid}")]])); return

    if state=="q_bank_meta":
        parts=[x.strip() for x in text.split("|")]
        category=parts[0] if parts else "Без категории"
        try: difficulty=int(parts[1]) if len(parts)>1 else 1
        except Exception: difficulty=1
        tags=parts[2] if len(parts)>2 else ""
        bid=tv2_bank_add(d["q_type"],d["question_text"],d.get("options",[]),d.get("correct",[]),d["points"],d.get("explanation", ""),category,difficulty,tags,update.effective_user.id); tv2_clear(context); await update.message.reply_text(f"✅ Вопрос добавлен в банк (ID {bid}).",reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Банк вопросов",callback_data="help:testv2:bank")]])); return

    if state.startswith("edit_q_"):
        qid=int(d["question_id"]); qq=tv2_question_by_id(qid)
        if state=="edit_q_text": tv2_update_question(qid,"question_text",text)
        elif state=="edit_q_points":
            try: tv2_update_question(qid,"points",float(text.replace(",",".")))
            except Exception: await update.message.reply_text("Введите число."); return
        elif state=="edit_q_options":
            opts=[x.strip() for x in text.splitlines() if x.strip()]
            if len(opts)<2: await update.message.reply_text("Минимум два варианта."); return
            tv2_update_question(qid,"options_json",_safe_json_dumps(opts))
        elif state=="edit_q_correct":
            try: idx=[int(x)-1 for x in re.split(r"[,; ]+",text) if x]
            except Exception: await update.message.reply_text("Проверьте номера."); return
            tv2_update_question(qid,"correct_json",_safe_json_dumps(idx))
        elif state=="edit_q_explanation": tv2_update_question(qid,"explanation",None if text=="-" else text)
        tv2_clear(context); await update.message.reply_text("✅ Изменение сохранено.",reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Открыть вопрос",callback_data=f"help:testv2:qedit:{qid}")]])); return

    if state.startswith("custom_"):
        setting=state[len("custom_"):]; tid=int(d["template_id"])
        if setting == "attempts":
            tv2_clear(context)
            await update.message.reply_text(
                "✅ Для всех тестов доступна только одна попытка.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("Настройки", callback_data=f"help:testv2:settings:{tid}")]
                ]),
            )
            return
        try: value=int(text); assert value>=0
        except Exception: await update.message.reply_text("Введите целое неотрицательное число."); return
        col={"passing":"passing_score","time":"default_time_limit_sec"}[setting]
        if setting=="time": value*=60
        with tv2_connect() as con: con.execute(f"UPDATE test_templates SET {col}=? WHERE id=?",(value,tid))
        tv2_clear(context); await update.message.reply_text("✅ Настройка сохранена.",reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Настройки",callback_data=f"help:testv2:settings:{tid}")]])); return

    if state in ("assign_due", "assign_due_buttons"):
        await update.message.reply_text(
            "Выберите срок кнопкой под сообщением.",
            reply_markup=tv3_assignment_due_main_keyboard(),
        )
        return

    if state=="assign_time":
        t=tv2_get_template(int(d["template_id"])) or {}
        if text.lower() in ("по умолчанию","default","-"): duration=t.get("default_time_limit_sec")
        elif text.lower() in ("нет","0","без ограничения"): duration=None
        else:
            try: duration=int(text)*60; assert duration>0
            except Exception: await update.message.reply_text("Введите число минут, «нет» или «по умолчанию»."); return
        d["time_limit_sec"]=duration; context.user_data[TV2_DATA]=d; context.user_data[TV2_STATE]="assign_ready"; await update.message.reply_text("Настройки назначения готовы.",reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📋 Проверить",callback_data="help:testv2:assignconfirm")],[InlineKeyboardButton("❌ Отмена",callback_data="help:testv2:admin")]])); return

    if state=="open_answer":
        aid=int(d["assignment_id"]); qid=int(d["question_id"]); qq=tv2_question_by_id(qid); tv2_save_answer(aid,qid,{"text":text},None,None,"pending"); tv2_clear(context); a=tv2_get_assignment(aid); pos=int(d.get("position") or 0)+1; order=tv2_assignment_order(a)
        if pos>=len(order):
            review,kb=tv2_review_page_text(aid); await update.message.reply_text(review,parse_mode=ParseMode.HTML,reply_markup=kb)
        else:
            # synthetic update without callback: sends next question as a new message
            await tv2_send_question(update,context,aid,pos)
        return

    if state=="review_comment":
        aid=int(d["assignment_id"])
        with tv2_connect() as con: con.execute("UPDATE test_assignments SET reviewer_comment=? WHERE id=?",(text,aid)); con.execute("INSERT INTO test_admin_comments(assignment_id,admin_user_id,comment,created_at) VALUES(?,?,?,?)",(aid,update.effective_user.id,text,datetime.utcnow().isoformat()))
        tv2_clear(context); await update.message.reply_text("✅ Общий комментарий сохранён.",reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Вернуться к проверке",callback_data=f"help:testv2:reviewopen:{aid}")]])); return

    if state=="answer_comment":
        aid=int(d["assignment_id"]); qid=int(d["question_id"])
        with tv2_connect() as con: con.execute("UPDATE test_answers SET reviewer_comment=? WHERE assignment_id=? AND question_id=?",(text,aid,qid))
        tv2_clear(context); await update.message.reply_text("✅ Комментарий сохранён.",reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Вернуться",callback_data=f"help:testv2:reviewanswer:{aid}:{qid}")]])); return

    return await _tv2_legacy_on_text(update,context)

# =================== END TESTING V2 ===================

# ===================== EMPLOYEE REMINDERS V1 =====================
# Личные напоминания сотрудников: создание, редактирование, удаление,
# отображение на рабочей панели и надёжная отправка в ЛС.

REMINDER_BUILD = "EMPLOYEE-REMINDERS-2026-07-21-V1"
REMINDER_MAX_ACTIVE = 5
REMINDER_TEXT_MAX_LENGTH = 160
REMINDER_STATE = "employee_reminder_state"
REMINDER_DATA = "employee_reminder_data"
REMINDER_TIMEZONE_LABELS = {
    0: "МСК",
    1: "МСК+1",
    2: "МСК+2",
}

# Сохраняем актуальные реализации, включая переопределения TESTING V2.
_reminder_legacy_db_init = db_init
_reminder_legacy_help_text_main = help_text_main
_reminder_legacy_kb_help_main = kb_help_main
_reminder_legacy_cb_help = cb_help
_reminder_legacy_on_text = on_text
_reminder_legacy_check_and_send_jobs = check_and_send_jobs


def _reminder_utc_now() -> datetime:
    return datetime.now(pytz.UTC)


def _reminder_tz(timezone_delta: int):
    delta = int(timezone_delta)
    if delta not in REMINDER_TIMEZONE_LABELS:
        raise ValueError("Недопустимый часовой пояс")
    # Москва — UTC+3. Для регионов поддерживаем UTC+4 и UTC+5.
    return pytz.FixedOffset((3 + delta) * 60)


def _reminder_parse_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value))
    except Exception:
        return None
    if dt.tzinfo is None:
        return pytz.UTC.localize(dt)
    return dt.astimezone(pytz.UTC)


def _reminder_local_to_utc(
    reminder_date: date,
    time_text: str,
    timezone_delta: int,
) -> datetime:
    match = re.fullmatch(r"\s*(\d{1,2})[:.](\d{2})\s*", time_text or "")
    if not match:
        raise ValueError("Время нужно указать в формате ЧЧ:ММ, например 18:30")
    hour = int(match.group(1))
    minute = int(match.group(2))
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError("Укажите корректное время от 00:00 до 23:59")

    tz = _reminder_tz(timezone_delta)
    local_dt = datetime(
        reminder_date.year,
        reminder_date.month,
        reminder_date.day,
        hour,
        minute,
        tzinfo=tz,
    )
    return local_dt.astimezone(pytz.UTC)


def _reminder_parse_date_text(value: str, timezone_delta: int) -> date:
    clean = (value or "").strip()
    tz = _reminder_tz(timezone_delta)
    today_local = _reminder_utc_now().astimezone(tz).date()

    for fmt in ("%d.%m.%Y", "%d.%m.%y"):
        try:
            return datetime.strptime(clean, fmt).date()
        except ValueError:
            pass

    try:
        parsed = datetime.strptime(clean, "%d.%m").date().replace(year=today_local.year)
        if parsed < today_local:
            parsed = parsed.replace(year=today_local.year + 1)
        return parsed
    except ValueError:
        raise ValueError("Введите дату в формате ДД.ММ или ДД.ММ.ГГГГ")


def _reminder_format_when(item: dict, include_timezone: bool = True) -> str:
    utc_dt = _reminder_parse_utc(item.get("remind_at_utc"))
    if not utc_dt:
        return "дата не определена"
    delta = int(item.get("timezone_delta") or 0)
    tz = _reminder_tz(delta)
    local_dt = utc_dt.astimezone(tz)
    today = _reminder_utc_now().astimezone(tz).date()
    if local_dt.date() == today:
        day_text = "Сегодня"
    elif local_dt.date() == today + timedelta(days=1):
        day_text = "Завтра"
    else:
        day_text = local_dt.strftime("%d.%m.%Y")
    result = f"{day_text}, {local_dt:%H:%M}"
    if include_timezone:
        result += f" · {REMINDER_TIMEZONE_LABELS.get(delta, 'МСК')}"
    return result


def _reminder_short_text(value: str, limit: int = 42) -> str:
    clean = re.sub(r"\s+", " ", (value or "").strip())
    return clean if len(clean) <= limit else clean[: max(1, limit - 1)].rstrip() + "…"


def db_init():
    _reminder_legacy_db_init()
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS employee_reminders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            reminder_text TEXT NOT NULL,
            remind_at_utc TEXT NOT NULL,
            timezone_delta INTEGER NOT NULL DEFAULT 0
                CHECK(timezone_delta IN (0, 1, 2)),
            status TEXT NOT NULL DEFAULT 'pending'
                CHECK(status IN ('pending', 'sending', 'sent', 'canceled', 'failed')),
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            sent_at TEXT,
            attempt_count INTEGER NOT NULL DEFAULT 0,
            last_error TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_employee_reminders_due
        ON employee_reminders(status, remind_at_utc)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_employee_reminders_user
        ON employee_reminders(user_id, status, remind_at_utc)
        """
    )
    # После перезапуска безопасно возвращаем незавершённые отправки в очередь.
    cur.execute(
        """
        UPDATE employee_reminders
        SET status='pending', updated_at=?
        WHERE status='sending'
        """,
        (_reminder_utc_now().isoformat(),),
    )
    con.commit()
    con.close()


def db_reminders_active(user_id: int, limit: int = REMINDER_MAX_ACTIVE) -> list[dict]:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        SELECT id, user_id, reminder_text, remind_at_utc, timezone_delta,
               status, created_at, updated_at
        FROM employee_reminders
        WHERE user_id=? AND status IN ('pending', 'sending')
        ORDER BY remind_at_utc ASC, id ASC
        LIMIT ?
        """,
        (int(user_id), int(limit)),
    )
    rows = cur.fetchall()
    con.close()
    return [
        {
            "id": int(r[0]),
            "user_id": int(r[1]),
            "reminder_text": r[2],
            "remind_at_utc": r[3],
            "timezone_delta": int(r[4] or 0),
            "status": r[5],
            "created_at": r[6],
            "updated_at": r[7],
        }
        for r in rows
    ]


def db_reminders_active_count(user_id: int) -> int:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        SELECT COUNT(*)
        FROM employee_reminders
        WHERE user_id=? AND status IN ('pending', 'sending')
        """,
        (int(user_id),),
    )
    count = int((cur.fetchone() or [0])[0] or 0)
    con.close()
    return count


def db_reminder_get(reminder_id: int, user_id: int) -> dict | None:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        SELECT id, user_id, reminder_text, remind_at_utc, timezone_delta,
               status, created_at, updated_at, sent_at, last_error
        FROM employee_reminders
        WHERE id=? AND user_id=?
        """,
        (int(reminder_id), int(user_id)),
    )
    row = cur.fetchone()
    con.close()
    if not row:
        return None
    return {
        "id": int(row[0]),
        "user_id": int(row[1]),
        "reminder_text": row[2],
        "remind_at_utc": row[3],
        "timezone_delta": int(row[4] or 0),
        "status": row[5],
        "created_at": row[6],
        "updated_at": row[7],
        "sent_at": row[8],
        "last_error": row[9],
    }


def db_reminder_create(
    user_id: int,
    reminder_text: str,
    remind_at_utc: datetime,
    timezone_delta: int,
) -> int:
    clean = re.sub(r"\s+", " ", (reminder_text or "").strip())
    if not clean:
        raise ValueError("Описание не может быть пустым")
    if len(clean) > REMINDER_TEXT_MAX_LENGTH:
        raise ValueError(f"Описание должно быть не длиннее {REMINDER_TEXT_MAX_LENGTH} символов")
    if int(timezone_delta) not in REMINDER_TIMEZONE_LABELS:
        raise ValueError("Недопустимый часовой пояс")
    when_utc = remind_at_utc.astimezone(pytz.UTC)
    if when_utc <= _reminder_utc_now() + timedelta(seconds=30):
        raise ValueError("Время напоминания должно быть в будущем")

    con = sqlite3.connect(DB_PATH)
    try:
        con.execute("BEGIN IMMEDIATE")
        cur = con.cursor()
        cur.execute(
            """
            SELECT COUNT(*)
            FROM employee_reminders
            WHERE user_id=? AND status IN ('pending', 'sending')
            """,
            (int(user_id),),
        )
        active_count = int((cur.fetchone() or [0])[0] or 0)
        if active_count >= REMINDER_MAX_ACTIVE:
            raise ValueError(
                "У тебя уже 5 активных напоминаний. Удали одно из них "
                "или дождись его отправки."
            )
        now_iso = _reminder_utc_now().isoformat()
        cur.execute(
            """
            INSERT INTO employee_reminders(
                user_id, reminder_text, remind_at_utc, timezone_delta,
                status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, 'pending', ?, ?)
            """,
            (
                int(user_id),
                clean,
                when_utc.isoformat(),
                int(timezone_delta),
                now_iso,
                now_iso,
            ),
        )
        reminder_id = int(cur.lastrowid)
        con.commit()
        return reminder_id
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def db_reminder_update_text(reminder_id: int, user_id: int, new_text: str) -> bool:
    clean = re.sub(r"\s+", " ", (new_text or "").strip())
    if not clean:
        raise ValueError("Описание не может быть пустым")
    if len(clean) > REMINDER_TEXT_MAX_LENGTH:
        raise ValueError(f"Описание должно быть не длиннее {REMINDER_TEXT_MAX_LENGTH} символов")
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        UPDATE employee_reminders
        SET reminder_text=?, updated_at=?, last_error=NULL
        WHERE id=? AND user_id=? AND status='pending'
        """,
        (clean, _reminder_utc_now().isoformat(), int(reminder_id), int(user_id)),
    )
    changed = cur.rowcount > 0
    con.commit()
    con.close()
    return changed


def db_reminder_update_schedule(
    reminder_id: int,
    user_id: int,
    remind_at_utc: datetime,
    timezone_delta: int,
) -> bool:
    if int(timezone_delta) not in REMINDER_TIMEZONE_LABELS:
        raise ValueError("Недопустимый часовой пояс")
    when_utc = remind_at_utc.astimezone(pytz.UTC)
    if when_utc <= _reminder_utc_now() + timedelta(seconds=30):
        raise ValueError("Время напоминания должно быть в будущем")
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        UPDATE employee_reminders
        SET remind_at_utc=?, timezone_delta=?, updated_at=?, last_error=NULL
        WHERE id=? AND user_id=? AND status='pending'
        """,
        (
            when_utc.isoformat(),
            int(timezone_delta),
            _reminder_utc_now().isoformat(),
            int(reminder_id),
            int(user_id),
        ),
    )
    changed = cur.rowcount > 0
    con.commit()
    con.close()
    return changed


def db_reminder_cancel(reminder_id: int, user_id: int) -> bool:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        UPDATE employee_reminders
        SET status='canceled', updated_at=?
        WHERE id=? AND user_id=? AND status='pending'
        """,
        (_reminder_utc_now().isoformat(), int(reminder_id), int(user_id)),
    )
    changed = cur.rowcount > 0
    con.commit()
    con.close()
    return changed


def db_reminders_due(limit: int = 50) -> list[dict]:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        SELECT id, user_id, reminder_text, remind_at_utc, timezone_delta
        FROM employee_reminders
        WHERE status='pending' AND remind_at_utc<=?
        ORDER BY remind_at_utc ASC, id ASC
        LIMIT ?
        """,
        (_reminder_utc_now().isoformat(), int(limit)),
    )
    rows = cur.fetchall()
    con.close()
    return [
        {
            "id": int(r[0]),
            "user_id": int(r[1]),
            "reminder_text": r[2],
            "remind_at_utc": r[3],
            "timezone_delta": int(r[4] or 0),
        }
        for r in rows
    ]


def db_reminder_reserve(reminder_id: int) -> bool:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        UPDATE employee_reminders
        SET status='sending', updated_at=?, attempt_count=attempt_count+1
        WHERE id=? AND status='pending'
        """,
        (_reminder_utc_now().isoformat(), int(reminder_id)),
    )
    reserved = cur.rowcount > 0
    con.commit()
    con.close()
    return reserved


def db_reminder_mark_sent(reminder_id: int):
    now_iso = _reminder_utc_now().isoformat()
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        UPDATE employee_reminders
        SET status='sent', sent_at=?, updated_at=?, last_error=NULL
        WHERE id=? AND status='sending'
        """,
        (now_iso, now_iso, int(reminder_id)),
    )
    con.commit()
    con.close()


def db_reminder_return_pending(reminder_id: int, error: str | None = None):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        UPDATE employee_reminders
        SET status='pending', updated_at=?, last_error=?
        WHERE id=? AND status='sending'
        """,
        (_reminder_utc_now().isoformat(), (error or "")[:1000] or None, int(reminder_id)),
    )
    con.commit()
    con.close()


def db_reminder_mark_failed(reminder_id: int, error: str | None = None):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        UPDATE employee_reminders
        SET status='failed', updated_at=?, last_error=?
        WHERE id=? AND status='sending'
        """,
        (_reminder_utc_now().isoformat(), (error or "")[:1000] or None, int(reminder_id)),
    )
    con.commit()
    con.close()


def clear_reminder_flow(context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop(REMINDER_STATE, None)
    context.user_data.pop(REMINDER_DATA, None)


def _reminder_set_flow(context: ContextTypes.DEFAULT_TYPE, state: str, **data):
    context.user_data[REMINDER_STATE] = state
    context.user_data[REMINDER_DATA] = dict(data)


def _reminder_intro_text(user_id: int) -> str:
    count = db_reminders_active_count(user_id)
    return (
        "⏰ <b>Напоминалка</b>\n\n"
        "Здесь ты можешь создать личное уведомление. Придумай краткое описание, "
        "выбери дату, время и свой часовой пояс — в нужный момент я вернусь "
        "к тебе в личные сообщения.\n\n"
        "Доступные часовые пояса: <b>МСК, МСК+1 и МСК+2</b>.\n"
        f"Активных напоминаний: <b>{count} из {REMINDER_MAX_ACTIVE}</b>"
    )


def kb_reminders_list(user_id: int) -> InlineKeyboardMarkup:
    items = db_reminders_active(user_id, limit=REMINDER_MAX_ACTIVE)
    rows = []
    if len(items) < REMINDER_MAX_ACTIVE:
        rows.append([InlineKeyboardButton("➕ Создать напоминание", callback_data="help:reminder:new")])
    for item in items:
        when = _reminder_format_when(item, include_timezone=False)
        label = f"⏰ {when} · {_reminder_short_text(item['reminder_text'], 28)}"
        rows.append([
            InlineKeyboardButton(label[:64], callback_data=f"help:reminder:open:{item['id']}")
        ])
    rows.append([InlineKeyboardButton("🏠 Главное меню", callback_data="help:main")])
    return InlineKeyboardMarkup(rows)


def kb_reminder_timezone(prefix: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("МСК", callback_data=f"{prefix}:0"),
            InlineKeyboardButton("МСК+1", callback_data=f"{prefix}:1"),
            InlineKeyboardButton("МСК+2", callback_data=f"{prefix}:2"),
        ],
        [InlineKeyboardButton("❌ Отмена", callback_data="help:reminder:cancel")],
    ])


def kb_reminder_date(timezone_delta: int) -> InlineKeyboardMarkup:
    today = _reminder_utc_now().astimezone(_reminder_tz(timezone_delta)).date()
    tomorrow = today + timedelta(days=1)
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Сегодня", callback_data=f"help:reminder:date:{today.isoformat()}"),
            InlineKeyboardButton("Завтра", callback_data=f"help:reminder:date:{tomorrow.isoformat()}"),
        ],
        [InlineKeyboardButton("📅 Ввести дату", callback_data="help:reminder:date:custom")],
        [InlineKeyboardButton("❌ Отмена", callback_data="help:reminder:cancel")],
    ])


def _reminder_item_text(item: dict) -> str:
    status_labels = {
        "pending": "активно",
        "sending": "отправляется",
        "sent": "отправлено",
        "canceled": "удалено",
        "failed": "ошибка отправки",
    }
    return (
        "⏰ <b>Напоминание</b>\n\n"
        f"📝 {escape(item['reminder_text'])}\n\n"
        f"🕒 <b>{escape(_reminder_format_when(item))}</b>\n"
        f"Статус: <b>{escape(status_labels.get(item.get('status'), item.get('status') or '—'))}</b>"
    )


def kb_reminder_item(item: dict) -> InlineKeyboardMarkup:
    rid = int(item["id"])
    rows = []
    if item.get("status") == "pending":
        rows.extend([
            [InlineKeyboardButton("✏️ Изменить описание", callback_data=f"help:reminder:edittext:{rid}")],
            [InlineKeyboardButton("📅 Изменить дату и время", callback_data=f"help:reminder:editwhen:{rid}")],
            [InlineKeyboardButton("🌍 Изменить часовой пояс", callback_data=f"help:reminder:edittz:{rid}")],
            [InlineKeyboardButton("🗑 Удалить", callback_data=f"help:reminder:delete:{rid}")],
        ])
    rows.append([InlineKeyboardButton("⬅️ К напоминаниям", callback_data="help:reminder:list")])
    return InlineKeyboardMarkup(rows)


def _reminder_confirm_text(data: dict) -> str:
    item = {
        "remind_at_utc": data.get("remind_at_utc"),
        "timezone_delta": int(data.get("timezone_delta") or 0),
    }
    return (
        "✅ <b>Проверь напоминание</b>\n\n"
        f"📝 {escape(data.get('reminder_text') or '')}\n"
        f"🕒 <b>{escape(_reminder_format_when(item))}</b>\n\n"
        "Сохранить?"
    )


def help_text_main(
    bot_username: str,
    profile: dict | None = None,
    unread_count: int = 0,
    is_admin_user: bool = False,
    user_full_name: str | None = None,
) -> str:
    if not profile:
        return _reminder_legacy_help_text_main(
            bot_username,
            profile=profile,
            unread_count=unread_count,
            is_admin_user=is_admin_user,
            user_full_name=user_full_name,
        )

    profile_full_name = (profile.get("full_name") or "Коллега").strip()
    display_name = (user_full_name or "").strip() or profile_full_name
    tests = db_profile_test_summary(int(profile["id"]))
    achievements_count = db_achievements_count(int(profile["id"]))
    attention: list[str] = []

    if tests.get("assigned"):
        attention.append(f"📝 новых тестов: <b>{tests['assigned']}</b>")
    if tests.get("in_progress"):
        attention.append(f"⏳ тестов в процессе: <b>{tests['in_progress']}</b>")
    if unread_count:
        attention.append(f"🔔 непрочитанных уведомлений: <b>{unread_count}</b>")

    user_id = profile.get("tg_user_id")
    reminders = db_reminders_active(int(user_id), REMINDER_MAX_ACTIVE) if user_id else []
    if reminders:
        nearest = reminders[0]
        attention.append(
            "⏰ ближайшее: "
            f"<b>{escape(_reminder_format_when(nearest, include_timezone=False))}</b> — "
            f"{escape(_reminder_short_text(nearest['reminder_text'], 58))}"
        )
        if len(reminders) > 1:
            attention.append(f"⏰ ещё активных напоминаний: <b>{len(reminders) - 1}</b>")

    if attention:
        # Эмодзи уже выполняют роль визуальных маркеров — точки не нужны.
        attention_block = "\n".join(attention)
    else:
        attention_block = "✅ срочных задач сейчас нет"

    admin_line = ""
    if is_admin_user:
        pending = len(db_nominations_pending(100))
        if pending:
            admin_line = f"\n⚙️ Ожидают решения номинации: <b>{pending}</b>\n"

    return (
        f"👋 <b>Привет, {escape(display_name)}!</b>\n\n"
        "Это твоя рабочая панель. Здесь видно, что требует внимания, "
        "и доступны основные разделы команды.\n\n"
        f"📌 <b>Сейчас:</b>\n{attention_block}\n"
        f"🏆 Всего достижений: <b>{achievements_count}</b>"
        f"{admin_line}\n\n"
        "Выберите нужный раздел 👇"
    )


def kb_help_main(is_admin_user: bool, unread_count: int = 0):
    legacy_markup = _reminder_legacy_kb_help_main(
        is_admin_user=is_admin_user,
        unread_count=unread_count,
    )
    legacy_rows = [list(row) for row in legacy_markup.inline_keyboard]
    cases_row = [
        InlineKeyboardButton(
            "📚 Кейсы",
            callback_data="help:cases",
        )
    ]
    reminder_row = [
        InlineKeyboardButton(
            "⏰ Напоминалка",
            callback_data="help:reminder:list",
        )
    ]

    # Ставим длинные кнопки «Кейсы» и «Напоминалка» непосредственно перед
    # администраторской кнопкой «Управление ботом». Для обычного сотрудника,
    # у которого такой кнопки нет, они будут последними строками меню.
    settings_row_index = next(
        (
            index
            for index, row in enumerate(legacy_rows)
            if any(
                getattr(button, "callback_data", None) == "help:settings"
                for button in row
            )
        ),
        len(legacy_rows),
    )
    rows = (
        legacy_rows[:settings_row_index]
        + [cases_row, reminder_row]
        + legacy_rows[settings_row_index:]
    )
    return InlineKeyboardMarkup(rows)


async def _render_reminders_list(query, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    clear_reminder_flow(context)
    await replace_callback_message_with_text(
        query,
        context,
        _reminder_intro_text(user_id),
        parse_mode=ParseMode.HTML,
        reply_markup=kb_reminders_list(user_id),
    )


async def _render_reminder_item(query, context: ContextTypes.DEFAULT_TYPE, item: dict):
    clear_reminder_flow(context)
    await replace_callback_message_with_text(
        query,
        context,
        _reminder_item_text(item),
        parse_mode=ParseMode.HTML,
        reply_markup=kb_reminder_item(item),
    )


async def cb_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = (update.callback_query.data or "") if update.callback_query else ""
    if not data.startswith("help:reminder"):
        # Выход в любой другой раздел отменяет незавершённый мастер напоминания.
        if data.startswith("help:") and context.user_data.get(REMINDER_STATE):
            clear_reminder_flow(context)
        return await _reminder_legacy_cb_help(update, context)

    if await deny_no_access(update, context):
        return
    await sync_profile_user_id_from_update(update)

    q = update.callback_query
    try:
        await q.answer()
    except (TimedOut, NetworkError):
        pass

    user = update.effective_user
    if not user:
        return
    user_id = int(user.id)

    if not update.effective_chat or update.effective_chat.type != "private":
        try:
            await q.answer("Напоминалка работает в личных сообщениях с ботом.", show_alert=True)
        except Exception:
            pass
        return

    if data in ("help:reminder", "help:reminder:list"):
        await _render_reminders_list(q, context, user_id)
        return

    if data == "help:reminder:cancel":
        clear_reminder_flow(context)
        await _render_reminders_list(q, context, user_id)
        return

    if data == "help:reminder:new":
        if db_reminders_active_count(user_id) >= REMINDER_MAX_ACTIVE:
            await q.answer(
                "У тебя уже 5 активных напоминаний. Удали одно или дождись отправки.",
                show_alert=True,
            )
            return
        _reminder_set_flow(context, "create_text", mode="create")
        await replace_callback_message_with_text(
            q,
            context,
            "➕ <b>Новое напоминание</b>\n\n"
            "Напиши коротко, о чём тебе напомнить.\n"
            f"Не более {REMINDER_TEXT_MAX_LENGTH} символов.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Отмена", callback_data="help:reminder:cancel")]
            ]),
        )
        return

    if data.startswith("help:reminder:create:tz:"):
        try:
            delta = int(data.rsplit(":", 1)[-1])
            if delta not in REMINDER_TIMEZONE_LABELS:
                raise ValueError
        except ValueError:
            await q.answer("Неизвестный часовой пояс.", show_alert=True)
            return
        draft = context.user_data.get(REMINDER_DATA) or {}
        if draft.get("mode") != "create" or not draft.get("reminder_text"):
            clear_reminder_flow(context)
            await q.answer("Создание напоминания уже завершено. Начни заново.", show_alert=True)
            await _render_reminders_list(q, context, user_id)
            return
        draft["timezone_delta"] = delta
        context.user_data[REMINDER_DATA] = draft
        context.user_data[REMINDER_STATE] = "create_date"
        await replace_callback_message_with_text(
            q,
            context,
            "📅 <b>Когда напомнить?</b>\n\nВыбери дату.",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_reminder_date(delta),
        )
        return

    if data.startswith("help:reminder:date:"):
        draft = context.user_data.get(REMINDER_DATA) or {}
        state = context.user_data.get(REMINDER_STATE)
        if state not in ("create_date", "edit_date"):
            clear_reminder_flow(context)
            await q.answer("Этот шаг уже неактуален. Начни заново.", show_alert=True)
            await _render_reminders_list(q, context, user_id)
            return
        value = data[len("help:reminder:date:"):]
        if value == "custom":
            context.user_data[REMINDER_STATE] = (
                "create_date_text" if state == "create_date" else "edit_date_text"
            )
            await replace_callback_message_with_text(
                q,
                context,
                "📅 <b>Введи дату</b>\n\n"
                "Формат: <code>ДД.ММ</code> или <code>ДД.ММ.ГГГГ</code>.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ Отмена", callback_data="help:reminder:cancel")]
                ]),
            )
            return
        try:
            selected_date = date.fromisoformat(value)
        except ValueError:
            await q.answer("Не удалось распознать дату.", show_alert=True)
            return
        draft["date"] = selected_date.isoformat()
        context.user_data[REMINDER_DATA] = draft
        context.user_data[REMINDER_STATE] = (
            "create_time" if state == "create_date" else "edit_time"
        )
        await replace_callback_message_with_text(
            q,
            context,
            "🕒 <b>Во сколько напомнить?</b>\n\n"
            "Введи время в формате <code>ЧЧ:ММ</code>, например <code>18:30</code>.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Отмена", callback_data="help:reminder:cancel")]
            ]),
        )
        return

    if data == "help:reminder:save":
        draft = context.user_data.get(REMINDER_DATA) or {}
        if draft.get("mode") != "create" or not draft.get("remind_at_utc"):
            clear_reminder_flow(context)
            await q.answer("Черновик не найден. Начни создание заново.", show_alert=True)
            await _render_reminders_list(q, context, user_id)
            return
        when_utc = _reminder_parse_utc(draft.get("remind_at_utc"))
        if not when_utc:
            await q.answer("Не удалось определить время.", show_alert=True)
            return
        try:
            db_reminder_create(
                user_id=user_id,
                reminder_text=draft.get("reminder_text") or "",
                remind_at_utc=when_utc,
                timezone_delta=int(draft.get("timezone_delta") or 0),
            )
        except ValueError as exc:
            await q.answer(str(exc), show_alert=True)
            return
        clear_reminder_flow(context)
        await replace_callback_message_with_text(
            q,
            context,
            "✅ <b>Напоминание создано</b>\n\n" + _reminder_intro_text(user_id),
            parse_mode=ParseMode.HTML,
            reply_markup=kb_reminders_list(user_id),
        )
        return

    if data.startswith("help:reminder:open:"):
        try:
            reminder_id = int(data.rsplit(":", 1)[-1])
        except ValueError:
            return
        item = db_reminder_get(reminder_id, user_id)
        if not item or item.get("status") not in ("pending", "sending"):
            await q.answer("Напоминание уже не активно.", show_alert=True)
            await _render_reminders_list(q, context, user_id)
            return
        await _render_reminder_item(q, context, item)
        return

    if data.startswith("help:reminder:edittext:"):
        try:
            reminder_id = int(data.rsplit(":", 1)[-1])
        except ValueError:
            return
        item = db_reminder_get(reminder_id, user_id)
        if not item or item.get("status") != "pending":
            await q.answer("Это напоминание уже нельзя редактировать.", show_alert=True)
            return
        _reminder_set_flow(context, "edit_text", mode="edit", reminder_id=reminder_id)
        await replace_callback_message_with_text(
            q,
            context,
            "✏️ <b>Новое описание</b>\n\n"
            f"Сейчас: {escape(item['reminder_text'])}\n\n"
            "Отправь новый текст напоминания.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Отмена", callback_data="help:reminder:cancel")]
            ]),
        )
        return

    if data.startswith("help:reminder:editwhen:"):
        try:
            reminder_id = int(data.rsplit(":", 1)[-1])
        except ValueError:
            return
        item = db_reminder_get(reminder_id, user_id)
        if not item or item.get("status") != "pending":
            await q.answer("Это напоминание уже нельзя редактировать.", show_alert=True)
            return
        delta = int(item.get("timezone_delta") or 0)
        _reminder_set_flow(
            context,
            "edit_date",
            mode="edit_when",
            reminder_id=reminder_id,
            timezone_delta=delta,
        )
        await replace_callback_message_with_text(
            q,
            context,
            "📅 <b>Новая дата</b>\n\nВыбери дату напоминания.",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_reminder_date(delta),
        )
        return

    if data.startswith("help:reminder:edittzsave:"):
        parts = data.split(":")
        try:
            reminder_id = int(parts[-2])
            new_delta = int(parts[-1])
            if new_delta not in REMINDER_TIMEZONE_LABELS:
                raise ValueError
        except ValueError:
            await q.answer("Некорректные данные.", show_alert=True)
            return
        item = db_reminder_get(reminder_id, user_id)
        if not item or item.get("status") != "pending":
            await q.answer("Это напоминание уже нельзя редактировать.", show_alert=True)
            return
        old_utc = _reminder_parse_utc(item.get("remind_at_utc"))
        if not old_utc:
            await q.answer("Не удалось определить время напоминания.", show_alert=True)
            return
        old_local = old_utc.astimezone(_reminder_tz(int(item.get("timezone_delta") or 0)))
        new_local = datetime(
            old_local.year,
            old_local.month,
            old_local.day,
            old_local.hour,
            old_local.minute,
            tzinfo=_reminder_tz(new_delta),
        )
        try:
            changed = db_reminder_update_schedule(
                reminder_id,
                user_id,
                new_local.astimezone(pytz.UTC),
                new_delta,
            )
        except ValueError as exc:
            await q.answer(str(exc), show_alert=True)
            return
        if not changed:
            await q.answer("Не удалось изменить часовой пояс.", show_alert=True)
            return
        updated = db_reminder_get(reminder_id, user_id)
        await _render_reminder_item(q, context, updated)
        return

    if data.startswith("help:reminder:edittz:"):
        try:
            reminder_id = int(data.rsplit(":", 1)[-1])
        except ValueError:
            return
        item = db_reminder_get(reminder_id, user_id)
        if not item or item.get("status") != "pending":
            await q.answer("Это напоминание уже нельзя редактировать.", show_alert=True)
            return
        await replace_callback_message_with_text(
            q,
            context,
            "🌍 <b>Выбери новый часовой пояс</b>\n\n"
            "Дата и время на часах останутся прежними, изменится только часовой пояс.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("МСК", callback_data=f"help:reminder:edittzsave:{reminder_id}:0"),
                    InlineKeyboardButton("МСК+1", callback_data=f"help:reminder:edittzsave:{reminder_id}:1"),
                    InlineKeyboardButton("МСК+2", callback_data=f"help:reminder:edittzsave:{reminder_id}:2"),
                ],
                [InlineKeyboardButton("⬅️ Назад", callback_data=f"help:reminder:open:{reminder_id}")],
            ]),
        )
        return

    if data.startswith("help:reminder:deleteconfirm:"):
        try:
            reminder_id = int(data.rsplit(":", 1)[-1])
        except ValueError:
            return
        if not db_reminder_cancel(reminder_id, user_id):
            await q.answer("Напоминание уже не активно.", show_alert=True)
        await _render_reminders_list(q, context, user_id)
        return

    if data.startswith("help:reminder:delete:"):
        try:
            reminder_id = int(data.rsplit(":", 1)[-1])
        except ValueError:
            return
        item = db_reminder_get(reminder_id, user_id)
        if not item or item.get("status") != "pending":
            await q.answer("Напоминание уже не активно.", show_alert=True)
            return
        await replace_callback_message_with_text(
            q,
            context,
            "🗑 <b>Удалить напоминание?</b>\n\n"
            f"{escape(item['reminder_text'])}\n"
            f"🕒 {_reminder_format_when(item)}",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🗑 Да, удалить", callback_data=f"help:reminder:deleteconfirm:{reminder_id}")],
                [InlineKeyboardButton("⬅️ Не удалять", callback_data=f"help:reminder:open:{reminder_id}")],
            ]),
        )
        return


async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = context.user_data.get(REMINDER_STATE)
    if not state:
        return await _reminder_legacy_on_text(update, context)

    if await deny_no_access(update, context):
        return
    await sync_profile_user_id_from_update(update)

    if not update.effective_user or not update.message:
        return
    if not update.effective_chat or update.effective_chat.type != "private":
        clear_reminder_flow(context)
        await update.message.reply_text("Напоминалка работает только в личных сообщениях с ботом.")
        return

    user_id = int(update.effective_user.id)
    text = (update.message.text or "").strip()
    draft = context.user_data.get(REMINDER_DATA) or {}

    if state == "create_text":
        clean = re.sub(r"\s+", " ", text)
        if not clean:
            await update.message.reply_text("Описание не может быть пустым.")
            return
        if len(clean) > REMINDER_TEXT_MAX_LENGTH:
            await update.message.reply_text(
                f"Слишком длинно. Максимум {REMINDER_TEXT_MAX_LENGTH} символов."
            )
            return
        draft["reminder_text"] = clean
        draft["mode"] = "create"
        context.user_data[REMINDER_DATA] = draft
        context.user_data[REMINDER_STATE] = "create_timezone"
        await update.message.reply_text(
            "🌍 <b>Выбери свой часовой пояс</b>\n\n"
            "В нём будут указаны дата и время напоминания.",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_reminder_timezone("help:reminder:create:tz"),
        )
        return

    if state == "edit_text":
        clean = re.sub(r"\s+", " ", text)
        try:
            changed = db_reminder_update_text(
                int(draft.get("reminder_id")),
                user_id,
                clean,
            )
        except (TypeError, ValueError) as exc:
            await update.message.reply_text(str(exc))
            return
        clear_reminder_flow(context)
        if not changed:
            await update.message.reply_text(
                "Не удалось изменить напоминание: возможно, оно уже отправлено.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("⏰ Мои напоминания", callback_data="help:reminder:list")]
                ]),
            )
            return
        await update.message.reply_text(
            "✅ Описание обновлено.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Открыть напоминание", callback_data=f"help:reminder:open:{int(draft['reminder_id'])}")]
            ]),
        )
        return

    if state in ("create_date_text", "edit_date_text"):
        delta = int(draft.get("timezone_delta") or 0)
        try:
            selected_date = _reminder_parse_date_text(text, delta)
        except ValueError as exc:
            await update.message.reply_text(str(exc))
            return
        draft["date"] = selected_date.isoformat()
        context.user_data[REMINDER_DATA] = draft
        context.user_data[REMINDER_STATE] = (
            "create_time" if state == "create_date_text" else "edit_time"
        )
        await update.message.reply_text(
            "🕒 <b>Во сколько напомнить?</b>\n\n"
            "Введи время в формате <code>ЧЧ:ММ</code>, например <code>18:30</code>.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Отмена", callback_data="help:reminder:cancel")]
            ]),
        )
        return

    if state in ("create_time", "edit_time"):
        try:
            selected_date = date.fromisoformat(str(draft.get("date") or ""))
            delta = int(draft.get("timezone_delta") or 0)
            when_utc = _reminder_local_to_utc(selected_date, text, delta)
            if when_utc <= _reminder_utc_now() + timedelta(seconds=30):
                raise ValueError("Это время уже прошло. Выбери время в будущем.")
        except ValueError as exc:
            await update.message.reply_text(str(exc))
            return

        if state == "edit_time":
            try:
                changed = db_reminder_update_schedule(
                    int(draft.get("reminder_id")),
                    user_id,
                    when_utc,
                    delta,
                )
            except (TypeError, ValueError) as exc:
                await update.message.reply_text(str(exc))
                return
            reminder_id = int(draft.get("reminder_id"))
            clear_reminder_flow(context)
            if not changed:
                await update.message.reply_text(
                    "Не удалось изменить время: возможно, напоминание уже отправлено.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("⏰ Мои напоминания", callback_data="help:reminder:list")]
                    ]),
                )
                return
            await update.message.reply_text(
                "✅ Дата и время обновлены.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("Открыть напоминание", callback_data=f"help:reminder:open:{reminder_id}")]
                ]),
            )
            return

        draft["remind_at_utc"] = when_utc.isoformat()
        draft["time"] = text
        context.user_data[REMINDER_DATA] = draft
        context.user_data[REMINDER_STATE] = "create_confirm"
        await update.message.reply_text(
            _reminder_confirm_text(draft),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Сохранить", callback_data="help:reminder:save")],
                [InlineKeyboardButton("❌ Отмена", callback_data="help:reminder:cancel")],
            ]),
        )
        return

    # На шагах, где ожидается нажатие кнопки, подсказываем пользователю.
    await update.message.reply_text("Выбери один из вариантов кнопкой под сообщением.")


async def send_due_employee_reminders(context: ContextTypes.DEFAULT_TYPE):
    for item in db_reminders_due(limit=50):
        reminder_id = int(item["id"])
        if not db_reminder_reserve(reminder_id):
            continue
        try:
            await context.bot.send_message(
                chat_id=int(item["user_id"]),
                text=(
                    "⏰ <b>Напоминание</b>\n\n"
                    f"{escape(item['reminder_text'])}\n\n"
                    f"🕒 {escape(_reminder_format_when(item))}"
                ),
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📋 Мои напоминания", callback_data="help:reminder:list")]
                ]),
            )
            db_reminder_mark_sent(reminder_id)
        except Forbidden as exc:
            db_reminder_mark_failed(reminder_id, f"Forbidden: {exc}")
        except (TimedOut, NetworkError) as exc:
            db_reminder_return_pending(reminder_id, str(exc))
        except Exception as exc:
            logger.exception("Employee reminder %s failed: %s", reminder_id, exc)
            db_reminder_return_pending(reminder_id, str(exc))


async def check_and_send_jobs(context: ContextTypes.DEFAULT_TYPE):
    await _reminder_legacy_check_and_send_jobs(context)
    try:
        await send_due_employee_reminders(context)
    except Exception as exc:
        logger.exception("Employee reminders checker failed: %s", exc)

# =================== END EMPLOYEE REMINDERS V1 ===================

# ===================== TESTING MODES V3 =====================
# Два варианта прохождения:
# 1) результат публикуется только после проверки;
# 2) правильный ответ и пояснение показываются сразу.
# Сохраняются одиночный/множественный выбор, открытые вопросы,
# версии, банк вопросов, попытки, аналитика, сроки и карточки сотрудников.

TEST_MODES_V3_BUILD = "TESTING-MODES-V3-2026-07-22"

_test_modes_legacy_db_init = db_init
_test_modes_legacy_cb_help = cb_help
_test_modes_legacy_cb_test = cb_test
_test_modes_legacy_on_text = on_text
_test_modes_legacy_get_assignment = tv2_get_assignment
_test_modes_legacy_add_question = tv2_add_question
_test_modes_legacy_bank_add = tv2_bank_add
_test_modes_legacy_copy_bank_question = tv2_copy_bank_question
_test_modes_legacy_publish_template = tv2_publish_template
_test_modes_legacy_update_question = tv2_update_question


def db_init():
    _test_modes_legacy_db_init()
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    _tv2_add_column(cur, "test_templates", "grading_mode TEXT NOT NULL DEFAULT 'review'")
    _tv2_add_column(cur, "test_questions", "correct_text TEXT")
    _tv2_add_column(cur, "test_question_bank", "correct_text TEXT")
    _tv2_add_column(cur, "test_assignments", "result_released INTEGER NOT NULL DEFAULT 0")
    _tv2_add_column(cur, "test_assignments", "verified_at TEXT")
    _tv2_add_column(cur, "test_assignments", "completion_notified INTEGER NOT NULL DEFAULT 0")

    # Старые обучающие тесты становятся мгновенными, остальные — с проверкой.
    cur.execute(
        """
        UPDATE test_templates
        SET grading_mode=CASE
            WHEN COALESCE(immediate_feedback,0)=1 OR test_mode='learning' THEN 'instant'
            ELSE 'review'
        END
        WHERE grading_mode IS NULL OR grading_mode NOT IN ('review','instant')
        """
    )
    cur.execute(
        "UPDATE test_templates SET immediate_feedback=CASE WHEN grading_mode='instant' THEN 1 ELSE 0 END"
    )
    cur.execute(
        "UPDATE test_assignments SET result_released=1 "
        "WHERE status='finished' AND COALESCE(result_released,0)=0"
    )
    con.commit()
    con.close()
    logger.warning("=== %s ===", TEST_MODES_V3_BUILD)


def tv2_get_assignment(aid: int) -> dict | None:
    item = _test_modes_legacy_get_assignment(aid)
    if not item:
        return None
    template = tv2_get_template(int(item["template_id"])) or {}
    item["grading_mode"] = template.get("grading_mode") or (
        "instant" if int(template.get("immediate_feedback") or 0) else "review"
    )
    return item


def tv3_grading_mode(value) -> str:
    if isinstance(value, dict):
        mode = value.get("grading_mode")
        if not mode and value.get("template_id"):
            template = tv2_get_template(int(value["template_id"])) or {}
            mode = template.get("grading_mode")
    else:
        template = tv2_get_template(int(value)) or {}
        mode = template.get("grading_mode")
    return "instant" if mode == "instant" else "review"


def tv3_mode_title(mode: str) -> str:
    return "⚡ Мгновенный результат" if mode == "instant" else "🕓 Результат после проверки"


def tv3_time_label(seconds: int | None) -> str:
    return "без времени" if not seconds else f"{int(seconds) // 60} минут"


def tv3_time_keyboard(callback_prefix: str, back_callback: str) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("Без времени", callback_data=f"{callback_prefix}:0")],
        [
            InlineKeyboardButton("5 минут", callback_data=f"{callback_prefix}:300"),
            InlineKeyboardButton("10 минут", callback_data=f"{callback_prefix}:600"),
        ],
        [
            InlineKeyboardButton("15 минут", callback_data=f"{callback_prefix}:900"),
            InlineKeyboardButton("20 минут", callback_data=f"{callback_prefix}:1200"),
        ],
        [
            InlineKeyboardButton("25 минут", callback_data=f"{callback_prefix}:1500"),
            InlineKeyboardButton("30 минут", callback_data=f"{callback_prefix}:1800"),
        ],
        [InlineKeyboardButton("⬅️ Назад", callback_data=back_callback)],
    ]
    return InlineKeyboardMarkup(rows)


TV3_RU_WEEKDAYS_SHORT = ("Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс")
TV3_DUE_TIME_VALUES = tuple([f"{hour:02d}:00" for hour in range(8, 24)] + ["23:59"])


def tv3_assignment_due_main_keyboard() -> InlineKeyboardMarkup:
    """Первый шаг выбора предельного срока прохождения теста."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Без срока", callback_data="help:testv2:assigndue:none")],
        [InlineKeyboardButton("Сегодня", callback_data="help:testv2:assigndue:today")],
        [InlineKeyboardButton("Другие даты", callback_data="help:testv2:assignduedates")],
        [InlineKeyboardButton("❌ Отмена", callback_data="help:testv2:admin")],
    ])


def tv3_assignment_due_dates_keyboard() -> InlineKeyboardMarkup:
    """Даты на неделю вперёд; сегодняшний день вынесен отдельной кнопкой."""
    today = datetime.now(MOSCOW_TZ).date()
    date_buttons = []
    for offset in range(1, 8):
        selected = today + timedelta(days=offset)
        weekday = TV3_RU_WEEKDAYS_SHORT[selected.weekday()]
        label = f"{weekday}, {selected.strftime('%d.%m.%Y')}"
        date_buttons.append(InlineKeyboardButton(
            label,
            callback_data=f"help:testv2:assigndue:{selected.isoformat()}",
        ))

    rows = []
    for index in range(0, len(date_buttons), 2):
        rows.append(date_buttons[index:index + 2])
    rows.extend([
        [InlineKeyboardButton("⬅️ К выбору срока", callback_data="help:testv2:assigndueback")],
        [InlineKeyboardButton("❌ Отмена", callback_data="help:testv2:admin")],
    ])
    return InlineKeyboardMarkup(rows)


def tv3_due_time_is_available(selected_date: date, time_text: str) -> bool:
    """Для сегодняшнего дня не показывает уже прошедшее пограничное время."""
    try:
        hour, minute = [int(part) for part in time_text.split(":", 1)]
        local_dt = MOSCOW_TZ.localize(datetime.combine(
            selected_date,
            datetime.min.time().replace(hour=hour, minute=minute),
        ))
    except Exception:
        return False
    return local_dt > datetime.now(MOSCOW_TZ) + timedelta(minutes=1)


def tv3_assignment_due_time_keyboard(
    selected_date: date,
    back_callback: str,
) -> InlineKeyboardMarkup:
    """Пограничное время срока: кнопки с 08:00 до 23:00 и 23:59 МСК."""
    values = [
        value for value in TV3_DUE_TIME_VALUES
        if tv3_due_time_is_available(selected_date, value)
    ]
    rows = []
    for index in range(0, len(values), 3):
        rows.append([
            InlineKeyboardButton(
                value,
                callback_data=(
                    f"help:testv2:assignduetime:"
                    f"{selected_date.isoformat()}:{value.replace(':', '')}"
                ),
            )
            for value in values[index:index + 3]
        ])
    if not values:
        rows.append([InlineKeyboardButton(
            "На сегодня доступного времени нет",
            callback_data="noop",
        )])
    rows.extend([
        [InlineKeyboardButton("⬅️ Назад", callback_data=back_callback)],
        [InlineKeyboardButton("❌ Отмена", callback_data="help:testv2:admin")],
    ])
    return InlineKeyboardMarkup(rows)


def tv3_due_at_for_date_time(selected_date: date, time_text: str) -> str:
    """Преобразует выбранные дату и время МСК в naive UTC ISO для базы."""
    hour, minute = [int(part) for part in time_text.split(":", 1)]
    local_deadline = MOSCOW_TZ.localize(datetime.combine(
        selected_date,
        datetime.min.time().replace(hour=hour, minute=minute),
    ))
    return local_deadline.astimezone(pytz.UTC).replace(tzinfo=None).isoformat()


def tv3_due_date_label(selected_date: date) -> str:
    weekday = TV3_RU_WEEKDAYS_SHORT[selected_date.weekday()]
    return f"{weekday}, {selected_date.strftime('%d.%m.%Y')}"


def tv3_due_selection_text(prefix: str = "") -> str:
    head = (prefix or "").strip()
    body = "Выберите предельный срок прохождения теста:"
    return f"{head}\n\n{body}" if head else body

def tv2_add_question(
    tid: int,
    q_type: str,
    text: str,
    options: list[str] | None,
    correct: list[int] | None,
    points: float = 1,
    explanation: str = "",
    category: str = "",
    difficulty: int = 1,
    tags: str = "",
    correct_text: str = "",
) -> int:
    with tv2_connect() as con:
        idx = int(
            con.execute(
                "SELECT COALESCE(MAX(idx),0)+1 FROM test_questions WHERE template_id=?",
                (int(tid),),
            ).fetchone()[0]
        )
        cur = con.execute(
            """
            INSERT INTO test_questions(
                template_id, idx, q_type, question_text, options_json, correct_json,
                created_at, points, explanation, category, difficulty, tags, correct_text
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                int(tid), idx, q_type, text.strip(), _safe_json_dumps(options or []),
                _safe_json_dumps(correct or []), datetime.utcnow().isoformat(), float(points),
                (explanation or "").strip() or None, (category or "").strip() or None,
                max(1, min(int(difficulty or 1), 5)), (tags or "").strip() or None,
                (correct_text or "").strip() or None,
            ),
        )
        con.execute(
            "UPDATE test_templates SET updated_at=? WHERE id=?",
            (datetime.utcnow().isoformat(), int(tid)),
        )
        return int(cur.lastrowid)


def tv2_bank_add(
    q_type: str,
    text: str,
    options: list[str],
    correct: list[int],
    points: float,
    explanation: str,
    category: str,
    difficulty: int,
    tags: str,
    created_by: int | None,
    correct_text: str = "",
) -> int:
    with tv2_connect() as con:
        cur = con.execute(
            """
            INSERT INTO test_question_bank(
                q_type, question_text, options_json, correct_json, points,
                explanation, category, difficulty, tags, created_by, created_at,
                correct_text
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                q_type, text.strip(), _safe_json_dumps(options), _safe_json_dumps(correct),
                float(points), explanation.strip() or None, category.strip() or "Без категории",
                max(1, min(int(difficulty), 5)), tags.strip() or None, created_by,
                datetime.utcnow().isoformat(), (correct_text or "").strip() or None,
            ),
        )
        return int(cur.lastrowid)


def tv2_copy_bank_question(bank_id: int, tid: int) -> bool:
    with tv2_connect() as con:
        row = con.execute(
            "SELECT * FROM test_question_bank WHERE id=? AND is_active=1",
            (int(bank_id),),
        ).fetchone()
    if not row:
        return False
    item = dict(row)
    tv2_add_question(
        tid,
        item["q_type"],
        item["question_text"],
        _safe_json_loads(item.get("options_json"), []),
        _safe_json_loads(item.get("correct_json"), []),
        item.get("points") or 1,
        item.get("explanation") or "",
        item.get("category") or "",
        item.get("difficulty") or 1,
        item.get("tags") or "",
        item.get("correct_text") or "",
    )
    return True


def tv2_update_question(qid: int, field: str, value):
    if field == "correct_text":
        with tv2_connect() as con:
            con.execute(
                "UPDATE test_questions SET correct_text=? WHERE id=?",
                ((value or "").strip() or None, int(qid)),
            )
        return
    return _test_modes_legacy_update_question(qid, field, value)


def tv2_publish_template(tid: int, user_id: int | None = None) -> int:
    src = tv2_get_template(tid)
    if not src:
        raise ValueError("template not found")
    if int(src.get("is_published") or 0) == 1:
        return int(tid)
    root_id = int(src.get("parent_template_id") or src["id"])
    with tv2_connect() as con:
        max_ver = int(
            con.execute(
                "SELECT COALESCE(MAX(version),0) FROM test_templates WHERE id=? OR parent_template_id=?",
                (root_id, root_id),
            ).fetchone()[0] or 0
        )
        version = max(1, max_ver + 1)
        fields = [
            "title", "created_by", "created_at", "is_draft_visible", "passing_score",
            "max_attempts", "scoring_policy", "result_mode", "test_mode",
            "shuffle_questions", "shuffle_options", "allow_back", "allow_skip",
            "immediate_feedback", "default_time_limit_sec", "version",
            "parent_template_id", "is_published", "published_at", "updated_at",
            "grading_mode",
        ]
        now = datetime.utcnow().isoformat()
        vals = [
            src.get("title"), user_id or src.get("created_by"), now, 1,
            src.get("passing_score", 70), src.get("max_attempts", 1),
            src.get("scoring_policy", "best"), src.get("result_mode", "errors"),
            src.get("test_mode", "exam"), src.get("shuffle_questions", 0),
            src.get("shuffle_options", 0), src.get("allow_back", 0),
            src.get("allow_skip", 1), src.get("immediate_feedback", 0),
            src.get("default_time_limit_sec"), version, root_id, 1, now, now,
            tv3_grading_mode(src),
        ]
        placeholders = ",".join("?" for _ in fields)
        cur = con.execute(
            f"INSERT INTO test_templates({','.join(fields)}) VALUES({placeholders})",
            vals,
        )
        pub_id = int(cur.lastrowid)
        for question in tv2_questions(tid):
            con.execute(
                """
                INSERT INTO test_questions(
                    template_id, idx, q_type, question_text, options_json, correct_json,
                    created_at, points, explanation, category, difficulty, tags, correct_text
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    pub_id, question["idx"], question["q_type"], question["question_text"],
                    question.get("options_json"), question.get("correct_json"), now,
                    question.get("points", 1), question.get("explanation"),
                    question.get("category"), question.get("difficulty", 1),
                    question.get("tags"), question.get("correct_text"),
                ),
            )
        return pub_id


def tv3_normalize_open_answer(value: str) -> str:
    value = (value or "").strip().lower().replace("ё", "е")
    value = re.sub(r"[^0-9a-zа-я\s]+", " ", value, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", value).strip()


def tv3_open_answer_is_correct(answer: str, correct_text: str | None) -> bool:
    actual = tv3_normalize_open_answer(answer)
    accepted = [
        tv3_normalize_open_answer(item)
        for item in (correct_text or "").split("|")
        if tv3_normalize_open_answer(item)
    ]
    return bool(actual and accepted and actual in accepted)


def tv3_correct_answer_text(question: dict) -> str:
    if question.get("q_type") == "open":
        raw = (question.get("correct_text") or "").strip()
        return " / ".join(x.strip() for x in raw.split("|") if x.strip()) or "—"
    options = question.get("options") or []
    indexes = [int(x) for x in (question.get("correct") or [])]
    values = [options[i] for i in indexes if 0 <= i < len(options)]
    return ", ".join(values) or "—"


def tv3_employee_answer_text(question: dict, answer: dict | None) -> str:
    if not answer:
        return "—"
    payload = answer.get("answer") or {}
    if question.get("q_type") == "open":
        return str(payload.get("text") or "—")
    options = question.get("options") or []
    selected = [int(x) for x in (payload.get("selected") or [])]
    values = [options[i] for i in selected if 0 <= i < len(options)]
    return ", ".join(values) or "—"


def tv3_feedback_text(question: dict, correct: bool) -> str:
    lines = ["✅ <b>Верно</b>" if correct else "❌ <b>Неверно</b>"]
    lines.extend(["", "Правильный ответ:", f"<b>{escape(tv3_correct_answer_text(question))}</b>"])
    if question.get("explanation"):
        lines.extend(["", "💡 <b>Пояснение</b>", escape(str(question["explanation"]))])
    return "\n".join(lines)


def tv3_answer_marker(answer: dict | None) -> str:
    if not answer:
        return "⚪"
    status = str(answer.get("review_status") or "")
    if status == "pending":
        return "⏳"
    if status == "partial":
        return "🟡"
    return "✅" if answer.get("is_correct") == 1 else "❌"


def tv3_calculation(aid: int) -> dict:
    return tv2_calculate(aid, finalize=False)


def tv3_submit_for_review(aid: int):
    calc = tv3_calculation(aid)
    with tv2_connect() as con:
        con.execute(
            """
            UPDATE test_assignments
            SET status='needs_review', review_status='pending', finished_at=?,
                score_percent=?, points_earned=?, points_total=?, passed=0,
                result_released=0
            WHERE id=?
            """,
            (
                datetime.utcnow().isoformat(), calc["percent"], calc["earned"],
                calc["total"], int(aid),
            ),
        )
    return calc


def tv3_pending_review_count(aid: int) -> int:
    with tv2_connect() as con:
        row = con.execute(
            """
            SELECT COUNT(*)
            FROM test_answers
            WHERE assignment_id=? AND review_status='pending'
            """,
            (int(aid),),
        ).fetchone()
    return int(row[0] or 0)


def tv3_release_result(aid: int) -> tuple[bool, dict]:
    assignment = tv2_get_assignment(aid)
    if not assignment:
        return False, {}
    if int(assignment.get("result_released") or 0) == 1 and assignment.get("status") == "finished":
        return False, tv3_calculation(aid)
    if tv3_pending_review_count(aid) > 0:
        return False, tv3_calculation(aid)
    calc = tv3_calculation(aid)
    now = datetime.utcnow().isoformat()
    with tv2_connect() as con:
        con.execute(
            """
            UPDATE test_assignments
            SET status='finished', review_status='reviewed', verified_at=?,
                result_released=1, finished_at=COALESCE(finished_at,?),
                score_percent=?, points_earned=?, points_total=?, passed=?
            WHERE id=?
            """,
            (
                now, now, calc["percent"], calc["earned"], calc["total"],
                1 if calc["passed"] else 0, int(aid),
            ),
        )
    tv2_update_profile_average(int(assignment["profile_id"]))
    tv2_award_test_achievements(
        int(assignment["profile_id"]), aid, calc["percent"], calc["passed"]
    )
    return True, calc


def tv2_result_text(aid: int) -> str:
    assignment = tv2_get_assignment(aid)
    if not assignment:
        return "Тест не найден."
    lines = [f"📝 <b>{escape(assignment['title'])}</b>", ""]
    if assignment.get("status") == "needs_review" or not int(assignment.get("result_released") or 0):
        lines.append("⏳ <b>Результат появится после проверки.</b>")
    else:
        calc = tv3_calculation(aid)
        lines.append(f"Результат: <b>{calc['percent']:.0f}%</b>")
        lines.append(f"Баллы: <b>{calc['earned']:.1f} из {calc['total']:.1f}</b>")
        lines.append(f"Проходной балл: <b>{int(assignment.get('passing_score') or 70)}%</b>")
        lines.append("Статус: " + ("✅ успешно пройден" if calc["passed"] else "❌ не пройден"))
    summary = tv2_attempts_summary(assignment)
    lines.append(
        f"Попытка: <b>{TEST_MAX_ATTEMPTS} из {TEST_MAX_ATTEMPTS}</b>"
    )
    if assignment.get("status") == "finished" and summary.get("final") is not None:
        names = {"best": "лучший", "last": "последний", "average": "средний"}
        lines.append(
            f"Зачётный результат ({names.get(assignment.get('scoring_policy'), 'лучший')}): "
            f"<b>{summary['final']:.0f}%</b>"
        )
    if assignment.get("reviewer_comment") and assignment.get("status") == "finished":
        lines.extend(["", "💬 <b>Комментарий</b>", escape(assignment["reviewer_comment"])])
    return "\n".join(lines)


def tv2_render_result_details(aid: int) -> str:
    assignment = tv2_get_assignment(aid)
    if not assignment:
        return "Тест не найден."
    if assignment.get("status") != "finished" or not int(assignment.get("result_released") or 0):
        return tv2_result_text(aid)

    mode = assignment.get("result_mode") or "errors"
    # После проверки всегда показываем полный разбор.
    if tv3_grading_mode(assignment) == "review":
        mode = "all"
    base = tv2_result_text(aid)
    if mode in ("score", "hidden"):
        return base

    lines = [base, "", "<b>Разбор ответов</b>"]
    for question in tv2_questions(int(assignment["template_id"])):
        answer = tv2_answer(aid, int(question["id"]))
        if mode == "errors" and answer and answer.get("is_correct") == 1:
            continue
        marker = tv3_answer_marker(answer)
        lines.extend([
            "",
            f"{marker} <b>{int(question['idx'])}. {escape(question['question_text'])}</b>",
            "Ваш ответ: " + escape(tv3_employee_answer_text(question, answer)),
            "Правильный ответ: " + escape(tv3_correct_answer_text(question)),
        ])
        if question.get("explanation"):
            lines.append("💡 " + escape(str(question["explanation"])))
        if answer and answer.get("reviewer_comment"):
            lines.append("💬 " + escape(str(answer["reviewer_comment"])))
    return "\n".join(lines)[:4000]


def tv2_template_text(tid: int) -> str:
    template = tv2_get_template(tid)
    if not template:
        return "Шаблон не найден."
    questions = tv2_questions(tid)
    result_mode = {
        "score": "только балл", "errors": "ошибки", "all": "все ответы", "hidden": "скрыто"
    }.get(template.get("result_mode"), template.get("result_mode"))
    type_counts = {
        "single": sum(1 for item in questions if item.get("q_type") == "single"),
        "multi": sum(1 for item in questions if item.get("q_type") == "multi"),
        "open": sum(1 for item in questions if item.get("q_type") == "open"),
    }
    return (
        f"📝 <b>{escape(template['title'])}</b>\n"
        f"Версия: <b>{int(template.get('version') or 1)}</b> · "
        f"{'опубликована' if int(template.get('is_published') or 0) else 'черновик'}\n\n"
        f"Вариант: <b>{escape(tv3_mode_title(tv3_grading_mode(template)))}</b>\n"
        f"Вопросов: <b>{len(questions)}</b> "
        f"(1 ответ: {type_counts['single']}, несколько: {type_counts['multi']}, открытые: {type_counts['open']})\n"
        f"Время: <b>{escape(tv3_time_label(template.get('default_time_limit_sec')))}</b>\n"
        f"Проходной балл: <b>{int(template.get('passing_score') or 70)}%</b>\n"
        f"Попыток: <b>{TEST_MAX_ATTEMPTS}</b>\n"
        f"Зачёт: <b>{escape(str(template.get('scoring_policy') or 'best'))}</b>\n"
        f"Показывать результат: <b>{escape(str(result_mode))}</b>\n"
        f"Перемешивать вопросы: <b>{'да' if int(template.get('shuffle_questions') or 0) else 'нет'}</b>\n"
        f"Перемешивать варианты: <b>{'да' if int(template.get('shuffle_options') or 0) else 'нет'}</b>\n"
        f"Назад/пропуск: <b>{'да' if int(template.get('allow_back') or 0) else 'нет'} / "
        f"{'да' if int(template.get('allow_skip') or 0) else 'нет'}</b>"
    )


def tv2_kb_admin_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Создать тест", callback_data="help:testv2:create")],
        [InlineKeyboardButton("🗂 Черновики и версии", callback_data="help:testv2:drafts:0")],
        [InlineKeyboardButton("📚 Банк вопросов", callback_data="help:testv2:bank")],
        [InlineKeyboardButton("👥 Назначить тест", callback_data="help:testv2:assign")],
        [InlineKeyboardButton("✅ Проверить результаты", callback_data="help:testv2:review")],
        [InlineKeyboardButton("📊 Результаты и аналитика", callback_data="help:testv2:analytics")],
        [InlineKeyboardButton("⌛ Просроченные", callback_data="help:testv2:overdue")],
        [InlineKeyboardButton("🏢 Отделы сотрудников", callback_data="help:testv2:departments:0")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="help:settings")],
    ])


def tv2_kb_settings(tid: int):
    template = tv2_get_template(tid) or {}
    def yn(value):
        return "✅" if int(value or 0) else "❌"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(
            f"Вариант: {'сразу' if tv3_grading_mode(template) == 'instant' else 'после проверки'}",
            callback_data=f"help:testv2:set:mode:{tid}",
        )],
        [InlineKeyboardButton(
            f"⏱ Время: {tv3_time_label(template.get('default_time_limit_sec'))}",
            callback_data=f"help:testv2:set:time:{tid}",
        )],
        [InlineKeyboardButton(
            f"Проходной: {int(template.get('passing_score') or 70)}%",
            callback_data=f"help:testv2:set:passing:{tid}",
        )],
        [InlineKeyboardButton(
            f"Зачёт: {template.get('scoring_policy','best')}",
            callback_data=f"help:testv2:set:policy:{tid}",
        )],
        [InlineKeyboardButton(
            f"Результаты: {template.get('result_mode','errors')}",
            callback_data=f"help:testv2:set:result:{tid}",
        )],
        [InlineKeyboardButton(
            f"{yn(template.get('shuffle_questions'))} Перемешивать вопросы",
            callback_data=f"help:testv2:toggle:shuffleq:{tid}",
        )],
        [InlineKeyboardButton(
            f"{yn(template.get('shuffle_options'))} Перемешивать варианты",
            callback_data=f"help:testv2:toggle:shuffleo:{tid}",
        )],
        [InlineKeyboardButton(
            f"{yn(template.get('allow_back'))} Разрешить назад",
            callback_data=f"help:testv2:toggle:back:{tid}",
        )],
        [InlineKeyboardButton(
            f"{yn(template.get('allow_skip'))} Разрешить пропуск",
            callback_data=f"help:testv2:toggle:skip:{tid}",
        )],
        [InlineKeyboardButton("⬅️ Назад", callback_data=f"help:testv2:template:{tid}")],
    ])


def tv2_question_text(question: dict) -> str:
    options = question.get("options") or []
    correct = set(int(x) for x in (question.get("correct") or []))
    type_name = {"single": "один ответ", "multi": "несколько ответов", "open": "открытый"}.get(
        question.get("q_type"), question.get("q_type")
    )
    lines = [
        f"❓ <b>{int(question['idx'])}. {escape(question['question_text'])}</b>",
        f"Тип: <b>{escape(str(type_name))}</b> · Баллы: <b>{float(question.get('points') or 1):g}</b>",
    ]
    if question.get("q_type") == "open":
        lines.append("✅ Эталон: " + escape(tv3_correct_answer_text(question)))
    else:
        for index, option in enumerate(options):
            lines.append(f"{'✅' if index in correct else '▫️'} {index + 1}. {escape(option)}")
    if question.get("explanation"):
        lines.extend(["", "💡 " + escape(str(question["explanation"]))])
    return "\n".join(lines)


def tv2_kb_question_edit(question: dict):
    qid = int(question["id"])
    tid = int(question["template_id"])
    rows = [
        [
            InlineKeyboardButton("✏️ Текст", callback_data=f"help:testv2:qfield:text:{qid}"),
            InlineKeyboardButton("⭐ Баллы", callback_data=f"help:testv2:qfield:points:{qid}"),
        ],
    ]
    if question.get("q_type") != "open":
        rows.append([
            InlineKeyboardButton("📋 Варианты", callback_data=f"help:testv2:qfield:options:{qid}"),
            InlineKeyboardButton("✅ Правильный", callback_data=f"help:testv2:qfield:correct:{qid}"),
        ])
    else:
        rows.append([
            InlineKeyboardButton("✅ Эталон ответа", callback_data=f"help:testv2:qfield:correct:{qid}")
        ])
    rows.extend([
        [InlineKeyboardButton("💡 Пояснение", callback_data=f"help:testv2:qfield:explanation:{qid}")],
        [
            InlineKeyboardButton("⬆️", callback_data=f"help:testv2:qmove:{qid}:-1"),
            InlineKeyboardButton("⬇️", callback_data=f"help:testv2:qmove:{qid}:1"),
        ],
        [InlineKeyboardButton("🗑 Удалить", callback_data=f"help:testv2:qdeleteconfirm:{qid}")],
        [InlineKeyboardButton("⬅️ К вопросам", callback_data=f"help:testv2:qeditlist:{tid}:0")],
    ])
    return InlineKeyboardMarkup(rows)


async def tv2_admin_guard(update, context) -> bool:
    if not await is_admin_scoped(update, context):
        try:
            await update.callback_query.answer("Недостаточно прав.", show_alert=True)
        except Exception:
            pass
        return False
    return True


def tv3_review_keyboard(aid: int) -> InlineKeyboardMarkup:
    assignment = tv2_get_assignment(aid)
    rows = []
    for question in tv2_questions(int(assignment["template_id"])):
        answer = tv2_answer(aid, int(question["id"]))
        rows.append([
            InlineKeyboardButton(
                f"{tv3_answer_marker(answer)} {int(question['idx'])}. {question['question_text'][:43]}",
                callback_data=f"help:testv2:reviewanswer:{aid}:{int(question['id'])}",
            )
        ])
    rows.append([
        InlineKeyboardButton("💬 Общий комментарий", callback_data=f"help:testv2:reviewcomment:{aid}")
    ])
    pending = tv3_pending_review_count(aid)
    if pending == 0:
        rows.append([
            InlineKeyboardButton(
                "✅ Проверено — отправить результат",
                callback_data=f"help:testv2:reviewfinish:{aid}",
            )
        ])
    else:
        rows.append([InlineKeyboardButton(f"⏳ Осталось проверить: {pending}", callback_data="noop")])
    rows.append([InlineKeyboardButton("⬅️ К списку", callback_data="help:testv2:review")])
    return InlineKeyboardMarkup(rows)


def tv3_review_answer_text(aid: int, qid: int) -> tuple[str, InlineKeyboardMarkup]:
    question = tv2_question_by_id(qid) or {}
    answer = tv2_answer(aid, qid)
    max_points = float(question.get("points") or 1)
    lines = [
        f"❓ <b>{escape(str(question.get('question_text') or ''))}</b>",
        "",
        "Ответ:",
        escape(tv3_employee_answer_text(question, answer)),
        "",
        "Правильный ответ:",
        f"<b>{escape(tv3_correct_answer_text(question))}</b>",
        "",
        f"Максимум: <b>{max_points:g}</b> балла",
    ]
    if question.get("explanation"):
        lines.extend(["", "💡 <b>Пояснение</b>", escape(str(question["explanation"]))])
    if answer and answer.get("reviewer_comment"):
        lines.extend(["", "💬 <b>Комментарий</b>", escape(str(answer["reviewer_comment"]))])
    rows = []
    if answer:
        rows.append([
            InlineKeyboardButton("✅ Верно", callback_data=f"help:testv2:grade:{aid}:{qid}:100"),
            InlineKeyboardButton("🟡 Частично", callback_data=f"help:testv2:grade:{aid}:{qid}:50"),
            InlineKeyboardButton("❌ Неверно", callback_data=f"help:testv2:grade:{aid}:{qid}:0"),
        ])
        rows.append([
            InlineKeyboardButton(
                "💬 Комментарий к ответу",
                callback_data=f"help:testv2:answercomment:{aid}:{qid}",
            )
        ])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"help:testv2:reviewopen:{aid}")])
    return "\n".join(lines), InlineKeyboardMarkup(rows)


async def tv3_notify_result_ready(context, aid: int):
    assignment = tv2_get_assignment(aid)
    if not assignment or not assignment.get("tg_user_id"):
        return
    calc = tv3_calculation(aid)
    result_status = "тест пройден" if calc.get("passed") else "тест не пройден"
    db_notification_add_once(
        int(assignment["tg_user_id"]),
        "test_result_ready",
        f"Результат теста готов: {assignment['title']}",
        (
            f"Результат: {calc.get('percent', 0):.0f}%. Статус: {result_status}. "
            f"{TEST_RETAKE_POLICY_TEXT}"
        ),
        callback_data=f"help:testv2:result:{aid}",
    )
    try:
        await context.bot.send_message(
            int(assignment["tg_user_id"]),
            tv2_render_result_details(aid),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Открыть результат", callback_data=f"help:testv2:result:{aid}")]
            ]),
        )
        with tv2_connect() as con:
            con.execute(
                "UPDATE test_assignments SET completion_notified=1 WHERE id=?",
                (int(aid),),
            )
    except Exception:
        pass


async def tv3_notify_assigned_by(context, aid: int, event: str):
    assignment = tv2_get_assignment(aid)
    if not assignment or not assignment.get("assigned_by"):
        return
    if int(assignment.get("assigned_by") or 0) == int(assignment.get("tg_user_id") or -1):
        return
    if event == "review":
        text = (
            f"📝 Тест завершён и ожидает проверки.\n\n"
            f"Сотрудник: <b>{escape(assignment['full_name'])}</b>\n"
            f"Тест: <b>{escape(assignment['title'])}</b>"
        )
        callback = f"help:testv2:reviewopen:{aid}"
        button = "Открыть проверку"
    else:
        calc = tv3_calculation(aid)
        text = (
            f"📊 Тест завершён.\n\n"
            f"Сотрудник: <b>{escape(assignment['full_name'])}</b>\n"
            f"Тест: <b>{escape(assignment['title'])}</b>\n"
            f"Результат: <b>{calc['percent']:.0f}%</b>\n"
            f"Баллы: <b>{calc['earned']:.1f} из {calc['total']:.1f}</b>"
        )
        callback = f"help:testv2:result:{aid}"
        button = "Открыть результат"
    try:
        await context.bot.send_message(
            int(assignment["assigned_by"]),
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(button, callback_data=callback)]]),
        )
    except Exception:
        pass


async def cb_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = (update.callback_query.data or "") if update.callback_query else ""
    if not data.startswith("help:testv2:"):
        return await _test_modes_legacy_cb_help(update, context)

    query = update.callback_query

    if data == "help:testv2:admin":
        try:
            await query.answer()
        except Exception:
            pass
        if not await tv2_admin_guard(update, context):
            return
        tv2_clear(context)
        await query.edit_message_text(
            "📝 <b>Тестирование</b>\n\nСоздание, назначение, проверка, результаты и аналитика.",
            parse_mode=ParseMode.HTML,
            reply_markup=tv2_kb_admin_menu(),
        )
        return

    if data.startswith("help:testv2:creategrading:"):
        try:
            await query.answer()
        except Exception:
            pass
        if not await tv2_admin_guard(update, context):
            return
        mode = data.rsplit(":", 1)[-1]
        draft = context.user_data.get(TV2_DATA) or {}
        title = draft.get("title") or "Новый тест"
        base_mode = "learning" if mode == "instant" else "exam"
        tid = tv2_create_template(title, update.effective_user.id, base_mode)
        with tv2_connect() as con:
            con.execute(
                """
                UPDATE test_templates
                SET grading_mode=?, immediate_feedback=?, test_mode=?, updated_at=?
                WHERE id=?
                """,
                (
                    "instant" if mode == "instant" else "review",
                    1 if mode == "instant" else 0,
                    base_mode,
                    datetime.utcnow().isoformat(),
                    int(tid),
                ),
            )
        tv2_clear(context)
        await query.edit_message_text(
            tv2_template_text(tid),
            parse_mode=ParseMode.HTML,
            reply_markup=tv2_kb_template(tid),
        )
        return

    if data.startswith("help:testv2:set:mode:"):
        try:
            await query.answer()
        except Exception:
            pass
        if not await tv2_admin_guard(update, context):
            return
        tid = int(data.rsplit(":", 1)[-1])
        await query.edit_message_text(
            "Выберите вариант теста:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(
                    "🕓 Результат после проверки",
                    callback_data=f"help:testv2:gradingvalue:{tid}:review",
                )],
                [InlineKeyboardButton(
                    "⚡ Мгновенный результат",
                    callback_data=f"help:testv2:gradingvalue:{tid}:instant",
                )],
                [InlineKeyboardButton("⬅️ Назад", callback_data=f"help:testv2:settings:{tid}")],
            ]),
        )
        return

    if data.startswith("help:testv2:gradingvalue:"):
        try:
            await query.answer()
        except Exception:
            pass
        if not await tv2_admin_guard(update, context):
            return
        parts = data.split(":")
        tid = int(parts[-2])
        mode = parts[-1]
        with tv2_connect() as con:
            con.execute(
                """
                UPDATE test_templates
                SET grading_mode=?, immediate_feedback=?, test_mode=?, updated_at=?
                WHERE id=?
                """,
                (
                    mode,
                    1 if mode == "instant" else 0,
                    "learning" if mode == "instant" else "exam",
                    datetime.utcnow().isoformat(),
                    tid,
                ),
            )
        await query.edit_message_text(
            "⚙️ <b>Настройки теста</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=tv2_kb_settings(tid),
        )
        return

    if data.startswith("help:testv2:set:time:"):
        try:
            await query.answer()
        except Exception:
            pass
        if not await tv2_admin_guard(update, context):
            return
        tid = int(data.rsplit(":", 1)[-1])
        await query.edit_message_text(
            "Выберите время на прохождение:",
            reply_markup=tv3_time_keyboard(
                f"help:testv2:timevalue:{tid}",
                f"help:testv2:settings:{tid}",
            ),
        )
        return

    if data.startswith("help:testv2:timevalue:"):
        try:
            await query.answer()
        except Exception:
            pass
        if not await tv2_admin_guard(update, context):
            return
        parts = data.split(":")
        tid = int(parts[-2])
        seconds = int(parts[-1])
        with tv2_connect() as con:
            con.execute(
                "UPDATE test_templates SET default_time_limit_sec=?, updated_at=? WHERE id=?",
                (None if seconds == 0 else seconds, datetime.utcnow().isoformat(), tid),
            )
        await query.edit_message_text(
            "⚙️ <b>Настройки теста</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=tv2_kb_settings(tid),
        )
        return

    if data == "help:testv2:assignduedates":
        try:
            await query.answer()
        except Exception:
            pass
        if not await tv2_admin_guard(update, context):
            return
        context.user_data[TV2_STATE] = "assign_due_buttons"
        await query.edit_message_text(
            "Выберите дату завершения теста. После даты нужно будет выбрать пограничное время:",
            reply_markup=tv3_assignment_due_dates_keyboard(),
        )
        return

    if data == "help:testv2:assigndueback":
        try:
            await query.answer()
        except Exception:
            pass
        if not await tv2_admin_guard(update, context):
            return
        draft = context.user_data.get(TV2_DATA) or {}
        draft.pop("due_at", None)
        draft.pop("due_date", None)
        draft.pop("due_time", None)
        draft.pop("due_source", None)
        context.user_data[TV2_DATA] = draft
        context.user_data[TV2_STATE] = "assign_due_buttons"
        await query.edit_message_text(
            tv3_due_selection_text(),
            reply_markup=tv3_assignment_due_main_keyboard(),
        )
        return

    if data == "help:testv2:assignduetimeback":
        try:
            await query.answer()
        except Exception:
            pass
        if not await tv2_admin_guard(update, context):
            return
        draft = context.user_data.get(TV2_DATA) or {}
        try:
            selected_date = date.fromisoformat(str(draft.get("due_date") or ""))
        except ValueError:
            context.user_data[TV2_STATE] = "assign_due_buttons"
            await query.edit_message_text(
                tv3_due_selection_text(),
                reply_markup=tv3_assignment_due_main_keyboard(),
            )
            return
        context.user_data[TV2_STATE] = "assign_due_time_buttons"
        back_callback = (
            "help:testv2:assigndueback"
            if draft.get("due_source") == "today"
            else "help:testv2:assignduedates"
        )
        await query.edit_message_text(
            f"Дата завершения: <b>{escape(tv3_due_date_label(selected_date))}</b>\n\n"
            "Выберите пограничное время прохождения теста:",
            parse_mode=ParseMode.HTML,
            reply_markup=tv3_assignment_due_time_keyboard(selected_date, back_callback),
        )
        return

    if data.startswith("help:testv2:assigndue:"):
        try:
            await query.answer()
        except Exception:
            pass
        if not await tv2_admin_guard(update, context):
            return

        token = data.rsplit(":", 1)[-1]
        draft = context.user_data.get(TV2_DATA) or {}
        today = datetime.now(MOSCOW_TZ).date()

        if token == "none":
            draft["due_at"] = None
            draft.pop("due_date", None)
            draft.pop("due_time", None)
            draft.pop("due_source", None)
            context.user_data[TV2_DATA] = draft
            context.user_data[TV2_STATE] = "assign_time_buttons"
            await query.edit_message_text(
                "Предельный срок: <b>без срока</b>\n\n"
                "Выберите время на сам тест:",
                parse_mode=ParseMode.HTML,
                reply_markup=tv3_time_keyboard(
                    "help:testv2:assigntime",
                    "help:testv2:assigndueback",
                ),
            )
            return

        if token == "today":
            selected_date = today
            due_source = "today"
            back_callback = "help:testv2:assigndueback"
        else:
            try:
                selected_date = date.fromisoformat(token)
            except ValueError:
                await query.answer("Некорректная дата.", show_alert=True)
                return
            if selected_date <= today or selected_date > today + timedelta(days=7):
                await query.answer("Эта дата недоступна для выбора.", show_alert=True)
                return
            due_source = "other"
            back_callback = "help:testv2:assignduedates"

        draft["due_date"] = selected_date.isoformat()
        draft["due_source"] = due_source
        draft.pop("due_at", None)
        draft.pop("due_time", None)
        context.user_data[TV2_DATA] = draft
        context.user_data[TV2_STATE] = "assign_due_time_buttons"
        await query.edit_message_text(
            f"Дата завершения: <b>{escape(tv3_due_date_label(selected_date))}</b>\n\n"
            "Выберите пограничное время прохождения теста:",
            parse_mode=ParseMode.HTML,
            reply_markup=tv3_assignment_due_time_keyboard(selected_date, back_callback),
        )
        return

    if data.startswith("help:testv2:assignduetime:"):
        try:
            await query.answer()
        except Exception:
            pass
        if not await tv2_admin_guard(update, context):
            return
        parts = data.split(":")
        if len(parts) < 5:
            await query.answer("Некорректные дата или время.", show_alert=True)
            return
        try:
            selected_date = date.fromisoformat(parts[-2])
            compact_time = parts[-1]
            if len(compact_time) != 4 or not compact_time.isdigit():
                raise ValueError
            time_text = f"{compact_time[:2]}:{compact_time[2:]}"
            if time_text not in TV3_DUE_TIME_VALUES:
                raise ValueError
            due_at = tv3_due_at_for_date_time(selected_date, time_text)
            if datetime.fromisoformat(due_at) <= datetime.utcnow() + timedelta(minutes=1):
                raise ValueError
        except ValueError:
            await query.answer("Это время уже прошло или недоступно.", show_alert=True)
            return

        draft = context.user_data.get(TV2_DATA) or {}
        draft["due_date"] = selected_date.isoformat()
        draft["due_time"] = time_text
        draft["due_at"] = due_at
        context.user_data[TV2_DATA] = draft
        context.user_data[TV2_STATE] = "assign_time_buttons"
        await query.edit_message_text(
            f"Предельный срок: <b>{escape(tv3_due_date_label(selected_date))}, "
            f"{escape(time_text)} МСК</b>\n\n"
            "Теперь выберите время на сам тест:",
            parse_mode=ParseMode.HTML,
            reply_markup=tv3_time_keyboard(
                "help:testv2:assigntime",
                "help:testv2:assignduetimeback",
            ),
        )
        return

    if data.startswith("help:testv2:assigntime:"):
        try:
            await query.answer()
        except Exception:
            pass
        if not await tv2_admin_guard(update, context):
            return
        seconds = int(data.rsplit(":", 1)[-1])
        draft = context.user_data.get(TV2_DATA) or {}
        draft["time_limit_sec"] = None if seconds == 0 else seconds
        context.user_data[TV2_DATA] = draft
        context.user_data[TV2_STATE] = "assign_ready"
        await query.edit_message_text(
            f"Предельный срок: <b>{escape(tv2_fmt_dt(draft.get('due_at')))}</b>\n"
            f"Время на сам тест: <b>{escape(tv3_time_label(draft['time_limit_sec']))}</b>\n\n"
            "Настройки назначения готовы.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📋 Проверить", callback_data="help:testv2:assignconfirm")],
                [InlineKeyboardButton("❌ Отмена", callback_data="help:testv2:admin")],
            ]),
        )
        return

    if data.startswith("help:testv2:qfield:correct:"):
        qid = int(data.rsplit(":", 1)[-1])
        question = tv2_question_by_id(qid)
        if question and question.get("q_type") == "open":
            try:
                await query.answer()
            except Exception:
                pass
            if not await tv2_admin_guard(update, context):
                return
            tv2_set_state(
                context,
                "edit_q_correct_text",
                question_id=qid,
                template_id=int(question["template_id"]),
            )
            await query.edit_message_text(
                "Введите эталонный ответ. Несколько допустимых формулировок разделите символом <code>|</code>.",
                parse_mode=ParseMode.HTML,
                reply_markup=tv2_kb_cancel(f"help:testv2:qedit:{qid}"),
            )
            return

    if data == "help:testv2:review":
        try:
            await query.answer()
        except Exception:
            pass
        if not await tv2_admin_guard(update, context):
            return
        items = tv2_admin_review_list()
        rows = [
            [InlineKeyboardButton(
                f"{item['full_name']} · {item['title']}",
                callback_data=f"help:testv2:reviewopen:{int(item['id'])}",
            )]
            for item in items[:40]
        ]
        rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="help:testv2:admin")])
        await query.edit_message_text(
            f"✅ <b>Ожидают проверки: {len(items)}</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(rows),
        )
        return

    if data.startswith("help:testv2:reviewopen:"):
        try:
            await query.answer()
        except Exception:
            pass
        if not await tv2_admin_guard(update, context):
            return
        aid = int(data.rsplit(":", 1)[-1])
        assignment = tv2_get_assignment(aid)
        if not assignment:
            await query.edit_message_text("Результат не найден.")
            return
        await query.edit_message_text(
            f"✅ <b>{escape(assignment['full_name'])}</b> · {escape(assignment['title'])}\n\n"
            "Откройте вопросы, проверьте ответы и нажмите «Проверено». ",
            parse_mode=ParseMode.HTML,
            reply_markup=tv3_review_keyboard(aid),
        )
        return

    if data.startswith("help:testv2:reviewanswer:"):
        try:
            await query.answer()
        except Exception:
            pass
        if not await tv2_admin_guard(update, context):
            return
        parts = data.split(":")
        aid = int(parts[-2])
        qid = int(parts[-1])
        text, keyboard = tv3_review_answer_text(aid, qid)
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
        return

    if data.startswith("help:testv2:grade:"):
        try:
            await query.answer("Оценка сохранена", show_alert=False)
        except Exception:
            pass
        if not await tv2_admin_guard(update, context):
            return
        parts = data.split(":")
        aid = int(parts[-3])
        qid = int(parts[-2])
        percent = int(parts[-1])
        question = tv2_question_by_id(qid) or {}
        awarded = float(question.get("points") or 1) * percent / 100
        status = {100: "full", 50: "partial", 0: "wrong"}[percent]
        with tv2_connect() as con:
            con.execute(
                """
                UPDATE test_answers
                SET awarded_points=?, is_correct=?, review_status=?
                WHERE assignment_id=? AND question_id=?
                """,
                (awarded, 1 if percent == 100 else 0, status, aid, qid),
            )
        assignment = tv2_get_assignment(aid)
        await query.edit_message_text(
            f"✅ <b>{escape(assignment['full_name'])}</b> · {escape(assignment['title'])}",
            parse_mode=ParseMode.HTML,
            reply_markup=tv3_review_keyboard(aid),
        )
        return

    if data.startswith("help:testv2:reviewfinish:"):
        try:
            await query.answer()
        except Exception:
            pass
        if not await tv2_admin_guard(update, context):
            return
        aid = int(data.rsplit(":", 1)[-1])
        if tv3_pending_review_count(aid) > 0:
            await query.answer("Сначала проверьте все открытые ответы.", show_alert=True)
            return
        released, calc = tv3_release_result(aid)
        if released:
            await tv3_notify_result_ready(context, aid)
            await query.edit_message_text(
                f"✅ Проверено. Результат отправлен.\n\n"
                f"Итог: <b>{calc['percent']:.0f}%</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📊 Открыть результат", callback_data=f"help:testv2:result:{aid}")],
                    [InlineKeyboardButton("⬅️ К проверкам", callback_data="help:testv2:review")],
                ]),
            )
        else:
            await query.edit_message_text(
                "Результат уже отправлен.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📊 Открыть результат", callback_data=f"help:testv2:result:{aid}")]
                ]),
            )
        return

    return await _test_modes_legacy_cb_help(update, context)


async def cb_test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = (update.callback_query.data or "") if update.callback_query else ""
    if not data.startswith("test:v2:"):
        return await _test_modes_legacy_cb_test(update, context)

    parts = data.split(":")
    action = parts[2]
    aid = int(parts[3]) if len(parts) > 3 else 0
    assignment = tv2_get_assignment(aid)
    profile = get_profile_for_user(update)
    if not assignment or not profile or int(assignment["profile_id"]) != int(profile["id"]):
        try:
            await update.callback_query.answer("Тест назначен другому сотруднику", show_alert=True)
        except Exception:
            pass
        return

    query = update.callback_query
    if action == "retry" or (
        action not in ("start", "continue")
        and assignment.get("status") not in ("assigned", "in_progress", "saved")
    ):
        try:
            await query.answer(
                "Этот тест уже завершён или недоступен. Новая попытка возможна "
                "только после повторного назначения администратором.",
                show_alert=True,
            )
        except Exception:
            pass
        return

    mode = tv3_grading_mode(assignment)

    if action == "single":
        try:
            await query.answer()
        except Exception:
            pass
        if tv2_is_expired(assignment):
            tv2_mark_expired(aid)
            await query.edit_message_text("⌛ Время теста истекло.")
            return
        qid = int(parts[4])
        option = int(parts[5])
        question = tv2_question_by_id(qid) or {}
        correct_options = set(int(x) for x in question.get("correct") or [])
        correct = {option} == correct_options
        points = float(question.get("points") or 1) if correct else 0
        tv2_save_answer(aid, qid, {"selected": [option]}, 1 if correct else 0, points, "auto")
        if mode == "instant":
            await query.edit_message_text(
                tv3_feedback_text(question, correct),
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("Далее ▶️", callback_data=f"test:v2:next:{aid}")]
                ]),
            )
            return
        return await cb_test_goto_next(update, context, aid)

    if action == "multisubmit":
        try:
            await query.answer()
        except Exception:
            pass
        if tv2_is_expired(assignment):
            tv2_mark_expired(aid)
            await query.edit_message_text("⌛ Время теста истекло.")
            return
        qid = int(parts[4])
        question = tv2_question_by_id(qid) or {}
        selected = set((context.user_data.get(TV2_MULTI) or {}).get(str(qid), []))
        correct_options = set(int(x) for x in question.get("correct") or [])
        correct = selected == correct_options
        points = float(question.get("points") or 1) if correct else 0
        tv2_save_answer(
            aid, qid, {"selected": sorted(selected)}, 1 if correct else 0, points, "auto"
        )
        if mode == "instant":
            await query.edit_message_text(
                tv3_feedback_text(question, correct),
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("Далее ▶️", callback_data=f"test:v2:next:{aid}")]
                ]),
            )
            return
        return await cb_test_goto_next(update, context, aid)

    if action == "finish":
        try:
            await query.answer()
        except Exception:
            pass
        tv2_clear(context)
        if mode == "review":
            tv3_submit_for_review(aid)
            await tv3_notify_assigned_by(context, aid, "review")
            await query.edit_message_text(
                "✅ Ответы отправлены.\n\n⏳ <b>Результат появится после проверки.</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("⬅️ Мои тесты", callback_data="help:testv2:my:all:0")]
                ]),
            )
            return

        calc = tv2_calculate(aid, finalize=True)
        if calc.get("pending"):
            with tv2_connect() as con:
                con.execute(
                    "UPDATE test_assignments SET result_released=0 WHERE id=?",
                    (int(aid),),
                )
            await tv3_notify_assigned_by(context, aid, "review")
            await query.edit_message_text(
                "✅ Ответы отправлены.\n\n⏳ <b>Результат появится после проверки.</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("⬅️ Мои тесты", callback_data="help:testv2:my:all:0")]
                ]),
            )
            return
        with tv2_connect() as con:
            con.execute(
                """
                UPDATE test_assignments
                SET result_released=1, verified_at=COALESCE(verified_at,?)
                WHERE id=?
                """,
                (datetime.utcnow().isoformat(), int(aid)),
            )
        await tv3_notify_result_ready(context, aid)
        await tv3_notify_assigned_by(context, aid, "instant")
        await query.edit_message_text(
            tv2_result_text(aid),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📊 Подробнее", callback_data=f"help:testv2:result:{aid}")],
                [InlineKeyboardButton("⬅️ Мои тесты", callback_data="help:testv2:my:all:0")],
            ]),
        )
        return

    return await _test_modes_legacy_cb_test(update, context)


async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get(CASES_WAITING_SEARCH):
        return await handle_cases_search_text(update, context)
    state = context.user_data.get(TV2_STATE)
    if not state:
        return await _test_modes_legacy_on_text(update, context)
    if await deny_no_access(update, context):
        return
    await sync_profile_user_id_from_update(update)
    text = (update.message.text or "").strip()
    draft = context.user_data.get(TV2_DATA) or {}

    if state == "create_title":
        if len(text) < 3:
            await update.message.reply_text("Название слишком короткое.")
            return
        context.user_data[TV2_DATA] = {"title": text}
        context.user_data[TV2_STATE] = "create_mode"
        await update.message.reply_text(
            "Выберите вариант теста:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(
                    "🕓 Результат после проверки",
                    callback_data="help:testv2:creategrading:review",
                )],
                [InlineKeyboardButton(
                    "⚡ Мгновенный результат",
                    callback_data="help:testv2:creategrading:instant",
                )],
                [InlineKeyboardButton("❌ Отмена", callback_data="help:testv2:admin")],
            ]),
        )
        return

    if state == "q_text":
        draft["question_text"] = text
        context.user_data[TV2_DATA] = draft
        if draft.get("q_type") == "open":
            context.user_data[TV2_STATE] = "q_open_correct"
            await update.message.reply_text(
                "Введите эталонный ответ. Несколько допустимых формулировок разделите символом |."
            )
            return
        context.user_data[TV2_STATE] = "q_options"
        await update.message.reply_text(
            "Введите варианты ответа, каждый с новой строки (минимум 2):"
        )
        return

    if state == "q_open_correct":
        if not text or text == "-":
            await update.message.reply_text("Укажите хотя бы один эталонный ответ.")
            return
        draft["correct_text"] = text
        context.user_data[TV2_DATA] = draft
        context.user_data[TV2_STATE] = "q_points"
        await update.message.reply_text("Сколько баллов даёт вопрос?")
        return

    if state == "q_explanation":
        draft["explanation"] = "" if text == "-" else text
        context.user_data[TV2_DATA] = draft
        if draft.get("target") == "bank":
            context.user_data[TV2_STATE] = "q_bank_meta"
            await update.message.reply_text(
                "Введите: категория | сложность 1-5 | теги через запятую\n"
                "Например: CRM | 2 | лиды, синхронизация"
            )
            return
        tv2_add_question(
            int(draft["template_id"]), draft["q_type"], draft["question_text"],
            draft.get("options", []), draft.get("correct", []), draft["points"],
            draft["explanation"], correct_text=draft.get("correct_text", ""),
        )
        tid = int(draft["template_id"])
        tv2_clear(context)
        await update.message.reply_text(
            "✅ Вопрос добавлен.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Открыть тест", callback_data=f"help:testv2:template:{tid}")]
            ]),
        )
        return

    if state == "q_bank_meta":
        parts = [item.strip() for item in text.split("|")]
        category = parts[0] if parts else "Без категории"
        try:
            difficulty = int(parts[1]) if len(parts) > 1 else 1
        except Exception:
            difficulty = 1
        tags = parts[2] if len(parts) > 2 else ""
        bank_id = tv2_bank_add(
            draft["q_type"], draft["question_text"], draft.get("options", []),
            draft.get("correct", []), draft["points"], draft.get("explanation", ""),
            category, difficulty, tags, update.effective_user.id,
            correct_text=draft.get("correct_text", ""),
        )
        tv2_clear(context)
        await update.message.reply_text(
            f"✅ Вопрос добавлен в банк (ID {bank_id}).",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Банк вопросов", callback_data="help:testv2:bank")]
            ]),
        )
        return

    if state == "edit_q_correct_text":
        if not text or text == "-":
            await update.message.reply_text("Укажите хотя бы один эталонный ответ.")
            return
        qid = int(draft["question_id"])
        tv2_update_question(qid, "correct_text", text)
        tv2_clear(context)
        await update.message.reply_text(
            "✅ Изменение сохранено.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Открыть вопрос", callback_data=f"help:testv2:qedit:{qid}")]
            ]),
        )
        return

    if state in ("assign_due", "assign_due_buttons"):
        await update.message.reply_text(
            "Выберите срок кнопкой под сообщением.",
            reply_markup=tv3_assignment_due_main_keyboard(),
        )
        return

    if state == "assign_due_time_buttons":
        await update.message.reply_text("Выберите пограничное время кнопкой под сообщением.")
        return

    if state == "assign_time_buttons":
        await update.message.reply_text("Выберите время на сам тест кнопкой под сообщением.")
        return

    if state == "open_answer":
        aid = int(draft["assignment_id"])
        qid = int(draft["question_id"])
        question = tv2_question_by_id(qid) or {}
        assignment = tv2_get_assignment(aid) or {}
        mode = tv3_grading_mode(assignment)
        has_reference = bool((question.get("correct_text") or "").strip())
        if mode == "instant" and has_reference:
            correct = tv3_open_answer_is_correct(text, question.get("correct_text"))
            points = float(question.get("points") or 1) if correct else 0
            tv2_save_answer(
                aid, qid, {"text": text}, 1 if correct else 0, points, "auto"
            )
        else:
            tv2_save_answer(aid, qid, {"text": text}, None, None, "pending")
        tv2_clear(context)
        if mode == "instant" and has_reference:
            await update.message.reply_text(
                tv3_feedback_text(question, correct),
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("Далее ▶️", callback_data=f"test:v2:next:{aid}")]
                ]),
            )
            return
        position = int(draft.get("position") or 0) + 1
        order = tv2_assignment_order(assignment)
        if position >= len(order):
            review_text, keyboard = tv2_review_page_text(aid)
            await update.message.reply_text(
                review_text, parse_mode=ParseMode.HTML, reply_markup=keyboard
            )
        else:
            await tv2_send_question(update, context, aid, position)
        return

    return await _test_modes_legacy_on_text(update, context)

# =================== END TESTING MODES V3 ===================

# ===================== TEST HISTORY + QUESTION BANK V4 =====================
# Администратор может убрать ненужные шаблоны, опубликованные версии и
# тестирования из рабочих списков, но история сотрудника, ответы и средний балл
# сохраняются. Каждый вопрос теста имеет независимую копию в банке вопросов.

TEST_HISTORY_V4_BUILD = "TEST-HISTORY-BANK-V4-2026-07-22"

_test_history_legacy_db_init = db_init
_test_history_legacy_cb_help = cb_help
_test_history_legacy_publish_template = tv2_publish_template
_test_history_legacy_update_question = tv2_update_question


def _tv4_question_payload(
    q_type: str,
    text: str,
    options: list[str] | None,
    correct: list[int] | None,
    points: float = 1,
    explanation: str = "",
    category: str = "",
    difficulty: int = 1,
    tags: str = "",
    correct_text: str = "",
) -> dict:
    return {
        "q_type": (q_type or "open").strip(),
        "question_text": (text or "").strip(),
        "options_json": _safe_json_dumps(options or []),
        "correct_json": _safe_json_dumps(correct or []),
        "points": float(points or 1),
        "explanation": (explanation or "").strip(),
        "category": (category or "").strip() or "Без категории",
        "difficulty": max(1, min(int(difficulty or 1), 5)),
        "tags": (tags or "").strip(),
        "correct_text": (correct_text or "").strip(),
    }


def _tv4_ensure_bank_question(
    con: sqlite3.Connection,
    question: dict,
    created_by: int | None = None,
    preferred_bank_id: int | None = None,
) -> int:
    """Возвращает постоянный ID вопроса в банке, создавая запись при необходимости."""
    if preferred_bank_id:
        row = con.execute(
            "SELECT id FROM test_question_bank WHERE id=?",
            (int(preferred_bank_id),),
        ).fetchone()
        if row:
            return int(row[0])

    payload = _tv4_question_payload(
        question.get("q_type") or "open",
        question.get("question_text") or "",
        _safe_json_loads(question.get("options_json"), question.get("options") or []),
        _safe_json_loads(question.get("correct_json"), question.get("correct") or []),
        question.get("points") or 1,
        question.get("explanation") or "",
        question.get("category") or "",
        question.get("difficulty") or 1,
        question.get("tags") or "",
        question.get("correct_text") or "",
    )
    row = con.execute(
        """
        SELECT id
        FROM test_question_bank
        WHERE is_active=1
          AND q_type=?
          AND question_text=?
          AND COALESCE(options_json,'[]')=?
          AND COALESCE(correct_json,'[]')=?
          AND COALESCE(points,1)=?
          AND COALESCE(explanation,'')=?
          AND COALESCE(category,'Без категории')=?
          AND COALESCE(difficulty,1)=?
          AND COALESCE(tags,'')=?
          AND COALESCE(correct_text,'')=?
        ORDER BY id
        LIMIT 1
        """,
        (
            payload["q_type"], payload["question_text"], payload["options_json"],
            payload["correct_json"], payload["points"], payload["explanation"],
            payload["category"], payload["difficulty"], payload["tags"],
            payload["correct_text"],
        ),
    ).fetchone()
    if row:
        return int(row[0])

    now = datetime.utcnow().isoformat()
    cur = con.execute(
        """
        INSERT INTO test_question_bank(
            q_type, question_text, options_json, correct_json, points,
            explanation, category, difficulty, tags, is_active,
            created_by, created_at, updated_at, correct_text
        ) VALUES(?,?,?,?,?,?,?,?,?,1,?,?,?,?)
        """,
        (
            payload["q_type"], payload["question_text"], payload["options_json"],
            payload["correct_json"], payload["points"],
            payload["explanation"] or None, payload["category"],
            payload["difficulty"], payload["tags"] or None,
            created_by, now, now, payload["correct_text"] or None,
        ),
    )
    return int(cur.lastrowid)


def db_init():
    _test_history_legacy_db_init()
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    _tv2_add_column(cur, "test_templates", "deleted_at TEXT")
    _tv2_add_column(cur, "test_assignments", "admin_deleted_at TEXT")
    _tv2_add_column(cur, "test_questions", "bank_question_id INTEGER")
    _tv2_add_column(cur, "test_question_bank", "deleted_at TEXT")

    # Единая политика: у каждого теста ровно одна попытка. Это также
    # нормализует старые шаблоны, где раньше были доступны 2/3/99 попыток,
    # и старые назначения, созданные через прежний retry-поток.
    cur.execute(
        "UPDATE test_templates SET max_attempts=? WHERE COALESCE(max_attempts, 1)<>?",
        (TEST_MAX_ATTEMPTS, TEST_MAX_ATTEMPTS),
    )
    cur.execute(
        "UPDATE test_assignments SET attempt_no=? WHERE COALESCE(attempt_no, 1)<>?",
        (TEST_MAX_ATTEMPTS, TEST_MAX_ATTEMPTS),
    )

    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_test_assign_admin_visible "
        "ON test_assignments(admin_deleted_at, status)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_test_questions_bank "
        "ON test_questions(bank_question_id)"
    )
    cur.execute(
        "UPDATE test_templates SET deleted_at=COALESCE(deleted_at, updated_at, created_at) "
        "WHERE COALESCE(is_draft_visible,1)=0 AND deleted_at IS NULL"
    )

    # Однократная миграция: все ранее созданные вопросы также становятся
    # независимыми записями банка. Опубликованные копии одинаковых вопросов
    # связываются с одной активной записью банка.
    questions = cur.execute(
        """
        SELECT q.*, t.created_by AS template_created_by
        FROM test_questions q
        LEFT JOIN test_templates t ON t.id=q.template_id
        WHERE q.bank_question_id IS NULL
        ORDER BY q.id
        """
    ).fetchall()
    for row in questions:
        item = dict(row)
        bank_id = _tv4_ensure_bank_question(
            con,
            item,
            item.get("template_created_by"),
        )
        cur.execute(
            "UPDATE test_questions SET bank_question_id=? WHERE id=?",
            (bank_id, int(item["id"])),
        )

    con.commit()
    con.close()
    logger.warning("=== %s ===", TEST_HISTORY_V4_BUILD)


def tv2_add_question(
    tid: int,
    q_type: str,
    text: str,
    options: list[str] | None,
    correct: list[int] | None,
    points: float = 1,
    explanation: str = "",
    category: str = "",
    difficulty: int = 1,
    tags: str = "",
    correct_text: str = "",
    bank_question_id: int | None = None,
) -> int:
    payload = _tv4_question_payload(
        q_type, text, options, correct, points, explanation,
        category, difficulty, tags, correct_text,
    )
    now = datetime.utcnow().isoformat()
    with tv2_connect() as con:
        template = con.execute(
            "SELECT created_by FROM test_templates WHERE id=?",
            (int(tid),),
        ).fetchone()
        persistent_bank_id = _tv4_ensure_bank_question(
            con,
            payload,
            int(template[0]) if template and template[0] is not None else None,
            bank_question_id,
        )
        idx = int(con.execute(
            "SELECT COALESCE(MAX(idx),0)+1 FROM test_questions WHERE template_id=?",
            (int(tid),),
        ).fetchone()[0])
        cur = con.execute(
            """
            INSERT INTO test_questions(
                template_id, idx, q_type, question_text, options_json, correct_json,
                created_at, points, explanation, category, difficulty, tags,
                correct_text, bank_question_id
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                int(tid), idx, payload["q_type"], payload["question_text"],
                payload["options_json"], payload["correct_json"], now,
                payload["points"], payload["explanation"] or None,
                None if payload["category"] == "Без категории" else payload["category"],
                payload["difficulty"], payload["tags"] or None,
                payload["correct_text"] or None, persistent_bank_id,
            ),
        )
        con.execute(
            "UPDATE test_templates SET updated_at=? WHERE id=?",
            (now, int(tid)),
        )
        return int(cur.lastrowid)


def tv2_copy_bank_question(bank_id: int, tid: int) -> bool:
    with tv2_connect() as con:
        row = con.execute(
            "SELECT * FROM test_question_bank WHERE id=? AND is_active=1",
            (int(bank_id),),
        ).fetchone()
    if not row:
        return False
    item = dict(row)
    tv2_add_question(
        tid,
        item["q_type"],
        item["question_text"],
        _safe_json_loads(item.get("options_json"), []),
        _safe_json_loads(item.get("correct_json"), []),
        item.get("points") or 1,
        item.get("explanation") or "",
        item.get("category") or "",
        item.get("difficulty") or 1,
        item.get("tags") or "",
        item.get("correct_text") or "",
        bank_question_id=int(bank_id),
    )
    return True


def tv2_update_question(qid: int, field: str, value):
    _test_history_legacy_update_question(qid, field, value)
    # Редактирование создаёт/выбирает самостоятельную актуальную запись банка,
    # не меняя вопрос в других тестах.
    with tv2_connect() as con:
        row = con.execute(
            """
            SELECT q.*, t.created_by AS template_created_by
            FROM test_questions q
            LEFT JOIN test_templates t ON t.id=q.template_id
            WHERE q.id=?
            """,
            (int(qid),),
        ).fetchone()
        if row:
            item = dict(row)
            bank_id = _tv4_ensure_bank_question(
                con, item, item.get("template_created_by")
            )
            con.execute(
                "UPDATE test_questions SET bank_question_id=? WHERE id=?",
                (bank_id, int(qid)),
            )


def tv2_publish_template(tid: int, user_id: int | None = None) -> int:
    published_id = _test_history_legacy_publish_template(tid, user_id)
    if int(published_id) == int(tid):
        return int(published_id)
    source_questions = {int(q["idx"]): q for q in tv2_questions(tid)}
    with tv2_connect() as con:
        for idx, question in source_questions.items():
            bank_id = question.get("bank_question_id")
            if not bank_id:
                bank_id = _tv4_ensure_bank_question(con, question, user_id)
                con.execute(
                    "UPDATE test_questions SET bank_question_id=? WHERE id=?",
                    (int(bank_id), int(question["id"])),
                )
            con.execute(
                "UPDATE test_questions SET bank_question_id=? "
                "WHERE template_id=? AND idx=?",
                (int(bank_id), int(published_id), idx),
            )
    return int(published_id)


def tv2_kb_template(tid: int):
    template = tv2_get_template(tid) or {}
    visible = bool(template) and int(template.get("is_draft_visible") or 0) == 1 \
        and not template.get("deleted_at")
    rows = [[InlineKeyboardButton(
        "👁 Предпросмотр", callback_data=f"help:testv2:preview:{tid}"
    )]]
    if visible and not int(template.get("is_published") or 0):
        rows.extend([
            [InlineKeyboardButton("➕ Добавить вопрос", callback_data=f"help:testv2:qadd:{tid}")],
            [InlineKeyboardButton("📚 Добавить из банка", callback_data=f"help:testv2:bankpick:{tid}:0")],
            [InlineKeyboardButton("🎲 Добавить 10 случайных", callback_data=f"help:testv2:bankrandom:{tid}")],
            [InlineKeyboardButton("✏️ Редактор вопросов", callback_data=f"help:testv2:qeditlist:{tid}:0")],
            [InlineKeyboardButton("⚙️ Настройки теста", callback_data=f"help:testv2:settings:{tid}")],
            [InlineKeyboardButton("🔒 Опубликовать версию", callback_data=f"help:testv2:publishconfirm:{tid}")],
        ])
    if visible:
        rows.extend([
            [InlineKeyboardButton("👥 Назначить", callback_data=f"help:testv2:assign_template:{tid}")],
            [InlineKeyboardButton("📊 Аналитика", callback_data=f"help:testv2:analytic:{tid}")],
            [InlineKeyboardButton(
                "🗑 Удалить опубликованную версию"
                if int(template.get("is_published") or 0)
                else "🗑 Удалить шаблон",
                callback_data=f"help:testv2:templatedeleteconfirm:{tid}",
            )],
        ])
    rows.append([InlineKeyboardButton("⬅️ К шаблонам", callback_data="help:testv2:drafts:0")])
    return InlineKeyboardMarkup(rows)


def _tv4_bank_question(bank_id: int) -> dict | None:
    with tv2_connect() as con:
        row = con.execute(
            "SELECT * FROM test_question_bank WHERE id=? AND is_active=1",
            (int(bank_id),),
        ).fetchone()
    if not row:
        return None
    item = dict(row)
    item["options"] = _safe_json_loads(item.get("options_json"), [])
    item["correct"] = _safe_json_loads(item.get("correct_json"), [])
    return item


def _tv4_bank_question_text(item: dict) -> str:
    type_name = {
        "single": "один ответ", "multi": "несколько ответов", "open": "открытый",
    }.get(item.get("q_type"), item.get("q_type") or "—")
    lines = [
        f"📚 <b>{escape(item.get('question_text') or 'Без текста')}</b>",
        "",
        f"Категория: <b>{escape(item.get('category') or 'Без категории')}</b>",
        f"Тип: <b>{escape(type_name)}</b>",
        f"Сложность: <b>{int(item.get('difficulty') or 1)}</b>",
        f"Баллы: <b>{float(item.get('points') or 1):g}</b>",
    ]
    if item.get("q_type") == "open":
        lines.append("Эталон: " + escape(tv3_correct_answer_text(item)))
    else:
        correct = set(int(x) for x in (item.get("correct") or []))
        for index, option in enumerate(item.get("options") or []):
            lines.append(
                f"{'✅' if index in correct else '▫️'} {index + 1}. {escape(str(option))}"
            )
    if item.get("explanation"):
        lines.extend(["", "💡 " + escape(str(item["explanation"]))])
    return "\n".join(lines)[:4000]


def tv2_analytics(tid: int) -> dict:
    template = tv2_get_template(tid) or {}
    root = int(template.get("parent_template_id") or tid)
    with tv2_connect() as con:
        row = con.execute(
            """
            SELECT COUNT(*),
                   SUM(CASE WHEN a.status!='assigned' THEN 1 ELSE 0 END),
                   SUM(CASE WHEN a.status IN ('finished','needs_review') THEN 1 ELSE 0 END),
                   SUM(CASE WHEN a.status='expired' THEN 1 ELSE 0 END),
                   AVG(CASE WHEN a.status='finished' THEN a.score_percent END),
                   SUM(CASE WHEN a.passed=1 THEN 1 ELSE 0 END)
            FROM test_assignments a
            JOIN test_templates t ON t.id=a.template_id
            WHERE (t.id=? OR t.parent_template_id=?)
              AND a.admin_deleted_at IS NULL
            """,
            (root, root),
        ).fetchone()
        hard = con.execute(
            """
            SELECT q.question_text,
                   AVG(CASE WHEN ans.is_correct=1 THEN 1.0 ELSE 0.0 END) rate,
                   COUNT(ans.id) cnt
            FROM test_questions q
            JOIN test_templates t ON t.id=q.template_id
            JOIN test_answers ans ON ans.question_id=q.id
            JOIN test_assignments a ON a.id=ans.assignment_id
            WHERE (t.id=? OR t.parent_template_id=?)
              AND q.q_type!='open'
              AND a.admin_deleted_at IS NULL
            GROUP BY q.id
            HAVING cnt>0
            ORDER BY rate ASC
            LIMIT 5
            """,
            (root, root),
        ).fetchall()
    return {
        "assigned": int(row[0] or 0), "started": int(row[1] or 0),
        "completed": int(row[2] or 0), "expired": int(row[3] or 0),
        "avg": float(row[4] or 0), "passed": int(row[5] or 0),
        "hard": [(str(r[0]), float(r[1] or 0), int(r[2] or 0)) for r in hard],
    }


async def _tv4_answer_callback(query):
    try:
        await query.answer()
    except Exception:
        pass


async def cb_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = (update.callback_query.data or "") if update.callback_query else ""
    intercepted = (
        data.startswith("help:testv2:drafts:")
        or data.startswith("help:testv2:templatedelete")
        or data.startswith("help:testv2:resultdelete")
        or data.startswith("help:testv2:qdelete")
        or data == "help:testv2:bank"
        or data.startswith("help:testv2:bankcat:")
        or data.startswith("help:testv2:bankquestion:")
        or data.startswith("help:testv2:bankdelete")
        or data == "help:testv2:resultspeople"
        or data.startswith("help:testv2:resultsperson:")
        or data.startswith("help:testv2:resultsperiod:")
        or data.startswith("help:testv2:results:")
        or data == "help:testv2:overdue"
    )
    if not intercepted:
        return await _test_history_legacy_cb_help(update, context)

    query = update.callback_query
    await _tv4_answer_callback(query)
    if not await tv2_admin_guard(update, context):
        return

    if data.startswith("help:testv2:drafts:"):
        page = int(data.rsplit(":", 1)[-1])
        await query.edit_message_text(
            "🗂 <b>Шаблоны и опубликованные версии</b>\n\n"
            "Удаление убирает тест из рабочих списков. Назначения, ответы и "
            "результаты сотрудников сохраняются в «Моём кабинете», а вопросы — в банке.",
            parse_mode=ParseMode.HTML,
            reply_markup=tv2_kb_drafts(page),
        )
        return

    if data.startswith("help:testv2:templatedeleteconfirm:"):
        tid = int(data.rsplit(":", 1)[-1])
        template = tv2_get_template(tid)
        if not template:
            await query.edit_message_text(
                "Тест не найден.", reply_markup=tv2_kb_drafts(0)
            )
            return
        kind = "опубликованную версию" if int(template.get("is_published") or 0) else "шаблон"
        await query.edit_message_text(
            f"⚠️ Удалить {kind} «{escape(template.get('title') or '')}» из рабочих списков?\n\n"
            "История сотрудников, ответы, результаты и средний балл сохранятся. "
            "Вопросы останутся в банке вопросов.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(
                    "🗑 Да, удалить", callback_data=f"help:testv2:templatedelete:{tid}"
                )],
                [InlineKeyboardButton(
                    "Отмена", callback_data=f"help:testv2:template:{tid}"
                )],
            ]),
        )
        return

    if data.startswith("help:testv2:templatedelete:"):
        tid = int(data.rsplit(":", 1)[-1])
        with tv2_connect() as con:
            con.execute(
                "UPDATE test_templates "
                "SET is_draft_visible=0, deleted_at=COALESCE(deleted_at,?), updated_at=? "
                "WHERE id=?",
                (datetime.utcnow().isoformat(), datetime.utcnow().isoformat(), tid),
            )
        await query.edit_message_text(
            "✅ Тест удалён из рабочих списков. История сотрудников и банк вопросов сохранены.",
            reply_markup=tv2_kb_drafts(0),
        )
        return

    if data.startswith("help:testv2:resultdeleteconfirm:"):
        aid = int(data.rsplit(":", 1)[-1])
        await query.edit_message_text(
            "⚠️ Удалить тестирование из административных отчётов?\n\n"
            "Для сотрудника тест, ответы и результат останутся в «Моём кабинете» "
            "и продолжат учитываться в среднем балле.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(
                    "🗑 Да, убрать из отчётов", callback_data=f"help:testv2:resultdelete:{aid}"
                )],
                [InlineKeyboardButton(
                    "Отмена", callback_data=f"help:testv2:result:{aid}"
                )],
            ]),
        )
        return

    if data.startswith("help:testv2:resultdelete:"):
        aid = int(data.rsplit(":", 1)[-1])
        with tv2_connect() as con:
            con.execute(
                "UPDATE test_assignments SET admin_deleted_at=COALESCE(admin_deleted_at,?) "
                "WHERE id=?",
                (datetime.utcnow().isoformat(), aid),
            )
        await query.edit_message_text(
            "✅ Тестирование убрано из административных отчётов. "
            "История и средний балл сотрудника сохранены.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("⬅️ К аналитике", callback_data="help:testv2:analytics")
            ]]),
        )
        return

    if data.startswith("help:testv2:qdeleteconfirm:"):
        question_id = int(data.rsplit(":", 1)[-1])
        question = tv2_question_by_id(question_id)
        if not question:
            await query.edit_message_text(
                "Вопрос не найден.", reply_markup=tv2_kb_drafts(0)
            )
            return
        await query.edit_message_text(
            "⚠️ Удалить вопрос только из текущего теста?\n\n"
            "Сам вопрос останется в банке и его можно будет добавить в любой новый тест.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(
                    "🗑 Удалить из теста", callback_data=f"help:testv2:qdelete:{question_id}"
                )],
                [InlineKeyboardButton(
                    "Отмена", callback_data=f"help:testv2:qedit:{question_id}"
                )],
            ]),
        )
        return

    if data.startswith("help:testv2:qdelete:"):
        question_id = int(data.rsplit(":", 1)[-1])
        question = tv2_question_by_id(question_id)
        if not question:
            await query.edit_message_text(
                "Вопрос уже удалён из теста.", reply_markup=tv2_kb_drafts(0)
            )
            return
        template_id = int(question["template_id"])
        tv2_delete_question(question_id)
        await query.edit_message_text(
            "✅ Вопрос удалён из текущего теста и сохранён в банке вопросов.",
            reply_markup=tv2_kb_question_list(template_id, 0),
        )
        return

    if data == "help:testv2:bank":
        categories = tv2_bank_categories()
        rows = [[InlineKeyboardButton(
            category, callback_data=f"help:testv2:bankcat:{category}"
        )] for category in categories[:30]]
        rows.extend([
            [InlineKeyboardButton("➕ Добавить вопрос в банк", callback_data="help:testv2:bankadd")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="help:testv2:admin")],
        ])
        await query.edit_message_text(
            "📚 <b>Банк вопросов</b>\n\n"
            "Все вопросы из тестов сохраняются здесь. Удалить вопрос можно только из его карточки в банке.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(rows),
        )
        return

    if data.startswith("help:testv2:bankcat:"):
        category = data.split(":", 3)[-1]
        items = tv2_bank_list(category, limit=100)
        rows = [[InlineKeyboardButton(
            item["question_text"][:55],
            callback_data=f"help:testv2:bankquestion:{int(item['id'])}",
        )] for item in items[:50]]
        rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="help:testv2:bank")])
        await query.edit_message_text(
            f"📚 <b>{escape(category)}</b>\nВопросов: {len(items)}\n\n"
            "Откройте вопрос, чтобы посмотреть или удалить его из банка.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(rows),
        )
        return

    if data.startswith("help:testv2:bankquestion:"):
        bank_id = int(data.rsplit(":", 1)[-1])
        item = _tv4_bank_question(bank_id)
        if not item:
            await query.edit_message_text(
                "Вопрос уже удалён из банка.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("⬅️ В банк", callback_data="help:testv2:bank")
                ]]),
            )
            return
        await query.edit_message_text(
            _tv4_bank_question_text(item),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(
                    "🗑 Удалить из банка", callback_data=f"help:testv2:bankdeleteconfirm:{bank_id}"
                )],
                [InlineKeyboardButton("⬅️ К категориям", callback_data="help:testv2:bank")],
            ]),
        )
        return

    if data.startswith("help:testv2:bankdeleteconfirm:"):
        bank_id = int(data.rsplit(":", 1)[-1])
        await query.edit_message_text(
            "⚠️ Удалить вопрос из банка?\n\n"
            "Копии вопроса в существующих тестах и исторических результатах сотрудников сохранятся.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(
                    "🗑 Да, удалить из банка", callback_data=f"help:testv2:bankdelete:{bank_id}"
                )],
                [InlineKeyboardButton(
                    "Отмена", callback_data=f"help:testv2:bankquestion:{bank_id}"
                )],
            ]),
        )
        return

    if data.startswith("help:testv2:bankdelete:"):
        bank_id = int(data.rsplit(":", 1)[-1])
        now = datetime.utcnow().isoformat()
        with tv2_connect() as con:
            con.execute(
                "UPDATE test_question_bank "
                "SET is_active=0, deleted_at=COALESCE(deleted_at,?), updated_at=? "
                "WHERE id=?",
                (now, now, bank_id),
            )
        await query.edit_message_text(
            "✅ Вопрос удалён из банка. Существующие тесты и результаты не изменены.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("⬅️ В банк", callback_data="help:testv2:bank")
            ]]),
        )
        return

    if data == "help:testv2:resultspeople":
        with tv2_connect() as con:
            people = con.execute(
                """
                SELECT p.id, p.full_name, COUNT(a.id) cnt
                FROM profiles p
                JOIN test_assignments a ON a.profile_id=p.id
                WHERE a.admin_deleted_at IS NULL
                GROUP BY p.id
                ORDER BY p.full_name
                """
            ).fetchall()
        rows = [[InlineKeyboardButton(
            f"{row[1]} · {int(row[2])}",
            callback_data=f"help:testv2:resultsperson:{int(row[0])}",
        )] for row in people[:60]]
        rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="help:testv2:resultsfilters")])
        await query.edit_message_text(
            "👤 Выберите сотрудника:", reply_markup=InlineKeyboardMarkup(rows)
        )
        return

    if data.startswith("help:testv2:resultsperson:"):
        profile_id = int(data.rsplit(":", 1)[-1])
        with tv2_connect() as con:
            rows = con.execute(
                """
                SELECT a.id, t.title, a.status, a.score_percent
                FROM test_assignments a
                JOIN test_templates t ON t.id=a.template_id
                WHERE a.profile_id=? AND a.admin_deleted_at IS NULL
                ORDER BY a.assigned_at DESC
                LIMIT 50
                """,
                (profile_id,),
            ).fetchall()
            person = con.execute(
                "SELECT full_name FROM profiles WHERE id=?", (profile_id,)
            ).fetchone()
        keyboard = [[InlineKeyboardButton(
            f"{row[1]} · {row[2]}{(' · '+str(round(row[3]))+'%') if row[3] is not None else ''}"[:60],
            callback_data=f"help:testv2:result:{int(row[0])}",
        )] for row in rows]
        keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="help:testv2:resultspeople")])
        await query.edit_message_text(
            f"Результаты: {escape(person[0] if person else str(profile_id))}",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return

    if data.startswith("help:testv2:resultsperiod:"):
        days = int(data.rsplit(":", 1)[-1])
        cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
        with tv2_connect() as con:
            rows = con.execute(
                """
                SELECT a.id, p.full_name, t.title, a.status, a.score_percent
                FROM test_assignments a
                JOIN profiles p ON p.id=a.profile_id
                JOIN test_templates t ON t.id=a.template_id
                WHERE COALESCE(a.finished_at,a.assigned_at)>=?
                  AND a.admin_deleted_at IS NULL
                ORDER BY COALESCE(a.finished_at,a.assigned_at) DESC
                LIMIT 60
                """,
                (cutoff,),
            ).fetchall()
        keyboard = [[InlineKeyboardButton(
            f"{row[1]} · {row[2]}{(' · '+str(round(row[4]))+'%') if row[4] is not None else ''}"[:60],
            callback_data=f"help:testv2:result:{int(row[0])}",
        )] for row in rows]
        keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="help:testv2:resultsfilters")])
        await query.edit_message_text(
            f"📅 Результаты за {days} дней: {len(rows)}",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return

    if data.startswith("help:testv2:results:"):
        result_filter = data.rsplit(":", 1)[-1]
        conditions = {
            "failed": "a.status='finished' AND COALESCE(a.passed,0)=0",
            "review": "a.status='needs_review'",
            "expired": "a.status='expired'",
            "passed": "a.status='finished' AND a.passed=1",
        }
        condition = conditions.get(result_filter)
        if condition is None:
            return await _test_history_legacy_cb_help(update, context)
        with tv2_connect() as con:
            rows = con.execute(
                f"""
                SELECT a.id, p.full_name, t.title, a.score_percent
                FROM test_assignments a
                JOIN profiles p ON p.id=a.profile_id
                JOIN test_templates t ON t.id=a.template_id
                WHERE {condition} AND a.admin_deleted_at IS NULL
                ORDER BY COALESCE(a.finished_at,a.assigned_at) DESC
                LIMIT 50
                """
            ).fetchall()
        keyboard = [[InlineKeyboardButton(
            f"{row[1]} · {row[2]}{(' · '+str(round(row[3]))+'%') if row[3] is not None else ''}"[:60],
            callback_data=f"help:testv2:result:{int(row[0])}",
        )] for row in rows]
        keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="help:testv2:resultsfilters")])
        await query.edit_message_text(
            f"Найдено: {len(rows)}", reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    if data == "help:testv2:overdue":
        with tv2_connect() as con:
            rows = con.execute(
                """
                SELECT a.id, p.full_name, t.title, a.due_at
                FROM test_assignments a
                JOIN profiles p ON p.id=a.profile_id
                JOIN test_templates t ON t.id=a.template_id
                WHERE a.status='expired' AND a.admin_deleted_at IS NULL
                ORDER BY a.due_at DESC
                LIMIT 50
                """
            ).fetchall()
        keyboard = [[InlineKeyboardButton(
            f"⌛ {row[1]} · {row[2]}"[:60],
            callback_data=f"help:testv2:result:{int(row[0])}",
        )] for row in rows]
        keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="help:testv2:admin")])
        await query.edit_message_text(
            f"⌛ Просроченные тесты: {len(rows)}",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return

    return await _test_history_legacy_cb_help(update, context)

# =================== END TEST HISTORY + QUESTION BANK V4 ===================

# ===================== FAQ PERSONAL FAVORITES V5 =====================
# Избранное FAQ хранится отдельно для каждого Telegram-пользователя. Оно не
# меняет общую базу вопросов и переживает обновление текста ответа.

FAQ_FAVORITES_V5_BUILD = "FAQ-PERSONAL-FAVORITES-V5-2026-07-22"

_faq_favorites_legacy_db_init = db_init
_faq_favorites_legacy_cb_help = cb_help


def db_init():
    _faq_favorites_legacy_db_init()
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS faq_favorites (
            user_id INTEGER NOT NULL,
            faq_id INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY(user_id, faq_id),
            FOREIGN KEY(faq_id) REFERENCES faq_items(id) ON DELETE CASCADE
        )
        """
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_faq_favorites_user "
        "ON faq_favorites(user_id, created_at DESC)"
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS case_favorites (
            user_id INTEGER NOT NULL,
            case_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY(user_id, case_id)
        )
        """
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_case_favorites_user "
        "ON case_favorites(user_id, created_at DESC)"
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS case_user_industries (
            user_id INTEGER PRIMARY KEY,
            category_key TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    con.commit()
    con.close()
    logger.warning("=== %s ===", FAQ_FAVORITES_V5_BUILD)


def db_faq_is_favorite(user_id: int | None, faq_id: int) -> bool:
    if user_id is None:
        return False
    with sqlite3.connect(DB_PATH) as con:
        row = con.execute(
            "SELECT 1 FROM faq_favorites WHERE user_id=? AND faq_id=?",
            (int(user_id), int(faq_id)),
        ).fetchone()
    return row is not None


def db_faq_toggle_favorite(user_id: int | None, faq_id: int) -> bool:
    if user_id is None:
        return False
    with sqlite3.connect(DB_PATH) as con:
        row = con.execute(
            "SELECT 1 FROM faq_favorites WHERE user_id=? AND faq_id=?",
            (int(user_id), int(faq_id)),
        ).fetchone()
        if row:
            con.execute(
                "DELETE FROM faq_favorites WHERE user_id=? AND faq_id=?",
                (int(user_id), int(faq_id)),
            )
            return False
        con.execute(
            "INSERT INTO faq_favorites(user_id, faq_id, created_at) VALUES(?,?,?)",
            (int(user_id), int(faq_id), datetime.utcnow().isoformat()),
        )
        return True


def db_faq_favorites(user_id: int | None, limit: int = 100) -> list[dict]:
    if user_id is None:
        return []
    with sqlite3.connect(DB_PATH) as con:
        rows = con.execute(
            """
            SELECT f.id, f.question, f.answer
            FROM faq_items f
            JOIN faq_favorites fav ON fav.faq_id=f.id
            WHERE fav.user_id=?
            ORDER BY fav.created_at DESC, f.id DESC
            LIMIT ?
            """,
            (int(user_id), max(1, min(int(limit), 500))),
        ).fetchall()
    return [
        {"id": int(row[0]), "question": row[1], "answer": row[2]}
        for row in rows
    ]


def db_faq_delete(fid: int) -> bool:
    """Удаляет FAQ и явно чистит закладки для БД без включённых FK."""
    with sqlite3.connect(DB_PATH) as con:
        con.execute("DELETE FROM faq_favorites WHERE faq_id=?", (int(fid),))
        cur = con.execute("DELETE FROM faq_items WHERE id=?", (int(fid),))
        return cur.rowcount > 0


def _faq_favorites_pack_pages(items: list[dict]) -> list[list[tuple[dict, str]]]:
    """Версия faq_pack_pages, которая сохраняет вопрос для кнопки избранного."""
    pages: list[list[tuple[dict, str]]] = []
    current: list[tuple[dict, str]] = []
    current_length = 0
    for number, item in enumerate(items, start=1):
        for block in faq_card_html(number, item):
            block_length = len(faq_plain_text(block))
            separator_length = 24 if current else 0
            if current and (
                len(current) >= FAQ_CARDS_PER_PAGE
                or current_length + separator_length + block_length > FAQ_PAGE_TEXT_LIMIT
            ):
                pages.append(current)
                current = []
                current_length = 0
            current.append((item, block))
            current_length += (24 if len(current) > 1 else 0) + block_length
    if current:
        pages.append(current)
    return pages or [[]]


def build_help_faq_menu(user_id: int | None = None) -> tuple[str, InlineKeyboardMarkup]:
    count = len(db_faq_list_full())
    count_line = (
        f"В базе знаний: <b>{count}</b> "
        f"{ru_word_form(count, 'вопрос', 'вопроса', 'вопросов')}"
        if count else "Пока вопросов и ответов нет."
    )
    text = (
        "❓ <b>FAQ</b>\n\n"
        f"{count_line}\n\n"
        "Откройте ответы, воспользуйтесь поиском или сохраните важные вопросы в избранное."
    )
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📚 Ответы на вопросы", callback_data="help:faq:answers:0")],
        [InlineKeyboardButton("⭐ Избранное", callback_data="help:faq:favorites:0")],
        [InlineKeyboardButton("🔎 Найти ответ", callback_data="help:faq:search")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="help:main")],
    ])
    return text, keyboard


def build_help_faq_cards_page(
    items: list[dict],
    page: int = 0,
    *,
    title: str = "📚 Ответы на вопросы",
    subtitle: str | None = None,
    callback_prefix: str = "help:faq:answers",
    show_search: bool = True,
    user_id: int | None = None,
    item_source: str = "all",
) -> tuple[str, InlineKeyboardMarkup]:
    pages = _faq_favorites_pack_pages(items)
    total_pages = max(1, len(pages))
    page = max(0, min(int(page), total_pages - 1))
    page_entries = pages[page]
    page_blocks = [entry[1] for entry in page_entries]

    text_lines = [f"<b>{title}</b>"]
    if subtitle:
        text_lines.extend(["", subtitle])
    if items:
        text_lines.extend([
            "",
            f"Страница <b>{page + 1}</b> из <b>{total_pages}</b> · "
            f"всего: <b>{len(items)}</b> "
            f"{ru_word_form(len(items), 'вопрос', 'вопроса', 'вопросов')}",
            "",
        ])
        text_lines.append("\n\n━━━━━━━━━━━━━━\n\n".join(page_blocks))
    else:
        text_lines.extend(["", "Ничего не найдено."])

    rows: list[list[InlineKeyboardButton]] = []
    seen_ids: set[int] = set()
    for item, _block in page_entries:
        faq_id = int(item["id"])
        if faq_id in seen_ids:
            continue
        seen_ids.add(faq_id)
        question_plain = faq_plain_text(item.get("question")) or "Вопрос"
        label = question_plain if len(question_plain) <= 42 else question_plain[:39] + "…"
        marked = db_faq_is_favorite(user_id, faq_id)
        rows.append([InlineKeyboardButton(
            ("★ " if marked else "☆ ") + label,
            callback_data=f"help:faq:item:{faq_id}:{page}:{item_source}",
        )])

    if total_pages > 1:
        nav_row: list[InlineKeyboardButton] = []
        if page > 0:
            nav_row.append(InlineKeyboardButton(
                "⬅️ Предыдущая", callback_data=f"{callback_prefix}:{page - 1}"
            ))
        nav_row.append(InlineKeyboardButton(f"{page + 1}/{total_pages}", callback_data="noop"))
        if page < total_pages - 1:
            nav_row.append(InlineKeyboardButton(
                "Следующая ➡️", callback_data=f"{callback_prefix}:{page + 1}"
            ))
        rows.append(nav_row)

    if show_search:
        rows.append([InlineKeyboardButton("🔎 Найти ответ", callback_data="help:faq:search")])
    else:
        rows.append([InlineKeyboardButton("📚 Все вопросы", callback_data="help:faq:answers:0")])
        rows.append([InlineKeyboardButton("🔎 Новый поиск", callback_data="help:faq:search")])
    rows.append([InlineKeyboardButton("⭐ Избранное", callback_data="help:faq:favorites:0")])
    rows.append([InlineKeyboardButton("⬅️ В FAQ", callback_data="help:faq")])
    rows.append([InlineKeyboardButton("🏠 В главное меню", callback_data="help:main")])
    return "\n".join(text_lines).rstrip(), InlineKeyboardMarkup(rows)


def build_help_faq_answers_page(
    page: int = 0, user_id: int | None = None,
) -> tuple[str, InlineKeyboardMarkup]:
    return build_help_faq_cards_page(
        db_faq_list_full(), page, callback_prefix="help:faq:answers",
        show_search=True, user_id=user_id, item_source="all",
    )


def build_help_faq_search_page(
    query: str, page: int = 0, user_id: int | None = None,
) -> tuple[str, InlineKeyboardMarkup]:
    items = faq_search_items(query)
    return build_help_faq_cards_page(
        items, page, title="🔎 Результаты поиска",
        subtitle=f"Запрос: <b>{html_lib.escape(query)}</b>",
        callback_prefix="help:faq:search_results", show_search=False,
        user_id=user_id, item_source="search",
    )


def kb_faq_item(
    faq_id: int,
    page: int,
    user_id: int | None,
    back_callback: str,
    source: str = "all",
):
    marked = db_faq_is_favorite(user_id, int(faq_id))
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(
            "★ Убрать из избранного" if marked else "☆ Добавить в избранное",
            callback_data=f"help:faq:favorite:{int(faq_id)}:{int(page)}:{source}",
        )],
        [InlineKeyboardButton("⬅️ К списку вопросов", callback_data=back_callback)],
        [InlineKeyboardButton("⭐ Избранное", callback_data="help:faq:favorites:0")],
        [InlineKeyboardButton("🏠 В главное меню", callback_data="help:main")],
    ])


def _faq_favorites_back_callback(source: str, page: int) -> str:
    if source == "favorites":
        return f"help:faq:favorites:{max(0, int(page))}"
    if source == "search":
        return f"help:faq:search_results:{max(0, int(page))}"
    return f"help:faq:answers:{max(0, int(page))}"


# ===================== CASES CATALOG =====================
# Снимок публичных карточек со страницы https://yandex.ru/routing/cases.
# Длинные статьи открываются отдельной URL-кнопкой; для карточек без
# отдельной статьи используется общий раздел кейсов.

CASES_HOME_URL = "https://yandex.ru/routing/cases"
CASES_WAITING_SEARCH = "cases_waiting_search"
CASES_SEARCH_QUERY = "cases_search_query"
CASES_PAGE_SIZE = 8

CASES_CATEGORY_DEFS = [
    ("all", "Все кейсы"),
    ("home", "Дом, ремонт, сад"),
    ("auto", "Автотовары"),
    ("alcohol", "Алкоголь и табак"),
    ("production", "Производство и промышленность"),
    ("fmcg", "FMCG"),
    ("logistics", "Логистика"),
    ("bakery", "Хлебобулочные изделия"),
    ("ecom", "E-com"),
    ("medicine", "Медицина и фармацевтика"),
    ("services", "Сервисные услуги"),
    ("finance", "Банки и финансы"),
    ("retail", "Ритейл"),
]
CASES_CATEGORY_LABELS = dict(CASES_CATEGORY_DEFS)


def _case(
    case_id: str,
    company: str,
    categories: tuple[str, ...],
    metric: str,
    summary: str,
    url: str = CASES_HOME_URL,
) -> dict:
    if isinstance(categories, str):
        categories = (categories,)
    return {
        "id": case_id,
        "company": company,
        "categories": list(categories),
        "metric": metric,
        "summary": summary,
        "url": url or CASES_HOME_URL,
    }


CASES_DATA = [
    _case(
        "maria", "Мария", ("Дом, ремонт, сад"),
        "−35% затрат на последнюю милю",
        "Утилизировали свой автопарк и ушли от наёмного; доля наёмных машин доходила до 40% всех ТС.",
        "https://yandex.ru/routing/articles/kukhni-mariya/",
    ),
    _case(
        "rossko", "Росско", ("Автотовары"),
        "25% сокращение затрат на логистику",
        "Взяли под контроль показатели и снизили влияние человеческого фактора.",
        "https://yandex.ru/routing/articles/rossko/",
    ),
    _case(
        "novabev", "Новабев", ("Алкоголь и табак"),
        "20% экономии на внутригородской логистике",
        "На 18,3% снизили время работы водителей.",
    ),
    _case(
        "euromebel", "Euromebel", ("Дом, ремонт, сад"),
        "30% экономии на логистике доставки",
        "Организовали централизованное управление доставкой по всей стране.",
        "https://yandex.ru/routing/articles/euromebel/",
    ),
    _case(
        "sibur", "Сибур", ("Производство и промышленность"),
        "350 тыс. перевозок мониторят в год",
        "Время получения статуса доставки сократилось с 2,5 часов до 30 секунд.",
        "https://yandex.ru/routing/articles/sibur/",
    ),
    _case(
        "iceberry", "Айсберри", ("FMCG"),
        "10% снижение пробега",
        "Ввели новую мотивацию водителей и окупили проект в первый год.",
        "https://yandex.ru/routing/articles/iceberry/",
    ),
    _case(
        "skif_cargo", "Скиф-Карго", ("Логистика"),
        "На 20% снизили стоимость перевозки м³/км",
        "До 98% возросло количество своевременных доставок.",
        "https://rutube.ru/video/f8ade0a0b42b599f71e89d021699852d/?r=wd",
    ),
    _case(
        "perekrestok", "Перекрёсток Впрок", ("FMCG"),
        "31 800 ₽ ежедневной экономии на ТС",
        "95% доставок попадают во временные интервалы.",
        "https://yandex.ru/routing/articles/perekrestok/",
    ),
    _case(
        "okraina", "Окраина", ("FMCG"),
        "−10% используемого транспорта",
        "Новых логистов обучают 2 недели вместо прежних 6 месяцев.",
        "https://yandex.ru/routing/articles/okraina/",
    ),
    _case(
        "lavka", "Яндекс Лавка", ("FMCG"),
        "Менее 15 минут дороги курьера до дарксторов",
        "Маршрутизируют двухуровневую логистику: распределительный центр → даркстор → покупатель.",
        "https://yandex.ru/routing/articles/lavka/",
    ),
    _case(
        "askona", "Askona", ("Дом, ремонт, сад"),
        "Рост оценки доставки до 4,65",
        "Снизили долю несогласованных интервалов с 3,6% до 2,5%.",
        "https://yandex.ru/routing/articles/askona/",
    ),
    _case(
        "heidelbergcement", "HeidelbergCement", ("Производство и промышленность"),
        "80 000+ доставок отслеживается через мониторинг",
        "В 2 раза сократили время ожидания и разгрузки.",
        "https://yandex.ru/routing/articles/heidelbergcement/",
    ),
    _case(
        "baltika", "Балтика", ("Алкоголь и табак"),
        "98% поставок отслеживают в реальном времени",
        "Плотность на маршруте выросла на 15%, время обслуживания машин снизилось на 30%.",
        "https://yandex.ru/routing/articles/baltika/",
    ),
    _case(
        "kolomenskoe", "БКК «Коломенский»", ("FMCG", "Хлебобулочные изделия"),
        "800+ торговых точек в едином контуре планирования",
        "Маршрутизируют ночную и раннюю доставку скоропортящейся продукции для 500 машин ежедневно.",
        "https://yandex.ru/routing/articles/kolomenskoe/",
    ),
    _case(
        "simple", "Simple Group", ("Алкоголь и табак"),
        "≈23,4% поставок отслеживают в реальном времени",
        "Плотность на маршруте выросла на 15%, время обслуживания машин снизилось на 30%.",
        "https://yandex.ru/routing/articles/simple/",
    ),
    _case(
        "yandex_market", "Яндекс Маркет", ("E-com"),
        "95% точность попадания во временной интервал доставки",
        "Автоматизация помогла масштабироваться на крупные города и перейти к собственной службе доставки.",
        "https://yandex.ru/routing/articles/yandex-market/",
    ),
    _case(
        "biocodex", "Биокодекс", ("Медицина и фармацевтика"),
        "До 97% контактов из базы успевают посетить представители",
        "Еженедельно планируют 4 500 визитов для 75 медицинских представителей.",
    ),
    _case(
        "apteka_aprel", "Аптеки Апрель", ("Медицина и фармацевтика"),
        "До 99% утилизации транспорта",
        "Время планирования отгрузки снизили в 4 раза.",
        "https://yandex.ru/routing/articles/apteka-aprel/",
    ),
    _case(
        "volga_ice", "Волга Айс", ("FMCG"),
        "До 40 точек на маршруте вместо 25",
        "Сохранили качество — мороженое не тает даже при большем количестве точек.",
        "https://yandex.ru/routing/articles/volga-ice1/",
    ),
    _case(
        "sever_avto", "Север Авто", ("Автотовары"),
        "100% клиентов получают уведомления о статусе доставки",
        "Время планирования сократили в 2 раза.",
        "https://yandex.ru/routing/articles/sever_avto/",
    ),
    _case(
        "gemotest", "Гемотест", ("Медицина и фармацевтика"),
        "Интервал визита 2–3 часа вместо целого дня",
        "Теперь пациенты знают, в какое время ожидать специалиста.",
        "https://yandex.ru/routing/articles/gemotest/",
    ),
    _case(
        "fasco", "Гарден Ритейл Сервис (Фаско)", ("Дом, ремонт, сад"),
        "−7% затрат на логистику при росте цен на транспорт",
        "Сократили планирование маршрутов с 2–3 часов до 40 минут.",
        "https://yandex.ru/routing/articles/fasco/",
    ),
    _case(
        "bulka", "Булка", ("FMCG", "Хлебобулочные изделия"),
        "В 2 раза сократилась протяжённость маршрутов",
        "Автоматизация поддерживает прибыльность бизнеса при резком росте заказов без расширения штата.",
        "https://yandex.ru/routing/articles/bulka/",
    ),
    _case(
        "baton", "Онлайн-булочная «Закажи Батон»", ("FMCG", "Хлебобулочные изделия"),
        "В 2 раза сократилась протяжённость маршрутов",
        "Поддержали рост спроса с 200 до 3 000 заказов в месяц без потери качества.",
        "https://yandex.ru/routing/articles/baton/",
    ),
    _case(
        "vishera", "Вишера-Плюс", ("FMCG"),
        "−13% пробега транспорта",
        "Сократили холостой ход на 2–4 часа в день.",
        "https://yandex.ru/routing/articles/vishera-plus/",
    ),
    _case(
        "love_pirogova", "Любовь Пирогова", ("FMCG"),
        "+70% скорости планирования",
        "Попадают в интервал доставки с вероятностью 95%.",
        "https://yandex.ru/routing/articles/lovepirogova/",
    ),
    _case(
        "expoparts", "Expoparts", ("Автотовары"),
        "Рост с 7–8 до 13–15 заказов в сутки на водителя",
        "Свели к нулю недовозы и внедрили доставку в день заказа.",
        "https://yandex.ru/routing/articles/expoparts/",
    ),
    _case(
        "mariara", "Мария-Ра", ("FMCG"),
        "До 400 км радиус доставки свежих продуктов",
        "Реализуют маршрутизацию тягачей с прицепами и многодневные маршруты.",
        "https://yandex.ru/routing/articles/mariara/",
    ),
    _case(
        "bianca", "BIANCA", ("Сервисные услуги"),
        "До 99% попадания во временной интервал",
        "Планирование маршрутов занимает от 10 минут.",
        "https://yandex.ru/routing/articles/bianca/",
    ),
    _case(
        "petline", "Pet Line", ("FMCG"),
        "В 1,5–2 раза увеличили количество выполняемых заявок",
        "Время на планирование сократили в 6 раз.",
        "https://yandex.ru/routing/articles/petline/",
    ),
    _case(
        "raiffeisen", "Raiffeisen Bank", ("Банки и финансы"),
        "В 2 раза выросла эффективность выездных консультантов",
        "Доля заказов, доставляемых на следующий день, выросла на 13%.",
        "https://yandex.ru/routing/articles/raiffeisen/",
    ),
    _case(
        "gkb52", "Городская клиническая больница № 52", ("Медицина и фармацевтика"),
        "В 2,5 раза сократилась общая длительность маршрута",
        "Маршрутизируют доставку пациентов домой с учётом комфортного времени в пути.",
        "https://yandex.ru/routing/articles/gkb52/",
    ),
    _case(
        "valta", "Валта Пет Продактс", ("FMCG"),
        "Масштабирование на 55 городов без увеличения штата логистов",
        "Время маршрутизации сократили в 4 раза.",
        "https://yandex.ru/routing/articles/valta/",
    ),
    _case(
        "repropark", "ReproПарк", ("Производство и промышленность"),
        "20% снижение времени доставки по Москве",
        "Ввели единый инструмент контроля KPI водителей и аналитики процессов.",
        "https://yandex.ru/routing/articles/repropark/",
    ),
    _case(
        "holodilnik", "Холодильник.ру", ("Дом, ремонт, сад"),
        "−15% затрат на последнюю милю",
        "Повысили доставляемость заказов на 4%.",
        "https://yandex.ru/routing/articles/holodilnik/",
    ),
    _case(
        "santekhnika", "Сантехника Онлайн", ("Дом, ремонт, сад"),
        "В 6 раз сократилось время маршрутизации",
        "Уровень выкупаемости заказов с маршрута вырос на 6–7%.",
        "https://yandex.ru/routing/articles/santekhnika-online/",
    ),
    _case(
        "sunlight", "SUNLIGHT", ("Ритейл"),
        "С 20 до 25 заказов на маршруте одного курьера",
        "Уровень удовлетворённости клиентов по итогам доставки вырос на 4%.",
        "https://yandex.ru/routing/articles/sunlight/",
    ),
    _case(
        "kidsway", "KidsWay", ("Сервисные услуги"),
        "≈15 минут время построения маршрута",
        "Система адаптируется к изменениям расписания и позволяет допланировать разовые заказы.",
        "https://yandex.ru/routing/articles/kidsway/",
    ),
    _case(
        "justfood", "Justfood", ("FMCG"),
        "−17% затрат на доставку",
        "Перешли на новую систему маршрутизации и снизили расходы ещё до полного внедрения.",
        "https://yandex.ru/routing/articles/justfood/",
    ),
]
CASES_BY_ID = {item["id"]: item for item in CASES_DATA}


def db_case_is_favorite(user_id: int | None, case_id: str) -> bool:
    if user_id is None:
        return False
    with sqlite3.connect(DB_PATH) as con:
        row = con.execute(
            "SELECT 1 FROM case_favorites WHERE user_id=? AND case_id=?",
            (int(user_id), str(case_id)),
        ).fetchone()
    return row is not None


def db_case_toggle_favorite(user_id: int | None, case_id: str) -> bool:
    if user_id is None or str(case_id) not in CASES_BY_ID:
        return False
    case_id = str(case_id)
    with sqlite3.connect(DB_PATH) as con:
        row = con.execute(
            "SELECT 1 FROM case_favorites WHERE user_id=? AND case_id=?",
            (int(user_id), case_id),
        ).fetchone()
        if row:
            con.execute(
                "DELETE FROM case_favorites WHERE user_id=? AND case_id=?",
                (int(user_id), case_id),
            )
            return False
        con.execute(
            "INSERT INTO case_favorites(user_id, case_id, created_at) VALUES(?,?,?)",
            (int(user_id), case_id, datetime.utcnow().isoformat()),
        )
        return True


def db_case_favorites(user_id: int | None, limit: int = 100) -> list[dict]:
    if user_id is None:
        return []
    with sqlite3.connect(DB_PATH) as con:
        rows = con.execute(
            """
            SELECT case_id
            FROM case_favorites
            WHERE user_id=?
            ORDER BY created_at DESC, case_id ASC
            LIMIT ?
            """,
            (int(user_id), max(1, min(int(limit), 500))),
        ).fetchall()
    return [
        CASES_BY_ID[row[0]]
        for row in rows
        if row[0] in CASES_BY_ID
    ]


def db_case_get_industry(user_id: int | None) -> str | None:
    if user_id is None:
        return None
    with sqlite3.connect(DB_PATH) as con:
        row = con.execute(
            "SELECT category_key FROM case_user_industries WHERE user_id=?",
            (int(user_id),),
        ).fetchone()
    key = row[0] if row else None
    if key not in CASES_CATEGORY_LABELS or key == "all":
        return None
    return key


def db_case_set_industry(user_id: int | None, category_key: str) -> bool:
    if user_id is None or category_key not in CASES_CATEGORY_LABELS or category_key == "all":
        return False
    with sqlite3.connect(DB_PATH) as con:
        con.execute(
            """
            INSERT INTO case_user_industries(user_id, category_key, updated_at)
            VALUES(?,?,?)
            ON CONFLICT(user_id) DO UPDATE SET
                category_key=excluded.category_key,
                updated_at=excluded.updated_at
            """,
            (int(user_id), category_key, datetime.utcnow().isoformat()),
        )
    return True


def db_case_clear_industry(user_id: int | None) -> bool:
    if user_id is None:
        return False
    with sqlite3.connect(DB_PATH) as con:
        cur = con.execute(
            "DELETE FROM case_user_industries WHERE user_id=?",
            (int(user_id),),
        )
    return cur.rowcount > 0


def _cases_normalize(value: str) -> str:
    value = html_lib.unescape(str(value or "")).casefold().replace("ё", "е")
    return re.sub(r"[^0-9a-zа-я]+", " ", value).strip()


def _cases_token_matches(token: str, search_blob: str) -> bool:
    if token in search_blob:
        return True
    # Учитываем русские окончания: например, «последняя миля» и
    # «последнюю милю» должны находиться одним запросом.
    if len(token) < 3:
        return False
    stem = token[:3] if re.search(r"[а-я]", token) else token[:4]
    return any(part.startswith(stem) for part in search_blob.split())


def cases_search_items(query: str = "", category_key: str = "all") -> list[dict]:
    category = CASES_CATEGORY_LABELS.get(category_key, CASES_CATEGORY_LABELS["all"])
    query_tokens = [token for token in _cases_normalize(query).split() if token]
    result = []
    for item in CASES_DATA:
        if category_key != "all" and category not in item["categories"]:
            continue
        search_blob = _cases_normalize(
            " ".join(
                [
                    item["company"],
                    item["metric"],
                    item["summary"],
                    " ".join(item["categories"]),
                    "кейс логистика доставка маршруты маршрутизация планирование транспорт",
                ]
            )
        )
        if query_tokens and not all(
            _cases_token_matches(token, search_blob) for token in query_tokens
        ):
            continue
        result.append(item)
    return result


def _cases_page(items: list[dict], page: int) -> tuple[list[dict], int, int]:
    total_pages = max(1, (len(items) + CASES_PAGE_SIZE - 1) // CASES_PAGE_SIZE)
    page = max(0, min(int(page), total_pages - 1))
    start = page * CASES_PAGE_SIZE
    return items[start:start + CASES_PAGE_SIZE], page, total_pages


def kb_cases_categories(user_id: int | None = None) -> InlineKeyboardMarkup:
    selected_key = db_case_get_industry(user_id)
    selected_label = CASES_CATEGORY_LABELS.get(selected_key or "", "")
    rows = [[InlineKeyboardButton("📚 Все кейсы", callback_data="help:cases:cat:all:0")]]
    rows.append(
        [
            InlineKeyboardButton(
                f"🎯 Моя отрасль: {selected_label}" if selected_label else "🎯 Моя отрасль",
                callback_data="help:cases:industry",
            )
        ]
    )
    category_buttons = [
        InlineKeyboardButton(label, callback_data=f"help:cases:cat:{key}:0")
        for key, label in CASES_CATEGORY_DEFS
        if key != "all"
    ]
    for index in range(0, len(category_buttons), 2):
        rows.append(category_buttons[index:index + 2])
    rows.extend(
        [
            [InlineKeyboardButton("🔎 Поиск по кейсам", callback_data="help:cases:search")],
            [InlineKeyboardButton("⭐ Избранные кейсы", callback_data="help:cases:favorites:0")],
            [InlineKeyboardButton("⬅️ Главное меню", callback_data="help:main")],
        ]
    )
    return InlineKeyboardMarkup(rows)


def cases_menu_text(user_id: int | None = None) -> str:
    selected_key = db_case_get_industry(user_id)
    selected_label = CASES_CATEGORY_LABELS.get(selected_key or "", "")
    industry_line = (
        f"\n🎯 <b>Моя отрасль:</b> {escape(selected_label)}\n"
        "Кнопка «Моя отрасль» позволяет изменить или убрать фильтр.\n"
        if selected_label
        else "\n🎯 Моя отрасль пока не выбрана.\n"
    )
    return (
        "📚 <b>Кейсы Яндекс Маршрутизации</b>\n\n"
        f"{industry_line}\n"
        "Выберите отрасль или найдите кейс по названию компании, "
        "отрасли и ключевым словам."
    )


def kb_cases_industry_picker(user_id: int | None = None) -> InlineKeyboardMarkup:
    selected_key = db_case_get_industry(user_id)
    rows = []
    for key, label in CASES_CATEGORY_DEFS:
        if key == "all":
            continue
        prefix = "✅ " if key == selected_key else ""
        rows.append(
            [
                InlineKeyboardButton(
                    f"{prefix}{label}",
                    callback_data=f"help:cases:industry:set:{key}",
                )
            ]
        )
    if selected_key:
        rows.append(
            [
                InlineKeyboardButton(
                    "📂 Показать кейсы моей отрасли",
                    callback_data="help:cases:my_cases",
                )
            ]
        )
        rows.append(
            [
                InlineKeyboardButton(
                    "🧹 Убрать мою отрасль",
                    callback_data="help:cases:industry:clear",
                )
            ]
        )
    rows.append([InlineKeyboardButton("⬅️ К кейсам", callback_data="help:cases")])
    return InlineKeyboardMarkup(rows)


def cases_list_text(
    items: list[dict],
    category_key: str = "all",
    page: int = 0,
    query: str = "",
    heading: str | None = None,
    empty_text: str = "Ничего не найдено.",
) -> tuple[str, int, int]:
    page_items, page, total_pages = _cases_page(items, page)
    if heading:
        title = heading
    elif query:
        title = f"🔎 <b>Результаты поиска</b>\nЗапрос: <b>{escape(query)}</b>"
    else:
        label = CASES_CATEGORY_LABELS.get(category_key, CASES_CATEGORY_LABELS["all"])
        title = f"📚 <b>{escape(label)}</b>"
    if not items:
        return f"{title}\n\n{empty_text}", page, total_pages
    lines = [title, f"\nНайдено кейсов: <b>{len(items)}</b>", "\nВыберите компанию:"]
    for item in page_items:
        lines.append(f"• {escape(item['company'])}")
    if total_pages > 1:
        lines.append(f"\nСтраница {page + 1} из {total_pages}")
    return "\n".join(lines), page, total_pages


def cases_my_industry_view(
    user_id: int | None,
    page: int = 0,
    notice: str | None = None,
) -> tuple[str, InlineKeyboardMarkup] | None:
    """Рендерит только кейсы сохранённой отрасли без остальных разделов каталога."""
    selected_key = db_case_get_industry(user_id)
    if not selected_key:
        return None
    label = CASES_CATEGORY_LABELS[selected_key]
    items = cases_search_items(category_key=selected_key)
    list_text, page, _ = cases_list_text(
        items,
        category_key=selected_key,
        page=page,
        heading=f"🎯 <b>Моя отрасль: {escape(label)}</b>",
    )
    if notice:
        list_text = f"{notice}\n\n{list_text}"
    return list_text, kb_cases_list(
        items,
        category_key=selected_key,
        page=page,
        industry=True,
    )


def kb_cases_list(
    items: list[dict],
    category_key: str = "all",
    page: int = 0,
    query: str = "",
    favorites: bool = False,
    industry: bool = False,
) -> InlineKeyboardMarkup:
    page_items, page, total_pages = _cases_page(items, page)
    rows = []
    if industry:
        open_prefix = "help:cases:my_open"
    elif favorites:
        open_prefix = "help:cases:favorites_open"
    elif query:
        open_prefix = "help:cases:search_open"
    else:
        open_prefix = f"help:cases:open:{category_key}"
    for item in page_items:
        if favorites or query:
            callback_data = f"{open_prefix}:{item['id']}:{page}"
        else:
            callback_data = f"{open_prefix}:{item['id']}:{page}"
        rows.append([InlineKeyboardButton(
            f"🏢 {item['company']}",
            callback_data=callback_data,
        )])
    if total_pages > 1:
        nav = []
        if page > 0:
            if industry:
                callback = f"help:cases:my_cases:{page - 1}"
            elif favorites:
                callback = f"help:cases:favorites:{page - 1}"
            else:
                callback = (
                f"help:cases:search_results:{page - 1}"
                if query else f"help:cases:cat:{category_key}:{page - 1}"
                )
            nav.append(InlineKeyboardButton("◀️", callback_data=callback))
        nav.append(InlineKeyboardButton(f"{page + 1} / {total_pages}", callback_data="noop"))
        if page < total_pages - 1:
            if industry:
                callback = f"help:cases:my_cases:{page + 1}"
            elif favorites:
                callback = f"help:cases:favorites:{page + 1}"
            else:
                callback = (
                f"help:cases:search_results:{page + 1}"
                if query else f"help:cases:cat:{category_key}:{page + 1}"
                )
            nav.append(InlineKeyboardButton("▶️", callback_data=callback))
        rows.append(nav)
    if industry:
        rows.append([
            InlineKeyboardButton("🔄 Сменить отрасль", callback_data="help:cases:industry")
        ])
        rows.append([
            InlineKeyboardButton("🧹 Убрать мою отрасль", callback_data="help:cases:industry:clear")
        ])
    else:
        rows.append([InlineKeyboardButton("🔎 Поиск по кейсам", callback_data="help:cases:search")])
        rows.append([
            InlineKeyboardButton(
                "⬅️ К категориям" if favorites or not query else "⬅️ К результатам",
                callback_data=(
                    "help:cases"
                    if favorites or not query else "help:cases:search_results:0"
                ),
            )
        ])
    rows.append([InlineKeyboardButton("🏠 Главное меню", callback_data="help:main")])
    return InlineKeyboardMarkup(rows)


def cases_detail_text(item: dict) -> str:
    categories = ", ".join(item["categories"])
    return (
        f"🏢 <b>{escape(item['company'])}</b>\n"
        f"🏷 Отрасль: <b>{escape(categories)}</b>\n\n"
        f"📊 <b>{escape(item['metric'])}</b>\n\n"
        "✅ <b>Что сделали и основные преимущества:</b>\n"
        f"• {escape(item['summary'])}"
    )


def kb_case_detail(
    item: dict,
    user_id: int | None,
    favorite_context: str,
    back_callback: str,
) -> InlineKeyboardMarkup:
    marked = db_case_is_favorite(user_id, item["id"])
    favorite_parts = favorite_context.split(":")
    favorite_callback = (
        f"help:cases:favorite:{item['id']}:{favorite_parts[0]}:"
        + ":".join(favorite_parts[1:])
    )
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "★ Убрать из избранных кейсов"
                    if marked else "☆ Добавить в избранные кейсы",
                    callback_data=favorite_callback,
                )
            ],
            [InlineKeyboardButton("🔗 Подробнее на сайте", url=item["url"])],
            [InlineKeyboardButton("⬅️ Назад к списку", callback_data=back_callback)],
            [InlineKeyboardButton("📚 К категориям", callback_data="help:cases")],
            [InlineKeyboardButton("🏠 Главное меню", callback_data="help:main")],
        ]
    )


async def handle_cases_search_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    if await deny_no_access(update, context):
        return
    await sync_profile_user_id_from_update(update)
    query_text = (update.message.text or "").strip()
    if len(query_text) < 2:
        await update.message.reply_text("Введите минимум 2 символа для поиска по кейсам.")
        return
    context.user_data[CASES_WAITING_SEARCH] = False
    context.user_data[CASES_SEARCH_QUERY] = query_text
    items = cases_search_items(query_text)
    result_text, _, _ = cases_list_text(items, query=query_text)
    await update.message.reply_text(
        result_text,
        parse_mode=ParseMode.HTML,
        reply_markup=kb_cases_list(items, query=query_text),
        disable_web_page_preview=True,
    )


async def handle_cases_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = (query.data or "") if query else ""
    if not query:
        return
    if await deny_no_access(update, context):
        return
    try:
        await query.answer()
    except Exception:
        pass
    user_id = update.effective_user.id if update.effective_user else None

    if data == "help:cases":
        context.user_data[CASES_WAITING_SEARCH] = False
        context.user_data.pop(CASES_SEARCH_QUERY, None)
        my_industry = cases_my_industry_view(user_id)
        if my_industry:
            list_text, markup = my_industry
            await query.edit_message_text(
                list_text,
                parse_mode=ParseMode.HTML,
                reply_markup=markup,
                disable_web_page_preview=True,
            )
            return
        await query.edit_message_text(
            cases_menu_text(user_id),
            parse_mode=ParseMode.HTML,
            reply_markup=kb_cases_categories(user_id),
            disable_web_page_preview=True,
        )
        return

    if data == "help:cases:industry":
        context.user_data[CASES_WAITING_SEARCH] = False
        selected_key = db_case_get_industry(user_id)
        selected_label = CASES_CATEGORY_LABELS.get(selected_key or "", "")
        if selected_key:
            my_industry = cases_my_industry_view(user_id)
            if my_industry:
                list_text, markup = my_industry
                await query.edit_message_text(
                    list_text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=markup,
                    disable_web_page_preview=True,
                )
                return
        await query.edit_message_text(
            "🎯 <b>Моя отрасль</b>\n\n"
            + (
                f"Сейчас выбрана: <b>{escape(selected_label)}</b>\n"
                "Выберите другую отрасль или уберите текущую.\n"
                if selected_label
                else "Выберите отрасль, чтобы в один клик открывать подходящие кейсы.\n"
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=kb_cases_industry_picker(user_id),
        )
        return

    if data.startswith("help:cases:industry:set:"):
        key = data.rsplit(":", 1)[-1]
        if key not in CASES_CATEGORY_LABELS or key == "all":
            await query.answer("Неизвестная отрасль.", show_alert=True)
            return
        db_case_set_industry(user_id, key)
        label = CASES_CATEGORY_LABELS[key]
        items = cases_search_items(category_key=key)
        list_text, page, _ = cases_list_text(
            items,
            category_key=key,
            heading=f"🎯 <b>Моя отрасль: {escape(label)}</b>",
        )
        await query.edit_message_text(
            f"✅ Отрасль сохранена.\n\n{list_text}",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_cases_list(
                items,
                category_key=key,
                page=page,
                industry=True,
            ),
            disable_web_page_preview=True,
        )
        return

    if data == "help:cases:industry:clear":
        db_case_clear_industry(user_id)
        await query.answer("Фильтр отрасли убран.")
        await query.edit_message_text(
            cases_menu_text(user_id),
            parse_mode=ParseMode.HTML,
            reply_markup=kb_cases_categories(user_id),
            disable_web_page_preview=True,
        )
        return

    if data == "help:cases:my_cases" or data.startswith("help:cases:my_cases:"):
        selected_key = db_case_get_industry(user_id)
        if not selected_key:
            await query.edit_message_text(
                "🎯 <b>Моя отрасль</b>\n\nСначала выберите отрасль.",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_cases_industry_picker(user_id),
            )
            return
        try:
            page = int(data.rsplit(":", 1)[-1]) if len(data.split(":")) > 3 else 0
        except (TypeError, ValueError):
            page = 0
        my_industry = cases_my_industry_view(user_id, page=page)
        if not my_industry:
            await query.edit_message_text(
                cases_menu_text(user_id),
                parse_mode=ParseMode.HTML,
                reply_markup=kb_cases_categories(user_id),
                disable_web_page_preview=True,
            )
            return
        list_text, markup = my_industry
        await query.edit_message_text(
            list_text,
            parse_mode=ParseMode.HTML,
            reply_markup=markup,
            disable_web_page_preview=True,
        )
        return

    if data == "help:cases:search":
        context.user_data[CASES_WAITING_SEARCH] = True
        context.user_data.pop(CASES_SEARCH_QUERY, None)
        await query.edit_message_text(
            "🔎 <b>Поиск по кейсам</b>\n\n"
            "Введите название компании, отрасль или ключевое слово.\n"
            "Например: <code>медицина</code>, <code>последняя миля</code> или <code>маршруты</code>.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("❌ Отмена", callback_data="help:cases")]]
            ),
        )
        return

    if data == "help:cases:favorites" or data.startswith("help:cases:favorites:"):
        context.user_data[CASES_WAITING_SEARCH] = False
        try:
            page = int(data.rsplit(":", 1)[-1])
        except (TypeError, ValueError):
            page = 0
        items = db_case_favorites(user_id)
        list_text, page, _ = cases_list_text(
            items,
            page=page,
            heading="⭐ <b>Избранные кейсы</b>",
            empty_text="Вы ещё не добавили ни одного кейса в избранное.",
        )
        await query.edit_message_text(
            list_text,
            parse_mode=ParseMode.HTML,
            reply_markup=kb_cases_list(items, page=page, favorites=True),
            disable_web_page_preview=True,
        )
        return

    if data.startswith("help:cases:favorite:"):
        parts = data.split(":")
        case_id = parts[3] if len(parts) > 3 else ""
        source = parts[4] if len(parts) > 4 else ""
        item = CASES_BY_ID.get(case_id)
        if not item or source not in ("s", "c", "f", "m"):
            await query.answer("Кейс не найден.", show_alert=True)
            return
        if source == "s":
            try:
                page = int(parts[5]) if len(parts) > 5 else 0
            except (TypeError, ValueError):
                page = 0
            back_callback = f"help:cases:search_results:{page}"
            favorite_context = f"s:{page}"
        elif source == "f":
            try:
                page = int(parts[5]) if len(parts) > 5 else 0
            except (TypeError, ValueError):
                page = 0
            back_callback = f"help:cases:favorites:{page}"
            favorite_context = f"f:{page}"
        elif source == "m":
            try:
                page = int(parts[5]) if len(parts) > 5 else 0
            except (TypeError, ValueError):
                page = 0
            back_callback = f"help:cases:my_cases:{page}"
            favorite_context = f"m:{page}"
        else:
            key = parts[5] if len(parts) > 5 else "all"
            try:
                page = int(parts[6]) if len(parts) > 6 else 0
            except (TypeError, ValueError):
                page = 0
            if key not in CASES_CATEGORY_LABELS:
                key = "all"
            back_callback = f"help:cases:cat:{key}:{page}"
            favorite_context = f"c:{key}:{page}"
        marked = db_case_toggle_favorite(user_id, case_id)
        try:
            await query.edit_message_reply_markup(
                reply_markup=kb_case_detail(
                    item, user_id, favorite_context, back_callback
                )
            )
        except Exception:
            pass
        await query.answer(
            "Кейс добавлен в избранное." if marked
            else "Кейс убран из избранного."
        )
        return

    if data.startswith("help:cases:my_open:"):
        parts = data.split(":")
        case_id = parts[3] if len(parts) > 3 else ""
        try:
            page = int(parts[4]) if len(parts) > 4 else 0
        except (TypeError, ValueError):
            page = 0
        item = CASES_BY_ID.get(case_id)
        if not item:
            await query.answer("Кейс не найден.", show_alert=True)
            return
        await query.edit_message_text(
            cases_detail_text(item),
            parse_mode=ParseMode.HTML,
            reply_markup=kb_case_detail(
                item,
                user_id,
                f"m:{page}",
                f"help:cases:my_cases:{page}",
            ),
            disable_web_page_preview=True,
        )
        return

    if data == "help:cases:search_results" or data.startswith("help:cases:search_results:"):
        query_text = (context.user_data.get(CASES_SEARCH_QUERY) or "").strip()
        if not query_text:
            context.user_data[CASES_WAITING_SEARCH] = True
            await query.edit_message_text(
                "🔎 Введите запрос для поиска по кейсам.",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("❌ Отмена", callback_data="help:cases")]]
                ),
            )
            return
        try:
            page = int(data.rsplit(":", 1)[-1])
        except (TypeError, ValueError):
            page = 0
        items = cases_search_items(query_text)
        result_text, page, _ = cases_list_text(items, page=page, query=query_text)
        await query.edit_message_text(
            result_text,
            parse_mode=ParseMode.HTML,
            reply_markup=kb_cases_list(items, page=page, query=query_text),
            disable_web_page_preview=True,
        )
        return

    if data.startswith("help:cases:cat:"):
        parts = data.split(":")
        key = parts[3] if len(parts) > 3 else "all"
        try:
            page = int(parts[4]) if len(parts) > 4 else 0
        except (TypeError, ValueError):
            page = 0
        if key not in CASES_CATEGORY_LABELS:
            key = "all"
        context.user_data[CASES_WAITING_SEARCH] = False
        context.user_data.pop(CASES_SEARCH_QUERY, None)
        items = cases_search_items(category_key=key)
        list_text, page, _ = cases_list_text(items, category_key=key, page=page)
        await query.edit_message_text(
            list_text,
            parse_mode=ParseMode.HTML,
            reply_markup=kb_cases_list(items, category_key=key, page=page),
            disable_web_page_preview=True,
        )
        return

    if data.startswith("help:cases:search_open:"):
        parts = data.split(":")
        case_id = parts[3] if len(parts) > 3 else ""
        try:
            page = int(parts[4]) if len(parts) > 4 else 0
        except (TypeError, ValueError):
            page = 0
        item = CASES_BY_ID.get(case_id)
        if not item:
            await query.answer("Кейс не найден.", show_alert=True)
            return
        await query.edit_message_text(
            cases_detail_text(item),
            parse_mode=ParseMode.HTML,
            reply_markup=kb_case_detail(
                item, user_id, f"s:{page}", f"help:cases:search_results:{page}"
            ),
            disable_web_page_preview=True,
        )
        return

    if data.startswith("help:cases:favorites_open:"):
        parts = data.split(":")
        case_id = parts[3] if len(parts) > 3 else ""
        try:
            page = int(parts[4]) if len(parts) > 4 else 0
        except (TypeError, ValueError):
            page = 0
        item = CASES_BY_ID.get(case_id)
        if not item:
            await query.answer("Кейс не найден.", show_alert=True)
            return
        await query.edit_message_text(
            cases_detail_text(item),
            parse_mode=ParseMode.HTML,
            reply_markup=kb_case_detail(
                item, user_id, f"f:{page}", f"help:cases:favorites:{page}"
            ),
            disable_web_page_preview=True,
        )
        return

    if data.startswith("help:cases:open:"):
        parts = data.split(":")
        key = parts[3] if len(parts) > 3 else "all"
        case_id = parts[4] if len(parts) > 4 else ""
        try:
            page = int(parts[5]) if len(parts) > 5 else 0
        except (TypeError, ValueError):
            page = 0
        item = CASES_BY_ID.get(case_id)
        if not item:
            await query.answer("Кейс не найден.", show_alert=True)
            return
        await query.edit_message_text(
            cases_detail_text(item),
            parse_mode=ParseMode.HTML,
            reply_markup=kb_case_detail(
                item, user_id, f"c:{key}:{page}", f"help:cases:cat:{key}:{page}"
            ),
            disable_web_page_preview=True,
        )
        return


async def cb_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = (update.callback_query.data or "") if update.callback_query else ""
    if data.startswith("help:cases"):
        return await handle_cases_callback(update, context)
    if not data.startswith("help:faq"):
        return await _faq_favorites_legacy_cb_help(update, context)

    query = update.callback_query
    user_id = update.effective_user.id if update.effective_user else None
    try:
        await query.answer()
    except Exception:
        pass

    if data == "help:faq":
        clear_faq_search_flow(context)
        text, keyboard = build_help_faq_menu(user_id)
        await query.edit_message_text(
            text, parse_mode=ParseMode.HTML, reply_markup=keyboard,
            disable_web_page_preview=True,
        )
        return

    if data == "help:faq:favorites" or data.startswith("help:faq:favorites:"):
        clear_faq_search_flow(context)
        try:
            page = int(data.rsplit(":", 1)[-1])
        except (TypeError, ValueError):
            page = 0
        text, keyboard = build_help_faq_cards_page(
            db_faq_favorites(user_id), page,
            title="⭐ Избранные вопросы",
            callback_prefix="help:faq:favorites",
            show_search=False, user_id=user_id, item_source="favorites",
        )
        await query.edit_message_text(
            text, parse_mode=ParseMode.HTML, reply_markup=keyboard,
            disable_web_page_preview=True,
        )
        return

    if data == "help:faq:answers" or data.startswith("help:faq:answers:") \
            or data.startswith("help:faq:page:"):
        clear_faq_search_flow(context)
        try:
            page = int(data.rsplit(":", 1)[-1])
        except (TypeError, ValueError):
            page = 0
        text, keyboard = build_help_faq_answers_page(page, user_id)
        await query.edit_message_text(
            text, parse_mode=ParseMode.HTML, reply_markup=keyboard,
            disable_web_page_preview=True,
        )
        return

    if data == "help:faq:search":
        clear_faq_search_flow(context)
        context.chat_data[WAITING_FAQ_SEARCH] = True
        await query.edit_message_text(
            "🔎 <b>Поиск по FAQ</b>\n\n"
            "Напишите слово или фразу. Поиск выполняется одновременно по вопросам и ответам.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Отмена", callback_data="help:faq")]
            ]),
            disable_web_page_preview=True,
        )
        return

    if data == "help:faq:search_results" or data.startswith("help:faq:search_results:"):
        clear_faq_search_flow(context, drop_query=False)
        query_text = (context.chat_data.get(FAQ_SEARCH_QUERY) or "").strip()
        if not query_text:
            context.chat_data[WAITING_FAQ_SEARCH] = True
            await query.edit_message_text(
                "🔎 Напишите запрос для поиска по FAQ.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ Отмена", callback_data="help:faq")]
                ]),
            )
            return
        try:
            page = int(data.rsplit(":", 1)[-1])
        except (TypeError, ValueError):
            page = 0
        text, keyboard = build_help_faq_search_page(query_text, page, user_id)
        await query.edit_message_text(
            text, parse_mode=ParseMode.HTML, reply_markup=keyboard,
            disable_web_page_preview=True,
        )
        return

    if data.startswith("help:faq:item:"):
        parts = data.split(":")
        try:
            faq_id = int(parts[3])
            page = max(0, int(parts[4]))
        except (IndexError, TypeError, ValueError):
            await query.answer("Некорректный вопрос", show_alert=True)
            return
        source = parts[5] if len(parts) > 5 else (
            "search" if context.chat_data.get(FAQ_SEARCH_QUERY) else "all"
        )
        if source not in ("all", "search", "favorites"):
            source = "all"
        item = db_faq_get(faq_id)
        if not item:
            await query.edit_message_text(
                "Вопрос не найден (возможно, его удалил администратор).",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("⬅️ В FAQ", callback_data="help:faq")
                ]]),
            )
            return
        back_callback = _faq_favorites_back_callback(source, page)
        await query.edit_message_text(
            f"❓ {item['question']}\n\n{item['answer']}",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_faq_item(faq_id, page, user_id, back_callback, source),
            disable_web_page_preview=True,
        )
        return

    if data.startswith("help:faq:favorite:"):
        parts = data.split(":")
        try:
            faq_id = int(parts[3])
            page = max(0, int(parts[4]))
            source = parts[5] if len(parts) > 5 else "all"
        except (IndexError, TypeError, ValueError):
            await query.answer("Некорректный вопрос", show_alert=True)
            return
        item = db_faq_get(faq_id)
        if not item:
            await query.answer("Вопрос уже удалён", show_alert=True)
            return
        enabled = db_faq_toggle_favorite(user_id, faq_id)
        back_callback = _faq_favorites_back_callback(source, page)
        try:
            await query.edit_message_reply_markup(
                reply_markup=kb_faq_item(faq_id, page, user_id, back_callback, source)
            )
        except Exception:
            pass
        await query.answer("Добавлено в избранное" if enabled else "Удалено из избранного")
        return

    return await _faq_favorites_legacy_cb_help(update, context)

# =================== END FAQ PERSONAL FAVORITES V5 ===================


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

    # callbacks: help
    app.add_handler(CallbackQueryHandler(cb_help, pattern=r"^(help:|noop)"))

    # employee chat membership sync + welcome
    app.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, on_new_members))
    app.add_handler(MessageHandler(filters.StatusUpdate.LEFT_CHAT_MEMBER, on_left_member))


    # document upload
    app.add_handler(MessageHandler(filters.Document.ALL, on_document))

    # broadcast media (photo/video)
    app.add_handler(MessageHandler(filters.PHOTO, on_photo))
    app.add_handler(MessageHandler(filters.VIDEO, on_video))

    # text input
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    # schedule checker
    app.job_queue.run_repeating(check_and_send_jobs, interval=60, first=10, name="meetings_checker")

    logger.warning(
        "=== BOT BUILD: %s | FILE: %s | DB: %s ===",
        BUILD_VERSION,
        os.path.abspath(__file__),
        os.path.abspath(DB_PATH),
    )
    try:
        app.run_polling(allowed_updates=Update.ALL_TYPES)
    except Exception as e:
        logger.exception("run_polling crashed: %s", e)
        raise

if __name__ == "__main__":
    main()
