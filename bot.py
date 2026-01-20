import os
import asyncio
import logging
import random
import re
import sqlite3
from datetime import datetime, date, timedelta

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder

from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ----------------- ЛОГИ -----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("tg-bot")

# ----------------- ENV -----------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
ZOOM_URL = os.getenv("ZOOM_URL")
DB_PATH = os.getenv("DB_PATH", "bot.db")

if not BOT_TOKEN:
    raise RuntimeError("Переменная окружения BOT_TOKEN не задана")
if not ZOOM_URL:
    raise RuntimeError("Переменная окружения ZOOM_URL не задана")

# Таймзона
TZ = "Europe/Moscow"

# ----------------- DB -----------------
def db_init():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    # Отмена "сегодняшней" стандартной планёрки (если сегодня ПН/СР/ПТ)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS standup_state (
            standup_date TEXT PRIMARY KEY,
            canceled INTEGER NOT NULL DEFAULT 0,
            reason TEXT,
            reschedule_date TEXT
        )
    """)

    # Чаты для рассылки (/setchat)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS standup_chats (
            chat_id INTEGER PRIMARY KEY,
            added_at TEXT NOT NULL
        )
    """)

    # Переносы: из какой даты в какую, и отправлено ли уведомление в новую дату
    cur.execute("""
        CREATE TABLE IF NOT EXISTS standup_reschedules (
            original_date TEXT PRIMARY KEY,
            new_date TEXT NOT NULL,
            created_at TEXT NOT NULL,
            sent INTEGER NOT NULL DEFAULT 0
        )
    """)

    con.commit()
    con.close()


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


def db_get_due_reschedules(target_day: date) -> list[tuple[str, str]]:
    """
    Возвращает [(original_date_iso, new_date_iso), ...] для переносов,
    которые должны быть отправлены сегодня (new_date=today, sent=0).
    """
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        SELECT original_date, new_date
        FROM standup_reschedules
        WHERE sent=0 AND new_date = ?
        ORDER BY original_date ASC
    """, (target_day.isoformat(),))
    rows = cur.fetchall()
    con.close()
    return [(r[0], r[1]) for r in rows]


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


# ----------------- FSM -----------------
class RescheduleFSM(StatesGroup):
    waiting_for_date = State()


# ----------------- ТЕКСТЫ -----------------
DAY_RU = {
    0: "понедельник",
    2: "среда",
    4: "пятница",
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

def build_text(
    today_d: date,
    rescheduled_from: list[date] | None = None,
) -> str:
    greet = random.choice(GREETINGS)
    dow = today_label_ru(today_d)

    extra = ""
    if rescheduled_from:
        items = ", ".join(x.strftime("%d.%m.%y") for x in rescheduled_from)
        extra = (
            f"\n\n📌 <b>Также сегодня пройдёт перенесённая планёрка</b> (перенос(ы) с дат: {items})."
        )

    return (
        f"{greet}\n\n"
        f"Сегодня <b>{dow}</b> 🗓️{extra}\n\n"
        f"Планёрка стартует через <b>15 минут</b> — в <b>09:30 (МСК)</b> ⏰\n\n"
        f'👉 <a href="{ZOOM_URL}">Присоединиться к Zoom</a>\n\n'
        f"Если нужно — можно отменить/перенести ниже 👇"
    )


# ----------------- КЛАВИАТУРЫ -----------------
def kb_cancel_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="Отменить/перенести планёрку 🧩", callback_data="cancel:open")
    kb.adjust(1)
    return kb.as_markup()

def kb_cancel_options():
    kb = InlineKeyboardBuilder()
    kb.button(text="1) Нет срочных тем 💤", callback_data="cancel:reason:no_topics")
    kb.button(text="2) Технические причины 🛠️", callback_data="cancel:reason:tech")
    kb.button(text="3) Перенести на другой день 📆", callback_data="cancel:reason:move")
    kb.button(text="4) Не отменять ✅", callback_data="cancel:close")
    kb.adjust(1)
    return kb.as_markup()

def next_mon_wed_fri(from_d: date, count=3):
    res = []
    d = from_d + timedelta(days=1)
    while len(res) < count:
        if d.weekday() in (0, 2, 4):
            res.append(d)
        d += timedelta(days=1)
    return res

def kb_reschedule_dates(from_d: date):
    kb = InlineKeyboardBuilder()
    options = next_mon_wed_fri(from_d, count=3)
    for d in options:
        label = f"{DAY_RU.get(d.weekday(), '')[:2].upper()} {d.strftime('%d.%m.%y')}"
        kb.button(text=label, callback_data=f"reschedule:pick:{d.strftime('%d.%m.%y')}")
    kb.button(text="Ввести дату (ДД.ММ.ГГ) ✍️", callback_data="reschedule:manual")
    kb.button(text="Назад ↩️", callback_data="cancel:open")
    kb.adjust(1)
    return kb.as_markup()


# ----------------- ПРОВЕРКА АДМИНА -----------------
async def is_admin(bot: Bot, chat_id: int, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id, user_id)
        return member.status in ("administrator", "creator")
    except Exception:
        return False


# ----------------- HELPERS -----------------
def parse_ddmmyy_to_date(s: str) -> date:
    dd, mm, yy = s.split(".")
    return date(int("20" + yy), int(mm), int(dd))

def date_to_ddmmyy(d: date) -> str:
    return d.strftime("%d.%m.%y")


# ----------------- РАССЫЛКА В 09:15 (ЕДИНАЯ) -----------------
async def send_915_notification(bot: Bot):
    """
    Единая отправка в 09:15 МСК каждый день:
      - если сегодня ПН/СР/ПТ и не отменено -> стандарт
      - если сегодня есть переносы (new_date=today) -> переносы
      - если и то, и то -> одно объединённое сообщение (без дублей)
    """
    today_d = datetime.now().date()

    chat_ids = db_list_chats()
    if not chat_ids:
        logger.warning("No chats for notifications. Add via /setchat.")
        return

    # что должно уйти сегодня?
    weekday_due = today_d.weekday() in (0, 2, 4)
    state = db_get_state(today_d)
    standard_due = weekday_due and state["canceled"] != 1

    due_reschedules = db_get_due_reschedules(today_d)  # [(orig_iso, new_iso)]
    reschedule_due = len(due_reschedules) > 0

    if not standard_due and not reschedule_due:
        logger.info("09:15: nothing to send today (%s)", today_d.isoformat())
        return

    # если есть переносы — собираем даты-источники
    resched_from_dates: list[date] = []
    resched_original_isos: list[str] = []
    if reschedule_due:
        for orig_iso, _new_iso in due_reschedules:
            resched_original_isos.append(orig_iso)
            try:
                resched_from_dates.append(date.fromisoformat(orig_iso))
            except Exception:
                pass

    # одно сообщение:
    # - если есть переносы, вшиваем их в текст (и для случая "только переносы", и для "и то и то")
    text = build_text(
        today_d=today_d,
        rescheduled_from=resched_from_dates if reschedule_due else None,
    )

    for chat_id in chat_ids:
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode="HTML",
                disable_web_page_preview=True,
                reply_markup=kb_cancel_menu(),
            )
        except Exception as e:
            logger.exception("Cannot send 09:15 notification to chat_id=%s: %s", chat_id, e)

    # отметим переносы отправленными (чтобы завтра/после рестарта не повторились)
    if reschedule_due:
        db_mark_reschedules_sent(resched_original_isos)

    logger.info(
        "09:15 sent to %d chats. standard_due=%s reschedules=%d",
        len(chat_ids), standard_due, len(resched_original_isos)
    )


# ----------------- ROUTER -----------------
router = Router()

@router.message(Command("ping"))
async def ping(message: Message):
    await message.answer("pong 🏓")

@router.message(Command("setchat"))
async def setchat(message: Message, bot: Bot):
    if message.chat.type == "private":
        await message.answer("Эта команда работает только в групповом чате.")
        return
    if not await is_admin(bot, message.chat.id, message.from_user.id):
        await message.answer("Только администраторы могут назначить чат для уведомлений.")
        return

    db_add_chat(message.chat.id)
    await message.answer("✅ Готово! Этот чат добавлен для уведомлений о планёрке.")

@router.message(Command("unsetchat"))
async def unsetchat(message: Message, bot: Bot):
    if message.chat.type == "private":
        await message.answer("Эта команда работает только в групповом чате.")
        return
    if not await is_admin(bot, message.chat.id, message.from_user.id):
        await message.answer("Только администраторы могут отключить уведомления.")
        return

    db_remove_chat(message.chat.id)
    await message.answer("🧹 Этот чат убран из рассылки уведомлений.")

@router.message(Command("chats"))
async def chats(message: Message, bot: Bot):
    if not await is_admin(bot, message.chat.id, message.from_user.id):
        await message.answer("Только администраторы.")
        return

    ids = db_list_chats()
    if not ids:
        await message.answer("Список чатов пуст. Добавь чат командой /setchat.")
        return

    await message.answer("Чаты для уведомлений:\n" + "\n".join(str(i) for i in ids))

@router.message(Command("test915"))
async def test915(message: Message, bot: Bot):
    if not await is_admin(bot, message.chat.id, message.from_user.id):
        await message.answer("Недостаточно прав.")
        return
    await send_915_notification(bot)
    await message.answer("Ок, отправил тестовую 09:15-рассылку (по правилам на сегодня).")


@router.callback_query(F.data == "cancel:open")
async def cancel_open(cb: CallbackQuery, bot: Bot):
    if not cb.message:
        return
    if not await is_admin(bot, cb.message.chat.id, cb.from_user.id):
        await cb.answer("Только администраторы могут отменять/переносить.", show_alert=True)
        return

    await cb.message.edit_reply_markup(reply_markup=kb_cancel_options())
    await cb.answer()

@router.callback_query(F.data == "cancel:close")
async def cancel_close(cb: CallbackQuery, bot: Bot):
    if not cb.message:
        return
    if not await is_admin(bot, cb.message.chat.id, cb.from_user.id):
        await cb.answer("Только администраторы.", show_alert=True)
        return

    await cb.message.edit_reply_markup(reply_markup=None)
    await cb.answer("Ок, не отменяем ✅")

@router.callback_query(F.data.startswith("cancel:reason:"))
async def cancel_reason(cb: CallbackQuery, bot: Bot, state: FSMContext):
    if not cb.message:
        return
    if not await is_admin(bot, cb.message.chat.id, cb.from_user.id):
        await cb.answer("Только администраторы.", show_alert=True)
        return

    reason_key = cb.data.split(":")[-1]
    today = datetime.now().date()

    if reason_key == "no_topics":
        db_set_canceled(today, "Нет срочных тем для обсуждения")
        await cb.message.edit_reply_markup(reply_markup=None)
        await bot.send_message(
            cb.message.chat.id,
            "✅ Планёрка сегодня отменена.\nПричина: нет срочных тем для обсуждения 💤",
        )
        await cb.answer("Отменено.")
        return

    if reason_key == "tech":
        db_set_canceled(today, "Перенос по техническим причинам")
        await cb.message.edit_reply_markup(reply_markup=None)
        await bot.send_message(
            cb.message.chat.id,
            "✅ Планёрка сегодня отменена/перенесена.\nПричина: технические причины 🛠️",
        )
        await cb.answer("Ок.")
        return

    if reason_key == "move":
        await cb.message.edit_reply_markup(reply_markup=kb_reschedule_dates(today))
        await cb.answer("Выберите дату переноса 📆")
        return

@router.callback_query(F.data.startswith("reschedule:pick:"))
async def reschedule_pick(cb: CallbackQuery, bot: Bot):
    if not cb.message:
        return
    if not await is_admin(bot, cb.message.chat.id, cb.from_user.id):
        await cb.answer("Только администраторы.", show_alert=True)
        return

    picked = cb.data.split(":")[-1]  # dd.mm.yy
    today = datetime.now().date()

    try:
        new_d = parse_ddmmyy_to_date(picked)
    except Exception:
        await cb.answer("Не смог распознать дату.", show_alert=True)
        return

    # 1) отменяем сегодня
    db_set_canceled(today, "Перенос на другой день", reschedule_date=picked)

    # 2) сохраняем перенос (отправится автоматически в 09:15 выбранного дня)
    db_upsert_reschedule(today, new_d)

    await cb.message.edit_reply_markup(reply_markup=None)
    await bot.send_message(
        cb.message.chat.id,
        f"✅ Планёрка сегодня перенесена.\nНовая дата: {picked} 📌\n"
        f"Уведомление придёт в {picked} в 09:15 (МСК).",
    )
    await cb.answer("Перенесено.")

@router.callback_query(F.data == "reschedule:manual")
async def reschedule_manual(cb: CallbackQuery, bot: Bot, state: FSMContext):
    if not cb.message:
        return
    if not await is_admin(bot, cb.message.chat.id, cb.from_user.id):
        await cb.answer("Только администраторы.", show_alert=True)
        return

    await state.set_state(RescheduleFSM.waiting_for_date)
    await cb.answer()
    await cb.message.reply("Введите дату переноса в формате ДД.ММ.ГГ (например 22.01.26):")

@router.message(RescheduleFSM.waiting_for_date)
async def reschedule_manual_input(message: Message, bot: Bot, state: FSMContext):
    if not await is_admin(bot, message.chat.id, message.from_user.id):
        await message.answer("Только администраторы могут переносить планёрку.")
        await state.clear()
        return

    raw = (message.text or "").strip()
    if not re.fullmatch(r"\d{2}\.\d{2}\.\d{2}", raw):
        await message.answer("Неверный формат. Нужно ДД.ММ.ГГ (например 22.01.26).")
        return

    try:
        new_d = parse_ddmmyy_to_date(raw)
    except Exception:
        await message.answer("Похоже, такой даты не существует. Попробуйте ещё раз.")
        return

    today = datetime.now().date()

    db_set_canceled(today, "Перенос на другой день", reschedule_date=raw)
    db_upsert_reschedule(today, new_d)

    await message.answer(
        f"✅ Ок, перенесли планёрку.\nНовая дата: {raw} 📌\n"
        f"Уведомление придёт в {raw} в 09:15 (МСК)."
    )
    await state.clear()


# ----------------- MAIN -----------------
async def main():
    db_init()

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    scheduler = AsyncIOScheduler(timezone=TZ)

    # ЕДИНАЯ рассылка каждый день в 09:15 (МСК)
    scheduler.add_job(
        send_915_notification,
        trigger=CronTrigger(hour=9, minute=15, timezone=TZ),
        args=[bot],
        id="standup_915",
        replace_existing=True,
        misfire_grace_time=60 * 60,
    )

    scheduler.start()
    logger.info("Scheduler started (%s). Job: every day 09:15", TZ)

    logger.info("Bot started (polling)")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
