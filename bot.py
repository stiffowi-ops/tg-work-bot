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

# -------------------------------------------------
#                НАСТРОЙКИ
# -------------------------------------------------

load_dotenv(dotenv_path=Path(__file__).with_name(".env"), override=True)

BOT_TOKEN = os.getenv("BOT_TOKEN")
DAILY_FACT_TIME_STR = os.getenv("DAILY_FACT_TIME", "09:10")
STANDUP_REMINDER_TIME_STR = os.getenv("STANDUP_REMINDER_TIME", "09:00")
STANDUP_MEETING_TIME_STR = os.getenv("STANDUP_MEETING_TIME", "09:30")
MOVIE_RECOMMEND_TIME_STR = os.getenv("MOVIE_RECOMMEND_TIME", "18:00")
WEEKLY_SUMMARY_TIME_STR = os.getenv("WEEKLY_SUMMARY_TIME", "17:00")

TZ_MSK = tz.gettz("Europe/Moscow")

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

_current_quiz: dict[int, dict] = {}
_scheduled_chats: set[int] = set()
_tracked_participants: dict[int, dict[int, str]] = {}
RU_HOLIDAYS = holidays.Russia()

_weekly_scores: dict[str, dict[str, dict[str, int]]] = {}
SCORES_FILE = Path(__file__).with_name("quiz_scores.json")

# -------------------------------------------------
#           ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# -------------------------------------------------

def parse_hhmm(hhmm: str) -> time:
    h, m = hhmm.split(":")
    return time(hour=int(h), minute=int(m), tzinfo=TZ_MSK)

def now_msk() -> datetime:
    return datetime.now(tz=TZ_MSK)

def get_current_week_id(dt: datetime) -> str:
    iso_year, iso_week, _ = dt.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"

def load_weekly_scores() -> None:
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
    except Exception:
        _weekly_scores = {}

def save_weekly_scores() -> None:
    try:
        with SCORES_FILE.open("w", encoding="utf-8") as f:
            json.dump(_weekly_scores, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def update_weekly_scores(chat_id: int, participants_ids: set[int], answers: list[dict]) -> None:
    if not participants_ids:
        return
    now = now_msk()
    week_id = get_current_week_id(now)
    chat_key = str(chat_id)
    chat_weeks = _weekly_scores.setdefault(chat_key, {})
    week_scores = chat_weeks.setdefault(week_id, {})

    answers_by_uid: dict[int, dict] = {a["uid"]: a for a in answers}

    for uid in participants_ids:
        uid_key = str(uid)
        current_score = week_scores.get(uid_key, 0)
        ans = answers_by_uid.get(uid)
        if ans:
            if ans.get("ok"):
                current_score += 1
        else:
            current_score -= 1
        week_scores[uid_key] = current_score

    save_weekly_scores()

def get_on_this_day_fact(dt: datetime):
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

        for tag in ("<b>", "</b>", "<i>", "</i>", "<br>", "</br>"):
            text = text.replace(tag, "")
        text_without_year = text.replace(str(year), "***").replace(f"в {year}", "в ***")
        return text_without_year, year
    except Exception:
        return None, None

def generate_year_options(correct_year: str) -> list[str]:
    correct = int(correct_year)
    options = {correct}
    while len(options) < 4:
        y = correct + random.randint(-50, 50)
        options.add(y)
    options = list(options)
    random.shuffle(options)
    return [str(y) for y in options]

def get_ru_holiday_name(dt: datetime):
    try:
        name = RU_HOLIDAYS.get(dt.date())
        if not name:
            return None
        return name if isinstance(name, str) else ", ".join(name)
    except Exception:
        return None

# -------------------------------------------------
#         УТИЛИТЫ И ДОПОЛНИТЕЛЬНЫЕ ФУНКЦИИ
# -------------------------------------------------

async def is_user_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    chat = update.effective_chat
    user = update.effective_user
    if not chat or not user:
        return False
    try:
        member = await context.bot.get_chat_member(chat.id, user.id)
    except Exception:
        return False
    return member.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER)

def remember_tracked_user(chat_id: int, user) -> None:
    if not user or user.is_bot:
        return
    username = (user.username or "").lower()
    if not username or username not in WATCHED_USERNAMES:
        return
    chat_users = _tracked_participants.setdefault(chat_id, {})
    chat_users[user.id] = user.mention_html()

# -------------------------------------------------
#                   ВИКТОРИНА
# -------------------------------------------------

async def quiz_timeout_job(context: ContextTypes.DEFAULT_TYPE):
    job = context.job
    chat_id = job.chat_id
    data = job.data or {}
    message_id = data.get("message_id")

    quiz = _current_quiz.get(chat_id)
    if not quiz or quiz.get("message_id") != message_id:
        return

    # Удаляем кнопки
    try:
        await context.bot.edit_message_reply_markup(
            chat_id=chat_id,
            message_id=message_id,
            reply_markup=None
        )
    except Exception:
        pass

    correct_year = quiz["correct_year"]
    answers = quiz.get("answers", [])
    winner = quiz.get("winner")

    participants_map = _tracked_participants.get(chat_id, {})
    participants_ids = set(participants_map.keys())
    answered_ids = {a["uid"] for a in answers}
    not_answered_ids = participants_ids - answered_ids
    not_answered_mentions = [participants_map[uid] for uid in not_answered_ids]

    update_weekly_scores(chat_id, participants_ids, answers)

    max_listed = 10
    listed_not_answered = not_answered_mentions[:max_listed]
    suffix = " и другие" if len(not_answered_mentions) > max_listed else ""

    if not answers:
        text = (
            "⏰ Время вышло. Никто не ответил.\n\n"
            f"Правильный ответ: <b>{correct_year}</b>"
        )
        if listed_not_answered:
            text += "\n😴 Не ответили: " + ", ".join(listed_not_answered) + suffix

        await context.bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML)
        _current_quiz.pop(chat_id, None)
        return

    incorrect_mentions = [
        a["mention"] for a in answers if not a["ok"]
    ]

    lines = [
        f"⏰ Итоги викторины!\nПравильный ответ: <b>{correct_year}</b>"
    ]

    if winner:
        lines.append(f"🥇 Первый правильный ответ: {winner['mention']}")
    else:
        lines.append("❌ Нет правильных ответов.")

    if incorrect_mentions:
        lines.append("🙃 Неверно ответили: " + ", ".join(incorrect_mentions))

    if listed_not_answered:
        lines.append("😴 Не ответили: " + ", ".join(listed_not_answered) + suffix)

    await context.bot.send_message(
        chat_id=chat_id,
        text="\n\n".join(lines),
        parse_mode=ParseMode.HTML
    )

    _current_quiz.pop(chat_id, None)

async def handle_quiz_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = query.from_user
    chat_id = query.message.chat_id
    message_id = query.message.message_id
    selected_year = query.data

    remember_tracked_user(chat_id, user)

    quiz = _current_quiz.get(chat_id)
    if not quiz or quiz["message_id"] != message_id:
        await query.answer("Викторина завершена", show_alert=False)
        return

    if now_msk() > quiz["deadline"]:
        await query.answer("⏰ Время вышло", show_alert=True)
        return

    answered = quiz.setdefault("answered_users", set())
    if user.id in answered:
        await query.answer("Вы уже отвечали", show_alert=False)
        return

    correct = selected_year == quiz["correct_year"]
    answered.add(user.id)
    quiz["answers"].append({
        "uid": user.id,
        "mention": user.mention_html(),
        "year": selected_year,
        "ts": now_msk(),
        "ok": correct,
    })

    if correct and quiz.get("winner") is None:
        quiz["winner"] = {"uid": user.id, "mention": user.mention_html()}
        await query.answer("✅ Верно! Вы — первый!", show_alert=True)
    elif correct:
        await query.answer("✅ Верно!", show_alert=False)
    else:
        await query.answer(f"❌ Неверно ({selected_year})", show_alert=False)

async def daily_fact_job(context: ContextTypes.DEFAULT_TYPE):
    chat_id = context.job.chat_id
    today = now_msk()

    holiday = get_ru_holiday_name(today)
    global _last_holiday_congrats_date
    if holiday and _last_holiday_congrats_date != today.date():
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"🎉 Сегодня праздник: *{holiday}*! 🇷🇺",
                parse_mode=ParseMode.MARKDOWN
            )
            _last_holiday_congrats_date = today.date()
        except Exception:
            pass

    fact_text, correct_year = get_on_this_day_fact(today)
    if fact_text and correct_year:
        options = generate_year_options(correct_year)
        keyboard = [[InlineKeyboardButton(y, callback_data=y)] for y in options]
        msg = await context.bot.send_message(
            chat_id=chat_id,
            text=(
                "📚 Интересный факт:\n\n"
                f"{fact_text}\n\n"
                "🔍 В каком году произошло событие?\n"
                "⏳ На ответ — 5 минут."
            ),
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.HTML
        )

        _current_quiz[chat_id] = {
            "message_id": msg.message_id,
            "correct_year": correct_year,
            "answered_users": set(),
            "answers": [],
            "winner": None,
            "deadline": now_msk() + timedelta(minutes=5),
        }

        context.application.job_queue.run_once(
            quiz_timeout_job,
            when=timedelta(minutes=5),
            chat_id=chat_id,
            name=f"quiz_timeout_{chat_id}_{msg.message_id}",
            data={"message_id": msg.message_id},
        )
    else:
        await context.bot.send_message(chat_id, "📚 Факт не найден сегодня 🙂")

# -------------------------------------------------
#                    ПЛАНЁРКА
# -------------------------------------------------

async def send_standup_reminder(bot, chat_id: int):
    try:
        await bot.send_message(
            chat_id=chat_id,
            text=f"🌅 Доброе утро! Сегодня в {STANDUP_MEETING_TIME_STR} — планёрка."
        )
    except Exception:
        pass

async def standup_reminder_job(context: ContextTypes.DEFAULT_TYPE):
    chat_id = context.job.chat_id
    await send_standup_reminder(context.bot, chat_id)

# -------------------------------------------------
#       РЕКОМЕНДАЦИЯ ФИЛЬМА (КИНОПОИСК)
# -------------------------------------------------

def kino_request(params: dict):
    if not KINOPOISK_API_KEY:
        return None
    url = "https://api.kinopoisk.dev/v1.4/movie"
    headers = {
        "X-API-KEY": KINOPOISK_API_KEY,
        "Accept": "application/json",
        "User-Agent": "tg-work-bot",
    }
    try:
        r = requests.get(url, headers=headers, params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None

def pick_movie_from_docs(docs: list[dict]):
    if not docs:
        return None
    return random.choice(docs)

def fetch_movie_recommendation():
    year = now_msk().year
    params_new = {
        "page": 1,
        "limit": 20,
        "type": "movie",
        "year": f"{year-1}-{year}",
        "rating.kp": "6-10",
        "sortField": "year",
        "sortType": -1,
    }
    data = kino_request(params_new)
    docs = (data or {}).get("docs") or []
    movie = pick_movie_from_docs(docs)
    if movie:
        return movie

    params_top = {
        "page": 1,
        "limit": 50,
        "type": "movie",
        "rating.kp": "7-10",
        "votes.kp": "10000-100000000",
        "sortField": "rating.kp",
        "sortType": -1,
    }
    data = kino_request(params_top)
    return pick_movie_from_docs((data or {}).get("docs") or [])

def build_movie_message(movie: dict):
    title = movie.get("name") or movie.get("alternativeName") or "Фильм"
    rating = (movie.get("rating") or {}).get("kp")
    description = movie.get("description") or "Описание отсутствует."
    if len(description) > 500:
        description = description[:500] + "..."

    kp_id = movie.get("id") or movie.get("kinopoiskId")
    kp_url = f"https://www.kinopoisk.ru/film/{kp_id}/" if kp_id else "https://www.kinopoisk.ru"

    poster = movie.get("poster") or {}
    poster_url = poster.get("url") or poster.get("previewUrl")

    text = (
        f"<b>{html.escape(title)}</b>\n\n"
        f"⭐ Оценка: <b>{rating}</b>\n\n"
        f"{html.escape(description)}\n\n"
        f"🔗 <a href=\"{html.escape(kp_url)}\">Смотреть на Кинопоиске</a>"
    )

    return text, poster_url

async def movie_recommendation_job(context: ContextTypes.DEFAULT_TYPE):
    chat_id = context.job.chat_id
    movie = fetch_movie_recommendation()
    if not movie:
        await context.bot.send_message(chat_id, "🎬 Рекомендация недоступна сегодня.")
        return

    text, poster_url = build_movie_message(movie)
    if poster_url:
        await context.bot.send_photo(
            chat_id, photo=poster_url, caption=text, parse_mode=ParseMode.HTML
        )
    else:
        await context.bot.send_message(chat_id, text, parse_mode=ParseMode.HTML)

# -------------------------------------------------
#             ЕЖЕНЕДЕЛЬНЫЕ ИТОГИ
# -------------------------------------------------

async def weekly_quiz_summary_job(context: ContextTypes.DEFAULT_TYPE):
    chat_id = context.job.chat_id
    now = now_msk()
    week_id = get_current_week_id(now)

    chat_key = str(chat_id)
    week_scores = _weekly_scores.get(chat_key, {}).get(week_id, {})

    if not week_scores:
        await context.bot.send_message(chat_id, "📊 На этой неделе нет результатов.")
        return

    max_score = max(week_scores.values())
    winners = [int(uid) for uid, s in week_scores.items() if s == max_score]

    participants_map = _tracked_participants.get(chat_id, {})
    winner_mentions = [
        participants_map.get(uid, f"<a href='tg://user?id={uid}'>участник</a>")
        for uid in winners
    ]

    text = (
        "🎉 <b>Итоги недели!</b>\n\n"
        f"🥇 Победители: {', '.join(winner_mentions)}\n"
        f"🔥 Счёт: <b>{max_score}</b>"
    )

    await context.bot.send_message(chat_id, text, parse_mode=ParseMode.HTML)

# -------------------------------------------------
#                 КОМАНДЫ
# -------------------------------------------------

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat and chat.type in ("group", "supergroup"):
        await ensure_jobs_for_chat(context, chat.id)

    await update.effective_message.reply_text(
        "Привет! Я корпоративный бот.\n"
        f"• Ежедневная викторина в {DAILY_FACT_TIME_STR}\n"
        f"• Планёрка в {STANDUP_MEETING_TIME_STR}\n"
        f"• Фильм по пятницам в {MOVIE_RECOMMEND_TIME_STR}\n"
        f"• Итоги недели в {WEEKLY_SUMMARY_TIME_STR}"
    )

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(
        "/start — инфо\n"
        "/when — расписание\n"
        "/debug — все jobs\n"
        "/test_quiz — тест викторины\n"
        "/test_movie — тест фильма\n"
        "/force_standup — отправить напоминание\n"
        "/top — топ недели\n"
        "/init_jobs — принудительно создать jobs"
    )

async def ping_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text("Я здесь 👋")

async def when_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(
        f"🕘 Викторина: {DAILY_FACT_TIME_STR}\n"
        f"📣 Планёрка: {STANDUP_REMINDER_TIME_STR}\n"
        f"🎬 Фильм: {MOVIE_RECOMMEND_TIME_STR}\n"
        f"🏆 Итоги недели: {WEEKLY_SUMMARY_TIME_STR}"
    )

# -------------------------------------------------
#            НОВАЯ ВЕРСИЯ /debug  (ВАЖНО!)
# -------------------------------------------------

async def debug_schedule_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    now = now_msk()

    text = (
        f"🕐 Сейчас: {now}\n"
        f"📅 День: {now.weekday()} ({['пн','вт','ср','чт','пт','сб','вс'][now.weekday()]})\n"
        f"💬 Chat ID: {chat_id}\n"
        f"📋 Запланированные чаты: {_scheduled_chats}\n"
    )

    jq = context.application.job_queue
    if jq:
        jobs = jq.jobs()
        chat_jobs = [j for j in jobs if str(chat_id) in j.name]

        text += f"\n🔧 Jobs для этого чата ({len(chat_jobs)} шт):"
        for job in chat_jobs:
            text += f"\n • {job.name} — next_run={job.next_t}"
    else:
        text += "\n❌ JobQueue не доступен"

    await update.effective_message.reply_text(text)

# -------------------------------------------------
#        ПРОЧИЕ КОМАНДЫ И ЛОГИКА
# -------------------------------------------------

async def test_quiz_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_user_admin(update, context):
        await update.effective_message.reply_text("Только для админов.")
        return
    fact_text, year = get_on_this_day_fact(now_msk())
    if not fact_text:
        await update.effective_message.reply_text("Факт не найден.")
        return

    opts = generate_year_options(year)
    keyboard = [[InlineKeyboardButton(o, callback_data=o)] for o in opts]

    msg = await update.effective_message.reply_text(
        f"{fact_text}\n\n⏳ 5 минут.",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.HTML
    )

    chat_id = update.effective_chat.id
    _current_quiz[chat_id] = {
        "message_id": msg.message_id,
        "correct_year": year,
        "answered_users": set(),
        "answers": [],
        "winner": None,
        "deadline": now_msk() + timedelta(minutes=5),
    }

    context.application.job_queue.run_once(
        quiz_timeout_job,
        when=timedelta(minutes=5),
        chat_id=chat_id,
        name=f"quiz_timeout_{chat_id}_{msg.message_id}",
        data={"message_id": msg.message_id},
    )

async def test_movie_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_user_admin(update, context):
        return await update.effective_message.reply_text("Только для админов.")

    movie = fetch_movie_recommendation()
    if not movie:
        return await update.effective_message.reply_text("Не удалось получить фильм.")

    text, poster_url = build_movie_message(movie)
    if poster_url:
        await context.bot.send_photo(
            chat_id=update.effective_chat.id,
            photo=poster_url,
            caption=text,
            parse_mode=ParseMode.HTML
        )
    else:
        await update.effective_message.reply_text(text, parse_mode=ParseMode.HTML)

async def force_standup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_user_admin(update, context):
        return
    await send_standup_reminder(context.bot, update.effective_chat.id)
    await update.effective_message.reply_text("Отправлено.")

async def top_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_user_admin(update, context):
        return

    chat_id = update.effective_chat.id
    now = now_msk()
    week_id = get_current_week_id(now)

    chat_key = str(chat_id)
    scores = _weekly_scores.get(chat_key, {}).get(week_id, {})

    if not scores:
        return await update.effective_message.reply_text("Данных нет.")

    participants_map = _tracked_participants.get(chat_id, {})
    sorted_scores = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)

    lines = [f"📈 Топ недели ({week_id}):"]
    for pos, (uid, score) in enumerate(sorted_scores[:20], start=1):
        uid_int = int(uid)
        mention = participants_map.get(uid_int, f"<a href='tg://user?id={uid_int}'>участник</a>")
        lines.append(f"{pos}. {mention}: <b>{score}</b>")

    await update.effective_message.reply_html("\n".join(lines))

async def init_jobs_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_user_admin(update, context):
        return await update.effective_message.reply_text("Только для админов.")
    chat_id = update.effective_chat.id
    ok = await ensure_jobs_for_chat(context, chat_id)
    if ok:
        await update.effective_message.reply_text("✅ Джобы инициализированы.")
    else:
        await update.effective_message.reply_text("❌ Ошибка.")

# -------------------------------------------------
#             СИСТЕМНЫЕ ХЕНДЛЕРЫ
# -------------------------------------------------

async def greet_new_members(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    for user in update.effective_message.new_chat_members:
        if not user.is_bot:
            remember_tracked_user(chat.id, user)
            await update.effective_message.reply_text(
                f"👋 Добро пожаловать, {user.mention_html()}!",
                parse_mode=ParseMode.HTML
            )

async def handle_my_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.my_chat_member.chat
    status = update.my_chat_member.new_chat_member.status
    if status in ("member", "administrator"):
        await ensure_jobs_for_chat(context, chat.id)

async def ensure_jobs_for_chat(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    jq = context.application.job_queue
    if not jq:
        logger.error("JobQueue missing")
        return False

    if chat_id in _scheduled_chats:
        return True

    try:
        jq.run_daily(
            daily_fact_job,
            time=parse_hhmm(DAILY_FACT_TIME_STR),
            days=(0, 1, 2, 3, 4),
            name=f"daily_fact_{chat_id}",
            chat_id=chat_id,
        )
        jq.run_daily(
            standup_reminder_job,
            time=parse_hhmm(STANDUP_REMINDER_TIME_STR),
            days=(0, 2, 4),
            name=f"standup_reminder_{chat_id}",
            chat_id=chat_id,
        )
        jq.run_daily(
            movie_recommendation_job,
            time=parse_hhmm(MOVIE_RECOMMEND_TIME_STR),
            days=(4,),
            name=f"movie_friday_{chat_id}",
            chat_id=chat_id,
        )
        jq.run_daily(
            weekly_quiz_summary_job,
            time=parse_hhmm(WEEKLY_SUMMARY_TIME_STR),
            days=(4,),
            name=f"weekly_quiz_summary_{chat_id}",
            chat_id=chat_id,
        )

        _scheduled_chats.add(chat_id)
        return True
    except Exception as e:
        logger.error(f"Failed to schedule jobs: {e}")
        return False

async def auto_ensure_jobs_for_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    if not chat or chat.type not in ("group", "supergroup"):
        return
    remember_tracked_user(chat.id, user)
    await ensure_jobs_for_chat(context, chat.id)

# -------------------------------------------------
#                     MAIN
# -------------------------------------------------

def main():
    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN не задан")

    load_weekly_scores()

    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .timezone(TZ_MSK)
        .build()
    )

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

    app.add_handler(MessageHandler(filters.ALL & ~filters.StatusUpdate.ALL, auto_ensure_jobs_for_chat))
    app.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, greet_new_members))
    app.add_handler(ChatMemberHandler(handle_my_chat_member, ChatMemberHandler.MY_CHAT_MEMBER))

    app.add_handler(CallbackQueryHandler(handle_quiz_answer))

    app.run_polling()

if __name__ == "__main__":
    main()
