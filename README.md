# WorkHub / Corporate Telegram Work Bot

## Current WorkHub Mini App (Go)

The current Mini App is deployed from `main` with the root `Dockerfile` and `docker-entrypoint.sh`. See [WORKHUB.md](WORKHUB.md) for deployment, Telegram authentication, CMS and storage instructions.

Production environment: `DATA_DIR=/app/data`, with the existing persistent volume mounted at that exact path. Use the host-provided `PORT` (or 8080). Keep secrets in the hosting environment, not in Git.

```bash
go test ./...
docker build -t workhub .
```

The Python polling bot below is a separate legacy service; do not use its start command for the Go Mini App.

## Separate Python polling bot

Асинхронный Telegram-бот для командной работы:

- 🧠 Ежедневная викторина по фактам из истории (пн–пт)
- 📣 Напоминания о планёрке по пн/ср/пт
- 🎉 Поздравления с праздниками РФ
- 🎬 Рекомендация фильма по пятницам (Кинопоиск API)
- 🏆 Еженедельный рейтинг участников викторины

## Требования

- Python 3.10+
- Токен Telegram-бота
- (опционально) API-ключ `https://api.kinopoisk.dev/` для рекомендаций фильмов

## Установка

```bash
git clone https://github.com/your-username/tg-work-bot.git
cd tg-work-bot

python -m venv venv
source venv/bin/activate      # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## Настройка окружения

### Вариант 1: .env (локальная разработка)

Создай файл `.env` рядом с `bot.py` по образцу `.env.example`:

```bash
cp .env.example .env
# затем отредактируй значения в .env
```

### Вариант 2: переменные окружения (сервер)

На проде можно не использовать `.env`, а просто задать переменные окружения, например:

```bash
export BOT_TOKEN="..."
export WATCHED_USERNAMES="@user1,@user2"
export KINOPOISK_API_KEY="..."
export DAILY_FACT_TIME="09:10"
export STANDUP_REMINDER_TIME="09:00"
export STANDUP_MEETING_TIME="09:30"
export MOVIE_RECOMMEND_TIME="18:00"
export WEEKLY_SUMMARY_TIME="17:00"
```

Бот использует `python-dotenv`, но если `.env` нет — он просто возьмёт значения из окружения.

## Запуск

```bash
python bot.py
```

Бот работает через long polling.

## Основные команды

- `/start` — краткая справка  
- `/help` — список команд  
- `/ping` — проверка связи  
- `/when` — расписание событий  
- `/debug` — отладочная информация по job'ам  
- `/test_quiz` — тестовая викторина (только админ чата)  
- `/test_movie` — тестовая рекомендация фильма (только админ чата)  
- `/force_standup` — принудительное напоминание о планёрке (только админ чата)  
- `/top` — топ участников недели (только админ чата)  

## Лицензия

MIT (можешь заменить на другую при необходимости).
