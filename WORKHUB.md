# WorkHub — closed employee Mini App

Deploy branch workspace-go-v1, Dockerfile, build context '.', port 8080.
Set BOT_TOKEN (the token of the bot that owns this Mini App), WORK_CHAT_ID (negative numeric group/supergroup ID, e.g. -1001234567890, example only), DATA_DIR=/app/runtime. Use a persistent writable volume at DATA_DIR owned by the runtime user (Alpine workhub). Run a single replica; the JSON store is atomic but not a shared database. Back up employees.json securely outside GitHub. Do not commit credentials or personal data.

Add the bot as an administrator in the work chat (minimum rights). Configure its Main Mini App in BotFather with the HTTPS hosting URL. This service does not run polling, and does not replace the existing Python bot. Telegram membership is checked on every personal API request; a closed browser without signed initData cannot access employee data. Missing configuration and Telegram errors fail closed. Auth data expires after 12 hours; reopen in Telegram. No demo bypass on hosted app.

On first login the server creates an employee profile with the verified Telegram name and, if available, the user's profile photo fetched through the Bot API. Users may edit name, role, description, photo, status, project. Saved items and demo progress are stored for that account. The team contains only users who have logged in and whose membership can currently be confirmed. Bot API does not expose a complete ordinary member list. Real internal documents, production tests and notifications are not yet integrated; existing catalog is placeholder data. Team can link to Telegram username when present.

GET /api/health is public; GET /api/me, PUT /api/me and GET /api/team are protected. Validate member and non-member on actual Telegram clients before rollout. Automated Go tests use mocked Telegram calls and execute during Docker build. Keep repository private before adding corporate material.
