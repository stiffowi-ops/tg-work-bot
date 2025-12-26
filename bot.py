async def _process_guess_internal(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int, guess: str) -> None:
    """Внутренняя обработка хода игрока (под блокировкой)."""
    game = active_games[chat_id]
    word = game.get("word", "")

    # Если игрок не зарегистрирован в игре
    if user_id not in game.get("players", {}):
        return

    # Проверяем, чья очередь ходить
    current_player = get_current_player(chat_id)
    if not current_player:
        return
        
    if current_player[0] != user_id:
        return  # Не очередь этого игрока

    player = game["players"][user_id]
    player_name = player.get("name", "Unknown")

    # Проверяем скорость хода (защита от флуда)
    user_key = f"{chat_id}_{user_id}"
    last_time = _last_guess_time.get(user_key)
    now_time = time.time()
    if last_time and now_time - last_time < 1:  # 1 секунда между ходами
        return
    _last_guess_time[user_key] = now_time

    # Проверяем, есть ли активное задание
    if has_active_penalty(chat_id, user_id):
        time_left = get_penalty_time_left(chat_id, user_id)
        minutes = time_left // 60
        seconds = time_left % 60
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                f"⚠️ {player_name}, у тебя есть активное задание!\n\n"
                f"📝 Задание: {PENALTY_TASK}\n"
                f"⏳ Осталось времени: {minutes}:{seconds:02d}\n\n"
                "💡 Сначала расскажи факт о себе и нажми кнопку в сообщении с заданием"
            ),
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    # Нормализуем букву
    if guess == 'Ё':
        guess = 'Е'
    
    # Проверяем, не угадывали ли эту букву уже
    guessed_letters = game.get("guessed_letters", set())
    wrong_letters = game.get("wrong_letters", set())
    
    if guess in guessed_letters:
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"❌ {player_name}, буква '{guess}' уже была угадана! Попробуйте другую букву.",
        )
        return
    
    if guess in wrong_letters:
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"❌ {player_name}, буква '{guess}' уже была ошибочной! Попробуйте другую букву.",
        )
        return
    
    if guess in word:
        # Правильная буква
        if "guessed_letters" not in game:
            game["guessed_letters"] = set()
        game["guessed_letters"].add(guess)
        player["correct_guesses"] = player.get("correct_guesses", 0) + 1
        
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"✅ {player_name}, буква '{guess}' есть в слове! {player_name} ходит ещё раз.",
        )
        
        # Обновляем отображение
        await safe_update_game_display(context, chat_id)

        # Проверяем, угадано ли слово полностью
        if all(letter in game.get("guessed_letters", set()) for letter in word if letter.isalpha()):
            await end_game_win(context, chat_id, user_id)
            return

    else:
        # Неправильная буква
        if "wrong_letters" not in game:
            game["wrong_letters"] = set()
        game["wrong_letters"].add(guess)
        player["wrong_guesses"] = player.get("wrong_guesses", 0) + 1
        
        # Вычисляем текущее количество попыток
        wrong_count = len(game["wrong_letters"])
        attempts_left = get_attempts_left(game)
        
        # Отправляем сообщение с заданием и кнопкой
        time_left = PENALTY_TIME_LIMIT
        minutes = time_left // 60
        seconds = time_left % 60
        
        message_text = f"""
❌ {player_name}, буквы '{guess}' нет в слове.

🎯 *Штрафное задание для {player_name}:*
📝 *{PENALTY_TASK}*

⏳ Осталось времени: {minutes}:{seconds:02d}
💡 Расскажи факт о себе в чате, затем нажми кнопку ниже:
        """.strip()
        
        # Кнопка "✅ Факт рассказан"
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Факт рассказан", callback_data=f"penalty_complete_{user_id}")]
        ])
        
        # Отправляем сообщение с заданием
        msg = await context.bot.send_message(
            chat_id=chat_id,
            text=message_text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=markup,
        )
        
        # Назначаем штрафное задание с ID сообщения
        assign_penalty_task(chat_id, user_id, msg.message_id)
        
        # Обновляем главное сообщение игры
        await safe_update_game_display(context, chat_id)
        
        # Запускаем таймер для обновления сообщения с заданием
        asyncio.create_task(update_penalty_timer(context, chat_id, user_id))
        
        # Запускаем таймер для проверки времени
        asyncio.create_task(check_penalty_timeout_delayed(context, chat_id, user_id))
        
        # ПРОВЕРЯЕМ ПОРАЖЕНИЕ СРАЗУ - ЕСЛИ ЭТО 6-Я ОШИБКА
        if attempts_left <= 0:
            # Отправляем уведомление, что это была последняя попытка
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"💀 *Это была 6-я ошибка!*\n\n📝 {player_name}, расскажите факт о себе и нажмите кнопку, чтобы увидеть результат игры.",
                parse_mode=ParseMode.MARKDOWN,
            )
            # Не завершаем игру здесь - дождемся, когда игрок нажмет кнопку "Факт рассказан"
            return
