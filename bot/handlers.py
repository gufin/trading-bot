from aiogram import types
from bot.loader import bot, db, dp
from bot.texts import button_texts, message_texts


@dp.message_handler(commands="start")
async def start_message(message: types.Message) -> None:
    """welcome message."""
    if await db.verification(message.from_user.id):
        await bot.send_message(message.chat.id, message_texts["welcome"])
    else:
        if message.from_user.first_name != "None":
            name = message.from_user.first_name
        elif message.from_user.username != "None":
            name = message.from_user.username
        elif message.from_user.last_name != "None":
            name = message.from_user.last_name
        else:
            name = ""
        await db.add_user(message.from_user.id, name, message.from_user.locale.language_name)
        await bot.send_message(message.chat.id, message_texts["about"])


@dp.message_handler(commands=("help", "info", "about"))
async def give_info(message: types.Message) -> None:
    """the target of this bot."""
    await bot.send_message(message.chat.id, message_texts["about"])


@dp.message_handler(commands="contacts")
async def give_contacts(message: types.Message) -> None:
    """ссылка на код проекта."""
    btn_link = types.InlineKeyboardButton(
        text=button_texts["github"], url="https://github.com/donBarbos/telegram-bot-template"
    )
    keyboard_link = types.InlineKeyboardMarkup().add(btn_link)
    await bot.send_message(
        message.chat.id,
        message_texts["github"],
        reply_markup=keyboard_link,
    )

@dp.message_handler(commands="chose_strategy")
async def chose_strategy(message: types.Message) -> None:
    """ссылка на код проекта."""
    strategies = await db.get_strategies()
    keyboard_link = types.InlineKeyboardMarkup()
    for strategy in strategies:
        btn_command = types.InlineKeyboardButton(text=strategy[1], callback_data=f"strategy_{strategy[0]}")
        keyboard_link.add(btn_command)

    await bot.send_message(
        message.chat.id,
        'Доступные стратегии',
        reply_markup=keyboard_link,
    )

@dp.callback_query_handler(lambda c: c.data.startswith('strategy_'))
async def process_strategy_button(callback_query: types.CallbackQuery):
    strategy_id = callback_query.data[len('strategy_'):]
    timeframes = await db.get_time_frames()
    keyboard_link = types.InlineKeyboardMarkup()
    for timeframe in timeframes:
        btn_command = types.InlineKeyboardButton(text=timeframe[1], callback_data=f"timeframe_{timeframe[0]}_{strategy_id}")
        keyboard_link.add(btn_command)

    await bot.send_message(
        callback_query.message.chat.id,
        'Доступные временные окна',
        reply_markup=keyboard_link,
    )


@dp.callback_query_handler(lambda c: c.data.startswith('timeframe_'))
async def process_strategy_button(callback_query: types.CallbackQuery):
    data = callback_query.data[len('timeframe_'):].split('_')
    await db.save_strategy(user_id=callback_query.from_user.id, strategy_id=data[1], timeframe_id=data[0])
    await bot.send_message(
        callback_query.message.chat.id,
        'Введите тикеры через запятую.',
    )



@dp.message_handler(commands="settings")
async def give_settings(message: types.Message) -> None:
    """справка по настройкам."""
    name = await db.get_name(message.from_user.id)
    lang = await db.get_lang(message.from_user.id)
    btn_name = types.InlineKeyboardButton(text=f"name: {name}", callback_data="name")
    btn_lang = types.InlineKeyboardButton(text=f"language: {lang}", callback_data="lang")
    keyboard_settings = types.InlineKeyboardMarkup().add(btn_name, btn_lang)
    await bot.send_message(message.chat.id, message_texts["settings"], reply_markup=keyboard_settings)


@dp.callback_query_handler(lambda c: c.data == "name")
async def alter_name(callback_query: types.CallbackQuery) -> None:
    await bot.send_message(callback_query.id, message_texts["address"])
    await bot.answer_callback_query(callback_query.id)


@dp.callback_query_handler(lambda c: c.data == "lang")
async def alter_lang(callback_query: types.CallbackQuery) -> None:
    await bot.send_message(callback_query.id, message_texts["language"])
    await bot.answer_callback_query(callback_query.id, message_texts["language"])


@dp.message_handler(content_types="text")
async def text_handler(message: types.Message) -> None:
    await bot.send_message(message.chat.id, message_texts["text"])


@dp.message_handler()
async def unknown_message(message: types.Message) -> None:
    if not message.is_command():
        await bot.send_message(message.chat.id, message_texts["format_error"])
    else:
        await message.answer(message_texts["command_error"])
