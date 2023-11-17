from aiogram import types

from bot.loader import bot, db, dp
from bot.texts import button_texts, message_texts
from bot.utils import format_active_orders_message, format_portfolio_message
from market_loader.market_processor import MarketProcessor


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


@dp.message_handler(commands="buy_ticker")
async def buy_ticker(message: types.Message) -> None:
    mp = MarketProcessor(db=db, sandbox_mode=True)
    # await mp.make_order("BBG004730RP0", 150.0, OrderDirection.buy, OrderType.limit)
    await mp.replace_order("BBG004730RP0", 153.0)
    await bot.send_message(
        message.chat.id,
        'Ваша воля исполнена',
    )

@dp.message_handler(commands="sell_all")
async def sell_all(message: types.Message) -> None:
    mp = MarketProcessor(db=db, sandbox_mode=True)
    await mp.sell_market("BBG004730RP0", 2700.0)
    await bot.send_message(
        message.chat.id,
        'Ваша воля исполнена',
    )


@dp.message_handler(commands="show_active_orders")
async def show_active_orders(message: types.Message) -> None:
    mp = MarketProcessor(db=db, sandbox_mode=True)
    account_id = await db.get_user_account(user_id=1)
    await mp.update_orders(account_id)
    active_orders = await mp.get_orders()
    text = await format_active_orders_message(active_orders)
    await bot.send_message(
        message.chat.id,
        text,
    )


@dp.message_handler(commands="cancel_active_orders")
async def cancel_active_orders(message: types.Message) -> None:
    mp = MarketProcessor(db=db, sandbox_mode=True)
    account_id = await db.get_user_account(user_id=1)
    await mp.cancel_all_orders(account_id)
    await bot.send_message(
        message.chat.id,
        'Ваша воля исполнена',
    )


@dp.message_handler(commands="show_portfolio")
async def show_portfolio(message: types.Message) -> None:
    """ссылка на код проекта."""
    account_id = await db.get_user_account(user_id=1)
    mp = MarketProcessor(db=db, sandbox_mode=True)
    portfolio = await mp.get_portfolio(account_id)
    text = await format_portfolio_message(portfolio)
    await bot.send_message(
        message.chat.id,
        text,
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
        btn_command = types.InlineKeyboardButton(text=timeframe[1],
                                                 callback_data=f"timeframe_{timeframe[0]}_{strategy_id}")
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
        'Введите тикеры через запятую. Пример ввода - tickers: AAPL, AMZN, GAZP',
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
    if 'tickers: ' in message.text:
        clean_data = message.text.replace('tickers: ', '')
        data = clean_data.split(',')
        for ticker_raw in data:
            ticker = ticker_raw.strip()
            await db.add_ticker(ticker=ticker)
            ticker_id = await db.get_ticker_id_by_name(ticker=ticker)
            await db.add_user_ticker(user_id=message.from_user.id, ticker_id=ticker_id)
        await bot.send_message(
            message.chat.id,
            'Тикеры добавлены, аби. Теперь жди топовых сигналов.',
        )
    # else:
    #     await bot.send_message(
    #         message.chat.id,
    #         'Не понимаю тебя, аби.',
    #     )


@dp.message_handler()
async def unknown_message(message: types.Message) -> None:
    if not message.is_command():
        await bot.send_message(message.chat.id, message_texts["format_error"])
    else:
        await message.answer(message_texts["command_error"])
