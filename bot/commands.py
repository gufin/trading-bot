from aiogram import Dispatcher, types


async def set_default_commands(dp: Dispatcher) -> None:
    await dp.bot.set_my_commands(
        [
            # types.BotCommand("help", "help"),
            # types.BotCommand("contacts", "developer contact details"),
            # types.BotCommand("settings", "setting information about you"),
            types.BotCommand("chose_strategy", "chose strategy"),
            types.BotCommand("show_portfolio", "show portfolio"),
            types.BotCommand("buy_ticker", "buy ticker"),
            types.BotCommand("show_active_orders", "show active orders"),
            types.BotCommand("cancel_active_orders", "cancel active orders"),
            types.BotCommand("sell_all", "sell all positions"),
            types.BotCommand("get_deals", "trade journal"),
        ]
    )
