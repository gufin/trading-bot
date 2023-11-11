from aiogram import Dispatcher, types


async def set_default_commands(dp: Dispatcher) -> None:
    await dp.bot.set_my_commands(
        [
            types.BotCommand("help", "help"),
            types.BotCommand("contacts", "developer contact details"),
            types.BotCommand("settings", "setting information about you"),
            types.BotCommand("chose_strategy", "chose strategy"),
            types.BotCommand("show_portfolio", "show portfolio"),
        ]
    )
