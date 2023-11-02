import asyncio
import os
from datetime import datetime

from dotenv import load_dotenv
from loguru import logger

from bot.database import Database
from market_loader.constants import mine_circle_sleep_time
from market_loader.loader import MarketDataLoader
from market_loader.models import ApiConfig
from market_loader.strategy_evaluator import StrategyEvaluator
from market_loader.technical_indicators_calculator import TechnicalIndicatorsCalculator

load_dotenv()
loop = asyncio.get_event_loop()
db = Database(
    name=os.getenv("PG_NAME"),
    user=os.getenv("PG_USER"),
    password=os.getenv("PG_PASSWORD"),
    host=os.getenv("PG_HOST"),
    port=os.getenv("PG_PORT"),
    loop=loop,
)

config = ApiConfig()
config.token = os.getenv("TOKEN")
config.base_url = "https://invest-public-api.tinkoff.ru/rest/"
config.share_by = "tinkoff.public.invest.api.contract.v1.InstrumentsService/ShareBy"
config.get_candles = "tinkoff.public.invest.api.contract.v1.MarketDataService/GetCandles"
config.find_instrument = "tinkoff.public.invest.api.contract.v1.InstrumentsService/FindInstrument"

loader = MarketDataLoader(db=db, config=config)
ti_calculator = TechnicalIndicatorsCalculator(db=db)
strategy_evaluator = StrategyEvaluator(db=db, token=os.getenv("BOT_TOKEN"), chat_id=int(os.getenv("DEBUG_CHAT_ID")))


async def main():
    logger.info("Загрузка началась")
    while True:
        start_time = datetime.now()
        await loader.load_data()
        await ti_calculator.calculate()
        await strategy_evaluator.check_strategy()
        end_time = datetime.now()
        sleep_time = mine_circle_sleep_time - (end_time - start_time).total_seconds()
        if sleep_time > 0:
            await asyncio.sleep(sleep_time)


if __name__ == "__main__":
    logger.add(
        "logs/debug.log",
        level="DEBUG",
        format="{time} | {level} | {module}:{function}:{line} | {message}",
        rotation="30 KB",
        compression="zip",
    )
    loop.run_until_complete(main())
