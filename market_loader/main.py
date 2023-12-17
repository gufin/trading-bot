import asyncio
import os
from datetime import datetime

from loguru import logger

from market_loader.market_processor import MarketProcessor
from market_loader.settings import settings
from market_loader.infrasturcture.entities import get_sessionmaker
from market_loader.infrasturcture.postgres_repository import BotPostgresRepository
from market_loader.loader import MarketDataLoader
from market_loader.strategy_evaluator import StrategyEvaluator
from market_loader.technical_indicators_calculator import TechnicalIndicatorsCalculator

sessionmaker = get_sessionmaker()
db = BotPostgresRepository(sessionmaker)

loader = MarketDataLoader(db=db)
ti_calculator = TechnicalIndicatorsCalculator(db=db)
mp = MarketProcessor(db, settings.send_box_mode)
strategy_evaluator = StrategyEvaluator(db=db,mp=mp)


async def main():
    logger.info("Загрузка началась")
    start_time_300 = datetime.now()
    start_time_45 = datetime.now()
    first_load_exist = False
    start = True
    while True:
        end_time = datetime.now()
        if ((end_time - start_time_300).total_seconds() >= 300) or start:
            first_load_exist = True
            start = False
            start_time_300 = datetime.now()
            await loader.load_data()
            await ti_calculator.calculate()
            await strategy_evaluator.check_strategy()

        end_time = datetime.now()
        if (end_time - start_time_45).total_seconds() >= 45 and first_load_exist:
            start_time_45 = datetime.now()
            await strategy_evaluator.check_orders()

        await asyncio.sleep(10)


if __name__ == "__main__":
    logger.add(
        "logs/debug.log",
        level="DEBUG",
        format="{time} | {level} | {module}:{function}:{line} | {message}",
        rotation="30000 KB",
        compression="zip",
    )
    asyncio.run(main())
