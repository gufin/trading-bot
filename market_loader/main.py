import asyncio
import os
from datetime import datetime

from loguru import logger

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
strategy_evaluator = StrategyEvaluator(db=db)


async def main():
    logger.info("Загрузка началась")
    while True:
        start_time = datetime.now()
        await loader.load_data()
        await ti_calculator.calculate()
        await strategy_evaluator.check_strategy()
        end_time = datetime.now()
        sleep_time = settings.mine_circle_sleep_time - (end_time - start_time).total_seconds()
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
    asyncio.run(main())
