from asyncio import AbstractEventLoop
from loguru import logger
from typing import Optional

from market_loader.models import Ticker

import asyncpg


class Database:
    def __init__(
        self,
        name: Optional[str],
        user: Optional[str],
        password: Optional[str],
        host: Optional[str],
        port: Optional[str],
        loop: AbstractEventLoop,
    ) -> None:
        self.name = name
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.loop = loop
        self.pool = loop.run_until_complete(
            asyncpg.create_pool(
                database=name,
                user=user,
                password=password,
                host=host,
                port=port,
            )
        )

    async def create_tables(self) -> None:
        """create tables in the database."""
        with open("bot/sql/init.sql", "r") as f:
            sql = f.read()
        await self.pool.execute(sql)

    async def close_database(self) -> None:
        await self.pool.close()

    async def add_user(self, user_id: int, name: str, lang: str) -> None:
        """add a new user to the database."""
        await self.pool.execute(f"INSERT INTO Users VALUES({user_id}, '{name}', '{lang}')")
        logger.info(f"added new user | user_id: {user_id}; name: {name}; language: {lang}")

    async def verification(self, user_id: int) -> bool:
        """checks if the user is in the database."""
        response = await self.pool.fetchrow("SELECT EXISTS(SELECT user_id FROM Users WHERE user_id=$1)", user_id)
        return response[0]

    async def get_name(self, user_id: int) -> str:
        return await self.pool.fetchval(f"SELECT name FROM Users WHERE user_id={user_id}")

    async def get_lang(self, user_id: int) -> str:
        return await self.pool.fetchval(f"SELECT lang FROM Users WHERE user_id={user_id}")

    async def get_strategies(self):
        response = await self.pool.fetch("SELECT strategy_id, name FROM strategy")
        return response

    async def get_time_frames(self):
        response = await self.pool.fetch("SELECT timeframe_id, name FROM timeframes")
        return response

    async def save_strategy(self, user_id, strategy_id, timeframe_id):
        await self.pool.execute(
            f"INSERT INTO user_strategies (user_id, strategy_id, timeframe_id) VALUES ({user_id}, {strategy_id}, {timeframe_id}) ON CONFLICT (user_id, strategy_id, timeframe_id) DO NOTHING"
        )

    async def add_ticker(self, ticker):
        query = f"INSERT INTO tickers (name) VALUES ('{ticker}') ON CONFLICT (name) DO NOTHING"
        await self.pool.execute(query)

    async def get_ticker_id_by_name(self, ticker):
        query = f"SELECT ticker_id FROM tickers WHERE name = '{ticker}'"
        result = await self.pool.fetchval(query)
        return result

    async def add_user_ticker(self, user_id, ticker_id):
        query = f"INSERT INTO user_tickers (user_id, ticker_id) VALUES ({user_id}, {ticker_id})"
        await self.pool.execute(query)

    async def get_tickers_without_figi(self) -> list[Ticker]:
        query = f"SELECT * FROM tickers WHERE figi IS NULL"
        results = await self.pool.fetch(query)
        res = []
        for result in results:
            res.append(Ticker(ticker_id=result[0], name=result[4]))
        return res

    async def get_tickers_with_figi(self) -> list[Ticker]:
        query = f"SELECT * FROM tickers WHERE figi IS NOT NULL"
        results = await self.pool.fetch(query)
        res = []
        for result in results:
            res.append(Ticker(ticker_id=result[0], figi=result[1], classCode=result[2], currency=result[3], name=result[4]))
        return res

    async def update_tickers(self, ticker_id, new_figi, new_classCode, new_currency):
        query = f"UPDATE tickers SET figi = '{new_figi}', classCode = '{new_classCode}', currency = '{new_currency}' WHERE ticker_id = {ticker_id}"
        await self.pool.execute(query)

    async def add_candle(self, ticker_id, interval, timestamp, open, high, low, close):
        query = f"INSERT INTO candles (ticker_id, interval, timestamp_column, open, high, low, close) " \
                f"VALUES ({ticker_id}, '{interval}', '{timestamp}', {open}, {high}, {low}, {close}) " \
                f"ON CONFLICT (ticker_id, interval, timestamp_column) DO NOTHING"
        await self.pool.execute(query)
