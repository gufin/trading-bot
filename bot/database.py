from asyncio import AbstractEventLoop
from datetime import timedelta, timezone
from typing import Optional
from datetime import datetime
import asyncpg
import pandas as pd
from loguru import logger

from market_loader.models import Candle, CandleInterval, Ema, EmaToCalc, Ticker, TickerToUpdateEma


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
        query = f"SELECT * FROM tickers WHERE figi IS NOT NULL AND NOT disable"
        results = await self.pool.fetch(query)
        res = []
        for result in results:
            res.append(
                Ticker(ticker_id=result[0], figi=result[1], classCode=result[2], currency=result[3], name=result[4]))
        return res

    async def update_tickers(self, ticker_id, new_figi, new_classCode, new_currency) -> Optional[Ticker]:
        query = f"""
            UPDATE tickers 
            SET figi = $1, classCode = $2, currency = $3
            WHERE ticker_id = $4
            RETURNING ticker_id, figi, classCode, currency, name;
        """
        row = await self.pool.fetchrow(query, new_figi, new_classCode, new_currency, ticker_id)

        if not row:
            return None  # или можно вернуть какое-либо исключение

        # Возвращаем модель Ticker на основе результата
        return Ticker(
            ticker_id=row['ticker_id'],
            figi=row['figi'],
            classCode=row['classcode'],
            currency=row['currency'],
            name=row['name']
        )

    async def add_candle(self, ticker_id, interval, timestamp, open, high, low, close):
        query = f"INSERT INTO candles (ticker_id, interval, timestamp_column, open, high, low, close) " \
                f"VALUES ({ticker_id}, '{interval}', '{timestamp}', {open}, {high}, {low}, {close}) " \
                f"ON CONFLICT (ticker_id, interval, timestamp_column) DO NOTHING"
        await self.pool.execute(query)

    async def get_ema_params_to_calc(self) -> list[EmaToCalc]:
        query = f"SELECT interval, span FROM ema_to_calc"
        results = await self.pool.fetch(query)
        res = []
        for result in results:
            res.append(EmaToCalc(interval=result[0], span=result[1]))
        return res

    async def get_data_for_init_ema(self, ticker_id: int, interval: str):
        query = """
            SELECT timestamp_column, close
            FROM candles
            WHERE ticker_id = $1 AND interval = $2
            ORDER BY timestamp_column
            """
        rows = await self.pool.fetch(query, ticker_id, interval)

        # Преобразуем результаты в DataFrame
        df = pd.DataFrame(rows, columns=['timestamp_column', 'close'])

        return df

    async def add_ema(self, ticker_id, interval, span, timestamp_column, ema_value):
        query = """
        INSERT INTO ema (ticker_id, interval, span, timestamp_column, ema) 
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (ticker_id, interval, span, timestamp_column) DO NOTHING
        """
        await self.pool.execute(query, ticker_id, interval, span, timestamp_column, ema_value)

    async def get_tickers_to_init_ema(self) -> list[TickerToUpdateEma]:
        query = """
            SELECT t.ticker_id, t.name, etc.interval, etc.span
            FROM tickers t
            CROSS JOIN ema_to_calc etc
            LEFT JOIN ema e ON t.ticker_id = e.ticker_id AND etc.interval = e.interval AND etc.span = e.span
            WHERE e.ema_id IS NULL AND t.figi IS NOT NULL AND t.figi <> '' AND NOT t.disable;
        """
        results = await self.pool.fetch(query)
        res = []
        for result in results:
            res.append(
                TickerToUpdateEma(ticker_id=result[0], name=result[1], interval=result[2], span=result[3]))
        return res

    async def get_data_for_ema(self, ticker_id, interval, span) -> list[TickerToUpdateEma]:
        query = """
               SELECT * FROM (
                SELECT timestamp_column, close 
                FROM candles 
                WHERE ticker_id = $1 AND interval = $2 
                ORDER BY timestamp_column DESC 
                LIMIT $3
            ) AS subquery
            ORDER BY subquery.timestamp_column ASC;
               """
        rows = await self.pool.fetch(query, ticker_id, interval, span * 2)

        # Преобразуем результаты в DataFrame
        df = pd.DataFrame(rows, columns=['timestamp_column', 'close'])

        return df

    async def get_last_two_candles_for_each_ticker(self, interval: str) -> dict[int, list[Candle]]:
        query = """
        WITH NumberedCandles AS (
            SELECT 
                ticker_id,
                timestamp_column,
                open,
                high,
                low,
                close,
                ROW_NUMBER() OVER(PARTITION BY ticker_id ORDER BY timestamp_column DESC) AS rn
            FROM candles
            WHERE interval = $1
        )
        SELECT 
            ticker_id,
            timestamp_column,
            open,
            high,
            low,
            close
        FROM NumberedCandles
        WHERE rn <= 2
        ORDER BY ticker_id, timestamp_column DESC;
        """
        rows = await self.pool.fetch(query, interval)
        candles_dict = {}

        for row in rows:
            candle = Candle(
                timestamp_column=str(row['timestamp_column']),
                open=row['open'],
                high=row['high'],
                low=row['low'],
                close=row['close']
            )
            if row['ticker_id'] in candles_dict:
                candles_dict[row['ticker_id']].append(candle)
            else:
                candles_dict[row['ticker_id']] = [candle]

        return candles_dict

    async def get_latest_ema_for_ticker(self, ticker_id: int, interval: str, span) -> Optional[Ema]:
        query = """
        SELECT 
            timestamp_column,
            span,
            ema
        FROM ema
        WHERE ticker_id = $1 AND interval = $2 AND span = $3
        ORDER BY timestamp_column DESC
        LIMIT 1;
        """
        row = await self.pool.fetchrow(query, ticker_id, interval, span)

        if row:
            return Ema(
                timestamp_column=str(row['timestamp_column']),
                span=row['span'],
                ema=row['ema']
            )
        return None

    async def get_users_for_ticker(self, ticker_id: int) -> list[int]:
        query = """
        SELECT 
            user_id
        FROM user_tickers
        WHERE ticker_id = $1;
        """
        rows = await self.pool.fetch(query, ticker_id)

        return [row['user_id'] for row in rows]

    async def get_ticker_name_by_id(self, ticker_id: int):
        query = """
            SELECT name
            FROM tickers
            WHERE ticker_id = $1;
        """
        row = await self.pool.fetchrow(query, ticker_id)
        if not row:
            return None  # или можно вернуть какое-либо исключение
        return row[0]

    async def get_last_timestamp_by_interval_and_ticker(self, ticker_id: int, interval: CandleInterval):
        query = """
            SELECT timestamp_column
            FROM candles
            WHERE ticker_id = $1 AND interval = $2
            ORDER BY timestamp_column DESC
            LIMIT 1;
        """
        row = await self.pool.fetchrow(query, ticker_id, interval.value)
        if not row:
            return datetime.now(timezone.utc) - timedelta(days=60)  # или можно вернуть какое-либо исключение
        return row[0].replace(tzinfo=timezone.utc, microsecond=999999)
