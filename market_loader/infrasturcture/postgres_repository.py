from datetime import datetime
from datetime import timedelta, timezone
from typing import Optional

import pandas as pd
from sqlalchemy import exists, func, select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession

from market_loader.infrasturcture.entities import (CandleModel, EMACrossModel, EMAModel, EMAToCalcModel, StrategyModel,
                                                   TickerModel,
                                                   TimeframeModel, UserModel,
                                                   UserStrategyModel, UserTickerModel)
from market_loader.models import Candle, CandleInterval, Ema, EmaToCalc, Ticker, TickerToUpdateEma
from market_loader.utils import transform_candle_result


class BotPostgresRepository:
    def __init__(self, sessionmaker: async_sessionmaker[AsyncSession]):
        self.sessionmaker = sessionmaker

    async def add_user(self, user_id: int, name: str, lang: str) -> None:
        async with self.sessionmaker() as session:
            new_user = UserModel(user_id=user_id, name=name, lang=lang)
            session.add(new_user)
            await session.commit()

    async def verification(self, user_id: int) -> bool:
        async with self.sessionmaker() as session:
            result = await session.execute(
                select(exists().where(UserModel.user_id == user_id))
            )
            return result.scalar()

    async def get_name(self, user_id: int) -> str:
        async with self.sessionmaker() as session:
            result = await session.execute(
                select(UserModel.name).where(UserModel.user_id == user_id)
            )
            return result.scalar_one_or_none()

    async def get_lang(self, user_id: int) -> str:
        async with self.sessionmaker() as session:
            result = await session.execute(
                select(UserModel.lang).where(UserModel.user_id == user_id)
            )
            return result.scalar_one_or_none()

    async def get_strategies(self):
        async with self.sessionmaker() as session:
            result = await session.execute(
                select(StrategyModel.strategy_id, StrategyModel.name)
            )
            return result.all()

    async def get_time_frames(self):
        async with self.sessionmaker() as session:
            result = await session.execute(
                select(TimeframeModel.timeframe_id, TimeframeModel.name)
            )
            return result.all()

    async def save_strategy(self, user_id: int, strategy_id: int, timeframe_id: int) -> None:
        async with self.sessionmaker() as session:
            user_strategy = UserStrategyModel(
                user_id=user_id,
                strategy_id=strategy_id,
                timeframe_id=timeframe_id
            )
            session.add(user_strategy)
            try:
                await session.commit()
            except IntegrityError:
                await session.rollback()

    async def add_ticker(self, name: str) -> None:
        async with self.sessionmaker() as session:
            new_ticker = TickerModel(name=name)
            session.add(new_ticker)
            try:
                await session.commit()
            except IntegrityError:
                await session.rollback()

    async def get_ticker_id_by_name(self, name: str) -> int:
        async with self.sessionmaker() as session:
            result = await session.execute(
                select(TickerModel.ticker_id).where(TickerModel.name == name)
            )
            return result.scalar_one_or_none()

    async def add_user_ticker(self, user_id: int, ticker_id: int) -> None:
        async with self.sessionmaker() as session:
            new_user_ticker = UserTickerModel(user_id=user_id, ticker_id=ticker_id)
            session.add(new_user_ticker)
            await session.commit()

    async def get_tickers_without_figi(self) -> list[Ticker]:
        async with self.sessionmaker() as session:
            result = await session.execute(
                select(TickerModel).where(TickerModel.figi.is_(None))
            )
            return [Ticker(ticker_id=row.ticker_id, name=row.name) for row in result.scalars()]

    async def get_tickers_with_figi(self) -> list[Ticker]:
        async with self.sessionmaker() as session:
            result = await session.execute(
                select(TickerModel).where(TickerModel.figi.isnot(None), TickerModel.disable.is_(False))
            )
            return [
                Ticker(
                    ticker_id=row.ticker_id,
                    figi=row.figi,
                    classCode=row.classcode,
                    currency=row.currency,
                    name=row.name
                ) for row in result.scalars()]

    async def update_tickers(self, ticker_id: int, new_figi: str, new_class_code: str,
                             new_currency: str) -> Optional[Ticker]:
        async with self.sessionmaker() as session:
            result = await session.execute(
                update(TickerModel).where(TickerModel.ticker_id == ticker_id).
                values(figi=new_figi, classcode=new_class_code, currency=new_currency).
                returning(TickerModel)
            )
            await session.commit()
            if row := result.scalars().first():
                return Ticker(
                    ticker_id=row.ticker_id,
                    figi=row.figi,
                    classCode=row.classcode,
                    currency=row.currency,
                    name=row.name
                )
            else:
                return None

    async def add_candle(self, ticker_id: int, interval: str, timestamp: datetime, open: float, high: float,
                         low: float, close: float) -> None:
        async with self.sessionmaker() as session:
            try:
                new_candle = CandleModel(
                    ticker_id=ticker_id,
                    interval=interval,
                    timestamp_column=timestamp,
                    open=open,
                    high=high,
                    low=low,
                    close=close
                )
                session.add(new_candle)
                await session.commit()
            except IntegrityError:
                await session.rollback()

    async def get_ema_params_to_calc(self) -> list[EmaToCalc]:
        async with self.sessionmaker() as session:
            result = await session.execute(
                select(EMAToCalcModel)
            )
            return [EmaToCalc(interval=row.interval, span=row.span) for row in result.scalars()]

    async def get_data_for_init_ema(self, ticker_id: int, interval: str) -> pd.DataFrame:
        async with self.sessionmaker() as session:
            result = await session.execute(
                select(CandleModel).
                where(CandleModel.ticker_id == ticker_id, CandleModel.interval == interval).
                order_by(CandleModel.timestamp_column)
            )
            return pd.DataFrame(
                result.all(),
                columns=['timestamp_column', 'close', 'open', 'high', 'low'],
            )

    async def add_ema(self, ticker_id: int, interval: str, span: int, timestamp_column: datetime, ema_value: float,
                      atr: float) -> None:
        async with self.sessionmaker() as session:
            new_ema = EMAModel(
                ticker_id=ticker_id,
                interval=interval,
                span=span,
                timestamp_column=timestamp_column,
                ema=ema_value,
                atr=atr
            )
            session.add(new_ema)
            await session.commit()

    async def get_tickers_to_init_ema(self) -> list[TickerToUpdateEma]:
        async with self.sessionmaker() as session:
            sql = text("""
                SELECT t.ticker_id, t.name, etc.interval, etc.span
                FROM tickers t
                CROSS JOIN ema_to_calc etc
                LEFT JOIN ema e ON t.ticker_id = e.ticker_id AND etc.interval = e.interval AND etc.span = e.span
                WHERE e.ema_id IS NULL AND t.figi IS NOT NULL AND t.figi <> '' AND NOT t.disable;
            """)

            result = await session.execute(sql)

            return [
                TickerToUpdateEma(
                    ticker_id=row['ticker_id'],
                    name=row['name'],
                    interval=row['interval'],
                    span=row['span']
                )
                for row in result.mappings().all()
            ]

    async def get_data_for_ema(self, ticker_id: int, interval: str, span: int) -> pd.DataFrame:
        async with self.sessionmaker() as session:
            result = await session.execute(
                select(CandleModel).
                where(CandleModel.ticker_id == ticker_id, CandleModel.interval == interval).
                order_by(CandleModel.timestamp_column.desc()).
                limit(span * 2)
            )
            candles = result.scalars().all()
            candles_data = [
                {
                    'timestamp_column': candle.timestamp_column,
                    'close': candle.close,
                    'open': candle.open,
                    'high': candle.high,
                    'low': candle.low
                }
                for candle in candles
            ]
            df = pd.DataFrame(candles_data)
            df = df.iloc[::-1]
            return df

    async def get_latest_ema_for_ticker(self, ticker_id: int, interval: str, span: int) -> Optional[EMAModel]:
        async with self.sessionmaker() as session:
            result = await session.execute(
                select(EMAModel).
                where(EMAModel.ticker_id == ticker_id, EMAModel.interval == interval, EMAModel.span == span).
                order_by(EMAModel.timestamp_column.desc()).
                limit(1)
            )
            row = result.scalars().first()
            return Ema(
                timestamp_column=str(row.timestamp_column),
                span=row.span,
                ema=row.ema,
                atr=row.atr
            ) if row else None

    async def get_penultimate_ema_for_ticker(self, ticker_id: int, interval: str, span) -> Optional[Ema]:
        async with self.sessionmaker() as session:
            result = await session.execute(
                select(EMAModel).
                where(EMAModel.ticker_id == ticker_id, EMAModel.interval == interval, EMAModel.span == span).
                order_by(EMAModel.timestamp_column.desc()).
                offset(1).
                limit(1)
            )
            row = result.scalars().first()
            return Ema(
                timestamp_column=row.timestamp_column,
                span=row.span,
                ema=row.ema,
                atr=row.atr
            ) if row else None

    async def get_users_for_ticker(self, ticker_id: int) -> list[int]:
        async with self.sessionmaker() as session:
            result = await session.execute(
                select(UserTickerModel.user_id).
                where(UserTickerModel.ticker_id == ticker_id)
            )
            return [row.user_id for row in result.scalars()]

    async def get_ticker_name_by_id(self, ticker_id: int) -> Optional[str]:
        async with self.sessionmaker() as session:
            result = await session.execute(
                select(TickerModel.name).
                where(TickerModel.ticker_id == ticker_id)
            )
            return result.scalar()

    async def get_last_timestamp_by_interval_and_ticker(self, ticker_id: int, interval: CandleInterval) -> datetime:
        async with self.sessionmaker() as session:
            result = await session.execute(
                select(CandleModel.timestamp_column).
                where(CandleModel.ticker_id == ticker_id, CandleModel.interval == interval.value).
                order_by(CandleModel.timestamp_column.desc()).
                limit(1)
            )
            row = result.fetchone()
            return row.timestamp_column.replace(tzinfo=timezone.utc, microsecond=999999) if row else datetime.now(
                timezone.utc) - timedelta(days=60)

    async def add_ema_cross(self, ticker_id: int, interval: str, span: int, timestamp_column) -> None:
        async with self.sessionmaker() as session:
            await session.merge(
                EMACrossModel(
                    ticker_id=ticker_id,
                    interval=interval,
                    span=span,
                    timestamp_column=timestamp_column
                )
            )
            try:
                await session.commit()
            except IntegrityError:
                await session.rollback()

    async def get_ema_cross_count(self, ticker_id: int, interval: str, span: int, start_time: datetime,
                                  end_time: datetime) -> int:
        async with self.sessionmaker() as session:
            result = await session.execute(
                select(func.count()).
                where(
                    EMACrossModel.ticker_id == ticker_id,
                    EMACrossModel.interval == interval,
                    EMACrossModel.span == span,
                    EMACrossModel.timestamp_column.between(start_time, end_time)
                )
            )
            return result.scalar()

    async def bulk_add_ema(self, ema_data: list[Ema]) -> None:
        async with self.sessionmaker() as session:
            ema_dicts = [ema.model_dump(exclude_unset=True) for ema in ema_data]
            for ema_dict in ema_dicts:
                ema_model = EMAModel(**ema_dict)
                session.add(ema_model)
            await session.commit()

    async def get_ema_for_ticker_by_period(self, ticker_id: int, interval: str, span,
                                           end_time: datetime) -> Optional[Ema]:
        async with self.sessionmaker() as session:
            result = await session.execute(
                select(EMAModel).
                where(
                    EMAModel.ticker_id == ticker_id,
                    EMAModel.interval == interval,
                    EMAModel.span == span,
                    EMAModel.timestamp_column <= end_time
                ).
                order_by(EMAModel.timestamp_column.desc()).
                limit(1)
            )
            if row := result.scalars().first():
                return Ema(
                    timestamp_column=str(row.timestamp_column),
                    span=row.span,
                    ema=row.ema,
                    atr=row.atr
                )
            return None

    async def get_penultimate_ema_for_ticker_by_period(self, ticker_id: int, interval: str, span: int,
                                                       end_time: datetime) -> Optional[Ema]:
        async with self.sessionmaker() as session:
            subquery = (
                select(EMAModel).
                where(
                    EMAModel.ticker_id == ticker_id,
                    EMAModel.interval == interval,
                    EMAModel.span == span,
                    EMAModel.timestamp_column <= end_time
                ).
                order_by(EMAModel.timestamp_column.desc()).
                limit(2)
            ).alias('subquery')

            result = await session.execute(
                select(subquery.c.timestamp_column, subquery.c.span, subquery.c.ema, subquery.c.atr).
                order_by(subquery.c.timestamp_column.desc()).
                offset(1)
            )
            if row := result.first():
                return Ema(
                    timestamp_column=row[0],
                    span=row[1],
                    ema=row[2],
                    atr=row[3]
                )
            return None

    async def get_last_two_candles_for_each_ticker(self, interval: str) -> dict[int, list[Candle]]:
        async with self.sessionmaker() as session:
            sql = text("""
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
                WHERE interval = :interval
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
               """)

            result = await session.execute(sql, {'interval': interval})
            return transform_candle_result(result)

    async def get_two_candles_for_each_ticker_by_period(self, interval: str,
                                                        timestamp: datetime) -> dict[int, list[Candle]]:
        async with self.sessionmaker() as session:
            sql = text("""
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
               WHERE interval = :interval AND timestamp_column <= :timestamp
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
           """)

            result = await session.execute(sql, {'interval': interval, 'timestamp': timestamp})
            return transform_candle_result(result)
