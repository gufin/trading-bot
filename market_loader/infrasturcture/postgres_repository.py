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


class BotPostgresRepository:
    def __init__(self, sessionmaker: async_sessionmaker[AsyncSession]):
        self.sessionmaker = sessionmaker

    async def add_user(self, user_id: int, name: str, lang: str) -> None:
        """Add a new user to the database."""
        async with self.sessionmaker() as session:
            new_user = UserModel(user_id=user_id, name=name, lang=lang)
            session.add(new_user)
            await session.commit()

    async def verification(self, user_id: int) -> bool:
        """Checks if the user is in the database."""
        async with self.sessionmaker() as session:
            result = await session.execute(
                select(exists().where(UserModel.user_id == user_id))
            )
            return result.scalar()

    # Получение имени пользователя
    async def get_name(self, user_id: int) -> str:
        """Retrieve user name from the database."""
        async with self.sessionmaker() as session:
            result = await session.execute(
                select(UserModel.name).where(UserModel.user_id == user_id)
            )
            return result.scalar_one_or_none()

    # Получение языка пользователя
    async def get_lang(self, user_id: int) -> str:
        """Retrieve user language from the database."""
        async with self.sessionmaker() as session:
            result = await session.execute(
                select(UserModel.lang).where(UserModel.user_id == user_id)
            )
            return result.scalar_one_or_none()

    # Получение списка стратегий
    async def get_strategies(self):
        """Retrieve strategies from the database."""
        async with self.sessionmaker() as session:
            result = await session.execute(
                select(StrategyModel.strategy_id, StrategyModel.name)
            )
            return result.all()

    # Получение списка временных рамок
    async def get_time_frames(self):
        """Retrieve time frames from the database."""
        async with self.sessionmaker() as session:
            result = await session.execute(
                select(TimeframeModel.timeframe_id, TimeframeModel.name)
            )
            return result.all()

    # Сохранение стратегии пользователя
    async def save_strategy(self, user_id: int, strategy_id: int, timeframe_id: int):
        """Save user strategy to the database."""
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

    # Добавление тикера
    async def add_ticker(self, name: str):
        """Add a new ticker to the database."""
        async with self.sessionmaker() as session:
            new_ticker = TickerModel(name=name)
            session.add(new_ticker)
            try:
                await session.commit()
            except IntegrityError:
                await session.rollback()

    # Получение ID тикера по имени
    async def get_ticker_id_by_name(self, name: str) -> int:
        """Retrieve ticker ID by name from the database."""
        async with self.sessionmaker() as session:
            result = await session.execute(
                select(TickerModel.ticker_id).where(TickerModel.name == name)
            )
            return result.scalar_one_or_none()

    # Добавление тикера пользователю
    async def add_user_ticker(self, user_id: int, ticker_id: int):
        """Add a user ticker to the database."""
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

    # Получение списка тикеров с FIGI
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

    # Обновление информации о тикере
    async def update_tickers(self, ticker_id, new_figi, new_classCode, new_currency) -> Optional[Ticker]:
        async with self.sessionmaker() as session:
            result = await session.execute(
                update(TickerModel).where(TickerModel.ticker_id == ticker_id).
                values(figi=new_figi, classCode=new_classCode, currency=new_currency).
                returning(TickerModel)
            )
            await session.commit()
            row = result.fetchone()
            if row:
                return Ticker(
                    ticker_id=row.ticker_id,
                    figi=row.figi,
                    classCode=row.classCode,
                    currency=row.currency,
                    name=row.name
                )
            else:
                return None

    # Добавление свечи
    async def add_candle(self, ticker_id, interval, timestamp, open, high, low, close):
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
                await session.rollback()  # Обработка конфликтов должна быть реализована на уровне модели

    # Получение параметров EMA для расчета
    async def get_ema_params_to_calc(self) -> list[EmaToCalc]:
        async with self.sessionmaker() as session:
            result = await session.execute(
                select(EMAToCalcModel)
            )
            return [EmaToCalc(interval=row.interval, span=row.span) for row in result.scalars()]

    # Получение данных для инициализации EMA
    async def get_data_for_init_ema(self, ticker_id: int, interval: str):
        async with self.sessionmaker() as session:
            result = await session.execute(
                select(CandleModel).
                where(CandleModel.ticker_id == ticker_id, CandleModel.interval == interval).
                order_by(CandleModel.timestamp_column)
            )
            df = pd.DataFrame(result.all(), columns=['timestamp_column', 'close', 'open', 'high', 'low'])
            return df

    # Добавление EMA
    async def add_ema(self, ticker_id, interval, span, timestamp_column, ema_value, atr):
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
            # SQL запрос используя многострочный строковый литерал
            sql = text("""
                SELECT t.ticker_id, t.name, etc.interval, etc.span
                FROM tickers t
                CROSS JOIN ema_to_calc etc
                LEFT JOIN ema e ON t.ticker_id = e.ticker_id AND etc.interval = e.interval AND etc.span = e.span
                WHERE e.ema_id IS NULL AND t.figi IS NOT NULL AND t.figi <> '' AND NOT t.disable;
            """)

            result = await session.execute(sql)

            # Преобразуем результаты в список объектов TickerToUpdateEma
            return [
                TickerToUpdateEma(
                    ticker_id=row['ticker_id'],
                    name=row['name'],
                    interval=row['interval'],
                    span=row['span']
                )
                for row in result.mappings().all()
            ]

    # Получение данных для EMA
    async def get_data_for_ema(self, ticker_id, interval, span) -> pd.DataFrame:
        async with self.sessionmaker() as session:
            result = await session.execute(
                select(CandleModel).
                where(CandleModel.ticker_id == ticker_id, CandleModel.interval == interval).
                order_by(CandleModel.timestamp_column.desc()).
                limit(span * 2)
            )
            # Извлекаем объекты CandleModel из результата
            candles = result.scalars().all()

            # Преобразуем каждый объект CandleModel в словарь
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

            # Создаём DataFrame
            df = pd.DataFrame(candles_data)
            df = df.iloc[::-1]  # Переворачиваем DataFrame для правильного порядка
            return df

    # Получение последней EMA для тикера
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

    # Получение предпоследней EMA для тикера
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

    # Получение списка пользователей, подписанных на тикер
    async def get_users_for_ticker(self, ticker_id: int) -> list[int]:
        async with self.sessionmaker() as session:
            result = await session.execute(
                select(UserTickerModel.user_id).
                where(UserTickerModel.ticker_id == ticker_id)
            )
            return [row.user_id for row in result.scalars()]

    # Получение названия тикера по ID
    async def get_ticker_name_by_id(self, ticker_id: int) -> Optional[str]:
        async with self.sessionmaker() as session:
            result = await session.execute(
                select(TickerModel.name).
                where(TickerModel.ticker_id == ticker_id)
            )
            ticker_name = result.scalar()
            return ticker_name

    # Получение последней временной отметки по интервалу и тикеру
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

    # Добавление пересечения EMA
    async def add_ema_cross(self, ticker_id: int, interval: str, span: int, timestamp_column):
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

    # Получение количества пересечений EMA
    async def get_ema_cross_count(self, ticker_id, interval, span, start_time, end_time):
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

    # Получение существующих ключей EMA
    async def get_existing_ema_keys(self, ticker_id, interval, span):
        async with self.sessionmaker() as session:
            result = await session.execute(
                select(EMAModel.ticker_id, EMAModel.interval, EMAModel.span, EMAModel.timestamp_column).
                where(EMAModel.ticker_id == ticker_id, EMAModel.interval == interval, EMAModel.span == span)
            )
            return result.all()

    # Массовое добавление EMA
    async def bulk_add_ema(self, ema_data: list[Ema]):
        async with self.sessionmaker() as session:
            # Convert Ema Pydantic models to dictionaries and exclude unset or None values.
            ema_dicts = [ema.dict(exclude_unset=True) for ema in ema_data]

            # Insert Ema model instances using asynchronous execution.
            for ema_dict in ema_dicts:
                # Create a new instance of EMAModel for each dictionary.
                ema_model = EMAModel(**ema_dict)
                session.add(ema_model)

            # Asynchronously commit all the objects that were added.
            await session.commit()

    # Получение EMA для тикера по периоду
    async def get_ema_for_ticker_by_period(self, ticker_id: int, interval: str, span, end_time: datetime) -> Optional[
        Ema]:
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
            row = result.scalars().first()
            if row:
                return Ema(
                    timestamp_column=str(row.timestamp_column),
                    span=row.span,
                    ema=row.ema,
                    atr=row.atr
                )
            return None

    # Получение предпоследней EMA для тикера по периоду
    async def get_penultimate_ema_for_ticker_by_period(self, ticker_id: int, interval: str, span,
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
            row = result.first()
            if row:
                return Ema(
                    timestamp_column=row[0],
                    span=row[1],
                    ema=row[2],
                    atr=row[3]
                )
            return None

        # Получение последних двух свечей для каждого тикера

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

            # Передаем параметры в execute
            result = await session.execute(sql, {'interval': interval})
            candles_dict = {}

            for row in result.mappings():
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

    async def get_two_candles_for_each_ticker_by_period(self, interval: str, timestamp: datetime) -> dict[
        int, list[Candle]]:
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

            # Передаем параметры в execute
            result = await session.execute(sql, {'interval': interval, 'timestamp': timestamp})
            candles_dict = {}

            for row in result.mappings():
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
