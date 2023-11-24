import uuid
from datetime import datetime
from datetime import timedelta, timezone
from typing import Optional

import pandas as pd
from sqlalchemy import and_, desc, exists, func, or_, select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession
from sqlalchemy.orm import aliased

from market_loader.infrasturcture.entities import (BrokerAccount, CandleModel, Deal, EMACrossModel, EMAModel,
                                                   EMAToCalcModel,
                                                   Order, Position, PositionCheckTask, StrategyModel,
                                                   TickerModel,
                                                   TimeframeModel, UserModel,
                                                   UserStrategyModel, UserTickerModel)
from market_loader.models import Candle, CandleInterval, Ema, EmaToCalc, OrderDirection, OrderInfo, \
    ReplaceOrderRequest, Ticker, \
    TickerToUpdateEma
from market_loader.utils import get_uuid, price_to_units_and_nano, transform_candle_result


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
                select(TickerModel).where(or_(TickerModel.figi.is_(None), TickerModel.lot.is_(None),
                                              TickerModel.min_price_increment.is_(None)))
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
                    name=row.name,
                    lot=row.lot,
                    min_price_increment=row.min_price_increment
                ) for row in result.scalars()]

    async def update_tickers(self, ticker_id: int, new_figi: str, new_class_code: str,
                             new_currency: str, lot: int, min_price_increment: float) -> Optional[Ticker]:
        async with self.sessionmaker() as session:
            result = await session.execute(
                update(TickerModel).where(TickerModel.ticker_id == ticker_id).
                values(figi=new_figi, classcode=new_class_code, currency=new_currency, lot=lot,
                       min_price_increment=min_price_increment).
                returning(TickerModel)
            )
            await session.commit()
            if row := result.scalars().first():
                return Ticker(
                    ticker_id=row.ticker_id,
                    figi=row.figi,
                    classCode=row.classcode,
                    currency=row.currency,
                    name=row.name,
                    lot=row.lot,
                    min_price_increment=row.min_price_increment
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
            return pd.DataFrame(candles_data)

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

    async def get_ticker_by_id(self, ticker_id: int) -> Optional[Ticker]:
        async with self.sessionmaker() as session:
            result = await session.execute(
                select(TickerModel).
                where(TickerModel.ticker_id == ticker_id)
            )

            if row := result.scalars().first():
                return Ticker(ticker_id=row.ticker_id,
                              figi=row.figi,
                              classCode=row.classcode,
                              currency=row.currency,
                              name=row.name,
                              lot=row.lot,
                              min_price_increment=row.min_price_increment)
            else:
                return None

    async def get_ticker_by_figi(self, figi: str) -> Optional[Ticker]:
        async with self.sessionmaker() as session:
            result = await session.execute(
                select(TickerModel).
                where(TickerModel.figi == figi)
            )

            if row := result.scalars().first():
                return Ticker(ticker_id=row.ticker_id,
                              figi=row.figi,
                              classCode=row.classcode,
                              currency=row.currency,
                              name=row.name,
                              lot=row.lot,
                              min_price_increment=row.min_price_increment)
            else:
                return None

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

    async def add_ema_cross(self, ticker_id: int, interval: str, span: int, timestamp_column) -> bool:
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
                return True
            except IntegrityError:
                await session.rollback()
                return False

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

    async def get_last_candle(self, ticker_id: int, interval: str) -> Optional[Candle]:
        async with self.sessionmaker() as session:
            result = await session.execute(
                select(CandleModel)
                .where(CandleModel.ticker_id == ticker_id, CandleModel.interval == interval)
                .order_by(CandleModel.timestamp_column.desc())
                .limit(1)
            )
            if candle_model := result.scalars().first():
                return Candle(
                    timestamp_column=candle_model.timestamp_column,
                    open=float(candle_model.open),
                    high=float(candle_model.high),
                    low=float(candle_model.low),
                    close=float(candle_model.close)
                )
            else:
                return None

    async def get_user_account(self, user_id: int) -> Optional[str]:
        async with self.sessionmaker() as session:
            result = await session.execute(
                select(BrokerAccount.broker_id)
                .where(BrokerAccount.user_id == user_id)
            )
            broker_id = result.scalars().first()
            return broker_id

    async def add_order(self, order_info: OrderInfo) -> None:
        async with self.sessionmaker() as session:
            order = Order(
                orderId=uuid.UUID(order_info.orderId),
                executionReportStatus=order_info.executionReportStatus,
                lotsRequested=order_info.lotsRequested,
                lotsExecuted=order_info.lotsExecuted,
                initialOrderPrice=order_info.initialOrderPrice,
                executedOrderPrice=order_info.executedOrderPrice,
                totalOrderAmount=order_info.totalOrderAmount,
                initialCommission=order_info.initialCommission,
                executedCommission=order_info.executedCommission,
                figi=order_info.figi,
                direction=order_info.direction,
                initialSecurityPrice=order_info.initialSecurityPrice,
                orderType=order_info.orderType,
                message=order_info.message,
                instrumentUid=uuid.UUID(order_info.instrumentUid),
                orderRequestId=order_info.orderRequestId,
                accountId=order_info.accountId,
                timestamp=datetime.now(timezone.utc).replace(tzinfo=None),
                atr=order_info.atr
            )

            try:
                session.add(order)
                await session.commit()
            except Exception as e:
                await session.rollback()

    async def update_order(self, order_info: OrderInfo) -> None:
        async with self.sessionmaker() as session:
            stmt = select(Order).where(Order.orderId == uuid.UUID(order_info.orderId))
            result = await session.execute(stmt)
            if existing_order := result.scalar():
                existing_order.executionReportStatus = order_info.executionReportStatus
                existing_order.lotsRequested = order_info.lotsRequested
                existing_order.lotsExecuted = order_info.lotsExecuted
                existing_order.initialOrderPrice = order_info.initialOrderPrice
                existing_order.executedOrderPrice = order_info.executedOrderPrice
                existing_order.totalOrderAmount = order_info.totalOrderAmount
                existing_order.initialCommission = order_info.initialCommission
                existing_order.executedCommission = order_info.executedCommission
                existing_order.figi = order_info.figi
                existing_order.direction = order_info.direction
                existing_order.initialSecurityPrice = order_info.initialSecurityPrice
                existing_order.orderType = order_info.orderType
                existing_order.message = order_info.message
                existing_order.instrumentUid = uuid.UUID(order_info.instrumentUid)
                existing_order.orderRequestId = order_info.orderRequestId

                try:
                    await session.commit()
                except Exception as e:
                    await session.rollback()

    async def get_active_orders(self, account_id: str) -> list[str]:
        async with self.sessionmaker() as session:
            stmt = select(Order.orderId).where(
                or_(
                    Order.executionReportStatus == 'EXECUTION_REPORT_STATUS_NEW',
                    Order.executionReportStatus == 'EXECUTION_REPORT_STATUS_PARTIALLYFILL'
                ),
                Order.accountId == account_id
            )

            # Выполнение запроса
            result = await session.execute(stmt)
            return [str(row[0]) for row in result.fetchall()]

    async def get_active_order_by_figi(self, account_id: str, figi: str, direction: OrderDirection) -> Optional[
        ReplaceOrderRequest]:
        async with self.sessionmaker() as session:
            stmt = (select(Order)
                    .where(Order.figi == figi,
                           Order.accountId == account_id,
                           Order.executionReportStatus == 'EXECUTION_REPORT_STATUS_NEW',
                           Order.direction == direction.value)
                    .order_by(desc(Order.timestamp)))
            result = await session.execute(stmt)
            if order := result.scalars().first():
                return ReplaceOrderRequest(accountId=account_id,
                                           orderId=str(order.orderId),
                                           idempotencyKey=get_uuid(),
                                           quantity=order.lotsRequested,
                                           price=price_to_units_and_nano(order.initialOrderPrice))
            return None

    async def get_latest_order_by_direction(self, account_id: str, figi: str, direction: OrderDirection) -> Optional[
        OrderInfo]:
        async with self.sessionmaker() as session:
            result = await session.execute(
                select(Order).filter(
                    Order.figi == figi,
                    Order.accountId == account_id,
                    Order.direction == direction.value
                ).order_by(desc(Order.timestamp)).limit(1)
            )

            order = result.scalars().first()
            return OrderInfo(orderId=str(order.orderId),
                             executionReportStatus=order.executionReportStatus,
                             lotsRequested=order.lotsRequested,
                             lotsExecuted=order.lotsExecuted,
                             initialOrderPrice=order.initialOrderPrice,
                             executedOrderPrice=order.executedOrderPrice,
                             totalOrderAmount=order.totalOrderAmount,
                             initialCommission=order.initialCommission,
                             executedCommission=order.executedCommission,
                             figi=order.figi,
                             direction=order.direction,
                             initialSecurityPrice=order.initialSecurityPrice,
                             orderType=order.orderType,
                             message=order.message,
                             instrumentUid=str(order.instrumentUid),
                             orderRequestId=order.orderRequestId,
                             accountId=order.accountId,
                             atr=order.atr) if order else None

    async def cancel_order(self, order_id: str) -> None:
        async with self.sessionmaker() as session:
            stmt = select(Order).where(Order.orderId == uuid.UUID(order_id))
            result = await session.execute(stmt)
            if existing_order := result.scalar():
                existing_order.executionReportStatus = 'EXECUTION_REPORT_STATUS_CANCELLED'
                try:
                    await session.commit()
                except Exception as e:
                    await session.rollback()

    async def get_cross_group_by_hour(self, ticker_id: int, interval: str, span: int, start_time: datetime,
                                      end_time: datetime) -> int:
        async with self.sessionmaker() as session:
            result = await session.execute(
                select(
                    func.count(func.distinct(func.date_trunc('hour', EMACrossModel.timestamp_column)))
                ).filter(
                    and_(
                        EMACrossModel.ticker_id == ticker_id,
                        EMACrossModel.interval == interval,
                        EMACrossModel.span == span,
                        EMACrossModel.timestamp_column >= start_time,
                        EMACrossModel.timestamp_column <= end_time
                    )
                )
            )

            return result.scalar()

    async def get_active_figi(self) -> list[str]:
        async with self.sessionmaker() as session:
            stmt = select(TickerModel.figi).where(TickerModel.disable == False)
            result = await session.execute(stmt)
            return [row for row in result.scalars().all()]

    async def get_latest_positions(self, account_id: str):
        async with self.sessionmaker() as session:
            result = await session.execute(
                select(PositionCheckTask).where(PositionCheckTask.broker_account_id == account_id).order_by(
                    desc(PositionCheckTask.timestamp)))
            if latest_task := result.scalars().first():
                result = await session.execute(select(Position).where(Position.task == latest_task.id))
                positions = result.scalars().all()

                # Получение данных тикеров
                tickers = []
                for position in positions:
                    result = await session.execute(
                        select(TickerModel).filter(TickerModel.ticker_id == position.ticker_id))
                    if ticker_data := result.scalars().first():
                        tickers.append(Ticker(
                            ticker_id=ticker_data.ticker_id,
                            figi=ticker_data.figi,
                            classCode=ticker_data.classcode,
                            currency=ticker_data.currency,
                            name=ticker_data.name,
                            lot=ticker_data.lot,
                            min_price_increment=float(ticker_data.min_price_increment)
                        ))

                return tickers

    async def create_new_position_check_task(self, account_id: str) -> uuid.UUID:
        async with self.sessionmaker() as session:
            record_time = datetime.now(timezone.utc).replace(tzinfo=None)
            new_task = PositionCheckTask(
                broker_account_id=account_id,
                timestamp=record_time
            )
            session.add(new_task)
            await session.commit()
            return new_task.id

    async def add_positions(self, task: uuid.UUID, tickers: list[Ticker]) -> None:
        async with self.sessionmaker() as session:
            for ticker in tickers:
                new_position = Position(
                    ticker_id=ticker.ticker_id,
                    task=task
                )
                session.add(new_position)
            await session.commit()

    async def get_order_id(self, order_id: str) -> Optional[uuid.UUID]:
        async with self.sessionmaker() as session:
                result = await session.execute(
                    select(Order.id).where(Order.orderId == uuid.UUID(order_id))
                )
                return result.scalars().first()

    async def add_deal(self, ticker_id: int, buy_order: str):
        async with self.sessionmaker() as session:
            buy_order_id = await self.get_order_id(buy_order)
            new_deal = Deal(
                ticker_id=ticker_id,
                buy_order=buy_order_id,
            )
            session.add(new_deal)
            try:
                await session.commit()
                return True
            except Exception as e:
                await session.rollback()
                return False

    async def update_deal(self, ticker_id: int, buy_order: str, sell_order: str):
        async with self.sessionmaker() as session:
            buy_order_id = await self.get_order_id(buy_order)
            stmt = select(Deal).where(
                Deal.ticker_id == ticker_id,
                Deal.buy_order == buy_order_id,
                Deal.sell_order == None
            )
            result = await session.execute(stmt)
            deal = result.scalar_one_or_none()

            if deal:
                sell_order_id = await self.get_order_id(sell_order)
                await session.execute(
                    update(Deal)
                    .where(Deal.id == deal.id)
                    .values(sell_order=sell_order_id)
                )
                await session.commit()
                return True

            return False

    async def get_deal_journal(self):
        async with self.sessionmaker() as session:
            buy_order_alias = aliased(Order)
            sell_order_alias = aliased(Order)

            stmt = (select(
                TickerModel.name,
                TickerModel.lot,
                TickerModel.min_price_increment,
                buy_order_alias.lotsRequested.label("buy_lotsRequested"),
                buy_order_alias.executedOrderPrice.label("buy_executedOrderPrice"),
                buy_order_alias.executedCommission.label("buy_executedCommission"),
                buy_order_alias.initialSecurityPrice.label("buy_initialSecurityPrice"),
                buy_order_alias.timestamp.label("buy_timestamp"),
                buy_order_alias.atr.label("buy_atr"),
                sell_order_alias.lotsRequested.label("sell_lotsRequested"),
                sell_order_alias.executedOrderPrice.label("sell_executedOrderPrice"),
                sell_order_alias.executedCommission.label("sell_executedCommission"),
                sell_order_alias.initialSecurityPrice.label("sell_initialSecurityPrice"),
                sell_order_alias.timestamp.label("sell_timestamp"),
                sell_order_alias.atr.label("sell_atr")
            ).select_from(
                Deal
            ).join(
                TickerModel, Deal.ticker_id == TickerModel.ticker_id
            ).join(
                buy_order_alias, Deal.buy_order == buy_order_alias.id
            ).outerjoin(
                sell_order_alias, Deal.sell_order == sell_order_alias.id
            ).order_by(
                desc(buy_order_alias.timestamp)
            ))

            result = await session.execute(stmt)
            rows = result.all()

            # Преобразование результатов в DataFrame
            df = pd.DataFrame(rows, columns=[
                "ticker_name",
                "lot",
                "min_price_increment",
                "buy_lotsRequested",
                "buy_executedOrderPrice",
                "buy_executedCommission",
                "buy_initialSecurityPrice",
                "buy_timestamp",
                "buy_atr",
                "sell_lotsRequested",
                "sell_executedOrderPrice",
                "sell_executedCommission",
                "sell_initialSecurityPrice",
                "sell_timestamp",
                "sell_atr"
            ])

            df['profit'] = df.apply(
                lambda row: row['sell_executedOrderPrice'] - row['buy_executedOrderPrice']
                if pd.notnull(row['sell_executedOrderPrice']) else 0,
                axis=1
            )
            column_order = ["ticker_name", "profit", "buy_timestamp", "sell_timestamp"] + \
                           [col for col in df.columns if
                            col not in ["ticker_name", "profit", "buy_timestamp", "sell_timestamp"]]
            df = df[column_order]

            return df