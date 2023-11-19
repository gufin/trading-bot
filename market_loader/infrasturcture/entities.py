import uuid

from sqlalchemy import (BIGINT, Boolean, Column, ForeignKey, Integer, Numeric, String, Text, TIMESTAMP,
                        UniqueConstraint)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import declarative_base, relationship

from market_loader.settings import settings

engine = create_async_engine(settings.storage_url, echo=False)
Base = declarative_base()
metadata = Base.metadata


def get_sessionmaker():
    return async_sessionmaker(bind=engine, expire_on_commit=False)


class UserModel(Base):
    __tablename__ = 'users'

    user_id = Column(BIGINT, primary_key=True, autoincrement=True)
    name = Column(String(32), nullable=False)
    lang = Column(String(20), nullable=True)
    disable = Column(Boolean, default=False)

    user_strategies = relationship('UserStrategyModel', back_populates='user')
    user_tickers = relationship('UserTickerModel', back_populates='user')


class StrategyModel(Base):
    __tablename__ = 'strategies'

    strategy_id = Column(BIGINT, primary_key=True, autoincrement=True)
    name = Column(String(64), nullable=False)
    description = Column(Text, nullable=True)

    user_strategies = relationship('UserStrategyModel', back_populates='strategy')


class TimeframeModel(Base):
    __tablename__ = 'timeframes'

    timeframe_id = Column(BIGINT, primary_key=True, autoincrement=True)
    name = Column(String(64), nullable=False)
    description = Column(Text, nullable=True)

    user_strategies = relationship('UserStrategyModel', back_populates='timeframe')


class UserStrategyModel(Base):
    __tablename__ = 'user_strategies'

    user_strategy_id = Column(BIGINT, primary_key=True, autoincrement=True)
    user_id = Column(BIGINT, ForeignKey('users.user_id'), nullable=False)
    strategy_id = Column(BIGINT, ForeignKey('strategies.strategy_id'), nullable=False)
    timeframe_id = Column(BIGINT, ForeignKey('timeframes.timeframe_id'), nullable=False)

    __table_args__ = (UniqueConstraint('user_id', 'strategy_id', 'timeframe_id',
                                       name='unique_user_strategy_timeframe'),)

    user = relationship('UserModel', back_populates='user_strategies')
    strategy = relationship('StrategyModel', back_populates='user_strategies')
    timeframe = relationship('TimeframeModel', back_populates='user_strategies')


class TickerModel(Base):
    __tablename__ = 'tickers'

    ticker_id = Column(BIGINT, primary_key=True, autoincrement=True)
    figi = Column(String(64), nullable=True)
    classcode = Column(String(64), nullable=True)
    currency = Column(String(64), nullable=True)
    name = Column(String(64), nullable=False)
    lot = Column(Integer, nullable=False, default=0)
    min_price_increment = Column(Numeric(10, 3), nullable=True)
    disable = Column(Boolean, default=False)

    __table_args__ = (UniqueConstraint('name', name='unique_ticker_name'),)

    user_tickers = relationship('UserTickerModel', back_populates='ticker')
    candles = relationship('CandleModel', back_populates='ticker')
    ema = relationship('EMAModel', back_populates='ticker')
    ema_cross = relationship('EMACrossModel', back_populates='ticker')


class UserTickerModel(Base):
    __tablename__ = 'user_tickers'

    user_ticker_id = Column(BIGINT, primary_key=True, autoincrement=True)
    user_id = Column(BIGINT, ForeignKey('users.user_id'), nullable=False)
    ticker_id = Column(BIGINT, ForeignKey('tickers.ticker_id'), nullable=False)

    user = relationship('UserModel', back_populates='user_tickers')
    ticker = relationship('TickerModel', back_populates='user_tickers')


class CandleModel(Base):
    __tablename__ = 'candles'

    candl_id = Column(BIGINT, primary_key=True, autoincrement=True)
    ticker_id = Column(BIGINT, ForeignKey('tickers.ticker_id'), nullable=False)
    interval = Column(String(64), nullable=False)
    timestamp_column = Column(TIMESTAMP, nullable=False)
    open = Column(Numeric(10, 3), nullable=False)
    high = Column(Numeric(10, 3), nullable=False)
    low = Column(Numeric(10, 3), nullable=False)
    close = Column(Numeric(10, 3), nullable=False)

    __table_args__ = (UniqueConstraint('ticker_id', 'interval', 'timestamp_column', name='unique_candle'),)

    ticker = relationship('TickerModel', back_populates='candles')


class EMAToCalcModel(Base):
    __tablename__ = 'ema_to_calc'

    ema_to_calc_id = Column(BIGINT, primary_key=True, autoincrement=True)
    interval = Column(String(64), nullable=False)
    span = Column(Integer, nullable=False)


class EMAModel(Base):
    __tablename__ = 'ema'

    ema_id = Column(BIGINT, primary_key=True, autoincrement=True)
    ticker_id = Column(BIGINT, ForeignKey('tickers.ticker_id'), nullable=False)
    interval = Column(String(64), nullable=False)
    span = Column(Integer, nullable=False)
    timestamp_column = Column(TIMESTAMP, nullable=False)
    ema = Column(Numeric(10, 3), nullable=False)
    atr = Column(Numeric(10, 3), nullable=False)
    __table_args__ = (UniqueConstraint('ticker_id', 'interval', 'timestamp_column', 'span', name='unique_ema'),)

    ticker = relationship('TickerModel', back_populates='ema')


class EMACrossModel(Base):
    __tablename__ = 'ema_cross'

    ema_cross_id = Column(BIGINT, primary_key=True)
    ticker_id = Column(BIGINT, ForeignKey('tickers.ticker_id'), nullable=False)
    interval = Column(String(64), nullable=False)
    span = Column(Integer, nullable=False)
    timestamp_column = Column(TIMESTAMP, nullable=False)

    __table_args__ = (UniqueConstraint(
        'ticker_id', 'interval', 'span', 'timestamp_column', name='unique_ema_cross_combination'),)

    ticker = relationship('TickerModel', back_populates='ema_cross')


class BrokerAccount(Base):
    __tablename__ = 'broker_account'

    id = Column(BIGINT, primary_key=True)
    broker_id = Column(String(64), nullable=False)
    user_id = Column(BIGINT, ForeignKey('users.user_id'), nullable=False)

    __table_args__ = (UniqueConstraint('broker_id', 'user_id', name='unique_broker_account'),)


class Order(Base):
    __tablename__ = 'orders'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    orderId = Column(UUID(as_uuid=True), nullable=False)
    executionReportStatus = Column(String, nullable=False)
    lotsRequested = Column(Integer, nullable=False)
    lotsExecuted = Column(Integer, nullable=False)
    initialOrderPrice = Column(Numeric(10, 3), nullable=False)
    executedOrderPrice = Column(Numeric(10, 3), nullable=False)
    totalOrderAmount = Column(Numeric(10, 3), nullable=False)
    initialCommission = Column(Numeric(10, 3), nullable=False)
    executedCommission = Column(Numeric(10, 3), nullable=False)
    figi = Column(String, nullable=False)
    direction = Column(String, nullable=False)
    initialSecurityPrice = Column(Numeric(10, 3), nullable=False)
    orderType = Column(String, nullable=False)
    message = Column(String)
    instrumentUid = Column(UUID(as_uuid=True), nullable=False)
    orderRequestId = Column(String)
    accountId = Column(String(64), nullable=False)
    timestamp = Column(TIMESTAMP, nullable=True)
    atr = Column(Numeric(10, 3), nullable=True)

    __table_args__ = (UniqueConstraint('orderId', name='unique_orderIdt'),)


class PositionCheckTask(Base):
    __tablename__ = 'positions_checks'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    broker_account_id = Column(BIGINT, ForeignKey('broker_account.id'), nullable=False)
    timestamp = Column(TIMESTAMP, nullable=False)

    __table_args__ = (UniqueConstraint('broker_account_id', 'timestamp', name='unique_positions_checks'),)


class Position(Base):
    __tablename__ = 'positions'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    ticker_id = Column(BIGINT, ForeignKey('tickers.ticker_id'), nullable=False)
    task = Column(UUID, ForeignKey('positions_checks.id'), nullable=False)

    __table_args__ = (UniqueConstraint('ticker_id', 'task', name='unique_positions'),)


class Deal(Base):
    __tablename__ = 'deals'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    ticker_id = Column(BIGINT, ForeignKey('tickers.ticker_id'), nullable=False)
    buy_order = Column(UUID, ForeignKey('orders.id'), nullable=False)
    sell_order = Column(UUID, ForeignKey('orders.id'), nullable=True)



