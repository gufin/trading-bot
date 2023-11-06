import os

from dotenv import load_dotenv
from sqlalchemy import (BIGINT, Boolean, Column, ForeignKey, Integer, Numeric, String, Text, TIMESTAMP,
                        UniqueConstraint)
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import declarative_base, relationship

load_dotenv()
name = os.getenv("PG_NAME")
user = os.getenv("PG_USER")
password = os.getenv("PG_PASSWORD")
host = os.getenv("PG_HOST")
port = os.getenv("PG_PORT")


def storage_url():
    return (
        f"postgresql+asyncpg://{user}:{password}"
        f"@{host}:{port}/{name}"
    )


engine = create_async_engine(storage_url(), echo=False)
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
