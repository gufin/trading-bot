from enum import Enum
from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class ApiConfig(BaseModel):
    token: str = None
    base_url: str = None
    share_by: str = None
    get_candles: str = None
    find_instrument: str = None


class FindInstrumentRequest(BaseModel):
    query: str
    instrumentKind: str = "INSTRUMENT_TYPE_SHARE"
    apiTradeAvailableFlag: bool = True


class Ticker(BaseModel):
    ticker_id: int
    figi: str = None
    classCode: str = None
    currency: str = None
    name: str


class TickerToUpdateEma(BaseModel):
    ticker_id: int
    name: str
    interval: str
    span: int


class EmaToCalc(BaseModel):
    interval: str
    span: int


class InstrumentRequest(BaseModel):
    id_type: str = "INSTRUMENT_ID_TYPE_TICKER"
    classCode: str
    id: str


class CandleData(BaseModel):
    figi: str
    from_: str
    to: str
    interval: str
    instrumentId: str


class CandleInterval(Enum):
    min_5 = 'CANDLE_INTERVAL_5_MIN'
    min_15 = 'CANDLE_INTERVAL_15_MIN'
    hour = 'CANDLE_INTERVAL_HOUR'
    day = 'CANDLE_INTERVAL_DAY'


class Candle(BaseModel):
    timestamp_column: datetime
    open: float
    high: float
    low: float
    close: float


class Ema(BaseModel):
    ticker_id: int = None
    interval: str = None
    span: int
    timestamp_column: datetime
    ema: float
    atr: float = 0


class ReboundParam(BaseModel):
    cross_count_4: int
    cross_count_1: int
    hour_candle: Optional[Candle] = None
