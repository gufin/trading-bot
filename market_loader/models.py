from enum import Enum

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
    CANDLE_INTERVAL_5_MIN = 'CANDLE_INTERVAL_5_MIN'
    CANDLE_INTERVAL_15_MIN = 'CANDLE_INTERVAL_15_MIN'
    CANDLE_INTERVAL_HOUR = 'CANDLE_INTERVAL_HOUR'
    CANDLE_INTERVAL_DAY = 'CANDLE_INTERVAL_DAY'


class Candle(BaseModel):
    timestamp_column: str
    open: float
    high: float
    low: float
    close: float


class Ema(BaseModel):
    timestamp_column: str
    span: float
    ema: float




