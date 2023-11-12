from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel


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
    lot: int = 1


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


class ExtendedReboundParam(BaseModel):
    cross_count_4: int
    cross_count_1: int
    cross_count_12: int
    hour_candle: Optional[Candle] = None


class MainReboundParam(BaseModel):
    curr_ema: Optional[Ema] = None
    prev_ema: Optional[Ema] = None
    older_ema: Optional[Ema] = None
    prev_candle: Candle
    latest_candle: Candle
    ticker_name: str


class OrderDirection(Enum):
    buy = 'ORDER_DIRECTION_BUY'
    sell = 'ORDER_DIRECTION_SELL'


class OrderType(Enum):
    limit = 'ORDER_TYPE_LIMIT'
    market = 'ORDER_TYPE_MARKET'
    best_price = 'ORDER_TYPE_BESTPRICE'


class Price(BaseModel):
    units: int
    nano: int


class Order(BaseModel):
    figi: str
    quantity: int
    price: Price
    direction: str
    accountId: str
    orderType: str
    orderId: str
    instrumentId: str


class AccountRequest(BaseModel):
    accountId: str


class PortfolioRequest(AccountRequest):
    currency: str = 'RUB'


class OrderInfo(BaseModel):
    orderId: str
    executionReportStatus: str
    lotsRequested: int
    lotsExecuted: int
    initialOrderPrice: float
    executedOrderPrice: float
    totalOrderAmount: float
    initialCommission: float
    executedCommission: float
    figi: str
    direction: str
    initialSecurityPrice: float
    orderType: str
    message: Optional[str] = ""
    instrumentUid: str
    orderRequestId: Optional[str] = ""
    accountId: str


class OrderUpdateRequest(AccountRequest):
    orderId: str


class ReplaceOrderRequest(BaseModel):
    accountId: str
    orderId: str
    idempotencyKey: str
    quantity: int
    price: Price
    price_type: str = 'PRICE_TYPE_CURRENCY'
