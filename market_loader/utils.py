import uuid
from datetime import datetime, timedelta

import pytz
from tzlocal import get_localzone

from market_loader.settings import settings
from market_loader.models import Candle, CandleInterval, Ema, Price


def dict_to_float(num_dict: dict) -> float:
    units = int(num_dict["units"])
    nano = int(num_dict["nano"])
    return units + nano / 1e9


def round_date(date: datetime) -> datetime:
    minutes = date.minute
    rounded_minutes = (minutes + 4) // 5 * 5
    difference = rounded_minutes - minutes
    rounded_dt = date + timedelta(minutes=difference)
    return rounded_dt.replace(microsecond=999999)


def get_correct_time_format(date: datetime) -> str:
    return date.isoformat().replace('+00:00', '')[:-3] + 'Z'


def get_interval(interval: CandleInterval) -> str:
    if interval.value == 'CANDLE_INTERVAL_5_MIN':
        return '5 мин'

    if interval.value == 'CANDLE_INTERVAL_15_MIN':
        return '15 мин'

    if interval.value == 'CANDLE_INTERVAL_HOUR':
        return 'час'

    if interval.value == 'CANDLE_INTERVAL_DAY':
        return 'день'


def get_interval_form_str(interval: str) -> str:
    if interval == 'CANDLE_INTERVAL_5_MIN':
        return '5 мин'

    if interval == 'CANDLE_INTERVAL_15_MIN':
        return '15 мин'

    if interval == 'CANDLE_INTERVAL_HOUR':
        return 'час'

    if interval == 'CANDLE_INTERVAL_DAY':
        return 'день'


def get_interval_form_str_for_tw(interval: str) -> str:
    if interval == 'CANDLE_INTERVAL_5_MIN':
        return '5'

    if interval == 'CANDLE_INTERVAL_15_MIN':
        return '15'

    if interval == 'CANDLE_INTERVAL_HOUR':
        return '60'

    if interval == 'CANDLE_INTERVAL_DAY':
        return 'D'


def need_for_calculation(cls, interval: str, current_time: datetime, update_time: bool) -> bool:
    if interval == CandleInterval.min_5.value:
        return True
    if (interval == CandleInterval.min_15.value
            and (current_time - cls.last_15_min_update).total_seconds() >= 900):
        if update_time:
            cls.last_15_min_update = current_time.replace(second=0, microsecond=0)
        return True
    if (interval == CandleInterval.hour.value
            and (current_time - cls.last_hour_update).total_seconds() >= 3660):
        if update_time:
            cls.last_hour_update = current_time.replace(minute=0, second=0, microsecond=0)
        return True
    if (interval == CandleInterval.day.value and (
            current_time - cls.last_day_update).total_seconds() >= 3600) * 24 + 60:
        if update_time:
            cls.last_day_update = current_time.replace(minute=0, second=0, microsecond=0)
        return True


def convert_utc_to_local(utc_time: datetime) -> str:
    utc_time = pytz.utc.localize(utc_time)
    local_tz = get_localzone()

    return utc_time.astimezone(local_tz).strftime("%H:%M")


def convert_to_date(utc_str: str) -> datetime:
    return datetime.strptime(utc_str, "%Y-%m-%d %H:%M:%S")


def convert_to_base_date(date: str) -> datetime:
    return datetime.fromisoformat(date.replace('Z', '+00:00'))


def make_tw_link(ticker: str, interval: str) -> str:
    stock_exchange = 'MOEX'
    return (f'https://www.tradingview.com/chart/?symbol={stock_exchange}:{ticker}'
            f'&interval={get_interval_form_str_for_tw(interval)}')


def get_start_time(end_time: datetime, cross_window: int) -> datetime:
    if end_time.hour >= (settings.trade_start_hour + cross_window):
        return end_time - timedelta(hours=cross_window)
    elif settings.trade_start_hour <= end_time.hour <= settings.trade_end_hour:
        delta_hours = (settings.trade_start_hour + cross_window) - end_time.hour
        delta_days = 3 if end_time.weekday() == 0 else 1
        return ((end_time - timedelta(days=delta_days)).replace(hour=settings.trade_end_hour, minute=50)
                - timedelta(hours=delta_hours)).replace(tzinfo=None)
    else:
        return end_time.replace(tzinfo=None)


def to_start_of_day(date: datetime) -> datetime:
    return date.replace(hour=0, minute=0, second=0, microsecond=999999)


def to_end_of_day(date: datetime) -> datetime:
    return date.replace(hour=23, minute=59, second=59, microsecond=999999)


def calculate_percentage(part: float, whole: float) -> float:
    try:
        return round((part / whole) * 100, 2)
    except ZeroDivisionError:
        return 0


def get_rebound_message(ticker_name: str, current_ema: Ema, older_ema: Ema, interval: CandleInterval,
                        older_interval: CandleInterval, latest_candle: Candle, prev_candle: Candle, cross_count: int,
                        type_msg: str) -> str:
    if type_msg == 'SHORT':
        candle_part = 'High'
        candle_val = latest_candle.high
        prev_candle_val = prev_candle.high
    else:
        candle_part = 'Low'
        candle_val = latest_candle.low
        prev_candle_val = prev_candle.low

    return (f'<b>{type_msg} #{ticker_name}</b> пересек EMA {int(current_ema.span)} ({current_ema.ema}) в интервале '
            f'{get_interval_form_str(interval.value)}.\nВремя: {convert_utc_to_local(current_ema.timestamp_column)}.\n'
            f'ATR: {calculate_percentage(current_ema.atr, current_ema.ema)}%.\nКоличество пересечений за последние '
            f'{settings.ema_cross_window} часа: {cross_count}.\n'
            f'{candle_part} свечи: {candle_val}. '
            f'Время свечи: {convert_utc_to_local(latest_candle.timestamp_column)}.\n'
            f'{candle_part} предыдущей свечи: {prev_candle_val}. Время свечи '
            f'{convert_utc_to_local(prev_candle.timestamp_column)}.\n'
            f'Старшая EMA {older_ema.span} в интервале {get_interval_form_str(older_interval.value)}: {older_ema.ema}.'
            f' Время: {convert_utc_to_local(older_ema.timestamp_column)}.\n'
            f'<a href="{make_tw_link(ticker_name, interval.value)}">График tradingview</a>')


def transform_candle_result(result) -> dict:
    candles_dict = {}
    for row in result.mappings():
        candle = Candle(
            timestamp_column=row['timestamp_column'],
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


def get_uuid():
    return str(uuid.uuid4())


def price_to_units_and_nano(price: float) -> Price:
    units = int(price)
    nano = int((price - units) * 1_000_000_000)
    return Price(units=units, nano=nano)


class MaxRetriesExceededError(Exception):
    def __init__(self, message="Max retries exceeded"):
        self.message = message
        super().__init__(self.message)
