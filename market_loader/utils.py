from datetime import datetime, timedelta

import pytz
from tzlocal import get_localzone

from market_loader.constants import ema_cross_window, trade_end_hour, trade_start_hour
from market_loader.models import CandleInterval


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


def convert_utc_to_local(utc_str: str) -> str:
    utc_time = datetime.strptime(utc_str, "%Y-%m-%d %H:%M:%S")
    utc_time = pytz.utc.localize(utc_time)
    local_tz = get_localzone()

    return utc_time.astimezone(local_tz).strftime("%H:%M")


def convert_to_date(utc_str: str) -> datetime:
    return datetime.strptime(utc_str, "%Y-%m-%d %H:%M:%S")


def make_tw_link(ticker: str, interval: str) -> str:
    stock_exchange = 'MOEX'
    return (f'https://www.tradingview.com/chart/?symbol={stock_exchange}:{ticker}'
            f'&interval={get_interval_form_str_for_tw(interval)}')


def get_start_time(end_time: datetime) -> datetime:
    if end_time.hour >= (trade_start_hour + ema_cross_window):
        return end_time - timedelta(hours=ema_cross_window)
    elif trade_start_hour <= end_time.hour <= trade_end_hour:
        delta_hours = (trade_start_hour + ema_cross_window) - end_time.hour
        delta_days = 3 if end_time.weekday() == 0 else 1
        return ((end_time - timedelta(days=delta_days)).replace(hour=trade_end_hour, minute=50)
                - timedelta(hours=delta_hours)).replace(tzinfo=None)
    else:
        return end_time.replace(tzinfo=None)


def to_start_of_day(date: datetime) -> datetime:
    return date.replace(hour=0, minute=0, second=0, microsecond=999999)


def to_end_of_day(date: datetime) -> datetime:
    return date.replace(hour=23, minute=59, second=59, microsecond=999999)


class MaxRetriesExceededError(Exception):
    def __init__(self, message="Max retries exceeded"):
        self.message = message
        super().__init__(self.message)
