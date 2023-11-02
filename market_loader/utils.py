from datetime import datetime, timedelta

import pytz
from tzlocal import get_localzone

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
    if (interval == CandleInterval.day.value and (current_time - cls.last_day_update).total_seconds() >= 3600) * 24+60:
        if update_time:
            cls.last_day_update = current_time.replace(minute=0, second=0, microsecond=0)
        return True


def convert_utc_to_local(utc_str: str) -> str:
    utc_time = datetime.strptime(utc_str, "%Y-%m-%d %H:%M:%S")
    utc_time = pytz.utc.localize(utc_time)
    local_tz = get_localzone()

    return utc_time.astimezone(local_tz).strftime("%H:%M")


def make_tw_link(ticker: str, interval: str) -> str:
    stock_exchange = 'MOEX'
    return (f'https://www.tradingview.com/chart/?symbol={stock_exchange}:{ticker}'
            f'&interval={get_interval_form_str_for_tw(interval)}')


def calculate_start_time(end_time, hours_to_subtract):

    # Если время после 20:50, то отсчет идет от 20:50 предыдущего рабочего дня
    if end_time.hour >= 20 and end_time.minute > 50:
        start_time = end_time.replace(hour=20, minute=50, second=0, microsecond=0) - timedelta(days=1)
    else:
        start_time = end_time.replace(minute=50, second=0, microsecond=0) - timedelta(hours=hours_to_subtract)

    # Проверяем, не попадает ли start_time на выходной
    while start_time.weekday() > 4:  # 5 и 6 - суббота и воскресенье
        start_time -= timedelta(days=1)

    return start_time

class MaxRetriesExceededError(Exception):
    def __init__(self, message="Max retries exceeded"):
        self.message = message
        super().__init__(self.message)
