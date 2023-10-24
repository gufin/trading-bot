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
    rounded_minutes = round(minutes / 5) * 5
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


def get_interval_form_str(interval):
    if interval == 'CANDLE_INTERVAL_5_MIN':
        return '5 мин'

    if interval == 'CANDLE_INTERVAL_15_MIN':
        return '15 мин'

    if interval == 'CANDLE_INTERVAL_HOUR':
        return 'час'

    if interval == 'CANDLE_INTERVAL_DAY':
        return 'день'


def need_for_calculation(cls, interval: str, current_time: datetime) -> bool:
    if interval == CandleInterval.min_5.value:
        return True
    if (interval == CandleInterval.min_15.value
            and (current_time - cls.last_15_min_update).total_seconds() >= 900):
        cls.last_15_min_update = current_time
        return True
    if (interval == CandleInterval.hour.value
            and (current_time - cls.last_hour_update).total_seconds() >= 3600):
        cls.last_hour_update = current_time
        return True
    if (interval == CandleInterval.day.value and (current_time - cls.last_day_update).total_seconds() >= 3600) * 24:
        cls.last_day_update = current_time
        return True


def convert_utc_to_local(utc_str):
    utc_time = datetime.strptime(utc_str, "%Y-%m-%d %H:%M:%S")
    utc_time = pytz.utc.localize(utc_time)
    local_tz = get_localzone()

    return utc_time.astimezone(local_tz).strftime("%H:%M")


class MaxRetriesExceededError(Exception):
    def __init__(self, message="Max retries exceeded"):
        self.message = message
        super().__init__(self.message)
