from datetime import datetime, timedelta

from market_loader.models import CandleInterval


def dict_to_float(num_dict):
    units = int(num_dict["units"])
    nano = int(num_dict["nano"])
    return units + nano / 1e9


def round_date(date: datetime):
    minutes = date.minute
    rounded_minutes = round(minutes / 5) * 5
    difference = rounded_minutes - minutes
    rounded_dt = date + timedelta(minutes=difference)
    return rounded_dt.replace(microsecond=999999)


def get_correct_time_format(date):
    return date.isoformat().replace('+00:00', '')[:-3] + 'Z'


def get_interval(interval: CandleInterval):
    if interval.value == 'CANDLE_INTERVAL_5_MIN':
        return '5 мин'

    if interval.value == 'CANDLE_INTERVAL_15_MIN':
        return '15 мин'

    if interval.value == 'CANDLE_INTERVAL_HOUR':
        return 'час'

    if interval.value == 'CANDLE_INTERVAL_DAY':
        return 'день'


class MaxRetriesExceededError(Exception):
    def __init__(self, message="Max retries exceeded"):
        self.message = message
        super().__init__(self.message)
