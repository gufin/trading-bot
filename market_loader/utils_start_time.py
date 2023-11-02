from datetime import datetime, timedelta

from market_loader.constants import ema_cross_window, trade_end_hour, trade_start_hour


def get_start_time(end_time: datetime) -> datetime:

    if end_time.hour >= (trade_start_hour + ema_cross_window):
        return end_time - timedelta(hours=ema_cross_window)
    elif trade_start_hour <= end_time.hour <= trade_end_hour:
        delta_hours = (trade_start_hour + ema_cross_window) - end_time.hour
        delta_days = 3 if end_time.weekday() == 0 else 1
        return ((end_time - timedelta(days=delta_days)).replace(hour=trade_end_hour, minute=50)
                - timedelta(hours=delta_hours))
    else:
        return end_time
