import asyncio
import uuid
from datetime import datetime, timedelta

import httpx
import pytz
from httpx import Response
from loguru import logger
from tzlocal import get_localzone

from market_loader.models import Candle, CandleInterval, Ema, MainReboundParam, Price
from market_loader.settings import settings


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


def get_start_time(end_time: datetime, cross_window_hours: int, cross_window_minutes: int = None) -> datetime:
    cross_window_minutes = cross_window_minutes if cross_window_minutes is not None else 0
    if (end_time.hour >= (settings.trade_start_hour + cross_window_hours) and
            (cross_window_minutes == 0 or end_time.minute >= cross_window_minutes)):
        total_cross_minutes = cross_window_hours * 60 + cross_window_minutes
        return end_time - timedelta(minutes=total_cross_minutes)
    elif settings.trade_start_hour <= end_time.hour <= settings.trade_end_hour:
        return extracted_from_get_start_time_15(
            cross_window_hours, end_time, cross_window_minutes
        )
    else:
        return end_time.replace(tzinfo=None)


def extracted_from_get_start_time_15(cross_window_hours: int, end_time: datetime,
                                     cross_window_minutes: int) -> datetime:
    delta_hours = (settings.trade_start_hour + cross_window_hours) - end_time.hour
    delta_minutes = cross_window_minutes - end_time.minute
    if delta_minutes < 0:
        delta_hours -= 1
        delta_minutes += 60

    delta_days = 3 if end_time.weekday() == 0 else 1
    start_time = ((end_time - timedelta(days=delta_days))
                  .replace(hour=settings.trade_end_hour, minute=50)
                  - timedelta(hours=delta_hours, minutes=delta_minutes))
    return start_time.replace(tzinfo=None)


def to_start_of_day(date: datetime) -> datetime:
    return date.replace(hour=0, minute=0, second=0, microsecond=999999)


def to_end_of_day(date: datetime) -> datetime:
    return date.replace(hour=23, minute=59, second=59, microsecond=999999)


def calculate_percentage(part: float, whole: float) -> float:
    try:
        return round((part / whole) * 100, 2)
    except ZeroDivisionError:
        return 0


def get_rebound_message(params: MainReboundParam, interval: CandleInterval, older_interval: CandleInterval, cross_count: int, type_msg: str) -> str:
    if type_msg == 'SHORT':
        candle_part = 'High'
        candle_val = params.latest_candle.high
        prev_candle_val = params.prev_candle.high
    else:
        candle_part = 'Low'
        candle_val = params.latest_candle.low
        prev_candle_val = params.prev_candle.low

    return (f'<b>{type_msg} #{params.ticker_name}</b> пересек EMA {int(params.curr_ema.span)} '
            f'({params.curr_ema.ema}) в интервале {get_interval_form_str(interval.value)}.\n'
            f'Время: {convert_utc_to_local(params.curr_ema.timestamp_column)}.\n'
            f'ATR: {calculate_percentage(params.curr_ema.atr, params.curr_ema.ema)}%.\n'
            f'Количество пересечений за последние {settings.ema_cross_window} часа: {cross_count}.\n'
            f'{candle_part} свечи: {candle_val}. '
            f'Время свечи: {convert_utc_to_local(params.latest_candle.timestamp_column)}.\n'
            f'{candle_part} предыдущей свечи: {prev_candle_val}. '
            f'Время свечи {convert_utc_to_local(params.prev_candle.timestamp_column)}.\n'
            f'Старшая EMA {params.older_ema.span} в интервале {get_interval_form_str(older_interval.value)}'
            f': {params.older_ema.ema}.Время: {convert_utc_to_local(params.older_ema.timestamp_column)}.\n'
            f'<a href="{make_tw_link(params.ticker_name, interval.value)}">График tradingview</a>')


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


async def send_telegram_message(text: str) -> None:
    base_url = f"https://api.telegram.org/bot{settings.bot_token}/sendMessage"

    payload = {
        "chat_id": settings.debug_chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    async with httpx.AsyncClient() as client:
        attempts = 0
        while attempts < settings.attempts_to_send_tg_msg:
            try:
                await client.post(base_url, data=payload)
                break
            except Exception as e:
                attempts += 1
                logger.error(f"Ошибка при выполнении запроса (Попытка {attempts}): {e}")
                await asyncio.sleep(settings.tg_send_timeout)


async def make_http_request(url: str, headers: dict, json: dict) -> Response:
    async with httpx.AsyncClient() as client:
        attempts = 0
        while attempts < settings.attempts_to_tcs_request:
            try:
                return await client.post(url, headers=headers, json=json)
            except Exception as e:
                attempts += 1
                logger.error(f"Ошибка при выполнении запроса (Попытка {attempts}): {e}")
                await asyncio.sleep(settings.tcs_request_timeout)

    raise MaxRetriesExceededError(f"Не удалось выполнить запрос после {settings.attempts_to_tcs_request} попыток.")


class MaxRetriesExceededError(Exception):
    def __init__(self, message="Max retries exceeded"):
        self.message = message
        super().__init__(self.message)
