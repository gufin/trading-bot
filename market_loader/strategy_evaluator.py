import asyncio
from datetime import datetime, timezone

import httpx
import pytz
from loguru import logger
from tzlocal import get_localzone

from bot.database import Database
from market_loader.models import CandleInterval


class StrategyEvaluator:

    def __init__(self, db: Database, token, chat_id):
        self.db = db
        self.token = token
        current_time = datetime.now(timezone.utc)
        self.last_15_min_update = current_time
        self.last_hour_update = current_time
        self.last_day_update = current_time
        self.chat_id = chat_id

    async def send_telegram_message(self, text: str) -> None:
        base_url = f"https://api.telegram.org/bot{self.token}/sendMessage"

        payload = {
            "chat_id": self.chat_id,
            "text": text
        }
        async with httpx.AsyncClient() as client:
            attempts = 0
            while attempts < 10:
                try:
                    await client.post(base_url, data=payload)
                    break
                except Exception as e:
                    attempts += 1
                    logger.error(f"Ошибка при выполнении запроса (Попытка {attempts}): {e}")
                    await asyncio.sleep(10)

    @staticmethod
    def convert_utc_to_local(utc_str):
        # Создайте объект datetime из строки, предполагая, что она в UTC
        utc_time = datetime.strptime(utc_str, "%Y-%m-%d %H:%M:%S")
        utc_time = pytz.utc.localize(utc_time)

        # Получите текущий временной пояс
        local_tz = get_localzone()

        return utc_time.astimezone(local_tz)

    @staticmethod
    def get_interval(interval):
        if interval == 'CANDLE_INTERVAL_5_MIN':
            return '5 min'

        if interval == 'CANDLE_INTERVAL_15_MIN':
            return '15 min'

        if interval == 'CANDLE_INTERVAL_HOUR':
            return 'hour'

        if interval == 'CANDLE_INTERVAL_DAY':
            return 'day'

    def need_for_calculation(self, interval, current_time):
        if interval == CandleInterval.CANDLE_INTERVAL_5_MIN.value:
            return True
        if (interval == CandleInterval.CANDLE_INTERVAL_15_MIN.value
                and (current_time - self.last_15_min_update).total_seconds() >= 900):
            self.last_15_min_update = current_time
            return True
        if (interval == CandleInterval.CANDLE_INTERVAL_HOUR.value
                and (current_time - self.last_hour_update).total_seconds() >= 3600):
            self.last_hour_update = current_time
            return True
        if (interval == CandleInterval.CANDLE_INTERVAL_DAY.value and
            (current_time - self.last_day_update).total_seconds() >= 3600) * 24:
            self.last_day_update = current_time
            return True

    async def check_strategy(self):
        logger.info("Начали проверку стратегии")
        current_time = datetime.now(timezone.utc)
        intervals = ['CANDLE_INTERVAL_5_MIN']
        for interval in intervals:
            if self.need_for_calculation(interval, current_time):
                candles = await self.db.get_last_two_candles_for_each_ticker(interval)
                for ticker_id in candles:
                    ema = await self.db.get_latest_ema_for_ticker(ticker_id, interval, 200)
                    candl1 = candles[ticker_id][1]
                    candl2 = candles[ticker_id][0]
                    if ema and candl1.low > ema.ema and (candl2.low <= ema.ema):
                        # users_id = await self.db.get_users_for_ticker(ticker_id)
                        ticker_name = await self.db.get_ticker_name_by_id(ticker_id)
                        message = (f'{ticker_name} пересек EMA {int(ema.span)} ({ema.ema}) в интервале '
                                   f'{self.get_interval(interval)}. '
                                   f'Время {self.convert_utc_to_local(ema.timestamp_column)}. '
                                   f'low свечи {candl2.low} время свечи {self.convert_utc_to_local(candl2.timestamp_column)}'
                                   f'low предыдущей свечи {candl1.low} время свечи {self.convert_utc_to_local(candl1.timestamp_column)}')
                        await self.send_telegram_message(message)
                        logger.info(f"Сигнал. {message}")
        logger.info("Завершили проверку стратегии")
