from datetime import datetime, timezone

import httpx

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
            response = await client.post(base_url, data=payload)

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
        current_time = datetime.now(timezone.utc)
        intervals = ['CANDLE_INTERVAL_5_MIN', 'CANDLE_INTERVAL_15_MIN', 'CANDLE_INTERVAL_HOUR', 'CANDLE_INTERVAL_DAY']
        for interval in intervals:
            if self.need_for_calculation(interval, current_time):
                candels = await self.db.get_last_two_candles_for_each_ticker(interval)
                for ticker_id in candels:
                    ema = await self.db.get_latest_ema_for_ticker(ticker_id, interval)
                    candel1 = candels[ticker_id][1]
                    candel2 = candels[ticker_id][0]
                    if candel1.low > ema.ema and (candel2.low <= ema.ema):
                        # users_id = await self.db.get_users_for_ticker(ticker_id)
                        ticker_name = await self.db.get_ticker_name_by_id(ticker_id)
                        messege = f'Тикер {ticker_name} пересек EMA {int(ema.span)} в интервале ' \
                                  f'{self.get_interval(interval)}. Покупайте, милорд.'
                        await self.send_telegram_message(messege)
