import asyncio
from datetime import datetime, timedelta, timezone

import httpx
from loguru import logger

from bot.database import Database
from market_loader.models import CandleInterval
from market_loader.utils import convert_utc_to_local, get_interval_form_str, make_tw_link, need_for_calculation


class StrategyEvaluator:

    def __init__(self, db: Database, token: str, chat_id: int):
        self.db = db
        self.token = token
        current_time = datetime.now(timezone.utc)
        self.last_15_min_update = current_time
        self.last_hour_update = current_time
        self.last_day_update = current_time
        self.chat_id = chat_id
        self.ema_window_count = 4

    async def send_telegram_message(self, text: str) -> None:
        base_url = f"https://api.telegram.org/bot{self.token}/sendMessage"

        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
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

    async def check_strategy(self):
        logger.info("Начали проверку стратегии")
        current_time = datetime.now(timezone.utc)
        intervals = [CandleInterval.min_5.value]
        quantity_of_intervals = len(intervals)
        for pos, interval in enumerate(intervals):
            update_time = pos == (quantity_of_intervals - 1)
            if need_for_calculation(self, interval, current_time, update_time):
                candles = await self.db.get_last_two_candles_for_each_ticker(interval)
                for ticker_id in candles:
                    ema = await self.db.get_latest_ema_for_ticker(ticker_id, interval, 200)
                    ema_200_hour = await self.db.get_latest_ema_for_ticker(ticker_id, CandleInterval.hour.value, 200)
                    candl1 = candles[ticker_id][1]
                    candl2 = candles[ticker_id][0]
                    if ema and candl1.low > ema.ema and (candl2.low <= ema.ema):
                        await self.db.add_ema_cross(ticker_id, interval, ema.span,
                                                    datetime.strptime(ema.timestamp_column, '%Y-%m-%d %H:%M:%S'))
                        end_time = datetime.now(timezone.utc)
                        start_time = end_time - timedelta(hours=self.ema_window_count)
                        cross_count = await self.db.get_ema_cross_count(ticker_id, interval, ema.span,
                                                                        start_time.replace(tzinfo=None),
                                                                        end_time.replace(tzinfo=None))
                        if 1 <= cross_count <= 2 and ema.ema > ema_200_hour.ema:
                            ticker_name = await self.db.get_ticker_name_by_id(ticker_id)
                            message = (
                                f'<b>#{ticker_name}</b> пересек EMA {int(ema.span)} ({ema.ema}) в интервале '
                                f'{get_interval_form_str(interval)}.\n'
                                f'Время: {convert_utc_to_local(ema.timestamp_column)}.\n'
                                f'ATR: {ema.atr}.\n'
                                f'Количество пересечений за последние {self.ema_window_count} часа: {cross_count}.\n'
                                f'Low свечи: {candl2.low}. Время свечи: '
                                f'{convert_utc_to_local(candl2.timestamp_column)}.\n'
                                f'Low предыдущей свечи: {candl1.low}. Время свечи '
                                f'{convert_utc_to_local(candl1.timestamp_column)}.\n'
                                f'Старшая EMA {200} в интервале {get_interval_form_str(CandleInterval.hour.value)}: '
                                f'{ema_200_hour.ema}. Время: {convert_utc_to_local(ema_200_hour.timestamp_column)}.\n'
                                f'<a href="{make_tw_link(ticker_name, interval)}">График tradingview</a>')
                            await self.send_telegram_message(message)
                            logger.info(f"Сигнал. {message}")
        logger.info("Завершили проверку стратегии")
