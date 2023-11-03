import asyncio
from datetime import datetime, timedelta, timezone

import httpx
from loguru import logger

from bot.database import Database
from market_loader.constants import attempts_to_send_tg_msg, ema_cross_window, tg_send_timeout
from market_loader.models import CandleInterval, Ema
from market_loader.utils import get_rebound_message, get_start_time, need_for_calculation


class StrategyEvaluator:

    def __init__(self, db: Database, token: str, chat_id: int):
        self.db = db
        self.token = token
        current_time = datetime.now(timezone.utc)
        self.last_15_min_update = current_time
        self.last_hour_update = current_time
        self.last_day_update = current_time
        self.chat_id = chat_id
        self.ema_window_count = ema_cross_window
        self.need_for_cross_update = True

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
            while attempts < attempts_to_send_tg_msg:
                try:
                    await client.post(base_url, data=payload)
                    break
                except Exception as e:
                    attempts += 1
                    logger.error(f"Ошибка при выполнении запроса (Попытка {attempts}): {e}")
                    await asyncio.sleep(tg_send_timeout)

    async def check_strategy(self) -> None:
        logger.info("Начали проверку стратегии")
        current_time = datetime.now(timezone.utc)
        intervals = [CandleInterval.min_5.value]
        quantity_of_intervals = len(intervals)
        for pos, interval in enumerate(intervals):
            update_time = pos == (quantity_of_intervals - 1)
            if need_for_calculation(self, interval, current_time, update_time):
                await self._check_rebound(200, CandleInterval.min_5, 1000, CandleInterval.min_5)
        logger.info("Завершили проверку стратегии")

    async def _save_and_get_cross_count(self, ticker_id: int, interval: CandleInterval, current_ema: Ema) -> int:
        await self.db.add_ema_cross(ticker_id, interval.value, current_ema.span,
                                    datetime.strptime(current_ema.timestamp_column, '%Y-%m-%d %H:%M:%S'))
        end_time = datetime.now(timezone.utc)
        return await self.db.get_ema_cross_count(ticker_id, interval.value, current_ema.span,
                                                 get_start_time(end_time).replace(tzinfo=None),
                                                 end_time.replace(tzinfo=None))

    async def _check_rebound(self, span: int, interval: CandleInterval, older_span: int,
                             older_interval: CandleInterval):
        if self.need_for_cross_update:
            await self._update_cross_data()
        candles = await self.db.get_last_two_candles_for_each_ticker(interval.value)
        for ticker_id in candles:
            current_ema = await self.db.get_latest_ema_for_ticker(ticker_id, interval.value, span)
            prev_ema = await self.db.get_penultimate_ema_for_ticker(ticker_id, interval.value, span)
            older_ema = await self.db.get_latest_ema_for_ticker(ticker_id, older_interval.value, older_span)
            prev_candle = candles[ticker_id][1]
            latest_candle = candles[ticker_id][0]
            ticker_name = await self.db.get_ticker_name_by_id(ticker_id)
            if current_ema and prev_ema and latest_candle.high >= current_ema.ema and prev_candle.high < prev_ema.ema:
                cross_count = await self._save_and_get_cross_count(ticker_id, interval, current_ema)
                if 1 <= cross_count <= 2 and current_ema.ema < older_ema.ema:
                    message = get_rebound_message(ticker_name, current_ema, older_ema, interval, older_interval,
                                                  latest_candle, prev_candle, cross_count, 'SHORT')
                    await self.send_telegram_message(message)
                    logger.info(f"Сигнал. {message}")
            if current_ema and prev_ema and prev_candle.low > prev_ema.ema and (latest_candle.low <= current_ema.ema):
                cross_count = await self._save_and_get_cross_count(ticker_id, interval, current_ema)
                if 1 <= cross_count <= 2 and current_ema.ema > older_ema.ema:
                    message = get_rebound_message(ticker_name, current_ema, older_ema, interval, older_interval,
                                                  latest_candle, prev_candle, cross_count, 'LONG')
                    await self.send_telegram_message(message)
                    logger.info(f"Сигнал. {message}")

    async def _update_cross_data(self):
        logger.info("Начали обновление данных о пересечении EMA")
        interval = CandleInterval.min_5
        span = 200

        end_time = datetime.now(timezone.utc).replace(tzinfo=None)
        start_time = get_start_time(end_time).replace(tzinfo=None)
        while start_time < end_time:
            candles = await self.db.get_two_candles_for_each_ticker_by_period(interval.value, start_time)
            for ticker_id in candles:
                prev_candle = candles[ticker_id][1]
                latest_candle = candles[ticker_id][0]
                current_ema = await self.db.get_ema_for_ticker_by_period(ticker_id, interval.value, span, start_time)
                prev_ema = await self.db.get_penultimate_ema_for_ticker_by_period(ticker_id, interval.value, span,
                                                                                  start_time)
                if (current_ema and prev_ema and
                        latest_candle.high >= current_ema.ema and prev_candle.high < prev_ema.ema):
                    await self.db.add_ema_cross(ticker_id, interval.value, current_ema.span,
                                                datetime.strptime(current_ema.timestamp_column, '%Y-%m-%d %H:%M:%S'))
                if current_ema and prev_ema and prev_candle.low > prev_ema.ema and (
                        latest_candle.low <= current_ema.ema):
                    await self.db.add_ema_cross(ticker_id, interval.value, current_ema.span,
                                                datetime.strptime(current_ema.timestamp_column, '%Y-%m-%d %H:%M:%S'))
            start_time = start_time + timedelta(seconds=300)
        self.need_for_cross_update = False
        logger.info("Закончили обновление данных о пересечении EMA")
