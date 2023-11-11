from datetime import datetime, timedelta, timezone

from loguru import logger

from market_loader.infrasturcture.postgres_repository import BotPostgresRepository
from market_loader.models import CandleInterval, Ema, ExtendedReboundParam, MainReboundParam
from market_loader.settings import settings
from market_loader.utils import get_rebound_message, get_start_time, need_for_calculation, send_telegram_message


class StrategyEvaluator:

    def __init__(self, db: BotPostgresRepository):
        self.db = db
        current_time = datetime.now(timezone.utc)
        self.last_15_min_update = current_time
        self.last_hour_update = current_time
        self.last_day_update = current_time
        self.need_for_cross_update = True

    async def check_strategy(self) -> None:
        if datetime.now(timezone.utc).weekday() >= 5:
            return
        logger.info("Начали проверку стратегии")
        current_time = datetime.now(timezone.utc)
        intervals = [CandleInterval.min_5.value]
        quantity_of_intervals = len(intervals)
        for pos, interval in enumerate(intervals):
            update_time = pos == (quantity_of_intervals - 1)
            if need_for_calculation(self, interval, current_time, update_time):
                await self._check_rebound(200, CandleInterval.min_5, 1000, CandleInterval.min_5)
        logger.info("Завершили проверку стратегии")

    async def _save_and_get_cross_count(self, ticker_id: int, interval: CandleInterval, curr_ema: Ema) -> int:
        not_exist = await self.db.add_ema_cross(ticker_id, interval.value, curr_ema.span, curr_ema.timestamp_column)
        if not_exist:
            end_time = datetime.now(timezone.utc)
            return await self.db.get_ema_cross_count(ticker_id, interval.value, curr_ema.span,
                                                     get_start_time(end_time,
                                                                    settings.ema_cross_window).replace(tzinfo=None),
                                                     end_time.replace(tzinfo=None))
        else:
            return -1

    async def _check_rebound(self, span: int, interval: CandleInterval, older_span: int,
                             older_interval: CandleInterval):
        if self.need_for_cross_update:
            await self._update_cross_data()
        candles = await self.db.get_last_two_candles_for_each_ticker(interval.value)
        for ticker_id in candles:
            main_params = await self._get_rebound_main_params(ticker_id, interval, candles, span, older_interval,
                                                              older_span)
            if self._check_params(main_params, 'SHORT'):
                params = await self._get_rebound_extended_params(ticker_id, interval, main_params.curr_ema)
                if (params.hour_candle and 1 <= params.cross_count_4 <= 2
                        and main_params.curr_ema.ema < main_params.older_ema.ema
                        and params.cross_count_1 == 1
                        and params.hour_candle.open < main_params.curr_ema.ema):
                    message = get_rebound_message(main_params, interval, older_interval, params.cross_count_4, 'SHORT')
                    await send_telegram_message(message)
                    logger.info(f"Сигнал. {message}")
            if self._check_params(main_params, 'LONG'):
                params = await self._get_rebound_extended_params(ticker_id, interval, main_params.curr_ema)
                if (params.hour_candle and 1 <= params.cross_count_4 <= 2
                        and main_params.curr_ema.ema > main_params.older_ema.ema
                        and params.cross_count_1 == 1
                        and params.hour_candle.open > main_params.curr_ema.ema):
                    message = get_rebound_message(main_params, interval, older_interval, params.cross_count_4, 'LONG')
                    await send_telegram_message(message)
                    logger.info(f"Сигнал. {message}")

    @staticmethod
    def _check_params(main_params: MainReboundParam, check_type: str) -> bool:
        if check_type == 'SHORT':
            return (main_params.curr_ema and main_params.prev_ema
                    and main_params.latest_candle.high >= main_params.curr_ema.ema
                    and main_params.prev_candle.high < main_params.prev_ema.ema)

        if check_type == 'LONG':
            return (main_params.curr_ema and main_params.prev_ema
                    and main_params.prev_candle.low > main_params.prev_ema.ema
                    and main_params.latest_candle.low <= main_params.curr_ema.ema)

    async def _get_rebound_main_params(self, ticker_id: int, interval: CandleInterval, candles: dict, span: int,
                                       older_interval: CandleInterval, older_span: int) -> MainReboundParam:
        curr_ema = await self.db.get_latest_ema_for_ticker(ticker_id, interval.value, span)
        prev_ema = await self.db.get_penultimate_ema_for_ticker(ticker_id, interval.value, span)
        older_ema = await self.db.get_latest_ema_for_ticker(ticker_id, older_interval.value, older_span)
        prev_candle = candles[ticker_id][1]
        latest_candle = candles[ticker_id][0]
        ticker_name = await self.db.get_ticker_name_by_id(ticker_id)
        return MainReboundParam(curr_ema=curr_ema,
                                prev_ema=prev_ema,
                                older_ema=older_ema,
                                prev_candle=prev_candle,
                                latest_candle=latest_candle,
                                ticker_name=ticker_name)

    async def _get_rebound_extended_params(self, ticker_id: int, interval: CandleInterval,
                                           curr_ema: Ema) -> ExtendedReboundParam:
        cross_count_4 = await self._save_and_get_cross_count(ticker_id, interval, curr_ema)
        end_time = datetime.now(timezone.utc)
        cross_count_1 = await self.db.get_ema_cross_count(ticker_id, interval.value, curr_ema.span,
                                                          get_start_time(end_time, 1, 30).replace(tzinfo=None),
                                                          end_time.replace(tzinfo=None))
        cross_count_12 = await self.db.get_ema_cross_count(ticker_id, interval.value, curr_ema.span,
                                                           get_start_time(end_time, 12).replace(tzinfo=None),
                                                           end_time.replace(tzinfo=None))
        hour_candle = await self.db.get_last_candle(ticker_id, CandleInterval.hour.value)
        return ExtendedReboundParam(cross_count_4=cross_count_4,
                                    cross_count_1=cross_count_1,
                                    cross_count_12=cross_count_12,
                                    hour_candle=hour_candle)

    async def _update_cross_data(self):
        logger.info("Начали обновление данных о пересечении EMA")
        interval = CandleInterval.min_5
        span = 200

        end_time = datetime.now(timezone.utc).replace(tzinfo=None)
        start_time = get_start_time(end_time, settings.ema_cross_window).replace(tzinfo=None)
        while start_time < end_time:
            candles = await self.db.get_two_candles_for_each_ticker_by_period(interval.value, start_time)
            for ticker_id in candles:
                prev_candle = candles[ticker_id][1]
                latest_candle = candles[ticker_id][0]
                curr_ema = await self.db.get_ema_for_ticker_by_period(ticker_id, interval.value, span, start_time)
                prev_ema = await self.db.get_penultimate_ema_for_ticker_by_period(ticker_id, interval.value, span,
                                                                                  start_time)
                if (curr_ema and prev_ema and
                        latest_candle.high >= curr_ema.ema and prev_candle.high < prev_ema.ema):
                    await self.db.add_ema_cross(ticker_id, interval.value, curr_ema.span,
                                                curr_ema.timestamp_column)
                if curr_ema and prev_ema and prev_candle.low > prev_ema.ema and (latest_candle.low <= curr_ema.ema):
                    await self.db.add_ema_cross(ticker_id, interval.value, curr_ema.span,
                                                curr_ema.timestamp_column)
            start_time = start_time + timedelta(seconds=300)
        self.need_for_cross_update = False
        logger.info("Закончили обновление данных о пересечении EMA")
