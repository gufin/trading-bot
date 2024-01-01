from datetime import datetime, timedelta, timezone

from loguru import logger

from market_loader.infrasturcture.postgres_repository import BotPostgresRepository
from market_loader.market_processor import MarketProcessor
from market_loader.models import CandleInterval, Ema, ExtendedReboundParam, MainReboundParam, OrderDirection, OrderType
from market_loader.settings import settings
from market_loader.tcs_market_processor import TCSMarketProcessor
from market_loader.utils import dict_to_float, get_market_message, get_rebound_message, get_start_time, \
    need_for_calculation, \
    send_telegram_message


class StrategyEvaluator:

    def __init__(self, db: BotPostgresRepository, mp: TCSMarketProcessor):
        self.db = db
        self.mp = mp
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

    async def _check_rebound(self, span: int, interval: CandleInterval, older_span: int,
                             older_interval: CandleInterval):
        if self.need_for_cross_update:
            await self._update_cross_data()
        candles = await self.db.get_last_two_candles_for_each_ticker(interval.value)
        await self._make_orders(candles, interval, span, older_interval, older_span)
        for ticker_id in candles:
            main_params = await self._get_rebound_main_params(ticker_id, interval, candles, span, older_interval,
                                                              older_span)
            if self._check_params(main_params, 'SHORT'):
                cross_is_new = await self.db.add_ema_cross(ticker_id, interval.value, main_params.curr_ema.span,
                                                           main_params.curr_ema.timestamp_column)
                params = await self._get_rebound_extended_params(ticker_id, interval, main_params.curr_ema)
                if (cross_is_new and params.hour_candle and 1 <= params.cross_count_4 <= 2
                        and main_params.curr_ema.ema < main_params.older_ema.ema
                        and params.cross_count_1 == 1
                        and params.cross_count_12 < 5
                        and params.hour_candle.open < main_params.curr_ema.ema):
                    message = get_rebound_message(main_params, interval, older_interval, params.cross_count_4, 'SHORT')
                    await send_telegram_message(message)
                    logger.info(f"Сигнал. {message}")
            if self._check_params(main_params, 'LONG'):
                cross_is_new = await self.db.add_ema_cross(ticker_id, interval.value, main_params.curr_ema.span,
                                                           main_params.curr_ema.timestamp_column)
                params = await self._get_rebound_extended_params(ticker_id, interval, main_params.curr_ema)
                if (cross_is_new and params.hour_candle and 1 <= params.cross_count_4 <= 2
                        and main_params.curr_ema.ema > main_params.older_ema.ema
                        and params.cross_count_1 == 1
                        and params.cross_count_12 < 5
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

        if check_type == 'LONG_ORDER':
            return (main_params.curr_ema and main_params.prev_ema
                    and main_params.prev_candle.low > main_params.prev_ema.ema
                    and main_params.latest_candle.low > main_params.curr_ema.ema)

    async def _get_rebound_main_params(self, ticker_id: int, interval: CandleInterval, candles: dict, span: int,
                                       older_interval: CandleInterval, older_span: int) -> MainReboundParam:
        curr_ema = await self.db.get_latest_ema_for_ticker(ticker_id, interval.value, span)
        prev_ema = await self.db.get_penultimate_ema_for_ticker(ticker_id, interval.value, span)
        older_ema = await self.db.get_latest_ema_for_ticker(ticker_id, older_interval.value, older_span)
        prev_candle = candles[ticker_id][1]
        latest_candle = candles[ticker_id][0]
        ticker = await self.db.get_ticker_by_id(ticker_id)
        return MainReboundParam(curr_ema=curr_ema,
                                prev_ema=prev_ema,
                                older_ema=older_ema,
                                prev_candle=prev_candle,
                                latest_candle=latest_candle,
                                ticker=ticker)

    async def _get_rebound_extended_params(self, ticker_id: int, interval: CandleInterval,
                                           curr_ema: Ema) -> ExtendedReboundParam:
        end_time = datetime.now(timezone.utc)
        cross_count_4 = await self.db.get_ema_cross_count(ticker_id, interval.value, curr_ema.span,
                                                          get_start_time(end_time, settings.ema_cross_window).replace(
                                                              tzinfo=None),
                                                          end_time.replace(tzinfo=None))
        cross_count_1 = await self.db.get_ema_cross_count(ticker_id, interval.value, curr_ema.span,
                                                          get_start_time(end_time, 1, 30).replace(tzinfo=None),
                                                          end_time.replace(tzinfo=None))
        cross_count_12 = await self.db.get_cross_group_by_hour(ticker_id, interval.value, curr_ema.span,
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

    async def _make_orders(self, candles: dict, interval: CandleInterval, span: int, older_interval: CandleInterval,
                           older_span: int):
        logger.info("Начали выставлять ордера на покупку")

        prices = await self.mp.get_current_prices()
        if prices is None:
            logger.critical("Не удалось получить текущие цены")
            return
        account_id = await self.db.get_user_account(user_id=1)
        await self.mp.update_orders(account_id)
        portfolio = await self.mp.get_portfolio(account_id)
        for ticker_id in candles:
            main_params = await self._get_rebound_main_params(ticker_id, interval, candles, span, older_interval,
                                                              older_span)

            in_position = await self.mp.in_position(main_params.ticker.figi, portfolio)
            if self._check_params(main_params, 'LONG_ORDER') and not in_position:
                make_order = await self._full_check_order(main_params, ticker_id, interval, account_id, prices)
                if make_order:
                    price = self.mp.round_price(main_params.curr_ema.ema, main_params.ticker)
                    result = await self.mp.buy_limit_with_replace(main_params.ticker.figi, price,
                                                                  main_params.curr_ema.atr)
                    # if result:
                    #     message = get_market_message(main_params.ticker, price, prices[main_params.ticker.figi],
                    #                                  OrderType.limit, OrderDirection.buy)
                    #     await send_telegram_message(message)
        logger.info("Закончили выставлять ордера на покупку")

    async def _full_check_order(self, main_params: MainReboundParam, ticker_id: int, interval: CandleInterval,
                                account_id: str, prices: dict) -> bool:
        params = await self._get_rebound_extended_params(ticker_id, interval, main_params.curr_ema)
        active_order = await self.db.get_active_order_by_figi(account_id, main_params.ticker.figi,
                                                              OrderDirection.buy)
        price = self.mp.round_price(main_params.curr_ema.ema, main_params.ticker)
        order_price_different = dict_to_float(
            active_order.price.model_dump()) / active_order.quantity != price if active_order else True
        return (params.hour_candle
                and order_price_different
                and main_params.ticker.figi in prices
                and prices[main_params.ticker.figi] > main_params.curr_ema.ema
                and params.cross_count_4 < 2
                and main_params.curr_ema.ema > main_params.older_ema.ema
                and params.cross_count_1 == 0
                and params.cross_count_12 < 4
                and params.hour_candle.open > main_params.curr_ema.ema)

    async def check_orders(self):
        logger.info("Начали проверку активных ордеров")
        account_id = await self.db.get_user_account(user_id=1)
        db_positions = await self.db.get_latest_positions(1)
        current_positions = await self._get_current_positions(account_id)
        prices = await self.mp.get_current_prices()
        active_figi = await self._get_figi_in_active_orders()
        if prices is None:
            logger.critical("Не удалось получить текущие цены")
            return
        for position in current_positions:
            latest_order = await self.db.get_latest_order_by_direction(account_id, position.figi, OrderDirection.buy)
            if latest_order is None:
                result = await self.mp.sell_market(position.figi, 1)
                if result:
                    message = f"<b>Открыта ошибочная позиция. Продали по рынку. Простите, милорд</b> #{position.name}"
                    await send_telegram_message(message)
                    continue
            base_price = latest_order.initialOrderPrice / latest_order.lotsRequested
            price = self.mp.round_price(base_price + 3 * latest_order.atr, position)
            if (
                position not in db_positions
                or position.figi not in active_figi
            ):
                await self.db.add_deal(position.ticker_id, latest_order.orderId)
                if position not in db_positions:
                    message = (f"<b>Открыта позиция</b> #{position.name}. Количество в последнем ордере "
                               f"{latest_order.lotsRequested} по цене {latest_order.initialOrderPrice }")
                    await send_telegram_message(message)
                if prices[position.figi] <= base_price - latest_order.atr*1.5:
                    result = await self.mp.sell_market(position.figi, price)
                    if result:
                        message = get_market_message(position, 'по рынку', prices[position.figi], OrderType.market,
                                                     OrderDirection.sell)
                        await send_telegram_message(message)
                else:
                    result = await self.mp.sell_limit_with_replace(position.figi, price, latest_order.atr)
                    if result:
                        message = get_market_message(position, price, prices[position.figi], OrderType.limit,
                                                     OrderDirection.sell)
                        await send_telegram_message(message)
            elif prices[position.figi] <= base_price - latest_order.atr*1.5:
                latest_order = await self.db.get_latest_order_by_direction(account_id, position.figi,
                                                                           OrderDirection.sell)
                if latest_order is not None:
                    await self.mp.cancel_order(account_id, latest_order.orderId)
                result = await self.mp.sell_market(position.figi, price)
                if result:
                    message = get_market_message(position, 'по рынку', prices[position.figi], OrderType.market,
                                                 OrderDirection.sell)
                    await send_telegram_message(message)
        if db_positions:
            for position in db_positions:
                if position not in current_positions:
                    await self.mp.close_deal(account_id, position)
                    message = f"<b>Закрыта позиция</b> #{position.name}"
                    await send_telegram_message(message)
        await self.mp.save_current_positions(account_id, current_positions)
        logger.info("Закончили проверку активных ордеров")

    async def _get_current_positions(self, account_id: str) -> list:
        current_positions_raw = await self.mp.get_positions(account_id)
        current_positions = []
        for security in current_positions_raw['securities']:
            if security['instrumentType'] == 'share':
                ticker = await self.db.get_ticker_by_figi(security['figi'])
                current_positions.append(ticker)
        return current_positions

    async def _get_figi_in_active_orders(self) -> list:
        active_orders = await self.mp.get_orders()
        return [
            order['figi']
            for order in active_orders["orders"]
            if order['executionReportStatus'] == 'EXECUTION_REPORT_STATUS_NEW'
        ]
