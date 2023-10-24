from datetime import datetime
from datetime import timezone

import talib
from loguru import logger
from pandas import DataFrame

from bot.database import Database
from market_loader.utils import get_interval_form_str, need_for_calculation


class TechnicalIndicatorsCalculator:

    def __init__(self, db: Database):
        self.db = db
        current_time = datetime.now(timezone.utc)
        self.last_15_min_update = current_time
        self.last_hour_update = current_time
        self.last_day_update = current_time

    async def _save_data_frame(self, df: DataFrame, ticker_id: int, interval: str, span: int) -> None:
        df['ema'] = df['close'].ewm(span=span, adjust=False).mean()
        df['atr'] = talib.ATR(df['high'], df['low'], df['close'], timeperiod=14)
        for index, row in df.iterrows():
            await self.db.add_ema(ticker_id=ticker_id,
                                  interval=interval,
                                  span=span,
                                  timestamp_column=row['timestamp_column'],
                                  ema_value=row['ema'],
                                  atr=row['atr'])

    async def _init_ema(self) -> None:
        logger.info("Начали инициализацию EMA")
        tickers_ema_init_data = await self.db.get_tickers_to_init_ema()
        for ticker in tickers_ema_init_data:
            logger.info((f"Инициализация EMA | тикер: {ticker.name}; "
                         f"интервал: {get_interval_form_str(ticker.interval)}; span: {ticker.span}"))
            df = await self.db.get_data_for_init_ema(ticker.ticker_id, ticker.interval)
            await self._save_data_frame(df, ticker.ticker_id, ticker.interval, ticker.span)
        logger.info("Заверишили инициализацию EMA")

    async def calculate(self) -> None:
        await self._init_ema()
        logger.info("Начали расчет EMA")
        current_time = datetime.now(timezone.utc)
        ema_to_calc = await self.db.get_ema_params_to_calc()
        tickers = await self.db.get_tickers_with_figi()
        for ticker in tickers:
            for ema_params in ema_to_calc:
                if need_for_calculation(self, ema_params.interval, current_time):
                    logger.info(
                        (f"EMA | тикер: {ticker.name}; интервал: {get_interval_form_str(ema_params.interval)}; "
                         f"span: {ema_params.span}"))
                    df = await self.db.get_data_for_ema(ticker.ticker_id, ema_params.interval, ema_params.span)
                    await self._save_data_frame(df, ticker.ticker_id, ema_params.interval, ema_params.span)
        logger.info("Заверишили расчет EMA")
