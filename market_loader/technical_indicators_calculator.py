from datetime import datetime
from datetime import timezone
import talib

from loguru import logger

from bot.database import Database
from market_loader.models import CandleInterval


class TechnicalIndicatorsCalculator:

    def __init__(self, db: Database):
        self.db = db
        current_time = datetime.now(timezone.utc)
        self.last_15_min_update = current_time
        self.last_hour_update = current_time
        self.last_day_update = current_time

    async def save_data_frame(self, df, ticker_id, interval, span):
        df['ema'] = df['close'].ewm(span=span, adjust=False).mean()
        df['atr'] = talib.ATR(df['high'], df['low'], df['close'], timeperiod=14)
        for index, row in df.iterrows():
            await self.db.add_ema(ticker_id=ticker_id,
                                  interval=interval,
                                  span=span,
                                  timestamp_column=row['timestamp_column'],
                                  ema_value=row['ema'],
                                  atr=row['atr'])

    def need_for_calculation(self, interval, current_time):
        if interval == CandleInterval.min_5.value:
            return True
        if (interval == CandleInterval.min_15.value
                and (current_time - self.last_15_min_update).total_seconds() >= 900):
            self.last_15_min_update = current_time
            return True
        if (interval == CandleInterval.hour.value
                and (current_time - self.last_hour_update).total_seconds() >= 3600):
            self.last_hour_update = current_time
            return True
        if (interval == CandleInterval.day.value and
            (current_time - self.last_day_update).total_seconds() >= 3600) * 24:
            self.last_day_update = current_time
            return True

    async def init_ema(self):
        logger.info("Начали инициализацию EMA")
        tickers_ema_init_data = await self.db.get_tickers_to_init_ema()
        for ticker in tickers_ema_init_data:
            logger.info(f"Инициализация EMA | тикер: {ticker.name}; интервал: {ticker.interval}; span: {ticker.span}")
            df = await self.db.get_data_for_init_ema(ticker.ticker_id, ticker.interval)
            await self.save_data_frame(df, ticker.ticker_id, ticker.interval, ticker.span)
        logger.info("Заверишили инициализацию EMA")

    async def calculate(self):
        await self.init_ema()
        logger.info("Начали расчет EMA")
        current_time = datetime.now(timezone.utc)
        ema_to_calc = await self.db.get_ema_params_to_calc()
        tickers = await self.db.get_tickers_with_figi()
        for ticker in tickers:
            for ema_params in ema_to_calc:
                if self.need_for_calculation(ema_params.interval, current_time):
                    logger.info(
                        f"EMA | тикер: {ticker.name}; интервал: {ema_params.interval}; span: {ema_params.span}")
                    df = await self.db.get_data_for_ema(ticker.ticker_id, ema_params.interval, ema_params.span)
                    await self.save_data_frame(df, ticker.ticker_id, ema_params.interval, ema_params.span)
        logger.info("Заверишили расчет EMA")
