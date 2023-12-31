from datetime import datetime
from datetime import timezone

from loguru import logger
from pandas import DataFrame

from market_loader.constants import atr_period
from market_loader.infrasturcture.postgres_repository import BotPostgresRepository
from market_loader.models import Ema
from market_loader.utils import convert_to_date, get_interval_form_str, need_for_calculation


class TechnicalIndicatorsCalculator:

    def __init__(self, db: BotPostgresRepository):
        self.db = db
        current_time = datetime.now(timezone.utc).replace(hour=7, minute=0, second=0, microsecond=0)
        self.last_15_min_update = current_time
        self.last_hour_update = current_time
        self.last_day_update = current_time

    async def _save_data_frame(self, df: DataFrame, ticker_id: int, interval: str, span: int) -> None:
        df['ema'] = df['close'].ewm(span=span, adjust=False).mean()
        df['high_minus_low'] = df['high'] - df['low']
        df['high_minus_close_prev'] = abs(df['high'] - df['close'].shift(1))
        df['low_minus_close_prev'] = abs(df['low'] - df['close'].shift(1))
        df['tr'] = df[['high_minus_low', 'high_minus_close_prev', 'low_minus_close_prev']].max(axis=1)
        df['atr'] = df['tr'].rolling(window=atr_period).mean()

        last_ema = await self.db.get_latest_ema_for_ticker(ticker_id, interval, span)
        if last_ema is not None:
            filtered_df = df[df['timestamp_column'] > last_ema.timestamp_column]
        else:
            filtered_df = df
        list_of_rows = [
            Ema(
                ticker_id=ticker_id,
                interval=interval,
                span=span,
                timestamp_column=row['timestamp_column'],
                ema=row['ema'],
                atr=row['atr'],
            )
            for index, row in filtered_df.iterrows()
        ]
        await self.db.bulk_add_ema(list_of_rows)

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
        ema_to_calc = await self.db.get_ema_params_to_calc()
        tickers = await self.db.get_tickers_with_figi()
        quantity_of_tickers = len(tickers)
        for pos, ticker in enumerate(tickers):
            for ema_params in ema_to_calc:
                update_time = pos == (quantity_of_tickers - 1)
                if need_for_calculation(self, ema_params.interval, datetime.now(timezone.utc), update_time):
                    logger.info(
                        (f"EMA | тикер: {ticker.name}; интервал: {get_interval_form_str(ema_params.interval)}; "
                         f"span: {ema_params.span}"))
                    df = await self.db.get_data_for_ema(ticker.ticker_id, ema_params.interval, ema_params.span)
                    await self._save_data_frame(df, ticker.ticker_id, ema_params.interval, ema_params.span)
        logger.info("Заверишили расчет EMA")
