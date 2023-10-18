from loguru import logger

from bot.database import Database


class TechnicalIndicatorsCalculator:

    def __init__(self, db: Database):
        self.db = db

    async def calculate(self):
        logger.info("Начали расчет EMA")
        ema_to_calc = await self.db.get_ema_params_to_calc()
        tickers = await self.db.get_tickers_with_figi()

        for ema_params in ema_to_calc:
            for ticker in tickers:
                logger.info(f"EMA | тикер: {ticker.name}; интервал: {ema_params.interval}; span: {ema_params.span}")
                df = await self.db.get_data_for_ema(ticker.ticker_id, ema_params.interval)
                df['ema'] = df['close'].ewm(span=ema_params.span, adjust=False).mean()
                for index, row in df.iterrows():
                    await self.db.add_ema(ticker_id=ticker.ticker_id,
                                          interval=ema_params.interval,
                                          span=ema_params.span,
                                          timestamp_column=row.iloc[0],
                                          ema_value=row.iloc[2])
