import asyncio
from datetime import datetime, timedelta
from datetime import timezone
from http import HTTPStatus

import httpx
from loguru import logger

from bot.database import Database
from market_loader.models import ApiConfig, CandleInterval, FindInstrumentRequest, InstrumentRequest, Ticker


class MarketDataLoader:

    def __init__(self, db: Database, config: ApiConfig):
        current_time = datetime.now(timezone.utc)
        self.db = db
        self.config = config
        self.time_counter = 0
        self.last_update = current_time
        self.last_15_min_update = current_time
        self.last_hour_update = current_time
        self.last_day_update = current_time
        self.last_request_time = current_time
        self.instrument_query_counter = 0
        self.market_query_counter = 0

    async def request_with_count(self, client, url, headers, json, query_type):

        current_time = datetime.now(timezone.utc)
        time_difference = (current_time - self.last_request_time).total_seconds()

        if time_difference >= 60:
            self.instrument_query_counter = 0
            self.market_query_counter = 0
            self.last_request_time = current_time

        if query_type == 'instrument':
            self.instrument_query_counter += 1
        else:
            self.market_query_counter += 1

        if self.instrument_query_counter == 99 or self.market_query_counter == 149:
            logger.info("Достигли предела запросов в минуту")
            await asyncio.sleep(60)
            self.instrument_query_counter = 0
            self.market_query_counter = 0

        return await client.post(url, headers=headers, json=json)

    async def _update_tickers(self):
        logger.info("Начали инициализацию тикеров")
        tickers = await self.db.get_tickers_without_figi()
        headers = {
            "Authorization": f"Bearer {self.config.token}"
        }
        async with httpx.AsyncClient() as client:
            for ticker in tickers:
                data = FindInstrumentRequest(query=ticker.name)
                url = f"{self.config.base_url}{self.config.find_instrument}"
                response = await self.request_with_count(client=client, url=url, headers=headers,
                                                         json=data.model_dump(), query_type='instrument')
                if response.status_code == HTTPStatus.OK:
                    response_data = response.json()
                    if len(response_data['instruments']) == 1:
                        ticker_data = response_data['instruments'][0]
                        share_url = f"{self.config.base_url}{self.config.share_by}"
                        share_request = InstrumentRequest(classCode=ticker_data['classCode'], id=ticker.name)
                        share_response = await self.request_with_count(client=client, url=share_url, headers=headers,
                                                                       json=share_request.model_dump(),
                                                                       query_type='instrument')
                        if share_response.status_code == HTTPStatus.OK:
                            share_data = share_response.json()['instrument']
                            await self.db.update_tickers(ticker_id=ticker.ticker_id,
                                                         new_figi=ticker_data['figi'],
                                                         new_classCode=ticker_data['classCode'],
                                                         new_currency=share_data['currency'])
                            logger.info(
                                f"Начали получать исторические данные | тикер: {ticker.name}; id: {ticker.ticker_id}")
                            await self.init_ticker_data(client, ticker_data['figi'], ticker.ticker_id)
                            logger.info(
                                f"Закончили получать исторические данные "
                                f"| тикер: {ticker.name}; id: {ticker.ticker_id}")

                        else:
                            logger.critical(f"Не доступен url {share_url}")
                    else:
                        logger.error(f"Введен не верный тикер {ticker.name}")
                else:
                    logger.critical(f"Не доступен url {url}")

    async def init_ticker_data(self, client, figi, ticker_id):
        current_time = self.last_update
        four_weeks_ago = self.last_update - timedelta(weeks=4)
        while current_time > four_weeks_ago:
            start_of_day = current_time.replace(hour=0, minute=0, second=0, microsecond=999999)
            end_of_day = current_time.replace(hour=23, minute=59, second=59, microsecond=999999)
            response_data = await self.get_ticker_candles(client, figi, start_of_day, end_of_day,
                                                          CandleInterval.CANDLE_INTERVAL_5_MIN)
            if response_data:
                await self.save_candles(response_data, ticker_id, CandleInterval.CANDLE_INTERVAL_5_MIN)
            response_data = await self.get_ticker_candles(client, figi, start_of_day, end_of_day,
                                                          CandleInterval.CANDLE_INTERVAL_15_MIN)
            if response_data:
                await self.save_candles(response_data, ticker_id, CandleInterval.CANDLE_INTERVAL_15_MIN)

            current_time -= timedelta(days=1)

        current_time = self.last_update
        two_months_ago = self.last_update - timedelta(days=60)
        while current_time > two_months_ago:
            day_of_week = current_time.weekday()
            start_of_week = (current_time - timedelta(days=day_of_week)).replace(hour=0, minute=0, second=0,
                                                                                 microsecond=999999)
            end_of_week = (start_of_week + timedelta(days=4)).replace(hour=23, minute=59, second=59,
                                                                      microsecond=999999)
            response_data = await self.get_ticker_candles(client, figi, start_of_week, end_of_week,
                                                          CandleInterval.CANDLE_INTERVAL_HOUR)
            if response_data:
                await self.save_candles(response_data, ticker_id, CandleInterval.CANDLE_INTERVAL_HOUR)
            current_time -= timedelta(weeks=1)

        year_ago = (self.last_update - timedelta(days=365)).replace(hour=0, minute=0, second=0, microsecond=999999)
        response_data = await self.get_ticker_candles(client, figi, year_ago, self.last_update,
                                                      CandleInterval.CANDLE_INTERVAL_DAY)
        if response_data:
            await self.save_candles(response_data, ticker_id, CandleInterval.CANDLE_INTERVAL_DAY)

    @staticmethod
    def dict_to_float(num_dict):
        units = num_dict['units']
        nano = str(num_dict['nano'])[:3]
        num = f'{units}.{nano}'
        return float(num)

    @staticmethod
    def get_correct_time_format(date):
        return date.isoformat().replace('+00:00', '')[:-3] + 'Z'

    def update_last_updates(self, current_time_utc):
        if (current_time_utc - self.last_15_min_update).total_seconds() >= 900:
            self.last_15_min_update = current_time_utc
        if (current_time_utc - self.last_hour_update).total_seconds() >= 3600:
            self.last_hour_update = current_time_utc
        if (current_time_utc - self.last_day_update).total_seconds() >= 3600 * 24:
            self.last_day_update = current_time_utc
        self.last_update = current_time_utc

    async def get_ticker_candles(self, client, figi, last_update, current_time_utc, interval: CandleInterval):
        headers = {
            "Authorization": f"Bearer {self.config.token}"
        }
        request = {
            "figi": figi,
            "from": self.get_correct_time_format(last_update),
            "to": self.get_correct_time_format(current_time_utc),
            "interval": interval.value,
            "instrumentId": figi
        }
        url = f"{self.config.base_url}{self.config.get_candles}"
        response = await self.request_with_count(client=client, url=url, headers=headers, json=request,
                                                 query_type='market')
        return response.json() if response.status_code == HTTPStatus.OK else None

    async def save_candles(self, response_data, ticker_id, interval: CandleInterval):
        for candle in response_data['candles']:
            if candle['isComplete']:
                await self.db.add_candle(ticker_id,
                                         interval.value,
                                         candle['time'],
                                         self.dict_to_float(candle['open']),
                                         self.dict_to_float(candle['high']),
                                         self.dict_to_float(candle['low']),
                                         self.dict_to_float(candle['close']),
                                         )

    async def load_and_save_ticker_interval(self, client, ticker: Ticker, interval: CandleInterval, start_time,
                                            end_time):
        logger.info(f"Загрузка | интервал: {interval}; тикер: {ticker.name}; id: {ticker.ticker_id}")
        response_data = await self.get_ticker_candles(client, ticker.figi, start_time, end_time, interval)
        if response_data:
            logger.info(f"Запись | интервал: {interval}; тикер: {ticker.name}; id: {ticker.ticker_id}")
            await self.save_candles(response_data, ticker.ticker_id, interval)

    async def load_data(self):
        await self._update_tickers()
        logger.info("Заверишили инициализацию тикеров")
        current_time_utc = datetime.now(timezone.utc)
        tickers = await self.db.get_tickers_with_figi()

        async with httpx.AsyncClient() as client:
            for ticker in tickers:
                await self.load_and_save_ticker_interval(client=client,
                                                         ticker=ticker,
                                                         interval=CandleInterval.CANDLE_INTERVAL_5_MIN,
                                                         start_time=self.last_update,
                                                         end_time=current_time_utc)

                if (current_time_utc - self.last_15_min_update).total_seconds() >= 900:
                    await self.load_and_save_ticker_interval(client=client,
                                                             ticker=ticker,
                                                             interval=CandleInterval.CANDLE_INTERVAL_15_MIN,
                                                             start_time=self.last_15_min_update,
                                                             end_time=current_time_utc)

                if (current_time_utc - self.last_hour_update).total_seconds() >= 3600:
                    await self.load_and_save_ticker_interval(client=client,
                                                             ticker=ticker,
                                                             interval=CandleInterval.CANDLE_INTERVAL_HOUR,
                                                             start_time=self.last_hour_update,
                                                             end_time=current_time_utc)

                if (current_time_utc - self.last_day_update).total_seconds() >= 3600 * 24:
                    await self.load_and_save_ticker_interval(client=client,
                                                             ticker=ticker,
                                                             interval=CandleInterval.CANDLE_INTERVAL_DAY,
                                                             start_time=self.last_day_update,
                                                             end_time=current_time_utc)

        self.update_last_updates(current_time_utc)
