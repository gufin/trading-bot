import asyncio
from datetime import datetime, timedelta
from datetime import timezone
from http import HTTPStatus

import httpx
from httpx import Response
from loguru import logger

from market_loader.constants import attempts_to_tcs_request, deep_for_hour_candles, tcs_request_timeout
from market_loader.infrasturcture.postgres_repository import BotPostgresRepository
from market_loader.models import ApiConfig, CandleInterval, FindInstrumentRequest, InstrumentRequest, Ticker
from market_loader.utils import (convert_to_base_date, dict_to_float, get_correct_time_format, get_interval,
                                 MaxRetriesExceededError,
                                 round_date, to_end_of_day, to_start_of_day)


class MarketDataLoader:

    def __init__(self, db: BotPostgresRepository, config: ApiConfig):
        current_time = datetime.now(timezone.utc)
        self.db = db
        self.config = config
        self.time_counter = 0
        self.last_request_time = current_time
        self.instrument_query_counter = 0
        self.market_query_counter = 0

    async def _request_with_count(self, url: str, headers: dict, json: dict, query_type: str) -> Response:

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

        async with httpx.AsyncClient() as client:
            attempts = 0
            while attempts < attempts_to_tcs_request:
                try:
                    return await client.post(url, headers=headers, json=json)
                except Exception as e:
                    attempts += 1
                    logger.error(f"Ошибка при выполнении запроса (Попытка {attempts}): {e}")
                    await asyncio.sleep(tcs_request_timeout)

        raise MaxRetriesExceededError(f"Не удалось выполнить запрос после {attempts_to_tcs_request} попыток.")

    async def _update_tickers(self) -> None:
        logger.info("Начали инициализацию тикеров")
        tickers = await self.db.get_tickers_without_figi()
        headers = {
            "Authorization": f"Bearer {self.config.token}"
        }

        for ticker in tickers:
            data = FindInstrumentRequest(query=ticker.name)
            url = f"{self.config.base_url}{self.config.find_instrument}"
            response = await self._request_with_count(url=url, headers=headers,
                                                      json=data.model_dump(), query_type='instrument')
            if response.status_code == HTTPStatus.OK:
                response_data = response.json()
                ticker_data = next(
                    (item for item in response_data['instruments'] if item["ticker"] == ticker.name), None)
                if ticker_data is not None:
                    share_url = f"{self.config.base_url}{self.config.share_by}"
                    share_request = InstrumentRequest(classCode=ticker_data['classCode'], id=ticker.name)
                    share_response = await self._request_with_count(url=share_url, headers=headers,
                                                                    json=share_request.model_dump(),
                                                                    query_type='instrument')
                    if share_response.status_code == HTTPStatus.OK:
                        share_data = share_response.json()['instrument']
                        ticker = await self.db.update_tickers(ticker_id=ticker.ticker_id,
                                                              new_figi=ticker_data['figi'],
                                                              new_class_code=ticker_data['classCode'],
                                                              new_currency=share_data['currency'])
                        logger.info(
                            f"Начали получать исторические данные | тикер: {ticker.name}; id: {ticker.ticker_id}")
                        await self._init_ticker_data(ticker)
                        logger.info(
                            f"Закончили получать исторические данные "
                            f"| тикер: {ticker.name}; id: {ticker.ticker_id}")

                    else:
                        logger.critical(f"Не доступен url {share_url}")
                else:
                    logger.error(f"Введен не верный тикер {ticker.name}")
            else:
                logger.critical(f"Не доступен url {url}")
        logger.info("Заверишили инициализацию тикеров")

    async def _minute_ticker_data(self, ticker: Ticker, current_time: datetime, end_time: datetime,
                                  interval: CandleInterval) -> None:
        while current_time > end_time:
            await self._load_and_save_ticker_interval(ticker, interval, to_start_of_day(current_time),
                                                      to_end_of_day(current_time))

            current_time -= timedelta(days=1)

    async def _hour_ticker_data(self, ticker: Ticker, current_time: datetime, end_time: datetime) -> None:
        while current_time > end_time:
            day_of_week = current_time.weekday()
            start_of_week = to_start_of_day((current_time - timedelta(days=day_of_week)))
            end_of_week = to_end_of_day(start_of_week + timedelta(days=4))
            await self._load_and_save_ticker_interval(ticker, CandleInterval.hour, start_of_week, end_of_week)
            current_time -= timedelta(weeks=1)

    async def _init_ticker_data(self, ticker: Ticker) -> None:
        current_time = datetime.now(timezone.utc)
        four_weeks_ago = current_time - timedelta(weeks=4)
        await self._minute_ticker_data(ticker, current_time, four_weeks_ago, CandleInterval.min_5)
        await self._minute_ticker_data(ticker, current_time, four_weeks_ago, CandleInterval.min_15)

        current_time = datetime.now(timezone.utc)
        days_ago = current_time - timedelta(days=deep_for_hour_candles)
        await self._hour_ticker_data(ticker, current_time, days_ago)

        current_time = datetime.now(timezone.utc)
        year_ago = to_start_of_day(current_time - timedelta(days=365))
        await self._load_and_save_ticker_interval(ticker, CandleInterval.day, current_time, year_ago)

    async def _get_ticker_candles(self, figi: str, last_update: datetime, current_time_utc: datetime,
                                  interval: CandleInterval) -> dict:
        headers = {
            "Authorization": f"Bearer {self.config.token}"
        }
        request = {
            "figi": figi,
            "from": get_correct_time_format(last_update),
            "to": get_correct_time_format(current_time_utc),
            "interval": interval.value,
            "instrumentId": figi
        }
        url = f"{self.config.base_url}{self.config.get_candles}"
        response = await self._request_with_count(url=url, headers=headers, json=request, query_type='market')
        return response.json() if response.status_code == HTTPStatus.OK else None

    async def _save_candles(self, response_data: dict, ticker: Ticker, interval: CandleInterval) -> None:
        if len(response_data['candles']) > 0:
            logger.info(
                f"Запись | интервал: {get_interval(interval)}; тикер: {ticker.name}; id: {ticker.ticker_id}")
        for candle in response_data['candles']:
            await self.db.add_candle(ticker.ticker_id,
                                     interval.value,
                                     convert_to_base_date(candle['time']).replace(tzinfo=None),
                                     dict_to_float(candle['open']),
                                     dict_to_float(candle['high']),
                                     dict_to_float(candle['low']),
                                     dict_to_float(candle['close']),
                                     )

    async def _load_and_save_ticker_interval(self, ticker: Ticker, interval: CandleInterval, start_time: datetime,
                                             end_time: datetime) -> None:
        if start_time.weekday() < 5:
            logger.info(
                f"Загрузка | интервал: {get_interval(interval)}; тикер: {ticker.name}; id: {ticker.ticker_id}")
            response_data = await self._get_ticker_candles(ticker.figi, start_time, end_time, interval)
            if response_data:
                if 'candles' in response_data:
                    await self._save_candles(response_data, ticker, interval)
                else:
                    logger.error(
                        (f"Ошибка записи свечей | интервал: {get_interval(interval)}; тикер: {ticker.name}; id: "
                         f"{ticker.ticker_id}; время с {end_time} по {start_time}"))

    async def load_data(self) -> None:
        await self._update_tickers()
        tickers = await self.db.get_tickers_with_figi()

        for ticker in tickers:
            last_5_min_update = await self.db.get_last_timestamp_by_interval_and_ticker(ticker.ticker_id,
                                                                                        CandleInterval.min_5)
            time_difference = datetime.now(timezone.utc) - last_5_min_update
            if time_difference > timedelta(days=1):
                await self._minute_ticker_data(ticker, datetime.now(timezone.utc), last_5_min_update,
                                               CandleInterval.min_5)
            else:
                await self._load_and_save_ticker_interval(
                    ticker=ticker,
                    interval=CandleInterval.min_5,
                    start_time=last_5_min_update,
                    end_time=round_date(datetime.now(timezone.utc)))

            last_15_min_update = await self.db.get_last_timestamp_by_interval_and_ticker(ticker.ticker_id,
                                                                                         CandleInterval.min_15)
            if (datetime.now(timezone.utc) - last_15_min_update).total_seconds() >= 3600 * 24:
                await self._minute_ticker_data(ticker, datetime.now(timezone.utc), last_15_min_update,
                                               CandleInterval.min_15)
            elif (datetime.now(timezone.utc) - last_15_min_update).total_seconds() >= 900:
                await self._load_and_save_ticker_interval(
                    ticker=ticker,
                    interval=CandleInterval.min_15,
                    start_time=last_15_min_update,
                    end_time=datetime.now(timezone.utc))

            last_hour_update = await self.db.get_last_timestamp_by_interval_and_ticker(ticker.ticker_id,
                                                                                       CandleInterval.hour)
            if (datetime.now(timezone.utc) - last_hour_update).total_seconds() >= 3600 * 24 * 7:
                await self._hour_ticker_data(ticker, datetime.now(timezone.utc), last_hour_update)
            elif (datetime.now(timezone.utc) - last_hour_update).total_seconds() >= 3600:
                await self._load_and_save_ticker_interval(
                    ticker=ticker,
                    interval=CandleInterval.hour,
                    start_time=last_hour_update,
                    end_time=datetime.now(timezone.utc))

            last_day_update = await self.db.get_last_timestamp_by_interval_and_ticker(ticker.ticker_id,
                                                                                      CandleInterval.day)
            if (datetime.now(timezone.utc) - last_day_update).total_seconds() >= 3600 * 24:
                await self._load_and_save_ticker_interval(
                    ticker=ticker,
                    interval=CandleInterval.day,
                    start_time=last_day_update,
                    end_time=datetime.now(timezone.utc))
