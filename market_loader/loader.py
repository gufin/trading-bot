from datetime import datetime
from datetime import timezone
from http import HTTPStatus

import httpx

from bot.database import Database
from market_loader.models import ApiConfig, CandleInterval, FindInstrumentRequest, InstrumentRequest


class MarketDataLoader:

    def __init__(self, db: Database, config: ApiConfig):
        # current_time = datetime.now(timezone.utc)
        date_string = '2023-10-12 11:49:20.205298+00:00'
        date_format = '%Y-%m-%d %H:%M:%S.%f%z'
        current_time = datetime.strptime(date_string, date_format)
        self.db = db
        self.config = config
        self.time_counter = 0
        self.last_update = current_time
        self.last_15_min_update = current_time
        self.last_hour_update = current_time
        self.last_day_update = current_time

    async def _update_tickers(self):
        tickers = await self.db.get_tickers_without_figi()
        headers = {
            "Authorization": f"Bearer {self.config.token}"
        }
        async with httpx.AsyncClient() as client:
            for ticker in tickers:
                data = FindInstrumentRequest(query=ticker.name)
                url = f"{self.config.base_url}{self.config.find_instrument}"
                response = await client.post(url, headers=headers, json=data.model_dump())
                if response.status_code == HTTPStatus.OK:
                    response_data = response.json()
                    if len(response_data['instruments']) == 1:
                        ticker_data = response_data['instruments'][0]
                        share_url = f"{self.config.base_url}{self.config.share_by}"
                        share_request = InstrumentRequest(classCode=ticker_data['classCode'], id=ticker.name)
                        share_response = await client.post(share_url, headers=headers, json=share_request.model_dump())
                        if share_response.status_code == HTTPStatus.OK:
                            share_data = share_response.json()['instrument']
                            await self.db.update_tickers(ticker_id=ticker.ticker_id,
                                                         new_figi=ticker_data['figi'],
                                                         new_classCode=ticker_data['classCode'],
                                                         new_currency=share_data['currency'])
                        else:
                            print(f"Не доступен url {share_url}")
                    else:
                        print(f"Введен не верный тикер {ticker.name}")
                else:
                    print(f"Не доступен url {url}")

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
        if (self.last_15_min_update - current_time_utc).seconds >= 900:
            self.last_15_min_update = current_time_utc
        if (self.last_hour_update - current_time_utc).seconds >= 3600:
            self.last_hour_update = current_time_utc
        if (self.last_day_update - current_time_utc).seconds >= 3600 * 24:
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
        response = await client.post(url, headers=headers, json=request)
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

    async def load_data(self):
        await self._update_tickers()
        current_time_utc = datetime.now(timezone.utc)
        tickers = await self.db.get_tickers_with_figi()

        async with httpx.AsyncClient() as client:
            for ticker in tickers:
                response_data = await self.get_ticker_candles(client, ticker.figi, self.last_update, current_time_utc,
                                                              CandleInterval.CANDLE_INTERVAL_5_MIN)
                if response_data:
                    await self.save_candles(response_data, ticker.ticker_id, CandleInterval.CANDLE_INTERVAL_5_MIN)

                if (self.last_15_min_update - current_time_utc).seconds >= 900:
                    response_data = await self.get_ticker_candles(client, ticker.figi, self.last_15_min_update,
                                                                  current_time_utc,
                                                                  CandleInterval.CANDLE_INTERVAL_15_MIN)
                    if response_data:
                        await self.save_candles(response_data, ticker.ticker_id, CandleInterval.CANDLE_INTERVAL_15_MIN)

                if (self.last_hour_update - current_time_utc).seconds >= 3600:
                    response_data = await self.get_ticker_candles(client, ticker.figi, self.last_hour_update,
                                                                  current_time_utc,
                                                                  CandleInterval.CANDLE_INTERVAL_HOUR)
                    if response_data:
                        await self.save_candles(response_data, ticker.ticker_id, CandleInterval.CANDLE_INTERVAL_HOUR)

                if (self.last_day_update - current_time_utc).seconds >= 3600 * 24:
                    response_data = await self.get_ticker_candles(client, ticker.figi, self.last_day_update,
                                                                  current_time_utc,
                                                                  CandleInterval.CANDLE_INTERVAL_DAY)
                    if response_data:
                        await self.save_candles(response_data, ticker.ticker_id, CandleInterval.CANDLE_INTERVAL_DAY)
                    self.last_hour_update = current_time_utc

        self.update_last_updates(current_time_utc)
