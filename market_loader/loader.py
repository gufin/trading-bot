from http import HTTPStatus

import httpx
from pydantic import parse_obj_as

from bot.database import Database
from market_loader.models import ApiConfig, FindInstrumentRequest, InstrumentRequest


class MarketDataLoader:

    def __init__(self, db: Database, config: ApiConfig):
        self.db = db
        self.config = config

    async def _update_tickers(self):
        tickers = await self.db.get_tickers_without_figi()
        headers = {
            "Authorization": f"Bearer {self.config.token}"
        }
        async with httpx.AsyncClient() as client:
            for ticker in tickers:
                data = FindInstrumentRequest(query=ticker.name)
                url = f"{self.config.base_url}{self.config.find_instrument}"
                response = await client.post(url, headers=headers,json=data.model_dump())
                if response.status_code == HTTPStatus.OK:
                    response_data = response.json()
                    if len(response_data['instruments']) == 1:
                        ticker_data = response_data['instruments'][0]
                        share_url = f"{self.config.base_url}{self.config.share_by}"
                        share_request = InstrumentRequest(classCode=ticker_data['classCode'], id=ticker.name)
                        share_response = await client.post(share_url, headers=headers,json=share_request.model_dump())
                        if share_response.status_code == HTTPStatus.OK:
                            share_data = share_response.json()['instrument']
                            await self.db.update_tickers(ticker_id=ticker.ticker_id,
                                                         new_figi=ticker_data['figi'],
                                                         new_classCode=ticker_data['classCode'],
                                                         new_currency=share_data['currency'])
                        else:
                            print(f"Не доступен url {url}")
                    else:
                        print(f"Введен не верный тикер {ticker.name}")
                else:
                    print(f"Не доступен url {url}")

    async def load_data(self):
        await self._update_tickers()
