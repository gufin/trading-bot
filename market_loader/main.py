import asyncio
import os

from dotenv import load_dotenv

from bot.database import Database
from market_loader.loader import MarketDataLoader
from market_loader.models import ApiConfig

load_dotenv()
loop = asyncio.get_event_loop()
db = Database(
    name=os.getenv("PG_NAME"),
    user=os.getenv("PG_USER"),
    password=os.getenv("PG_PASSWORD"),
    host=os.getenv("PG_HOST"),
    port=os.getenv("PG_PORT"),
    loop=loop,
)

config = ApiConfig()
config.token = os.getenv("TOKEN")
config.base_url = "https://invest-public-api.tinkoff.ru/rest/"
config.share_by = "tinkoff.public.invest.api.contract.v1.InstrumentsService/ShareBy"
config.get_candles = "tinkoff.public.invest.api.contract.v1.MarketDataService/GetCandles"
config.find_instrument = "tinkoff.public.invest.api.contract.v1.InstrumentsService/FindInstrument"

loader = MarketDataLoader(db=db, config=config)


async def main():
    while True:
        await loader.load_data()
        await asyncio.sleep(300)


if __name__ == "__main__":
    loop.run_until_complete(main())
