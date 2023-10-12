import asyncio
import os
import time
from dotenv import load_dotenv

from bot.database import Database
from market_loader.loader import MarketDataLoader

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

loader = MarketDataLoader()


while True:
    loader.load_data()
    time.sleep(300)