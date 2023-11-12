import asyncio
import os

from aiogram import Bot, Dispatcher
from aiogram.contrib.fsm_storage.redis import RedisStorage2
from dotenv import load_dotenv

from market_loader.infrasturcture.entities import get_sessionmaker
from market_loader.infrasturcture.postgres_repository import BotPostgresRepository

# import uvloop  # running only linux


load_dotenv()
# asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
token = os.getenv("BOT_TOKEN")
bot = Bot(token=token, parse_mode="html")
loop = asyncio.get_event_loop()
storage = RedisStorage2(os.getenv("REDIS_HOST"), os.getenv("REDIS_PORT"), db=5)
dp = Dispatcher(bot, loop=loop, storage=storage)
sessionmaker = get_sessionmaker()
db = BotPostgresRepository(sessionmaker)
