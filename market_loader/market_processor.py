import asyncio
from datetime import datetime
from datetime import timezone

from httpx import Response
from loguru import logger

from market_loader.infrasturcture.postgres_repository import BotPostgresRepository
from market_loader.models import Order, OrderDirection, OrderType
from market_loader.settings import settings
from market_loader.utils import make_http_request, price_to_units_and_nano


class MarketProcessor:

    def __init__(self, db: BotPostgresRepository, sandbox_mode: bool):
        self.db = db
        self.last_request_time = datetime.now(timezone.utc)
        self.sandbox_mode = sandbox_mode
        self.sandbox_query_counter = 0
        self.post_order_query_counter = 0
        self.get_order_query_counter = 0
        self.cancel_order_counter = 0

    async def _request_with_count(self, url: str, headers: dict, json: dict, query_type: str) -> Response:

        current_time = datetime.now(timezone.utc)
        time_difference = (current_time - self.last_request_time).total_seconds()

        if time_difference >= 60:
            self._reset_counters()

        if self.sandbox_mode:
            self.sandbox_query_counter += 1
        elif query_type == 'cancel_order':
            self.cancel_order_counter += 1
        elif query_type == 'get_order':
            self.get_order_query_counter += 1
        elif query_type == 'post_order':
            self.post_order_query_counter += 1

        if (self.sandbox_query_counter == settings.sandbox_query_limit
                or self.cancel_order_counter == settings.cancel_order_limit
                or self.get_order_query_counter == settings.get_order_limit
                or self.post_order_query_counter == settings.post_order_limit):
            logger.info("Достигли предела запросов в минуту")
            await asyncio.sleep(60 - time_difference)
            self._reset_counters()
        return await make_http_request(url, headers, json)

    def _reset_counters(self):
        current_time = datetime.now(timezone.utc)
        self.sandbox_query_counter = 0
        self.post_order_query_counter = 0
        self.get_order_query_counter = 0
        self.cancel_order_counter = 0
        self.last_request_time = current_time

    async def make_order(self, figi: str, price: float, direction: OrderDirection, order_type: OrderType):
        quantity = await self._get_order_quantity(direction, price)
        account_id = await self.db.get_user_account(user_id=1)
        if not quantity or not account_id:
            return None
        order = Order(figi=figi,
                      quantity=quantity,
                      price=price_to_units_and_nano(price),
                      direction=direction,
                      accountId=account_id,
                      orderType=order_type)

    async def _get_order_quantity(self, direction: OrderDirection, price: float):
        pass
