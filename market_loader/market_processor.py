import asyncio
import math
from datetime import datetime
from datetime import timezone
from http import HTTPStatus
from typing import Optional

from httpx import Response
from loguru import logger

from market_loader.infrasturcture.postgres_repository import BotPostgresRepository
from market_loader.models import AccountRequest, Order, OrderDirection, OrderType, PortfolioRequest
from market_loader.settings import settings
from market_loader.utils import dict_to_float, get_uuid, make_http_request, price_to_units_and_nano


class MarketProcessor:

    def __init__(self, db: BotPostgresRepository, sandbox_mode: bool):
        self.db = db
        self.last_request_time = datetime.now(timezone.utc)
        self.sandbox_mode = sandbox_mode
        self.sandbox_query_counter = 0
        self.post_order_query_counter = 0
        self.get_order_query_counter = 0
        self.cancel_order_counter = 0
        self.operations_counter = 0

    async def _request_with_count(self, url: str, json: dict, query_type: str) -> Response:

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
        elif query_type == 'operations':
            self.operations_counter += 1

        if (self.sandbox_query_counter == settings.sandbox_query_limit
                or self.cancel_order_counter == settings.cancel_order_limit
                or self.get_order_query_counter == settings.get_order_limit
                or self.post_order_query_counter == settings.post_order_limit):
            logger.info("Достигли предела запросов в минуту")
            await asyncio.sleep(60 - time_difference)
            self._reset_counters()

        headers = {
            "Authorization": f"Bearer {settings.token}"
        }

        return await make_http_request(url, headers, json)

    def _reset_counters(self):
        current_time = datetime.now(timezone.utc)
        self.sandbox_query_counter = 0
        self.post_order_query_counter = 0
        self.get_order_query_counter = 0
        self.cancel_order_counter = 0
        self.operations_counter = 0
        self.last_request_time = current_time

    async def make_order(self, figi: str, price: float, direction: OrderDirection, order_type: OrderType):
        account_id = await self.db.get_user_account(user_id=1)
        quantity = await self._get_order_quantity(figi, direction, price, account_id)
        if not quantity or not account_id:
            return None
        new_order_id = get_uuid()
        order = Order(figi=figi,
                      quantity=quantity,
                      price=price_to_units_and_nano(price),
                      direction=direction,
                      accountId=account_id,
                      orderType=order_type,
                      orderId=new_order_id,
                      instrumentId=figi)

    async def _get_order_quantity(self, figi: str, direction: OrderDirection, price: float, account_id: str,
                                  position_buy_percent=None):
        if direction == OrderDirection.sell:
            positions = await self.get_positions(account_id)
            if positions is None:
                logger.critical(f"Не удалось получить данные о позициях по счету {account_id}")
                return None
            for share in positions['securities']:
                if share['figi'] == figi:
                    return int(share['balance'])
            logger.critical(f"Не количество инструмента {figi} в аккаунте {account_id}")
            return None
        if direction == OrderDirection.buy:
            portfolio = await self.get_portfolio(account_id)
            if portfolio is None:
                logger.critical(f"Не удалось получить данные портфолио по счету {account_id}")
                return None
            total_amount_portfolio = dict_to_float({"units": portfolio["totalAmountPortfolio"]["units"],
                                                    "nano": portfolio["totalAmountPortfolio"]["nano"]})
            amount_for_investment = (position_buy_percent/100) * total_amount_portfolio
            number_of_shares = math.floor(amount_for_investment / price)
            if number_of_shares == 0:
                logger.warning(f"Недостаточная сумма портфеля для покупки {figi}")
                return None
            elif not settings.send_box_mode and settings.debug_mode:
                return 1
            else:
                return number_of_shares

    async def get_positions(self, account_id: str) -> Optional[dict]:
        positions_request = AccountRequest(accountId=account_id)
        url = f"{settings.base_url}{settings.positions}"
        response = await self._request_with_count(url, positions_request.model_dump(), 'operations')
        return response.json() if response.status_code == HTTPStatus.OK else None

    async def get_portfolio(self, account_id: str) -> Optional[dict]:
        portfolio_request = PortfolioRequest(accountId=account_id)
        url = f"{settings.base_url}{settings.portfolio}"
        response = await self._request_with_count(url, portfolio_request.model_dump(), 'operations')
        return response.json() if response.status_code == HTTPStatus.OK else None
