import asyncio
import math
from datetime import datetime
from datetime import timezone
from http import HTTPStatus
from http.client import responses
from typing import Optional

from httpx import Response
from loguru import logger

from market_loader.infrasturcture.postgres_repository import BotPostgresRepository
from market_loader.models import AccountRequest, Order, OrderDirection, OrderInfo, OrderType, OrderUpdateRequest, \
    PortfolioRequest, Ticker
from market_loader.settings import settings
from market_loader.utils import dict_to_float, get_uuid, make_http_request, price_to_units_and_nano


class MarketProcessor:

    def __init__(self, db: BotPostgresRepository, sandbox_mode: bool):
        self._db = db
        self._last_request_time = datetime.now(timezone.utc)
        self._sandbox_mode = sandbox_mode
        self._sandbox_query_counter = 0
        self._post_order_query_counter = 0
        self._get_order_query_counter = 0
        self._cancel_order_counter = 0
        self._operations_counter = 0
        self._market_query_counter = 0

    async def _request_with_count(self, url: str, json: dict, query_type: str) -> Response:

        current_time = datetime.now(timezone.utc)
        time_difference = (current_time - self._last_request_time).total_seconds()

        if time_difference >= 60:
            self._reset_counters()

        if self._sandbox_mode:
            self._sandbox_query_counter += 1
        elif query_type == 'cancel_order':
            self._cancel_order_counter += 1
        elif query_type == 'get_order':
            self._get_order_query_counter += 1
        elif query_type == 'post_order':
            self._post_order_query_counter += 1
        elif query_type == 'operations':
            self._operations_counter += 1
        elif query_type == 'market_query':
            self._market_query_counter += 1

        if (self._sandbox_query_counter == settings.sandbox_query_limit
                or self._cancel_order_counter == settings.cancel_order_limit
                or self._get_order_query_counter == settings.get_order_limit
                or self._post_order_query_counter == settings.post_order_limit):
            logger.info("Достигли предела запросов в минуту")
            await asyncio.sleep(60 - time_difference)
            self._reset_counters()

        headers = {
            "Authorization": f"Bearer {settings.token}"
        }

        return await make_http_request(url, headers, json)

    def _reset_counters(self):
        current_time = datetime.now(timezone.utc)
        self._sandbox_query_counter = 0
        self._post_order_query_counter = 0
        self._get_order_query_counter = 0
        self._cancel_order_counter = 0
        self._operations_counter = 0
        self._market_query_counter = 0
        self._last_request_time = current_time

    async def make_order(self, figi: str, price: float, direction: OrderDirection, order_type: OrderType) -> bool:
        account_id = await self._db.get_user_account(user_id=1)
        quantity = await self._get_order_quantity(figi, direction, price, account_id)
        if not quantity or not account_id:
            return False
        new_order_id = get_uuid()
        order = Order(figi=figi,
                      quantity=quantity,
                      price=price_to_units_and_nano(price),
                      direction=direction.value,
                      accountId=account_id,
                      orderType=order_type.value,
                      orderId=new_order_id,
                      instrumentId=figi)
        url = f"{settings.base_url}{settings.post_order}"
        response = await self._request_with_count(url, order.model_dump(), 'post_order')
        ticker = await self._db.get_ticker_by_figi(figi)
        if response.status_code == HTTPStatus.OK:
            order_model = self._convert_data_to_order(response.json(), account_id)
            await self._db.add_order(order_model)
            logger.info((f"Добавлен новый ордер ордер. Аккаунт {account_id}; тикер {ticker.name}; "
                         f"Направление {direction.value}; Тип: {order_type.value}"))
            return True
        else:
            logger.critical((f"Не удалось создать ордер. Аккаунт {account_id}; тикер {ticker.name}; "
                             f"Направление {direction.value}; Тип: {order_type.value}"))
            return False

    async def _get_order_quantity(self, figi: str, direction: OrderDirection, price: float,
                                  account_id: str) -> Optional[int]:
        if direction == OrderDirection.sell:
            return await self._get_quantity_to_sell(account_id, figi)
        if direction == OrderDirection.buy:
            return await self._get_quantity_to_buy(account_id, figi, price)

    async def _get_quantity_to_sell(self, account_id: str, figi: str) -> Optional[int]:
        positions = await self.get_positions(account_id)
        if positions is None:
            logger.critical(f"Не удалось получить данные о позициях по счету {account_id}")
            return None
        for share in positions['securities']:
            if share['figi'] == figi:
                ticker = await self._db.get_ticker_by_figi(figi)
                return int(int(share['balance']) / ticker.lot)
        logger.critical(f"Не количество инструмента {figi} в аккаунте {account_id}")
        return None

    async def _get_quantity_to_buy(self, account_id: str, figi: str, price: float) -> Optional[int]:
        portfolio = await self.get_portfolio(account_id)
        if portfolio is None:
            logger.critical(f"Не удалось получить данные портфолио по счету {account_id}")
            return None
        total_amount_portfolio = dict_to_float({"units": portfolio["totalAmountPortfolio"]["units"],
                                                "nano": portfolio["totalAmountPortfolio"]["nano"]})
        amount_for_investment = (settings.position_buy_percent / 100) * total_amount_portfolio
        ticker = await self._db.get_ticker_by_figi(figi)
        number_of_shares = math.floor(amount_for_investment / (price * ticker.lot))
        total_money = portfolio["totalAmountCurrencies"]
        money = dict_to_float({'units': total_money['units'], 'nano': total_money['nano']})
        if number_of_shares == 0 or amount_for_investment >= money:
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

    @staticmethod
    def _convert_data_to_order(data: dict, account_id: str) -> OrderInfo:
        converted_data = {
            **data,
            'lotsRequested': int(data['lotsRequested']),
            'lotsExecuted': int(data['lotsExecuted']),
            'initialOrderPrice': dict_to_float(data['initialOrderPrice']),
            'executedOrderPrice': dict_to_float(data['executedOrderPrice']),
            'totalOrderAmount': dict_to_float(data['totalOrderAmount']),
            'initialCommission': dict_to_float(data['initialCommission']),
            'executedCommission': dict_to_float(data['executedCommission']),
            'initialSecurityPrice': dict_to_float(data['initialSecurityPrice']),
            'accountId': account_id
        }

        return OrderInfo(**converted_data)

    async def get_orders(self) -> Optional[dict]:
        account_id = await self._db.get_user_account(user_id=1)
        orders_request = AccountRequest(accountId=account_id)
        url = f"{settings.base_url}{settings.get_orders}"
        response = await self._request_with_count(url, orders_request.model_dump(), 'get_order')
        return response.json() if response.status_code == HTTPStatus.OK else None

    async def update_order(self, account_id: str, order_id: str) -> bool:
        order_update_request = OrderUpdateRequest(accountId=account_id, orderId=order_id)
        url = f"{settings.base_url}{settings.update_order}"
        response = await self._request_with_count(url, order_update_request.model_dump(), 'get_order')
        if response.status_code == HTTPStatus.OK:
            order_model = self._convert_data_to_order(response.json(), account_id)
            await self._db.update_order(order_model)
            logger.info(f"Обновлен ордер {order_id}. Аккаунт {account_id}.")
            return True
        else:
            logger.info(f"Не удалось обновить ордер {order_id}. Аккаунт {account_id}.")
            return False

    async def update_orders(self, account_id: str) -> None:
        orders = await self._db.get_active_orders(account_id)
        for order_id in orders:
            await self.update_order(account_id, order_id)

    async def cancel_order(self, account_id: str, order_id: str) -> bool:
        order_cancel_request = OrderUpdateRequest(accountId=account_id, orderId=order_id)
        url = f"{settings.base_url}{settings.cancel_order}"
        response = await self._request_with_count(url, order_cancel_request.model_dump(), 'cancel_order')
        if response.status_code == HTTPStatus.OK:
            logger.info(f"Отменен ордер {order_id}. Аккаунт {account_id}.")
            return True
        else:
            logger.info(f"Не удалось отменить ордер {order_id}. Аккаунт {account_id}.")
            return False

    async def cancel_all_orders(self, account_id: str) -> None:
        orders = await self._db.get_active_orders(account_id)
        for order_id in orders:
            result = await self.cancel_order(account_id, order_id)
            if result:
                await self._db.cancel_order(order_id)

    async def replace_order(self, figi: str, price: float) -> bool:
        account_id = await self._db.get_user_account(user_id=1)
        active_order = await self._db.get_active_order_by_figi(account_id, figi)
        if active_order:
            active_order.price = price_to_units_and_nano(price)
            url = f"{settings.base_url}{settings.replace_order}"
            response = await self._request_with_count(url, active_order.model_dump(), 'post_order')
            if response.status_code == HTTPStatus.OK:
                order_model = self._convert_data_to_order(response.json(), account_id)
                await self._db.add_order(order_model)
                await self._db.cancel_order(active_order.orderId)
                logger.info(f"Обновлен (replace) ордер {order_model.orderId}. Аккаунт {account_id}.")
                return True
            else:
                logger.info(f"Не удалось обновить ордер {active_order.orderId}. Аккаунт {account_id}.")
                return False

    async def buy_limit_with_replace(self, figi: str, price: float) -> bool:
        account_id = await self._db.get_user_account(user_id=1)
        active_order = await self._db.get_active_order_by_figi(account_id, figi)
        if active_order:
            return await self.replace_order(figi, price)
        else:
            return await self.make_order(figi, price, OrderDirection.buy, OrderType.limit)

    async def buy_market(self, figi: str, price: float) -> None:
        await self.make_order(figi, price, OrderDirection.buy, OrderType.market)

    async def sell_limit_with_replace(self, figi: str, price: float) -> bool:
        account_id = await self._db.get_user_account(user_id=1)
        active_order = await self._db.get_active_order_by_figi(account_id, figi)
        if active_order:
            return await self.replace_order(figi, price)
        else:
            return await self.make_order(figi, price, OrderDirection.sell, OrderType.limit)

    async def sell_market(self, figi: str, price: float) -> None:
        await self.make_order(figi, price, OrderDirection.sell, OrderType.market)

    async def get_current_prices(self) -> Optional[dict]:
        active_figi = await self._db.get_active_figi()
        data = {"figi": active_figi, "instrumentId": active_figi}
        url = f"{settings.base_url}{settings.last_prices}"
        response = await self._request_with_count(url, data, 'market_query')
        if response.status_code == HTTPStatus.OK:
            price_data = response.json()
            return {
                price_row["figi"]: dict_to_float(price_row["price"])
                for price_row in price_data['lastPrices']
            }
        return None

    async def in_position(self, account_id: str, figi: str) -> bool:
        positions = await self.get_positions(account_id)
        if positions:
            if "positions" in positions and positions["positions"]:
                return any(
                    position["instrumentType"] == "share"
                    and position["figi"] == figi
                    for position in positions["positions"]
                )
            return True
        return True

    @staticmethod
    def round_price(price: float, ticker: Ticker) -> float:
        return (price // ticker.min_price_increment) * ticker.min_price_increment





