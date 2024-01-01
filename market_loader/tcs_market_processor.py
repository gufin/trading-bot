import math
from typing import Optional

from loguru import logger
from tinkoff.invest import AsyncClient, OrderDirection, OrderType
from tinkoff.invest.constants import INVEST_GRPC_API_SANDBOX
from tinkoff.invest.grpc.operations_pb2 import PortfolioResponse, PositionsResponse
from tinkoff.invest.grpc.orders_pb2 import GetOrdersResponse, OrderState, PostOrderResponse

from market_loader.infrasturcture.postgres_repository import BotPostgresRepository
from market_loader.models import OrderInfo, ReplaceOrderRequest, Ticker
from market_loader.settings import settings
from market_loader.utils import get_uuid, money_to_float, price_to_units_and_nano


class TCSMarketProcessor:
    def __init__(self, db: BotPostgresRepository, sandbox_mode: bool):
        self._db = db
        self._client = None
        self._sandbox_mode = sandbox_mode

    @property
    def _get_client(self) -> AsyncClient:
        if self._client is None:
            self._client = AsyncClient(settings.token, target=INVEST_GRPC_API_SANDBOX)
        return self._client

    async def get_portfolio(self, account_id: str) -> Optional[PortfolioResponse]:
        try:
            async with self._get_client as client:
                return await client.operations.get_portfolio(account_id=account_id)
        except Exception as e:
            return None

    async def get_positions(self, account_id: str) -> Optional[PositionsResponse]:
        try:
            async with self._get_client as client:
                return await client.operations.get_positions(account_id=account_id)
        except Exception as e:
            return None

    async def get_orders(self, account_id: str) -> Optional[GetOrdersResponse]:
        try:
            async with self._get_client as client:
                return await client.orders.get_orders(account_id=account_id)
        except Exception as e:
            return None

    async def update_order(self, account_id: str, order_id: str) -> bool:
        try:
            async with self._get_client as client:
                order_state = await client.orders.get_order_state(account_id=account_id, order_id=order_id)
                atr = await self._db.get_order_atr(order_id)
                order_model = self._convert_data_to_order(order_state, account_id, atr)
                await self._db.update_order(order_model)
                logger.info(f"Обновлен ордер {order_id}. Аккаунт {account_id}.")
                return True
        except Exception as e:
            logger.info(f"Не удалось обновить ордер {order_id}. Аккаунт {account_id}.")
            return False

    async def cancel_order(self, account_id: str, order_id: str) -> bool:
        try:
            async with self._get_client as client:
                await client.orders.cancel_order(account_id=account_id, order_id=order_id)
                await self._db.cancel_order(order_id)
                logger.info(f"Отменен ордер {order_id}. Аккаунт {account_id}.")
                return True
        except Exception as e:
            logger.info(f"Не удалось отменить ордер {order_id}. Аккаунт {account_id}.")
            return False

    async def update_orders(self, account_id: str) -> None:
        orders = await self._db.get_active_orders(account_id)
        for order_id in orders:
            await self.update_order(account_id, order_id)

    async def cancel_all_orders(self, account_id: str) -> None:
        orders = await self._db.get_active_orders(account_id)
        for order_id in orders:
            result = await self.cancel_order(account_id, order_id)
            if result:
                await self._db.cancel_order(order_id)

    async def replace_order(self, price: float, atr: float, account_id: str,
                            active_order: ReplaceOrderRequest) -> bool:
        active_order.price = price_to_units_and_nano(price)
        try:
            async with self._get_client as client:
                result: PostOrderResponse = await client.orders.replace_order(request=active_order)
                order_model = self._convert_data_to_order(result, account_id, atr)
                await self._db.add_order(order_model)
                await self._db.cancel_order(active_order.orderId)
                logger.info(
                    f"Ордер {active_order.orderId} заменен на ордер {order_model.orderId}. Аккаунт {account_id}.")
                return True
        except Exception as e:
            logger.info(f"Не удалось обновить ордер {active_order.orderId}. Аккаунт {account_id}.")
            return False

    async def make_order(self, figi: str, price: float, direction: OrderDirection, order_type: OrderType,
                         atr: float) -> bool:
        account_id = await self._db.get_user_account(user_id=1)
        quantity = await self._get_order_quantity(figi, direction, price, account_id)
        if not quantity or not account_id:
            return False
        new_order_id = get_uuid()
        try:
            async with self._get_client as client:
                result: PostOrderResponse = await client.orders.post_order(figi=figi,
                                                                           quantity=quantity,
                                                                           price=price_to_units_and_nano(price),
                                                                           direction=direction,
                                                                           account_id=account_id,
                                                                           order_type=order_type,
                                                                           order_id=new_order_id,
                                                                           instrument_id=figi, )
                order_model = self._convert_data_to_order(result, account_id, atr)
                await self._db.add_order(order_model)
                ticker = await self._db.get_ticker_by_figi(figi)
                logger.info(
                    (f"Добавлен новый ордер ордер {order_model.orderId}. Аккаунт {account_id}; тикер {ticker.name}; "
                     f"количество {quantity}; цена {price_to_units_and_nano(price)}. Направление {direction.value}; "
                     f"Тип: {order_type.value}"))
                return True
        except Exception as e:
            logger.critical((f"Не удалось создать ордер. Аккаунт {account_id}; тикер {ticker.name}; "
                             f"Направление {direction.value}; Тип: {order_type.value}"))
            return False

    async def buy_limit_with_replace(self, figi: str, price: float, atr: float) -> bool:
        account_id = await self._db.get_user_account(user_id=1)
        active_order = await self._db.get_active_order_by_figi(account_id, figi, OrderDirection.ORDER_DIRECTION_BUY)
        if active_order:
            return await self.replace_order(price, atr, account_id, active_order)
        return await self.make_order(figi, price, OrderDirection.ORDER_DIRECTION_BUY, OrderType.ORDER_TYPE_LIMIT, atr)

    async def sell_limit_with_replace(self, figi: str, price: float, atr: float) -> bool:
        account_id = await self._db.get_user_account(user_id=1)
        active_order = await self._db.get_active_order_by_figi(account_id, figi, OrderDirection.ORDER_DIRECTION_SELL)
        if active_order:
            return await self.replace_order(price, atr, account_id, active_order)
        return await self.make_order(figi, price, OrderDirection.ORDER_DIRECTION_SELL, OrderType.ORDER_TYPE_LIMIT, atr)

    async def sell_market(self, figi: str, price: float) -> bool:
        atr = 1
        return await self.make_order(figi, price, OrderDirection.ORDER_DIRECTION_SELL, OrderType.ORDER_TYPE_MARKET,
                                     atr)

    async def get_current_prices(self) -> Optional[dict]:
        active_figi = await self._db.get_active_figi()
        try:
            async with self._get_client as client:
                price_data = await client.market_data.get_last_prices(figi=active_figi)
                return {
                    price_row.figi: money_to_float(price_row.price)
                    for price_row in price_data.last_prices
                }
        except Exception as e:
            return None

    async def save_current_positions(self, account_id: str, positions: list[Ticker]) -> None:
        task_id = await self._db.create_new_position_check_task(1)
        await self._db.add_positions(task_id, positions)

    async def close_deal(self, account_id: str, ticker: Ticker):
        latest_sell_order = await self._db.get_latest_order_by_direction(account_id, ticker.figi,
                                                                         OrderDirection.ORDER_DIRECTION_SELL)
        latest_buy_order = await self._db.get_latest_order_by_direction(account_id, ticker.figi,
                                                                        OrderDirection.ORDER_DIRECTION_BUY)

        await self._db.update_deal(ticker.ticker_id, latest_buy_order.orderId, latest_sell_order.orderId)

    async def sell_all_position_market(self):
        account_id = await self._db.get_user_account(user_id=1)
        position = await self.get_positions(account_id)
        for security in position.securities:
            if security.instrument_type == 'share':
                ticker = await self._db.get_ticker_by_figi(security.figi)
                await self.sell_market(ticker.figi, 100)
                await self.close_deal(account_id, ticker)
        await self.update_orders(account_id)

    @staticmethod
    async def in_position(figi: str, positions: PositionsResponse) -> bool:
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
        return round((price // ticker.min_price_increment) * ticker.min_price_increment, 2)

    async def _get_order_quantity(self, figi: str, direction: OrderDirection, price: float,
                                  account_id: str) -> Optional[int]:
        if direction == OrderDirection.ORDER_DIRECTION_SELL:
            return await self._get_quantity_to_sell(account_id, figi)
        if direction == OrderDirection.ORDER_DIRECTION_BUY:
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
        total_amount_portfolio = money_to_float(portfolio.total_amount_portfolio)
        amount_for_investment = (settings.position_buy_percent / 100) * total_amount_portfolio
        ticker = await self._db.get_ticker_by_figi(figi)
        number_of_shares = math.floor(amount_for_investment / (price * ticker.lot))
        money = money_to_float(portfolio.total_amount_currencies)
        if number_of_shares == 0 or amount_for_investment >= money:
            logger.warning(f"Недостаточная сумма портфеля для покупки {figi}")
            return None
        elif not settings.send_box_mode and settings.debug_mode:
            return 1
        else:
            return number_of_shares

    @staticmethod
    def _convert_data_to_order(data: OrderState | PostOrderResponse, account_id: str, atr: float = None) -> OrderInfo:
        if atr is None:
            atr = 1.0
        converted_data = {
            **data,
            'lotsRequested': int(data.lots_requested),
            'lotsExecuted': int(data.lots_executed),
            'initialOrderPrice': money_to_float(data.initial_order_price),
            'executedOrderPrice': money_to_float(data.executed_order_price),
            'totalOrderAmount': money_to_float(data.total_order_amount),
            'initialCommission': money_to_float(data.initial_commission),
            'executedCommission': money_to_float(data.executed_commission),
            'initialSecurityPrice': money_to_float(data.initial_security_price),
            'accountId': account_id,
            'atr': atr
        }
        return OrderInfo(**converted_data)
