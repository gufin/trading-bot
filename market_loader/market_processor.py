from market_loader.infrasturcture.postgres_repository import BotPostgresRepository
from market_loader.models import Order, OrderDirection, OrderType
from market_loader.utils import price_to_units_and_nano


class MarketProcessor:

    def __init__(self, db: BotPostgresRepository):
        self.db = db

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
