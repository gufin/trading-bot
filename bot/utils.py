from tinkoff.invest.grpc.common_pb2 import MoneyValue
from tinkoff.invest.grpc.operations_pb2 import PortfolioResponse
from tinkoff.invest.grpc.orders_pb2 import GetOrdersResponse

from bot.loader import db
from market_loader.models import CandleInterval
from market_loader.utils import dict_to_float, make_tw_link


async def format_portfolio_message(data: PortfolioResponse) -> str:
    message = "Обзор портфолио:\n"
    total_portfolio = data.total_amount_portfolio
    message += f"Общая стоимость портфолио: {get_money_view(total_portfolio)}"

    total_money = data.total_amount_currencies
    message += f"Деньги: {get_money_view(total_money)}"

    total_shares = data.total_amount_shares
    message += f"Акции: {get_money_view(total_shares)}"

    header_added = False
    for position in data.positions:
        if position.instrument_type != "share":
            continue
        if not header_added:
            message += "Подробности по позициям:\n"
            header_added = True
        units = int(position.quantity.units)
        current_price = position.current_price
        price = dict_to_float({'units': current_price.units, 'nano': current_price.nano})
        ticker = await db.get_ticker_by_figi(position.figi)
        message += f"- {ticker.name}, Количество: {units}, Цена: {price}, Сумма: {round(price * units, 2)}\n"

    return message


def get_money_view(data: MoneyValue):
    return (f"{dict_to_float({'units': data.units, 'nano': data.nano})} "
            f"{data.currency}\n")


async def format_active_orders_message(data: GetOrdersResponse) -> str:
    message = ''
    header_added = False
    interval = CandleInterval.min_5
    order_count = 0
    total_sum = 0

    if len(data.orders) == 0:
        return 'Нет активных заявок, милорд'
    byu_orders = ''
    sell_orders = ''
    for order in data.orders:
        if order.execution_report_status == 'EXECUTION_REPORT_STATUS_NEW':
            if not header_added:
                message = "Активные заявки:\n"
                header_added = True
            ticker = await db.get_ticker_by_figi(order.figi)
            initial_price = order.average_position_price
            price = dict_to_float({'units': initial_price.units, 'nano': initial_price.nano})
            amount = int(order.lots_requested) * ticker.lot
            order_sum = round(price * amount, 2)
            order_type = 'Л' if order.order_type == 'ORDER_TYPE_LIMIT' else 'Р'
            tw_link = f'<a href="{make_tw_link(ticker.name, interval.value)}">tw</a>'
            if order.direction == 'ORDER_DIRECTION_BUY':
                byu_orders += (f'- Buy {ticker.name}, Кол-во {amount}, Цена: {price}, Тип: {order_type}, '
                               f'Сумма: {order_sum} {tw_link}\n')
                order_count += 1
                total_sum += order_sum
            else:
                sell_orders += (f'- Sell {ticker.name}, Количество {amount} Цена: {price}, Тип: {order_type}, '
                                f'Сумма: {order_sum} {tw_link}\n')

    message += byu_orders + sell_orders

    if not header_added:
        message = 'Нет активных заявок, милорд \n'

    message += "--------------------\n"
    message += f"Всего заявок на покупку {order_count} на сумму {round(total_sum, 2)}"

    return message
