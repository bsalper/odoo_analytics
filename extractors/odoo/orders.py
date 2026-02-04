from datetime import datetime
from dateutil.relativedelta import relativedelta
from .base import fetch_odoo_data
from .schemas import ORDER_FIELDS, ORDER_LINE_FIELDS

def get_orders_raw(client):
    two_months_ago = (datetime.today() - relativedelta(months=2)).strftime("%Y-%m-%d")

    domain = [("date_order", ">=", two_months_ago)]

    return fetch_odoo_data(
        client=client,
        model_name="sale.order",
        fields=ORDER_FIELDS,
        domain=domain
    )

def get_order_lines_raw(client):
    two_months_ago = (datetime.today() - relativedelta(months=2)).strftime("%Y-%m-%d")

    domain = [("create_date", ">=", two_months_ago)]

    return fetch_odoo_data(
        client=client,
        model_name="sale.order.line",
        fields=ORDER_LINE_FIELDS,
        domain=domain
    )
