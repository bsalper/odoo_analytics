from datetime import datetime
from dateutil.relativedelta import relativedelta
from .base import fetch_odoo_data
from .schemas import INVOICE_FIELDS, INVOICE_LINE_FIELDS


def get_invoices_raw(client):
    two_months_ago = (datetime.today() - relativedelta(months=2)).strftime("%Y-%m-%d")

    # domain = [
    #     ("move_type", "=", "out_invoice"),
    #     ("invoice_date", ">=", two_months_ago),
    #     ("state", "=", "posted")
    # ]

    domain = [
        ("move_type", "=", "out_invoice"),
        ("invoice_date", ">=", "2025-01-01"),
        ("invoice_date", "<=", "2025-12-31"),
        ("state", "=", "posted")
    ]


    return fetch_odoo_data(
        client=client,
        model_name="account.move",
        fields=INVOICE_FIELDS,
        domain=domain
    )


def get_invoice_lines_raw(client):
    two_months_ago = (datetime.today() - relativedelta(months=2)).strftime("%Y-%m-%d")

    domain = [
        ("move_id.move_type", "=", "out_invoice"),
        ("move_id.invoice_date", ">=", two_months_ago),
        ("move_id.state", "=", "posted")
    ]

    return fetch_odoo_data(
        client=client,
        model_name="account.move.line",
        fields=INVOICE_LINE_FIELDS,
        domain=domain
    )