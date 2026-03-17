from .base import fetch_odoo_data
from .schemas import INVOICE_FIELDS, INVOICE_LINE_FIELDS

def get_invoices_raw(client, fecha_inicio="2023-01-01"):
    """Extrae cabeceras desde la fecha que tú le pidas (default 2023)"""
    domain = [
        ("move_type", "in", ["out_invoice", "out_refund"]),
        ("invoice_date", ">=", fecha_inicio),
        ("state", "=", "posted")
    ]
    return fetch_odoo_data(client=client, model_name="account.move", fields=INVOICE_FIELDS, domain=domain)

def get_invoice_lines_raw(client, fecha_inicio="2023-01-01", fecha_fin=None):
    """Extrae detalles permitiendo rangos de fechas para cargas masivas"""
    domain = [
        ("move_id.move_type", "in", ["out_invoice", "out_refund"]),
        ("move_id.state", "=", "posted"),
        ("display_type", "=", "product"),
        ("date", ">=", fecha_inicio)
    ]

    if fecha_fin:
        domain.append(("date", "<=", fecha_fin))

    return fetch_odoo_data(client=client, model_name="account.move.line", fields=INVOICE_LINE_FIELDS, domain=domain)