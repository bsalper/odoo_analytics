from .base import fetch_odoo_data
from .schemas import INVOICE_FIELDS, INVOICE_LINE_FIELDS

def get_invoices_raw(client):
    """Extrae las cabeceras de las facturas (account.move)"""
    domain = [
        ("move_type", "in", ["out_invoice", "out_refund"]),
        ("invoice_date", ">=", "2025-01-01"),
        ("state", "=", "posted")
    ]

    return fetch_odoo_data(
        client=client,
        model_name="account.move",
        fields=INVOICE_FIELDS,
        domain=domain
    )

def get_invoice_lines_raw(client):
    """Extrae el detalle/líneas de las facturas (account.move.line)"""
    fecha_rescate = "2025-01-01"

    domain = [
        ("move_id.move_type", "in", ["out_invoice", "out_refund"]),
        ("move_id.state", "=", "posted"),
        ("date", ">=", fecha_rescate)
    ]

    return fetch_odoo_data(
        client=client,
        model_name="account.move.line",
        fields=INVOICE_LINE_FIELDS,
        domain=domain
    )