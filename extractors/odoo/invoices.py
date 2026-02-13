from datetime import datetime
from dateutil.relativedelta import relativedelta
from .base import fetch_odoo_data
from .schemas import INVOICE_FIELDS, INVOICE_LINE_FIELDS


def get_invoices_raw(client):
    # Esto traerá todo desde el 2025 hasta el infinito (incluyendo 2026)
    domain = [
        ("move_type", "in", ["out_invoice", "out_refund"]),
        ("invoice_date", ">=", "2024-01-01"),
        ("state", "=", "posted")
    ]

    return fetch_odoo_data(
        client=client,
        model_name="account.move",
        fields=INVOICE_FIELDS,
        domain=domain
    )

def get_invoice_lines_raw(client):
    # Para las líneas, asegúrate de que también incluya 2026
    # Si usas 'two_months_ago', hoy (Feb 2026) traerá desde Dic 2025. Está OK.
    fecha_rescate = "2024-01-01"

    domain = [
        ("move_id.move_type", "in", ["out_invoice", "out_refund"]),
        ("move_id.invoice_date", ">=", fecha_rescate),
        ("move_id.state", "=", "posted")
    ]

    return fetch_odoo_data(
        client=client,
        model_name="account.move.line",
        fields=INVOICE_LINE_FIELDS,
        domain=domain
    )