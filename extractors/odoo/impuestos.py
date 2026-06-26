from .base import fetch_odoo_data
from .schemas import TAX_FIELDS

def get_taxes_raw(client, limit=100):
    return fetch_odoo_data(
        client=client,
        model_name='account.tax',
        fields=TAX_FIELDS,
        limit=limit
    )