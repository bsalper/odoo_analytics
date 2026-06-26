from .base import fetch_odoo_data
from .schemas import CLIENT_FIELDS

def get_clients_raw(client, limit=None):
    return fetch_odoo_data(
        client=client,
        model_name='res.partner',
        fields=CLIENT_FIELDS,
        limit=limit
    )