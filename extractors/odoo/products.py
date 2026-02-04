from .base import fetch_odoo_data
from .schemas import PRODUCT_FIELDS

def get_products_raw(client, limit=None):
    return fetch_odoo_data(
        client=client,
        model_name='product.template',
        fields=PRODUCT_FIELDS,
        limit=limit
    )