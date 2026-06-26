from .base import fetch_odoo_data
from .schemas import PRODUCT_FIELDS

def get_products_raw(client, limit=None):
    # Forzamos a Odoo a incluir activos y archivados
    domain = ['|', ('active', '=', True), ('active', '=', False)]
    
    return fetch_odoo_data(
        client=client,
        model_name='product.product',
        fields=PRODUCT_FIELDS,
        limit=limit,
        domain=domain
    )