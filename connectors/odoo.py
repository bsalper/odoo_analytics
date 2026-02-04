# Aqui se hace la conexion a Odoo
import xmlrpc.client
import os

class OdooClient:
    def __init__(self):
        self.url = os.getenv("ODOO_URL")
        self.db = os.getenv("ODOO_DB")
        self.user = os.getenv("ODOO_USER")
        self.api_key = os.getenv("ODOO_API_KEY")

        common = xmlrpc.client.ServerProxy(
            f"{self.url}/xmlrpc/2/common", 
            allow_none=True
        )
        self.uid = common.authenticate(self.db, self.user, self.api_key, {})

        self.models = xmlrpc.client.ServerProxy(
            f"{self.url}/xmlrpc/2/object", 
            allow_none=True
        )

    def search_read(self, model, domain, fields, limit=None, offset=0):
        return self.models.execute_kw(
            self.db,
            self.uid,
            self.api_key,
            model,
            'search_read',
            [domain],
            {
                'fields': fields,
                'limit': limit,
                'offset': offset
            }
        )

# ESTA ES LA FUNCIÓN QUE BUSCA TU PIPELINE
def get_odoo_client():
    return OdooClient()