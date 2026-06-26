import os
import pandas as pd
from dotenv import load_dotenv
from connectors.odoo import get_odoo_client

load_dotenv(override=True)

def final_stock_check():
    print("--- INICIANDO PRUEBA DEFINITIVA ---")
    try:
        odoo = get_odoo_client()
        
        print("Consultando productos activos...")
        productos = odoo.search_read(
            'product.product',
            [('active', '=', True)], 
            ['display_name', 'qty_available', 'type'],
            limit=20
        )
        
        if not productos:
            print("Odoo no devolvió ningún producto.")
            return

        print(f"Se obtuvieron {len(productos)} productos.")
        
        # 2. Mostrar resultados
        df = pd.DataFrame(productos)
        print("\n--- LISTA DE PRODUCTOS Y STOCK ---")
        print(df[['display_name', 'qty_available', 'type']])
        
    except Exception as e:
        print(f"ERRORinesperado: {e}")

final_stock_check()