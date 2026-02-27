from extractors.odoo.products import get_products_raw
from transform.products import transform_products
from loaders.google_sheets_loader import upload_dataframe_to_sheet
from connectors.odoo import get_odoo_client
from utils.logger import get_logger

logger = get_logger("sync_products_sheets")

SPREADSHEET_ID = "1HaFlJZOFLQHqNJMPAHitsuUBcxbOXxVsvB_yzvz08Ok" 
SHEET_NAME = "Inventario Actual"

def run():
    logger.info("Iniciando Sync: Odoo -> Google Sheets")
    
    try:
        # Aquí es donde suele saltar el error si el .env está mal
        odoo_client = get_odoo_client()
        
        # 1. Extraer
        products_raw = get_products_raw(odoo_client)
        
        # 2. Transformar
        df_productos, _ = transform_products(products_raw, [])
        
        # 3. Cargar a Sheets (Adaptado al nuevo loader)
        upload_dataframe_to_sheet(
            df=df_productos,
            spreadsheet_id=SPREADSHEET_ID,  # Cambiamos name por ID
            sheet_name=SHEET_NAME
        )
        
        logger.info("Sincronización finalizada con éxito.")
        
    except Exception as e:
        logger.error(f"Falla en el pipeline de Sheets: {e}")

if __name__ == "__main__":
    run()