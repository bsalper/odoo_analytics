import os
import pandas as pd
from datetime import datetime
from connectors.odoo import get_odoo_client
from extractors.odoo.invoices import get_invoice_lines_raw
from transform.invoice_lines import transform_invoice_lines
from loaders.google_sheets_loader import upload_dataframe_to_sheet
from utils.logger import get_logger

logger = get_logger("sync_factura_detalle")

def run_sync_factura_detalle():
    SPREADSHEET_ID = "1pp3sS_70SX_-fir0xpWTz4E-mxAYp-9s3viaTkH_0b0"
    SHEET_NAME = "Base Facturación Det"

    try:
        # 1. Conexión
        client = get_odoo_client()
        
        # 2. Extracción
        logger.info("Extrayendo líneas de factura desde Odoo...")
        raw_data = get_invoice_lines_raw(client)

        # 3. Filtración
        df_raw = pd.DataFrame(raw_data)
        df_raw['date'] = pd.to_datetime(df_raw['date'])
        
        primer_dia_mes = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        df_filtered = df_raw[df_raw['date'] >= primer_dia_mes]

        logger.info(f"Filas originales: {len(df_raw)} | Filas mes actual: {len(df_filtered)}")

        # 4. Transformación
        logger.info("Transformando datos filtrados...")
        # .to_dict('records') convierte la tabla filtrada de nuevo al formato que entiende el transformador
        data_to_transform = df_filtered.to_dict('records')
        df_transformed = transform_invoice_lines(data_to_transform)

        # 5. CARGA
        if not df_transformed.empty:
            logger.info(f"Iniciando carga de {len(df_transformed)} registros en Google Sheets...")
            upload_dataframe_to_sheet(df_transformed, SPREADSHEET_ID, SHEET_NAME)
            logger.info("Sincronización exitosa.")
        else:
            logger.warning("No hay datos para el mes actual, la carga se saltó.")

    except Exception as e:
        logger.error(f"Falló la sincronización: {e}")

if __name__ == "__main__":
    run_sync_factura_detalle()