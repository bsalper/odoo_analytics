import os
import pandas as pd
from datetime import datetime
from connectors.odoo import get_odoo_client
from extractors.odoo.invoices import get_invoices_raw
from transform.google_sheets.cabecera import transform_invoices_sheets
from loaders.google_sheets_loader import upload_dataframe_to_sheet 
from utils.logger import get_logger

# Si usas gspread dentro de tu proyecto, asegúrate de importar lo necesario o adaptar el método.
# Aquí te muestro cómo adaptarlo usando la lógica de rango estricto A:L.

logger = get_logger("sync_factura_cabecera")

def run_sync_factura_cabecera():
    SPREADSHEET_ID = "1XXVXqIXwU0_AdaZ0levHkcxiueo2nj3Q6SpC11FzXS8"
    SHEET_NAME = "Facturas Cabecera"

    try:
        # 1. Conexión
        client = get_odoo_client()

        # Se calcula la fecha del mes actual
        primer_dia_mes = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        fecha_filtro_str = primer_dia_mes.strftime('%Y-%m-%d')

        # 2. Extracción
        logger.info(f"Extrayendo facturas desde Odoo desde {fecha_filtro_str}...")
        raw_data = get_invoices_raw(client, fecha_inicio=fecha_filtro_str)

        # 3. Filtración / Conversión a DataFrame
        df_filtered = pd.DataFrame(raw_data)
        
        logger.info(f"Registros del mes actual devueltos por Odoo: {len(df_filtered)}")

        # 4. Transformación dedicada para Sheets
        if not df_filtered.empty:
            logger.info("Transformando datos filtrados para el formato de Google Sheets...")
            data_to_transform = df_filtered.to_dict('records')
            df_to_upload = transform_invoices_sheets(data_to_transform)
        else:
            df_to_upload = pd.DataFrame()

        # 5. Carga
        if not df_to_upload.empty:
            logger.info(f"Iniciando carga de {len(df_to_upload)} registros en Google Sheets...")
            
            # PASAMOS "A:L"
            upload_dataframe_to_sheet(df_to_upload, SPREADSHEET_ID, SHEET_NAME, range_to_clear="A:L")
            
            logger.info("Sincronización exitosa.")

    except Exception as e:
        logger.error(f"Falló la sincronización: {e}")

if __name__ == "__main__":
    run_sync_factura_cabecera()