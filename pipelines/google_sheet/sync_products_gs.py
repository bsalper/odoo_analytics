import os
import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from extractors.odoo.products import get_products_raw
from transform.products import transform_products
from connectors.odoo import get_odoo_client
from utils.logger import get_logger

logger = get_logger("sync_products_sheets")

# --- CONFIGURACIÓN ---
SPREADSHEET_ID = "1HaFlJZOFLQHqNJMPAHitsuUBcxbOXxVsvB_yzvz08Ok" 
SHEET_NAME = "Inventario Actual"

# --- LOADER LOCAL (PARA EVITAR ERRORES DE RUTA) ---
def get_gs_service_local():
    base_dir = os.getcwd() 
    # Apuntamos a la ruta que SI existe (la del doble .json)
    cert_path = os.path.join(base_dir, "secrets", "gcp", "odoo_bigquery_sa.json.json")

    if not os.path.exists(cert_path):
        raise FileNotFoundError(f"No existe el archivo en: {cert_path}")

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    
    # IMPORTANTE: Usamos 'from_service_account_file' explícitamente
    # Esto ignora cualquier basura que haya en el .env
    creds = Credentials.from_service_account_file(cert_path, scopes=scopes)
    return build("sheets", "v4", credentials=creds)

def upload_to_sheets_local(df, spreadsheet_id, sheet_name):
    service = get_gs_service_local()
    
    # Limpieza básica para evitar errores de JSON
    df_clean = df.fillna("")
    data = [df_clean.columns.tolist()] + df_clean.values.tolist()

    # 1. Limpiar la hoja actual (A hasta la Z)
    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id, 
        range=f"'{sheet_name}'!A:Z"
    ).execute()

    # 2. Insertar nuevos datos
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"'{sheet_name}'!A1",
        valueInputOption="RAW",
        body={"values": data}
    ).execute()
    
    logger.info(f"Datos cargados exitosamente en Google Sheets ID: {spreadsheet_id}")

# --- PIPELINE PRINCIPAL ---
def run():
    logger.info("Iniciando Sync: Odoo -> Google Sheets (Versión Local Reforzada)")
    
    try:
        odoo_client = get_odoo_client()
        
        # 1. Extraer
        products_raw = get_products_raw(odoo_client)
        
        # 2. Transformar
        df_productos, _ = transform_products(products_raw, [])
        
        if df_productos.empty:
            logger.warning("No se obtuvieron productos para cargar.")
            return

        # 3. Cargar usando nuestra función LOCAL
        upload_to_sheets_local(
            df=df_productos,
            spreadsheet_id=SPREADSHEET_ID,
            sheet_name=SHEET_NAME
        )
        
        logger.info("Sincronización finalizada con éxito.")
        
    except Exception as e:
        logger.error(f"Falla en el pipeline de Sheets: {e}")

if __name__ == "__main__":
    run()