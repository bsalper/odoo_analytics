import os
import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from utils.logger import get_logger

logger = get_logger("google_sheets_loader")

def get_gs_service():
    # Ruta absoluta al archivo que ya confirmamos que existe
    cert_path = r"C:\Users\MF\Documents\odoo_analytics\credentials_google\odoo-analytics-482120-4a4cd8457bc7.json"
    
    # Scopes tal cual el código guía
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    
    # Autenticación explícita
    creds = Credentials.from_service_account_file(cert_path, scopes=scopes)
    service = build('sheets', 'v4', credentials=creds)
    return service

def upload_dataframe_to_sheet(df, spreadsheet_id, sheet_name="Sheet1"):
    try:
        service = get_gs_service()
        
        # Limpieza de datos: Convertir NaNs a strings vacíos para evitar errores
        df_clean = df.fillna("")
        
        # Convertir a lista de listas (incluyendo encabezados)
        data = [df_clean.columns.tolist()] + df_clean.values.tolist()

        # Limpiar el rango antes de escribir (B hasta E como tu guía o A:Z completo)
        service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id, 
            range=f"{sheet_name}!A:Z"
        ).execute()

        # Escribir los datos
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A1",
            valueInputOption="RAW",
            body={"values": data}
        ).execute()

        logger.info(f"Datos cargados exitosamente en Google Sheets ID: {spreadsheet_id}")

    except Exception as e:
        logger.error(f"Error en la carga a Sheets: {e}")
        raise