# loaders/google_sheets_loader.py
import pandas as pd
import numpy as np
from datetime import date, datetime
from connectors.google_sheets import get_gs_service
from utils.logger import get_logger

logger = get_logger("google_sheets_loader")

def upload_dataframe_to_sheet(df, spreadsheet_id, sheet_name="Sheet1"):
    try:
        if df.empty:
            logger.warning(f"El DataFrame está vacío. Saltando carga para {sheet_name}")
            return

        service = get_gs_service()
        
        # 1. Copia del dataframe para no afectar el original
        df_clean = df.copy()

        # 2. CONVERSIÓN DE FECHAS
        for col in df_clean.columns:
            if pd.api.types.is_datetime64_any_dtype(df_clean[col]) or \
               df_clean[col].apply(lambda x: isinstance(x, (date, datetime))).any():
                df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce').dt.strftime('%Y-%m-%d')

        # 3. Limpieza de Nulos (NaN, None)
        df_clean = df_clean.replace({np.nan: '', None: ''})
        
        # 4. Preparar datos para la API
        # Al sumarlos con +, se crea una gran lista donde la primera fila son los títulos y las siguientes son los datos.
        data = [df_clean.columns.tolist()] + df_clean.values.tolist()

        # Usamos comillas simples en el nombre de la hoja por si tiene espacios
        range_full = f"'{sheet_name}'!A:Z"
        range_start = f"'{sheet_name}'!A1"

        # Limpiar y Cargar con USER_ENTERED
        service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id, 
            range=range_full
        ).execute()

        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=range_start,
            valueInputOption="USER_ENTERED",   # este es el interprete, actua como si una persona estuviera escribiendo
            body={"values": data}
        ).execute()

        logger.info(f"{len(df)} filas cargadas en la pestaña: {sheet_name}")
        
    except Exception as e:
        logger.error(f"Error en la carga a Sheets ({sheet_name}): {e}")
        raise