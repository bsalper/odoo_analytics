# loaders/google_sheets_loader.py
import pandas as pd
import numpy as np
from datetime import date, datetime
from connectors.google_sheets import get_gs_service
from utils.logger import get_logger

logger = get_logger("google_sheets_loader")

def upload_dataframe_to_sheet(df, spreadsheet_id, sheet_name="Sheet1", range_to_clear="A:Z"):
    """
    Carga un DataFrame a una pestaña específica de Google Sheets.
    Mantiene a salvo las columnas de la derecha usando el parámetro range_to_clear.
    """
    try:
        if df.empty:
            logger.warning(f"El DataFrame está vacío. Saltando carga para {sheet_name}")
            return

        service = get_gs_service()
        
        # 1. copia del dataframe para no afectar el original
        df_clean = df.copy()

        # 2. CONVERSIÓN DE FECHAS
        for col in df_clean.columns:
            if pd.api.types.is_datetime64_any_dtype(df_clean[col]) or \
               df_clean[col].apply(lambda x: isinstance(x, (date, datetime))).any():
                df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce').dt.strftime('%Y-%m-%d')

        # 3. limpieza de Nulos (NaN, None)
        df_clean = df_clean.replace({np.nan: '', None: ''})
        
        # 4. preparar datos para la api
        data = [df_clean.columns.tolist()] + df_clean.values.tolist()

        # DEFINICIÓN DE RANGOS INTELIGENTES
        # usamos el parámetro dinamico para limpiar solo lo operacional (ej: A:L)
        range_full = f"'{sheet_name}'!{range_to_clear}"
        range_start = f"'{sheet_name}'!A1"

        # Limpiar SOLO el cuadrante asignado para no tocar las columnas del jefe
        logger.info(f"Limpiando rango {range_full} en {sheet_name}...")
        service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id, 
            range=range_full
        ).execute()

        # Cargar con USER_ENTERED actuando de forma acotada
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=range_start,
            valueInputOption="USER_ENTERED",   
            body={"values": data}
        ).execute()

        logger.info(f"{len(df)} filas cargadas en la pestaña: {sheet_name} (Rango limpio: {range_to_clear})")
        
    except Exception as e:
        logger.error(f"Error en la carga a Sheets ({sheet_name}): {e}")
        raise