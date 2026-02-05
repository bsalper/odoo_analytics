import pandas as pd
from datetime import date
from connectors.odoo import get_odoo_client
from extractors.odoo.invoices import get_invoices_raw
from transform.invoices import transform_invoices
from loaders.bigquery_loader import load_dataframe
from utils.logger import get_logger
from google.cloud import bigquery

PROJECT_ID = "odoo-analytics-482120"
DATASET_ANALYTICS = "odoo_analytics"

logger = get_logger("sync_invoices_cabecera_unificado")

def run():
    logger.info("Iniciando pipeline unificado: Factura Cabecera")

    odoo_client = get_odoo_client()
    
    today = date.today()
    month_start = today.replace(day=1)

    # 1. Extracción y Transformación
    invoices_raw = get_invoices_raw(odoo_client)
    df = transform_invoices(invoices_raw)
    df = df.loc[:, ~df.columns.duplicated()]
    df["fecha_dt"] = pd.to_datetime(df["fecha_factura"], errors="coerce").dt.date
    df_posted = df[df["estado"] == "posted"].copy()

    # 2. Separar datos
    df_historico = df_posted[df_posted["fecha_dt"] < month_start].copy()
    df_current = df_posted[df_posted["fecha_dt"] >= month_start].copy()

    # Limpiar columnas temporales
    if "fecha_dt" in df_historico.columns: df_historico.drop(columns=["fecha_dt"], inplace=True)
    if "fecha_dt" in df_current.columns: df_current.drop(columns=["fecha_dt"], inplace=True)

    # 3. CARGA DE HISTÓRICO (Solo el día 1 del mes)
    # Esto evita que se duplique la data todos los días.
    if today.day == 1:
        logger.info("Hoy es día 1: Sincronizando tabla histórica...")
        if not df_historico.empty:
            load_dataframe(
                df=df_historico,
                table_id=f"{PROJECT_ID}.{DATASET_ANALYTICS}.facturas_cabecera_historico",
                write_disposition="WRITE_TRUNCATE" # El día 1 refrescamos todo el histórico
            )
    else:
        logger.info(f"Hoy es día {today.day}: Se omite carga histórica para evitar duplicados.")

    # 4. CARGA DE MES ACTUAL (Todos los días)
    if not df_current.empty:
        load_dataframe(
            df=df_current,
            table_id=f"{PROJECT_ID}.{DATASET_ANALYTICS}.facturas_cabecera_mes_actual",
            write_disposition="WRITE_TRUNCATE" # Siempre al día y limpio
        )
        logger.info(f"Cargadas {len(df_current)} facturas del mes actual.")

    logger.info("Pipeline finalizado OK 🚀")

if __name__ == "__main__":
    run()