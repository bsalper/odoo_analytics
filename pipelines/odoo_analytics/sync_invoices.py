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

    # --- 1. Extract + Transform ---
    invoices_raw = get_invoices_raw(odoo_client)
    df = transform_invoices(invoices_raw)

    if df.empty:
        logger.info("No hay facturas para procesar.")
        return

    # --- 2. Solo facturas contabilizadas ---
    df = df[df["estado"] == "posted"].copy()

    # --- 3. Separación temporal ---
    df["fecha_dt"] = pd.to_datetime(df["fecha_factura"], errors="coerce").dt.date

    df_historico = df[df["fecha_dt"] < month_start].copy()
    df_current = df[df["fecha_dt"] >= month_start].copy()

    df_historico.drop(columns=["fecha_dt"], inplace=True, errors="ignore")
    df_current.drop(columns=["fecha_dt"], inplace=True, errors="ignore")

    # --- 4. Carga histórico (solo día 1) ---
    if today.day == 1 and not df_historico.empty:
        logger.info("Cargando histórico...")
        load_dataframe(
            df=df_historico,
            table_id=f"{PROJECT_ID}.{DATASET_ANALYTICS}.facturas_cabecera_historico",
            write_disposition="WRITE_TRUNCATE"
        )
    else:
        logger.info("Histórico omitido.")

    # --- 5. Carga mes actual ---
    if not df_current.empty:
        load_dataframe(
            df=df_current,
            table_id=f"{PROJECT_ID}.{DATASET_ANALYTICS}.facturas_cabecera_mes_actual",
            write_disposition="WRITE_TRUNCATE"
        )
        logger.info(f"Cargadas {len(df_current)} facturas del mes actual.")

    logger.info("Pipeline finalizado OK")


if __name__ == "__main__":
    run()