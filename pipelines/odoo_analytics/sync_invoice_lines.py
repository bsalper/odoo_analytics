import pandas as pd
from datetime import date
from dateutil.relativedelta import relativedelta
from google.cloud import bigquery
from connectors.odoo import get_odoo_client
from extractors.odoo.invoices import get_invoice_lines_raw
from transform.invoice_lines import transform_invoice_lines
from loaders.bigquery_loader import load_dataframe
from utils.logger import get_logger

PROJECT_ID = "odoo-analytics-482120"
DATASET = "odoo_analytics"

TABLE_CABECERA_HIST = f"{PROJECT_ID}.{DATASET}.facturas_cabecera_historico"

TABLE_HIST = f"{PROJECT_ID}.{DATASET}.facturas_detalle_historico"
TABLE_ACTUAL = f"{PROJECT_ID}.{DATASET}.facturas_detalle_mes_actual"

logger = get_logger("sync_detalle")

def check_if_month_exists(first_day_month):
    client = bigquery.Client()
    query = f"""
        SELECT COUNT(1) FROM `{TABLE_CABECERA_HIST}` 
        WHERE DATE(fecha_factura) = '{first_day_month}' 
        LIMIT 1
    """
    query_job = client.query(query)
    result = next(query_job.result())
    return result[0] > 0

def run():
    logger.info("Iniciando Sincronización de DETALLES...")
    odoo_client = get_odoo_client()
    today = date.today()
    
    # --- 1. CIERRE DE MES (Día 1) ---
    if today.day == 2:
        f_inicio = (today - relativedelta(months=1)).replace(day=1).strftime('%Y-%m-%d')
        f_fin = (today - relativedelta(days=1)).strftime('%Y-%m-%d')

        if check_if_month_exists(f_inicio):
            logger.warning(f"Mes {f_inicio} ya existe en histórico de detalles.")
        else:
            raw_cerrado = get_invoice_lines_raw(odoo_client, fecha_inicio=f_inicio, fecha_fin=f_fin)
            df_cerrado = transform_invoice_lines(raw_cerrado)

            if not df_cerrado.empty:
                load_dataframe(df_cerrado, TABLE_HIST, write_disposition="WRITE_APPEND")
                logger.info(f"Movidas {len(df_cerrado)} líneas al histórico.")

    # --- 2. MES ACTUAL (Diario) ---
    f_inicio_actual = today.replace(day=1).strftime('%Y-%m-%d')
    raw_actual = get_invoice_lines_raw(odoo_client, fecha_inicio=f_inicio_actual)
    df_actual = transform_invoice_lines(raw_actual)

    if not df_actual.empty:
        load_dataframe(df_actual, TABLE_ACTUAL, write_disposition="WRITE_TRUNCATE")
        logger.info(f"Actualizadas {len(df_actual)} líneas en Mes Actual.")

if __name__ == "__main__":
    run()