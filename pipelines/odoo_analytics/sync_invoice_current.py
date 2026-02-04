from connectors.odoo import get_odoo_client
from extractors.odoo.invoices import get_invoices_raw
from transform.invoices import transform_invoices
from loaders.bigquery_loader import load_dataframe
from utils.logger import get_logger
from google.cloud import bigquery
from datetime import date

PROJECT_ID = "odoo-analytics-482120"
DATASET_ANALYTICS = "odoo_analytics"

logger = get_logger("sync_invoice_cabecera_mes_actual")

def run():
    logger.info("Iniciando pipeline: factura_cabecera_mes_actual")

    odoo_client = get_odoo_client()

    # mes actual
    today = date.today()
    month_start = today.replace(day=1).isoformat()

    # 1. Extract
    invoices_raw = get_invoices_raw(odoo_client)

    # 2. Transform
    df = transform_invoices(invoices_raw)

    # 3. Filtro MES ACTUAL + POSTED
    df = df[
        (df["estado"] == "posted") &
        (df["fecha_factura"] >= month_start)
    ]

    logger.info(f"Facturas cabecera mes actual: {len(df)}")

    # 4. Load
    load_dataframe(
        df=df,
        table_id=f"{PROJECT_ID}.{DATASET_ANALYTICS}.facturas_cabecera_mes_actual",
        write_disposition="WRITE_TRUNCATE",
    )

    logger.info("Pipeline factura_cabecera_mes_actual finalizado OK")

if __name__ == "__main__":
    run()