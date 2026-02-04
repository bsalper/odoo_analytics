from connectors.odoo import get_odoo_client
from extractors.odoo.invoices import get_invoice_lines_raw
from transform.invoice_lines import transform_invoice_lines
from loaders.bigquery_loader import load_dataframe
from utils.logger import get_logger
from google.cloud import bigquery
from datetime import date

PROJECT_ID = "odoo-analytics-482120"
DATASET_RAW = "odoo_raw"
DATASET_ANALYTICS = "odoo_analytics"

logger = get_logger("sync_invoice_detalle_mes_actual")


def run():
    logger.info("Iniciando pipeline: factura_detalle_mes_actual")

    # 1. Conexiones
    odoo_client = get_odoo_client()
    client_bq = bigquery.Client()

    # 2. Mes actual
    today = date.today()
    month_start = today.replace(day=1).isoformat()

    # 3. Leer facturas válidas (posted + mes actual)
    query = f"""
        SELECT id
        FROM `{PROJECT_ID}.{DATASET_RAW}.invoices_raw`
        WHERE
            state = 'posted'
            AND SAFE_CAST(invoice_date AS DATE) >= DATE('{month_start}')
    """
    facturas_df = client_bq.query(query).to_dataframe()
    valid_invoice_ids = facturas_df["id"].astype(str).tolist()

    logger.info(f"Facturas válidas mes actual: {len(valid_invoice_ids)}")

    # 4. Extracción RAW desde Odoo
    invoice_lines_raw = get_invoice_lines_raw(odoo_client)

    # 5. Transformación
    df_lines = transform_invoice_lines(
        invoice_lines_raw=invoice_lines_raw,
        valid_invoice_ids=valid_invoice_ids
    )

    # 6. Carga a BigQuery
    load_dataframe(
        df=df_lines,
        table_id=f"{PROJECT_ID}.{DATASET_ANALYTICS}.facturas_detalle_mes_actual",
        write_disposition="WRITE_TRUNCATE"
    )

    logger.info("Pipeline factura_detalle_mes_actual finalizado OK")


if __name__ == "__main__":
    run()