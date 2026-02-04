from connectors.odoo import get_odoo_client
from extractors.odoo.invoices import get_invoice_lines_raw
from transform.invoice_lines import transform_invoice_lines
from loaders.bigquery_loader import load_dataframe
from utils.logger import get_logger
from google.cloud import bigquery
from datetime import date

PROJECT_ID = "odoo-analytics-482120"
DATASET_ANALYTICS = "odoo_analytics"

logger = get_logger("sync_invoice_detalle_unificado")

def run():
    logger.info("Iniciando pipeline: factura_detalle_unificado")

    # 1. Conexiones
    odoo_client = get_odoo_client()
    client_bq = bigquery.Client()

    # 2. Fecha de corte (primer día del mes actual)
    today = date.today()
    month_start = today.replace(day=1)

    # 3. Leer facturas válidas desde BigQuery para separar histórico y mes actual
    query = f"""
        SELECT id, SAFE_CAST(invoice_date AS DATE) AS fecha_factura
        FROM `{PROJECT_ID}.{DATASET_ANALYTICS}.invoices_raw`
        WHERE state = 'posted'
    """
    facturas_df = client_bq.query(query).to_dataframe()
    facturas_df["id"] = facturas_df["id"].astype(str)

    # Separar IDs por periodo
    historical_ids = facturas_df[facturas_df["fecha_factura"] < month_start]["id"].tolist()
    current_ids = facturas_df[facturas_df["fecha_factura"] >= month_start]["id"].tolist()

    logger.info(f"Facturas históricas: {len(historical_ids)}, mes actual: {len(current_ids)}")

    # 4. Extracción RAW desde Odoo
    invoice_lines_raw = get_invoice_lines_raw(odoo_client)

    # 5. Transformación
    df_lines = transform_invoice_lines(invoice_lines_raw=invoice_lines_raw)

    # 6. Separar data por periodo
    df_historical = df_lines[df_lines["id_factura"].isin(historical_ids)]
    df_current = df_lines[df_lines["id_factura"].isin(current_ids)]

    # 7. Cargar a BigQuery
    if not df_historical.empty:
        load_dataframe(
            df=df_historical,
            table_id=f"{PROJECT_ID}.{DATASET_ANALYTICS}.facturas_detalle_historico",
            write_disposition="WRITE_APPEND"
        )
        logger.info(f"Cargadas {len(df_historical)} facturas detalle históricas")

    if not df_current.empty:
        load_dataframe(
            df=df_current,
            table_id=f"{PROJECT_ID}.{DATASET_ANALYTICS}.facturas_detalle_mes_actual",
            write_disposition="WRITE_APPEND"
        )
        logger.info(f"Cargadas {len(df_current)} facturas detalle mes actual")

    logger.info("Pipeline factura_detalle_unificado finalizado OK 🚀")


if __name__ == "__main__":
    run()