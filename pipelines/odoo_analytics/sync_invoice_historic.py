import pandas as pd
from datetime import date
from connectors.odoo import get_odoo_client
from extractors.odoo.invoices import get_invoices_raw
from transform.invoices import transform_invoices
from loaders.bigquery_loader import load_dataframe
from utils.logger import get_logger
from google.cloud import bigquery

logger = get_logger("sync_invoice_cabecera_historico")

def run():
    logger.info("Iniciando pipeline: factura_cabecera_historico")

    odoo_client = get_odoo_client()
    client_bq = bigquery.Client()

    month_start = date.today().replace(day=1)

    invoices_raw = get_invoices_raw(odoo_client)
    df = transform_invoices(invoices_raw)

    # evitar columnas duplicadas
    df = df.loc[:, ~df.columns.duplicated()]

    # conversión SOLO para filtrar
    df["fecha_factura_dt"] = pd.to_datetime(
        df["fecha_factura"], errors="coerce"
    ).dt.date

    df_historico = df[
        (df["estado"] == "posted") &
        (df["fecha_factura_dt"] < month_start)
    ].drop(columns=["fecha_factura_dt"])

    logger.info(f"Facturas históricas a cargar: {len(df_historico)}")

    load_dataframe(
        df=df_historico,
        table_id="odoo-analytics-482120.odoo_analytics.facturas_cabecera_historico",
        write_disposition="WRITE_APPEND"
    )

    logger.info("Pipeline factura_cabecera_historico finalizado OK 🚀")

if __name__ == "__main__":
    run()