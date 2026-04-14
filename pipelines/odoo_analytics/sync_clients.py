import os
from google.cloud import bigquery
from connectors.odoo import get_odoo_client
from extractors.odoo.clients import get_clients_raw
from transform.clients import transform_clients
from loaders.bigquery_loader import load_dataframe
from utils.logger import get_logger

PROJECT_ID = "odoo-analytics-482120"
DATASET_ANALYTICS = "odoo_analytics"
TABLE_CLIENTES = "clientes"

logger = get_logger("sync_clients_analytics")

# Helpers
def get_valid_vendedor_ids(client_bq):
    logger.info("Leyendo vendedores permitidos desde BigQuery...")
    query = f"""
        SELECT CAST(id_vendedor AS INTEGER) AS id
        FROM `{PROJECT_ID}.{DATASET_ANALYTICS}.vendedores`
    """
    df = client_bq.query(query).to_dataframe()
    return df["id"].tolist()

def run():
    logger.info("Iniciando pipeline ANALYTICS: Odoo -> BigQuery (Clientes)")
    try:
        odoo_client = get_odoo_client()
        client_bq = bigquery.Client(project=PROJECT_ID)

        valid_vendedor_ids = get_valid_vendedor_ids(client_bq)
        clients_raw = get_clients_raw(odoo_client)

        if not clients_raw:
            logger.warning("No se obtuvieron clientes desde Odoo.")
            return

        df_clientes = transform_clients(
            clients_raw,
            tag_map={},
            valid_vendedor_ids=valid_vendedor_ids
        )

        if df_clientes.empty:
            logger.warning("Sin registros tras transformación.")
            return

        table_id = f"{PROJECT_ID}.{DATASET_ANALYTICS}.{TABLE_CLIENTES}"

        load_dataframe(
            df=df_clientes,
            table_id=table_id,
            write_disposition="WRITE_TRUNCATE"
        )

        logger.info(f"Éxito: {len(df_clientes)} registros cargados.")

    except Exception as e:
        logger.exception(f"Error en pipeline sync_clients: {e}")
        raise

if __name__ == "__main__":
    run()