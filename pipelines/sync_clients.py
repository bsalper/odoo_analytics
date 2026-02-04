from connectors.odoo import get_odoo_client
from extractors.odoo.clients import get_clients_raw
from transform.clients import transform_clients
from loaders.bigquery_loader import load_dataframe
from google.cloud import bigquery
from utils.logger import get_logger

logger = get_logger("sync_clients")

def run():
    logger.info("Iniciando pipeline: sync_clients")
    
    odoo_client = get_odoo_client()
    client_bq = bigquery.Client()

    # 1. Traer vendedores para que el filtro de clientes no los borre
    query = "SELECT id_vendedor FROM `odoo-analytics-482120.odoo_analytics.vendedores`"
    vendedores_df = client_bq.query(query).to_dataframe()
    valid_vendedor_ids = vendedores_df["id_vendedor"].astype(str).tolist()

    # 2. Extraer y Transformar
    clients_raw = get_clients_raw(odoo_client)
    df_clients = transform_clients(clients_raw, valid_vendedor_ids)

    # 3. Esquema
    schema_clients = [
        bigquery.SchemaField("id_cliente", "STRING"),
        bigquery.SchemaField("nombre_cliente", "STRING"),
        bigquery.SchemaField("rut", "STRING"),
        bigquery.SchemaField("id_vendedor", "STRING"),
        bigquery.SchemaField("fecha_creacion", "STRING"),
    ]

    # 4. Cargar
    if not df_clients.empty:
        load_dataframe(
            df=df_clients,
            table_id="odoo-analytics-482120.odoo_analytics.clientes",
            write_disposition="WRITE_TRUNCATE",
            schema=schema_clients
        )
        logger.info(f"Cargados {len(df_clients)} clientes.")
    else:
        logger.warning("El DataFrame de clientes está vacío.")

if __name__ == "__main__":
    run()