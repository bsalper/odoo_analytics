from connectors.odoo import get_odoo_client
from extractors.odoo.clients import get_clients_raw
from transform.clients import transform_clients
from loaders.bigquery_loader import load_dataframe
from utils.logger import get_logger
from google.cloud import bigquery
import pandas as pd

# Configuración
PROJECT_ID = "odoo-analytics-482120"
DATASET_ANALYTICS = "odoo_analytics"

logger = get_logger("sync_clients")

def run():
    logger.info("Iniciando pipeline ANALYTICS de clientes")

    client_bq = bigquery.Client()

    # 1. Obtener los IDs de vendedores válidos desde tu tabla de BigQuery
    query_vendedores = f"SELECT CAST(id_vendedor AS STRING) as id FROM `{PROJECT_ID}.{DATASET_ANALYTICS}.vendedores`"
    df_vendedores = client_bq.query(query_vendedores).to_dataframe()
    valid_ids = df_vendedores['id'].tolist()
    
    logger.info(f"Vendedores cargados desde BQ: {len(valid_ids)}")

    # 2. Extraer de RAW
    query = f"SELECT * FROM `{PROJECT_ID}.odoo_raw.clients_raw`"
    df_raw = client_bq.query(query).to_dataframe()
    
    if df_raw.empty:
        logger.warning("No hay datos en RAW para procesar.")
        return

    # 3. Transformar usando los IDs de tu tabla
    data_dict = df_raw.to_dict(orient="records")
    df_analytics = transform_clients(data_dict, valid_ids)

    # 4. Cargar
    load_dataframe(
        df=df_analytics,
        table_id=f"{PROJECT_ID}.{DATASET_ANALYTICS}.clientes",
        write_disposition="WRITE_TRUNCATE",
    )

    logger.info(f"Pipeline ANALYTICS finalizado. Tabla 'clientes' actualizada.")

if __name__ == "__main__":
    run()