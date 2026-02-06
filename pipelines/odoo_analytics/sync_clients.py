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

    # 1. Extraer de RAW
    query = f"SELECT * FROM `{PROJECT_ID}.odoo_raw.clients_raw`"
    df_raw = client_bq.query(query).to_dataframe()
    
    if df_raw.empty:
        logger.warning("No hay datos en RAW para procesar.")
        return

    # 2. Obtener IDs de vendedores para el transformador
    valid_vendedores = df_raw["user_id"].unique().tolist()

    # 3. Transformar datos
    data_dict = df_raw.to_dict(orient="records")
    df_analytics = transform_clients(data_dict, valid_vendedores)

    # 4. Cargar a BigQuery (Nombre solicitado: clientes)
    load_dataframe(
        df=df_analytics,
        table_id=f"{PROJECT_ID}.{DATASET_ANALYTICS}.clientes",
        write_disposition="WRITE_TRUNCATE",
    )

    logger.info(f"Pipeline ANALYTICS finalizado. Tabla 'clientes' actualizada.")

if __name__ == "__main__":
    run()