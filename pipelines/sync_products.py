import logging
import os
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from transform.products import transform_products
from utils.bigquery import load_df_to_bq

logger = logging.getLogger("sync_products")

PROJECT_ID = "odoo-analytics-482120"
DATASET_ANALYTICS = "odoo_analytics"

def run():
    logger.info("Iniciando sync de productos y tabla intermedia")

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    cert_path = os.path.join(base_dir, 'credentials_google', 'odoo-analytics-482120-4a4cd8457bc7.json')
    credentials = service_account.Credentials.from_service_account_file(cert_path)
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

    # 1. Obtener RAW
    raw_df = client.query(f"SELECT * FROM `{PROJECT_ID}.odoo_raw.products_raw`").to_dataframe()

    # 2. Obtener impuestos válidos
    valid_impuestos_ids = client.query(
        f"SELECT id_impuesto FROM `{PROJECT_ID}.{DATASET_ANALYTICS}.impuestos`"
    ).to_dataframe()["id_impuesto"].astype(str).tolist()

    # 3. Transformación (devuelve 2 DataFrames)
    clean_products_df, relacion_df = transform_products(raw_df, valid_impuestos_ids)

    # 4. CARGA TABLA A: Productos
    load_df_to_bq(
        df=clean_products_df,
        project_id=PROJECT_ID,
        dataset=DATASET_ANALYTICS,
        table="products",
        write_disposition="WRITE_TRUNCATE",
        client=client
    )

    # 5. CARGA TABLA B: Producto_Impuesto (Intermedia)
    if not relacion_df.empty:
        load_df_to_bq(
            df=relacion_df,
            project_id=PROJECT_ID,
            dataset=DATASET_ANALYTICS,
            table="producto_impuesto",
            write_disposition="WRITE_TRUNCATE",
            client=client
        )
        logger.info(f"Relaciones cargadas: {len(relacion_df)} filas en producto_impuesto")

    logger.info("Sincronización completa finalizada.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()