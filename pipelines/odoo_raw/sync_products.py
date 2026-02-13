from connectors.odoo import get_odoo_client
from extractors.odoo.products import get_products_raw
from loaders.bigquery_loader import load_dataframe
from utils.logger import get_logger
import pandas as pd

PROJECT_ID = "odoo-analytics-482120"
DATASET_RAW = "odoo_raw"

logger = get_logger("sync_products_raw")

def run():
    logger.info("Iniciando sincronización de PRODUCTOS hacia ODOO_RAW")

    # 1. Conexión y Extracción
    odoo_client = get_odoo_client()
    products_data = get_products_raw(odoo_client)

    if not products_data:
        logger.warning("No se obtuvieron datos de productos de Odoo.")
        return

    # 2. Convertir a DataFrame
    df = pd.DataFrame(products_data)

    # 3. Limpieza básica para RAW (evitar errores de tipos anidados en BQ)
    # Convertimos todo a string para que el RAW acepte cualquier cambio en Odoo sin romperse
    for col in df.columns:
        df[col] = df[col].astype(str)

    # 4. Carga a BigQuery
    load_dataframe(
        df=df,
        table_id=f"{PROJECT_ID}.{DATASET_RAW}.products_raw",
        write_disposition="WRITE_TRUNCATE"
    )

    logger.info(f"Sincronización finalizada. {len(df)} productos cargados en {DATASET_RAW}.products_raw")

if __name__ == "__main__":
    run()