from connectors.odoo import get_odoo_client
from extractors.odoo.clients import (get_clients_raw)
from loaders.bigquery_loader import load_dataframe
from utils.logger import get_logger
import pandas as pd

PROJECT_ID = "odoo-analytics-482120"
DATASET_RAW = "odoo_raw"

logger = get_logger("sync_clients_raw")

def cast_all_to_string(df: pd.DataFrame) -> pd.DataFrame:
    """
    RAW = todo como STRING.
    Evita errores de BigQuery (bool/int/list/null).
    """
    return df.astype(str)

def run ():
    logger.info("Inciciando pipeline RAW de clientes")

    odoo_client = get_odoo_client()

    clients_raw = get_clients_raw(odoo_client)

    df_clients = pd.DataFrame(clients_raw)

    df_clients = cast_all_to_string(df_clients)

    load_dataframe(
        df=df_clients,
        table_id=f"{PROJECT_ID}.{DATASET_RAW}.clients_raw",
        write_disposition="WRITE_TRUNCATE",
    )

    logger.info("Pipeline RAW de clientes finalizado OK")

if __name__ == "__main__":
    run()