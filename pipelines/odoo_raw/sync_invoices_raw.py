from connectors.odoo import get_odoo_client
from extractors.odoo.invoices import (
    get_invoices_raw,
    get_invoice_lines_raw,
)
from loaders.bigquery_loader import load_dataframe
from utils.logger import get_logger
import pandas as pd

PROJECT_ID = "odoo-analytics-482120"
DATASET_RAW = "odoo_raw"

logger = get_logger("sync_invoices_raw")


def normalize_many2one(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte campos many2one de Odoo:
    [id, "Nombre"]  ->  id
    """
    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: x[0] if isinstance(x, list) and len(x) > 0 else x
        )
    return df


def cast_all_to_string(df: pd.DataFrame) -> pd.DataFrame:
    """
    RAW = todo como STRING.
    Evita errores de BigQuery (bool/int/list/null).
    """
    return df.astype(str)


def run():
    logger.info("Iniciando pipeline RAW de facturas")

    # 1. Conexión a Odoo
    odoo_client = get_odoo_client()

    # 2. Extracción desde Odoo
    invoices_raw = get_invoices_raw(odoo_client)
    invoice_lines_raw = get_invoice_lines_raw(odoo_client)

    # 3. DataFrames
    df_invoices = pd.DataFrame(invoices_raw)
    df_lines = pd.DataFrame(invoice_lines_raw)

    # 4. Normalización Odoo + casteo seguro
    df_invoices = cast_all_to_string(normalize_many2one(df_invoices))
    df_lines = cast_all_to_string(normalize_many2one(df_lines))

    # 5. Carga a BigQuery (RAW)
    load_dataframe(
        df=df_invoices,
        table_id=f"{PROJECT_ID}.{DATASET_RAW}.invoices_raw",
        write_disposition="WRITE_TRUNCATE",
    )

    load_dataframe(
        df=df_lines,
        table_id=f"{PROJECT_ID}.{DATASET_RAW}.invoice_lines_raw",
        write_disposition="WRITE_TRUNCATE",
    )

    logger.info("Pipeline RAW de facturas finalizado OK")


if __name__ == "__main__":
    run()