import pandas as pd
from datetime import date
from connectors.odoo import get_odoo_client
from extractors.odoo.orders import get_orders_raw, get_order_lines_raw
from transform.lines import transform_pedido_detalle
from loaders.bigquery_loader import load_dataframe
from utils.logger import get_logger

PROJECT_ID = "odoo-analytics-482120"
DATASET_ANALYTICS = "odoo_analytics"

logger = get_logger("sync_pedido_detalle")


def run():
    logger.info("Iniciando pipeline: Pedido Detalle")

    odoo_client = get_odoo_client()

    today = date.today()
    month_start = today.replace(day=1).strftime("%Y-%m-%d")

    # --- 1. Obtener pedidos del mes actual ---
    orders_raw = get_orders_raw(
        odoo_client,
        domain=[("date_order", ">=", month_start)]
    )

    if not orders_raw:
        logger.info("No hay pedidos del mes actual.")
        return

    df_orders = pd.DataFrame(orders_raw)

    valid_order_ids = df_orders["id"].tolist()

    # --- 2. Obtener todas las líneas ---
    lines_raw = get_order_lines_raw(odoo_client)

    # --- 3. Transformar y filtrar por pedidos válidos ---
    df = transform_pedido_detalle(
        lines_raw,
        valid_product_ids=None,
        valid_order_ids=valid_order_ids
    )

    if df.empty:
        logger.info("No hay líneas de pedido para el mes actual.")
        return

    # --- 4. Cargar ---
    load_dataframe(
        df=df,
        table_id=f"{PROJECT_ID}.{DATASET_ANALYTICS}.pedidos_detalle_mes_actual",
        write_disposition="WRITE_TRUNCATE"
    )

    logger.info(f"Cargadas {len(df)} líneas del mes actual.")
    logger.info("Pipeline Pedido Detalle finalizado OK")


if __name__ == "__main__":
    run()