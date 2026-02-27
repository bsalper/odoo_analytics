import pandas as pd
from datetime import date, timedelta
from connectors.odoo import get_odoo_client
from extractors.odoo.orders import get_orders_raw, get_order_lines_raw
from transform.lines import transform_pedido_detalle
from loaders.bigquery_loader import load_dataframe
from utils.logger import get_logger

PROJECT_ID = "odoo-analytics-482120"
DATASET_ANALYTICS = "odoo_analytics"

logger = get_logger("sync_pedido_detalle")


def run():
    logger.info("Iniciando pipeline: Pedido Detalle (Mes Actual + Anterior unificados)")

    odoo_client = get_odoo_client()

    # --- 1. Lógica de Fechas ---
    today = date.today()
    # Primer día del mes actual (Ej: 2026-02-01)
    month_actual_start = today.replace(day=1)
    # Primer día del mes anterior (Ej: 2026-01-01)
    month_anterior_start = (month_actual_start - timedelta(days=1)).replace(day=1)
    
    # Convertimos a string para el filtro de Odoo
    fecha_filtro_odoo = month_anterior_start.strftime("%Y-%m-%d")

    # --- 2. Obtener pedidos desde el inicio del mes ANTERIOR ---
    # Esto trae automáticamente ambos meses (Anterior + Actual)
    orders_raw = get_orders_raw(
        odoo_client,
        domain=[("date_order", ">=", fecha_filtro_odoo)]
    )

    if not orders_raw:
        logger.info(f"No hay pedidos desde el {fecha_filtro_odoo}.")
        return

    df_orders = pd.DataFrame(orders_raw)
    valid_order_ids = df_orders["id"].tolist()

    logger.info(f"Pedidos encontrados: {len(valid_order_ids)} (desde el mes anterior)")

    # --- 3. Obtener todas las líneas ---
    lines_raw = get_order_lines_raw(odoo_client)

    # --- 4. Transformar filtrando por el set unificado de IDs ---
    df = transform_pedido_detalle(
        lines_raw,
        valid_product_ids=None,
        valid_order_ids=valid_order_ids
    )

    if df.empty:
        logger.info("No hay líneas de pedido para el periodo seleccionado.")
        return

    # --- 5. Cargar a tabla única ---
    # Usamos WRITE_TRUNCATE para que la tabla siempre tenga solo estos dos meses frescos
    load_dataframe(
        df=df,
        table_id=f"{PROJECT_ID}.{DATASET_ANALYTICS}.pedidos_detalle",
        write_disposition="WRITE_TRUNCATE"
    )

    logger.info(f"Cargadas {len(df)} líneas (Mes Actual + Anterior) en pedidos_detalle.")
    logger.info("Pipeline Pedido Detalle finalizado OK")


if __name__ == "__main__":
    run()