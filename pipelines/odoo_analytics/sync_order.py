import pandas as pd
from datetime import date
from connectors.odoo import get_odoo_client
from extractors.odoo.orders import get_orders_raw
from transform.orders import transform_orders
from loaders.bigquery_loader import load_dataframe
from utils.logger import get_logger

PROJECT_ID = "odoo-analytics-482120"
DATASET_ANALYTICS = "odoo_analytics"

logger = get_logger("sync_orders_cabecera")

def run():
    logger.info("Iniciando pipeline: Pedido Cabecera (Solo Mes Actual)")

    # 1. Conexión y Referencia Temporal
    odoo_client = get_odoo_client()
    today = date.today()
    month_start = today.replace(day=1)

    # 2. Extracción y Transformación
    orders_raw = get_orders_raw(odoo_client)
    df = transform_orders(orders_raw)
    
    if df.empty:
        logger.warning("No se obtuvieron datos de Odoo. Finalizando.")
        return

    # 3. Filtrado: Solo Mes Actual
    # Convertimos a datetime para comparar contra el inicio del mes
    df["fecha_tmp"] = pd.to_datetime(df["fecha_pedido"], errors="coerce").dt.date
    
    # Filtramos para quedarnos SOLO con lo que sea >= al primer día del mes
    df_current = df[df["fecha_tmp"] >= month_start].copy()
    
    # Quitamos la columna temporal antes de cargar
    df_current.drop(columns=["fecha_tmp"], inplace=True)

    # 4. Carga a BigQuery
    if not df_current.empty:
        logger.info(f"Cargando {len(df_current)} pedidos del mes actual a BigQuery.")
        load_dataframe(
            df=df_current,
            table_id=f"{PROJECT_ID}.{DATASET_ANALYTICS}.pedidos_cabecera_mes_actual",
            write_disposition="WRITE_TRUNCATE"
        )
        logger.info("Carga exitosa en tabla mes actual.")
    else:
        logger.info("No se encontraron pedidos pertenecientes al mes actual.")

    logger.info("Pipeline Pedido Cabecera finalizado OK")

if __name__ == "__main__":
    run()