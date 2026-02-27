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
    logger.info("Iniciando pipeline: Pedido Detalle (Filtro Estricto Mes Actual + Pendientes Anterior)")

    odoo_client = get_odoo_client()

    # 1. Referencias Temporales
    today = date.today()
    month_actual_start = today.replace(day=1)
    month_anterior_start = (month_actual_start - timedelta(days=1)).replace(day=1)

    # 2. Obtener las Cabeceras para identificar qué IDs filtrar
    # Traemos desde el mes anterior para tener el universo completo
    fecha_minima_odoo = month_anterior_start.strftime("%Y-%m-%d")
    orders_raw = get_orders_raw(odoo_client, domain=[("create_date", ">=", fecha_minima_odoo)])
    
    if not orders_raw:
        logger.warning("No se encontraron pedidos en el rango de fechas.")
        return

    df_orders = pd.DataFrame(orders_raw)
    
    # --- APLICAR MISMA LÓGICA DE FILTRADO QUE EN CABECERA ---
    df_orders["fecha_filtro_dt"] = pd.to_datetime(df_orders["create_date"]).dt.date
    
    # Regla: (Todo Febrero) O (Enero que NO esté 'invoiced')
    mask_actual = (df_orders["fecha_filtro_dt"] >= month_actual_start)
    mask_enero_rango = (df_orders["fecha_filtro_dt"] >= month_anterior_start) & (df_orders["fecha_filtro_dt"] < month_actual_start)
    mask_no_facturado = (df_orders["invoice_status"] != "invoiced")
    
    # Lista de IDs de pedidos que sí queremos procesar
    valid_order_ids = df_orders[mask_actual | (mask_enero_rango & mask_no_facturado)]["id"].tolist()
    
    logger.info(f"Pedidos válidos encontrados: {len(valid_order_ids)} (se descartaron los facturados de enero)")

    if not valid_order_ids:
        logger.warning("No hay IDs de pedidos válidos para procesar el detalle.")
        return

    # 3. Obtener todas las líneas (Detalle)
    lines_raw = get_order_lines_raw(odoo_client)

    # 4. Transformar filtrando solo por los pedidos válidos
    df_detalle = transform_pedido_detalle(
        lines_raw,
        valid_product_ids=None,
        valid_order_ids=valid_order_ids
    )

    if df_detalle.empty:
        logger.info("No hay líneas de pedido para los IDs seleccionados.")
        return

    # 5. Carga a BigQuery
    logger.info(f"Cargando {len(df_detalle)} líneas de detalle a BigQuery.")
    load_dataframe(
        df=df_detalle,
        table_id=f"{PROJECT_ID}.{DATASET_ANALYTICS}.pedidos_detalle",
        write_disposition="WRITE_TRUNCATE"
    )

    logger.info("Pipeline Pedido Detalle finalizado OK")

if __name__ == "__main__":
    run()