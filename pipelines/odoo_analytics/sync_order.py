import pandas as pd
from datetime import date, timedelta
from connectors.odoo import get_odoo_client
from extractors.odoo.orders import get_orders_raw
from transform.orders import transform_orders
from loaders.bigquery_loader import load_dataframe
from utils.logger import get_logger

PROJECT_ID = "odoo-analytics-482120"
DATASET_ANALYTICS = "odoo_analytics"

logger = get_logger("sync_orders_cabecera")

def run():
    logger.info("Iniciando pipeline: Pedido Cabecera (Filtro Estricto)")

    odoo_client = get_odoo_client()
    
    today = date.today()
    month_actual_start = today.replace(day=1)
    month_anterior_start = (month_actual_start - timedelta(days=1)).replace(day=1)

    orders_raw = get_orders_raw(odoo_client)
    df = transform_orders(orders_raw)
    
    if df.empty:
        logger.warning("No hay datos.")
        return

    # FILTRO
    
    # 1. Usamos fecha_creacion para el filtro
    df["fecha_filtro_dt"] = pd.to_datetime(df["fecha_creacion"]).dt.date
    
    # 2. Regla para el Mes Actual
    mask_actual = (df["fecha_filtro_dt"] >= month_actual_start)

    # 3. Regla para el Mes Anterior
    # Filtramos por rango Y por estado de facturación
    mask_rango = (df["fecha_filtro_dt"] >= month_anterior_start) & (df["fecha_filtro_dt"] < month_actual_start)
    
    # IMPORTANTE: transform_orders ya limpió los strings, así que comparamos directo
    mask_no_facturado = (df["estado_facturacion"] != "invoiced")
    
    mask_anterior_final = mask_rango & mask_no_facturado

    # 4. Unión de datos
    df_final = df[mask_actual | mask_anterior_final].copy()
    
    conteo_enero_invoiced = len(df[mask_rango & (df["estado_facturacion"] == "invoiced")])
    logger.info(f"Se descartaron {conteo_enero_invoiced} pedidos facturados de enero.")
    logger.info(f"Registros finales a cargar: {len(df_final)}")

    # Limpiar columna temporal
    df_final.drop(columns=["fecha_filtro_dt"], inplace=True)

    # 5. Carga
    if not df_final.empty:
        load_dataframe(
            df=df_final,
            table_id=f"{PROJECT_ID}.{DATASET_ANALYTICS}.pedidos_cabecera",
            write_disposition="WRITE_TRUNCATE"
        )

if __name__ == "__main__":
    run()