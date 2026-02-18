import pandas as pd
from datetime import date
from connectors.odoo import get_odoo_client
from extractors.odoo.orders import get_orders_raw
from transform.orders import transform_orders
from loaders.bigquery_loader import load_dataframe
from utils.logger import get_logger
from google.cloud import bigquery

PROJECT_ID = "odoo-analytics-482120"
DATASET_ANALYTICS = "odoo_analytics"

logger = get_logger("sync_orders_cabecera_unificado")


def run():
    logger.info("Iniciando pipeline unificado: Pedido Cabecera")

    # Conexión Odoo
    odoo_client = get_odoo_client()
    
    today = date.today()
    month_start = today.replace(day=1)

    # 1. Extracción y transformación
    orders_raw = get_orders_raw(odoo_client)
    df = transform_orders(orders_raw, valid_vendedor_ids=None, valid_client_ids=None)
    df = df.loc[:, ~df.columns.duplicated()]  # evitar duplicados por columnas
    df["fecha_dt"] = pd.to_datetime(df["fecha_pedido"], errors="coerce").dt.date
    df_posted = df[df["estado_pedido"] == "sale"].copy()  # Filtrar pedidos confirmados (ajusta según tu estado Odoo)

    # 2. Separar histórico vs mes actual
    df_historico = df_posted[df_posted["fecha_dt"] < month_start].copy()
    df_current = df_posted[df_posted["fecha_dt"] >= month_start].copy()

    # Limpiar columna temporal
    if "fecha_dt" in df_historico.columns: df_historico.drop(columns=["fecha_dt"], inplace=True)
    if "fecha_dt" in df_current.columns: df_current.drop(columns=["fecha_dt"], inplace=True)

    # 3. CARGA DE HISTÓRICO (Solo el día 1 del mes)
    # --- FORZAR CARGA DE HISTÓRICO ---
    if not df_historico.empty:
        logger.info("FORZANDO CARGA DEL HISTÓRICO DE PEDIDOS AHORA...")
        load_dataframe(
            df=df_historico,
            table_id=f"{PROJECT_ID}.{DATASET_ANALYTICS}.pedidos_cabecera_historico",
            write_disposition="WRITE_TRUNCATE"
        )
        logger.info(f"Cargadas {len(df_historico)} filas en pedidos_cabecera_historico")
    else:
        logger.info("No hay pedidos históricos para cargar.")


    # 4. CARGA DE MES ACTUAL (Todos los días)
    if not df_current.empty:
        load_dataframe(
            df=df_current,
            table_id=f"{PROJECT_ID}.{DATASET_ANALYTICS}.pedidos_cabecera_mes_actual",
            write_disposition="WRITE_TRUNCATE"
        )
        logger.info(f"Cargados {len(df_current)} pedidos del mes actual.")

    logger.info("Pipeline Pedido Cabecera finalizado OK")


if __name__ == "__main__":
    run()
