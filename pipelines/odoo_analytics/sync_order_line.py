import pandas as pd
from datetime import date
from connectors.odoo import get_odoo_client
from extractors.odoo.orders import get_order_lines_raw
from transform.lines import transform_pedido_detalle
from loaders.bigquery_loader import load_dataframe
from utils.logger import get_logger

PROJECT_ID = "odoo-analytics-482120"
DATASET_ANALYTICS = "odoo_analytics"

logger = get_logger("sync_pedido_detalle_unificado")


def run():
    logger.info("Iniciando pipeline unificado: Pedido Detalle")

    odoo_client = get_odoo_client()

    today = date.today()
    month_start = today.replace(day=1)

    # --- 1. Extracción y transformación ---
    lines_raw = get_order_lines_raw(odoo_client)

    df = transform_pedido_detalle(
        lines_raw,
        valid_product_ids=None,
        valid_order_ids=None
    )

    df = df.loc[:, ~df.columns.duplicated()]

    # Convertimos fecha para poder separar histórico
    df["fecha_dt"] = pd.to_datetime(df["fecha_creacion"], errors="coerce").dt.date

    # --- 2. Separación ---
    df_historico = df[df["fecha_dt"] < month_start].copy()
    df_current = df[df["fecha_dt"] >= month_start].copy()

    # Limpiar columna temporal
    df_historico.drop(columns=["fecha_dt"], inplace=True, errors="ignore")
    df_current.drop(columns=["fecha_dt"], inplace=True, errors="ignore")

    # --- 3. Carga histórico (solo día 1) ---
    # --- 3. FORZAR CARGA HISTÓRICO ---
    if not df_historico.empty:
        logger.info("FORZANDO CARGA HISTÓRICO DE PEDIDO DETALLE AHORA...")
        
        load_dataframe(
            df=df_historico,
            table_id=f"{PROJECT_ID}.{DATASET_ANALYTICS}.pedidos_detalle_historico",
            write_disposition="WRITE_TRUNCATE"
        )

        logger.info(f"Cargadas {len(df_historico)} líneas históricas correctamente.")
    else:
        logger.info("No hay líneas históricas para cargar.")

    # --- 4. Carga mes actual (todos los días) ---
    if not df_current.empty:
        load_dataframe(
            df=df_current,
            table_id=f"{PROJECT_ID}.{DATASET_ANALYTICS}.pedidos_detalle_mes_actual",
            write_disposition="WRITE_TRUNCATE"
        )
        logger.info(f"Cargadas {len(df_current)} líneas del mes actual.")

    logger.info("Pipeline Pedido Detalle finalizado OK")


if __name__ == "__main__":
    run()
