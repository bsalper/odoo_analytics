import pandas as pd
from datetime import date
from connectors.odoo import get_odoo_client
from extractors.odoo.invoices import get_invoices_raw
from transform.invoices import transform_invoices
from loaders.bigquery_loader import load_dataframe
from utils.logger import get_logger

PROJECT_ID = "odoo-analytics-482120"
DATASET_ANALYTICS = "odoo_analytics"

logger = get_logger("sync_invoices_cabecera_unificado")

def run():
    logger.info("Iniciando pipeline unificado: Factura Cabecera")

    # 1. Configuración de cliente y fechas
    odoo_client = get_odoo_client()
    today = date.today()
    # Definimos el inicio del mes actual para la segmentación
    month_start = today.replace(day=1)

    # 2. Extracción (Datos crudos de Odoo)
    invoices_raw = get_invoices_raw(odoo_client)
    
    if not invoices_raw:
        logger.warning("No se obtuvieron datos de Odoo.")
        return

    # 3. Transformación (Limpieza, Tipado e IDs ocurren aquí)
    df = transform_invoices(invoices_raw)

    if df.empty:
        logger.warning("El DataFrame está vacío después de la transformación.")
        return

    # 4. Segmentación Temporal
    # Como transform_invoices ya nos entrega objetos date, la comparación es directa
    df_historico = df[df["fecha_factura"] < month_start].copy()
    df_current = df[df["fecha_factura"] >= month_start].copy()

    logger.info(f"Segmentación finalizada -> Histórico: {len(df_historico)}, Mes Actual: {len(df_current)}")

    # 5. Carga de Datos a BigQuery
    
    # El histórico solo se refresca el día 1 del mes para ahorrar recursos
    if today.day == 1: # not df_historico.empty: forzar subida
        if not df_historico.empty:
            logger.info("Día 1 detectado: Actualizando tabla HISTÓRICA...")
            load_dataframe(
                df=df_historico,
                table_id=f"{PROJECT_ID}.{DATASET_ANALYTICS}.facturas_cabecera_historico",
                write_disposition="WRITE_TRUNCATE"
            )
    else:
        logger.info(f"Hoy es día {today.day}: Se omite la carga de datos históricos.")

    # El mes actual se carga siempre (Sincronización diaria)
    if not df_current.empty:
        logger.info("Actualizando tabla del MES ACTUAL...")
        load_dataframe(
            df=df_current,
            table_id=f"{PROJECT_ID}.{DATASET_ANALYTICS}.facturas_cabecera_mes_actual",
            write_disposition="WRITE_TRUNCATE"
        )
        logger.info(f"Éxito: {len(df_current)} facturas cargadas en la tabla de mes actual.")

    logger.info("Pipeline Cabecera finalizado con éxito")

if __name__ == "__main__":
    run()