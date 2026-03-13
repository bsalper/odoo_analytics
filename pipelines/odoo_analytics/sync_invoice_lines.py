import pandas as pd
from connectors.odoo import get_odoo_client
from extractors.odoo.invoices import get_invoice_lines_raw
from transform.invoice_lines import transform_invoice_lines
from loaders.bigquery_loader import load_dataframe
from utils.logger import get_logger
from datetime import date

PROJECT_ID = "odoo-analytics-482120"
DATASET_ANALYTICS = "odoo_analytics"

logger = get_logger("sync_invoice_detalle_unificado")

def run():
    logger.info("Iniciando pipeline unificado: factura detalle")

    # 1. Conexión a Odoo
    odoo_client = get_odoo_client()

    # 2. Fecha de corte (primer día del mes actual)
    today = date.today()
    month_start = today.replace(day=1)

    # 3. Extracción Detalle desde Odoo
    invoice_lines_raw = get_invoice_lines_raw(odoo_client)
    
    if not invoice_lines_raw:
        logger.info("No se obtuvieron registros de Odoo.")
        return

    # 4. Transformación 
    df_lines = transform_invoice_lines(invoice_lines_raw=invoice_lines_raw)

    if df_lines.empty:
        logger.info("El DataFrame transformado está vacío.")
        return

    # --- CAMBIO CRÍTICO 1: Asegurar que fecha_filtro sea datetime ---
    # Esto evita el error "AttributeError: Can only use .dt accessor..."
    df_lines["fecha_filtro"] = pd.to_datetime(df_lines["fecha_filtro"], errors='coerce')

    # 5. Separar data usando la columna temporal 'fecha_filtro'
    # Usamos .dt.date para comparar contra objetos date de Python
    df_historical = df_lines[df_lines["fecha_filtro"].dt.date < month_start].copy()
    df_current = df_lines[df_lines["fecha_filtro"].dt.date >= month_start].copy()

    # 6. Borrar la columna temporal antes de cargar
    if not df_historical.empty:
        df_historical = df_historical.drop(columns=["fecha_filtro"])
    if not df_current.empty:
        df_current = df_current.drop(columns=["fecha_filtro"])

    logger.info(f"Procesando - Históricas: {len(df_historical)}, Mes actual: {len(df_current)}")

    # --- CAMBIO CRÍTICO 2: Lógica del Día 1 ---
    # Solo sobreescribimos la tabla histórica si hoy es el primer día del mes.
    # Si no, estaríamos procesando medio millón de filas innecesariamente cada día.
    if today.day == 1:
        if not df_historical.empty:
            logger.info("Hoy es día 1: Refrescando histórico en BigQuery...")
            load_dataframe(
                df=df_historical,
                table_id=f"{PROJECT_ID}.{DATASET_ANALYTICS}.facturas_detalle_historico",
                write_disposition="WRITE_TRUNCATE"
            )
            logger.info(f"Cargadas {len(df_historical)} líneas en histórico.")
    else:
        logger.info(f"Hoy es día {today.day}: Se omite la actualización del histórico para ahorrar tiempo.")

    # 8. CARGA DE MES ACTUAL (Todos los días)
    if not df_current.empty:
        load_dataframe(
            df=df_current,
            table_id=f"{PROJECT_ID}.{DATASET_ANALYTICS}.facturas_detalle_mes_actual",
            write_disposition="WRITE_TRUNCATE"
        )
        logger.info(f"Cargadas {len(df_current)} líneas del mes actual.")

    logger.info("Pipeline finalizado OK")

if __name__ == "__main__":
    run()