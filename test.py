import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from connectors.odoo import get_odoo_client
from extractors.odoo.invoices import get_invoices_raw, get_invoice_lines_raw
from transform.invoices import transform_invoices
from transform.invoice_lines import transform_invoice_lines
from loaders.bigquery_loader import load_dataframe
from utils.logger import get_logger

# Configuración
PROJECT_ID = "odoo-analytics-482120"
DATASET = "odoo_analytics"
logger = get_logger("carga_masiva_historica")

def run_bulk_load():
    odoo_client = get_odoo_client()
    
    # Definimos el rango: Desde enero 2023
    fecha_inicio_proceso = datetime(2023, 1, 1)

    # Calculamos el último día del mes pasado
    # Si hoy es marzo 2026, esto dará 28 de febrero de 2026
    hoy = datetime.today()
    fecha_fin_proceso = (hoy.replace(day=1) - relativedelta(days=1))
    
    logger.info(f"Iniciando carga masiva desde {fecha_inicio_proceso.date()} hasta {fecha_fin_proceso.date()}")
    
    cursor_mes = fecha_inicio_proceso

    while cursor_mes <= fecha_fin_proceso:
        f_inicio = cursor_mes.strftime('%Y-%m-%d')
        f_fin = (cursor_mes + relativedelta(months=1, days=-1)).strftime('%Y-%m-%d')
        
        logger.info(f"=== PROCESANDO MES: {f_inicio} al {f_fin} ===")

        # --- 1. CABECERAS (Facturas) ---
        raw_invoices = get_invoices_raw(odoo_client, fecha_inicio=f_inicio)
        # Filtramos manualmente el fin de mes para las cabeceras si el extractor no lo hace
        df_invoices = transform_invoices(raw_invoices)
        if not df_invoices.empty:
            # Aseguramos que solo suba lo del mes actual del cursor
            df_invoices['fecha'] = pd.to_datetime(df_invoices['fecha_factura'])
            df_inv_mes = df_invoices[(df_invoices['fecha'] >= f_inicio) & (df_invoices['fecha'] <= f_fin)].copy()
            df_inv_mes = df_inv_mes.drop(columns=['fecha'])
            
            if not df_inv_mes.empty:
                load_dataframe(
                    df=df_inv_mes,
                    table_id=f"{PROJECT_ID}.{DATASET}.facturas_cabecera_historico",
                    write_disposition="WRITE_APPEND" # Sumamos al histórico
                )
                logger.info(f"Cabeceras: {len(df_inv_mes)} registros subidos.")

        # --- 2. DETALLES (Líneas) ---
        raw_lines = get_invoice_lines_raw(odoo_client, fecha_inicio=f_inicio, fecha_fin=f_fin)
        if raw_lines:
            df_lines = transform_invoice_lines(raw_lines)
            if not df_lines.empty:
                # Eliminamos la columna temporal si tu transformador la crea
                if "fecha_filtro" in df_lines.columns:
                    df_lines = df_lines.drop(columns=["fecha_filtro"])
                
                load_dataframe(
                    df=df_lines,
                    table_id=f"{PROJECT_ID}.{DATASET}.facturas_detalle_historico",
                    write_disposition="WRITE_APPEND" # Sumamos al histórico
                )
                logger.info(f"Detalles: {len(df_lines)} registros subidos.")

        # Avanzar al siguiente mes
        cursor_mes += relativedelta(months=1)

    logger.info("ARGA MASIVA FINALIZADA")

if __name__ == "__main__":
    run_bulk_load()