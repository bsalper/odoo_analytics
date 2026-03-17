import pandas as pd
from datetime import date
from dateutil.relativedelta import relativedelta
from google.cloud import bigquery
from connectors.odoo import get_odoo_client
from extractors.odoo.invoices import get_invoices_raw
from transform.invoices import transform_invoices
from loaders.bigquery_loader import load_dataframe
from utils.logger import get_logger

PROJECT_ID = "odoo-analytics-482120"
DATASET = "odoo_analytics"
TABLE_HIST = f"{PROJECT_ID}.{DATASET}.facturas_cabecera_historico"
TABLE_ACTUAL = f"{PROJECT_ID}.{DATASET}.facturas_cabecera_mes_actual"

logger = get_logger("sync_cabecera")

def check_if_month_exists(first_day_month):
    """Evita duplicados en el histórico revisando si el mes ya existe"""
    client = bigquery.Client()
    query = f"""
        SELECT COUNT(1) FROM `{TABLE_HIST}` 
        WHERE DATE(fecha_factura) = '{first_day_month}'
        LIMIT 1
    """
    query_job = client.query(query)
    result = next(query_job.result())
    return result[0] > 0

def run():
    logger.info("Iniciando Sincronización de Cabeceras...")
    odoo_client = get_odoo_client()
    today = date.today()
    
    # --- 1. LÓGICA DE DÍA 1 (PASAR MES CERRADO AL HISTÓRICO) ---
    if today.day == 1:
        primer_dia_pasado = (today - relativedelta(months=1)).replace(day=1)
        ultimo_dia_pasado = today - relativedelta(days=1)
        
        f_inicio = primer_dia_pasado.strftime('%Y-%m-%d')
        f_fin = ultimo_dia_pasado.strftime('%Y-%m-%d')

        if check_if_month_exists(f_inicio):
            logger.warning(f"Cierre omitido: El mes {f_inicio} ya existe en el histórico.")
        else:
            logger.info(f"Día 1 detectado. Consolidando mes pasado: {f_inicio} al {f_fin}")
            raw_cerrado = get_invoices_raw(odoo_client, fecha_inicio=f_inicio)
            df_cerrado = transform_invoices(raw_cerrado)
            
            # Filtro estricto para no traer nada del día 1 actual por error
            df_cerrado['fecha_tmp'] = pd.to_datetime(df_cerrado['fecha_factura']).dt.date
            df_final_cerrado = df_cerrado[df_cerrado['fecha_tmp'] <= ultimo_dia_pasado].copy()
            df_final_cerrado = df_final_cerrado.drop(columns=['fecha_tmp'])

            if not df_final_cerrado.empty:
                load_dataframe(df_final_cerrado, TABLE_HIST, write_disposition="WRITE_APPEND")
                logger.info(f"Éxito: {len(df_final_cerrado)} registros movidos al histórico.")

    # --- 2. LÓGICA DIARIA (ACTUALIZAR MES EN CURSO) ---
    f_inicio_actual = today.replace(day=1).strftime('%Y-%m-%d')
    logger.info(f"Sincronizando mes actual desde: {f_inicio_actual}")
    
    raw_actual = get_invoices_raw(odoo_client, fecha_inicio=f_inicio_actual)
    df_actual = transform_invoices(raw_actual)

    if not df_actual.empty:
        # Aquí siempre TRUNCATE porque solo borramos el mes actual 'sucio'
        load_dataframe(df_actual, TABLE_ACTUAL, write_disposition="WRITE_TRUNCATE")
        logger.info(f"Éxito: {len(df_actual)} registros actualizados en tabla Mes Actual.")

if __name__ == "__main__":
    run()