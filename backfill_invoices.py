import pandas as pd
import os
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

logger = get_logger("backfill_historico")

def delete_period_from_history(client, first_day, last_day):
    """Elimina el rango de fechas específico en BigQuery antes de escribir para evitar duplicados"""
    query = f"""
        DELETE FROM `{TABLE_HIST}` 
        WHERE DATE(fecha_factura) BETWEEN '{first_day}' AND '{last_day}'
    """
    query_job = client.query(query)
    query_job.result()  # Espera que termine el borrado
    logger.info(f"Limpieza: Registros eliminados entre {first_day} y {last_day}.")

def run_backfill():
    logger.info("Iniciando Carga Masiva Histórica por bloques de 6 meses...")
    odoo_client = get_odoo_client()
    
    # BUSCADOR DINÁMICO DE JSON: Busca cualquier archivo de credenciales en la carpeta actual
    carpeta_actual = os.getcwd()
    archivos_json = [f for f in os.listdir(carpeta_actual) if f.endswith('.json')]
    
    bq_client = None
    
    if archivos_json:
        # Toma el primer archivo JSON que encuentre en la carpeta
        nombre_json = archivos_json[0]
        ruta_json = os.path.join(carpeta_actual, nombre_json)
        
        logger.info(f"Archivo de credenciales detectado automáticamente: {nombre_json}")
        
        # Seteamos la variable global para que 'load_dataframe' también la lea
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ruta_json
        bq_client = bigquery.Client.from_service_account_json(ruta_json)
    else:
        logger.error("ERROR CRÍTICO!: No hay ningún archivo .json en la carpeta actual. Asegúrate de mover tus credenciales aquí.")
        return # Frena el script si no hay credenciales
        
    # --- CONFIGURACIÓN DEL RANGO TOTAL ---
    fecha_inicio_global = date(2023, 1, 1)
    fecha_fin_global = date.today() 
    
    bloque_inicio = fecha_inicio_global
    
    while bloque_inicio < fecha_fin_global:
        # Calcular el fin del bloque actual (6 meses después o el día de hoy, lo que ocurra primero)
        bloque_fin = bloque_inicio + relativedelta(months=6) - relativedelta(days=1)
        if bloque_fin > fecha_fin_global:
            bloque_fin = fecha_fin_global
            
        f_inicio_str = bloque_inicio.strftime('%Y-%m-%d')
        f_fin_str = bloque_fin.strftime('%Y-%m-%d')
        
        logger.info(f"PROCESANDO BLOQUE: {f_inicio_str} al {f_fin_str}")
        
        try:
            # 1. Extracción desde Odoo
            raw_data = get_invoices_raw(odoo_client, fecha_inicio=f_inicio_str)
            df = transform_invoices(raw_data)
            
            if not df.empty:
                # 2. Filtrado estricto del dataframe para que pertenezca solo a este bloque de 6 meses
                df['fecha_tmp'] = pd.to_datetime(df['fecha_factura']).dt.date
                df_filtrado = df[(df['fecha_tmp'] >= bloque_inicio) & (df['fecha_tmp'] <= bloque_fin)].copy()
                df_filtrado = df_filtrado.drop(columns=['fecha_tmp'])
                
                if not df_filtrado.empty:
                    # 3. Limpiar BigQuery para este bloque específico
                    delete_period_from_history(bq_client, f_inicio_str, f_fin_str)
                    
                    # 4. Cargar a BigQuery
                    load_dataframe(df_filtrado, TABLE_HIST, write_disposition="WRITE_APPEND")
                    logger.info(f"Éxito Bloque: Se insertaron {len(df_filtrado)} registros.")
                else:
                    logger.info(f"El dataframe quedó vacío tras aplicar el filtro del bloque.")
            else:
                logger.info(f"ℹOdoo no devolvió registros para el rango solicitado.")
                
        except Exception as e:
            logger.error(f"Error procesando el bloque {f_inicio_str} al {f_fin_str}: {str(e)}")
            break # Te agregué un break aquí por seguridad: si un bloque falla, frena para que puedas revisar
            
        # Desplazar el inicio al siguiente bloque de 6 meses
        bloque_inicio = bloque_inicio + relativedelta(months=6)

    logger.info("Proceso de Backfill Histórico Finalizado Exitosamente!")

if __name__ == "__main__":
    run_backfill()