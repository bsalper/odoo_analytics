from google.cloud import bigquery
from utils.logger import get_logger

logger = get_logger("bigquery_loader")

def load_dataframe(df, table_id, write_disposition="WRITE_TRUNCATE", schema=None):
    """
    Carga un DataFrame a BigQuery. 
    Si se pasa 'schema', define los tipos de datos (Número, Texto, Fecha).
    """
    if df.empty:
        logger.warning(f"No hay datos para cargar en {table_id}")
        return

    client = bigquery.Client()

    # Configuramos la carga
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        schema=schema  # <--- Esto es lo que quita los "ABC"
    )

    try:
        job = client.load_table_from_dataframe(
            df,
            table_id,
            job_config=job_config
        )

        job.result()  # Espera a que termine la carga
        logger.info(f"Cargados {len(df)} registros en {table_id} exitosamente.")
        
    except Exception as e:
        logger.error(f"Error cargando datos en {table_id}: {e}")
        raise e