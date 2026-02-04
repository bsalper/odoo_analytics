from google.cloud import bigquery
from utils.logger import get_logger

logger = get_logger("bigquery_utils")

def load_df_to_bq(
    df,
    project_id,
    dataset,
    table,
    write_disposition="WRITE_TRUNCATE",
    client=None
):
    if client is None:
        client = bigquery.Client(project=project_id)

    table_id = f"{project_id}.{dataset}.{table}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        autodetect=True
    )

    logger.info(f"Cargando {len(df)} registros en {table_id}")

    try:
        job = client.load_table_from_dataframe(
            df,
            table_id,
            job_config=job_config
        )
        job.result()
        logger.info(f"Carga finalizada exitosamente: {table_id}")
    except Exception as e:
        logger.error(f"Error en la carga a BigQuery: {str(e)}")
        raise e