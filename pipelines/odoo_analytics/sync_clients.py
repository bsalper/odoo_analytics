from connectors.odoo import get_odoo_client
from extractors.odoo.clients import get_clients_raw
from transform.clients import transform_clients
from loaders.bigquery_loader import load_dataframe
from utils.logger import get_logger
from google.cloud import bigquery
from dotenv import load_dotenv
import os

load_dotenv(override=True)

# Configuración
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_ANALYTICS = os.getenv("BQ_DATASET_ANALYTICS")
TABLE_CLIENTES = "clientes"

logger = get_logger("sync_clients_analytics")

SCHEMA_CLIENTES = [
    bigquery.SchemaField("id_cliente", "INTEGER"),
    bigquery.SchemaField("tipo_compania", "STRING"),
    bigquery.SchemaField("tipo_direccion", "STRING"),
    bigquery.SchemaField("nombre_cliente", "STRING"),
    bigquery.SchemaField("rut", "STRING"),
    bigquery.SchemaField("dia_visita", "STRING"),
    bigquery.SchemaField("calle", "STRING"),
    bigquery.SchemaField("ciudad", "STRING"),
    bigquery.SchemaField("correo", "STRING"),
    bigquery.SchemaField("telefono", "STRING"),
    bigquery.SchemaField("fecha_creacion", "TIMESTAMP"),
    bigquery.SchemaField("id_plazo_pago", "INTEGER"),
    bigquery.SchemaField("credito_limite", "FLOAT"),
    bigquery.SchemaField("id_tarifa", "INTEGER"),
    bigquery.SchemaField("geo_latitud", "FLOAT"),
    bigquery.SchemaField("geo_longitud", "FLOAT"),
    bigquery.SchemaField("id_vendedor", "INTEGER"),
    bigquery.SchemaField("etiquetas", "STRING"),
]

# Helpers
def get_valid_vendedor_ids(client_bq):
    logger.info("Leyendo vendedores permitidos desde BigQuery...")

    query = f"""
        SELECT CAST(id_vendedor AS INTEGER) AS id
        FROM `{PROJECT_ID}.{DATASET_ANALYTICS}.vendedores`
    """

    df = client_bq.query(query).to_dataframe()
    return df["id"].tolist()


# Pipeline principal
def run():
    logger.info("Iniciando pipeline ANALYTICS directo: Odoo -> BigQuery (Clientes)")

    try:
        # 1. Conexiones
        odoo_client = get_odoo_client()
        client_bq = bigquery.Client(project=PROJECT_ID)

        # 2. Datos auxiliares
        valid_vendedor_ids = get_valid_vendedor_ids(client_bq)

        # 3. Extracción
        clients_raw = get_clients_raw(odoo_client)

        if not clients_raw:
            logger.warning("No se obtuvieron clientes desde Odoo.")
            return

        logger.info(f"Clientes extraídos: {len(clients_raw)}")

        # 4. Transformación
        df_clientes = transform_clients(clients_raw, valid_vendedor_ids)

        if df_clientes.empty:
            logger.warning("Después de la transformación no quedaron registros válidos.")
            return

        logger.info(f"Clientes transformados: {len(df_clientes)}")

        # 5. Carga
        table_id = f"{PROJECT_ID}.{DATASET_ANALYTICS}.{TABLE_CLIENTES}"

        load_dataframe(
            df=df_clientes,
            table_id=table_id,
            write_disposition="WRITE_TRUNCATE",
            schema=SCHEMA_CLIENTES,
        )

        logger.info(f"Pipeline finalizado correctamente {len(df_clientes)} registros cargados.")

    except Exception as e:
        logger.exception(f"Error en pipeline sync_clients: {e}")
        raise


if __name__ == "__main__":
    run()
