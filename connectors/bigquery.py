import os
from google.cloud import bigquery
from google.oauth2 import service_account
from config.settings import GCP_PROJECT_ID
from utils.logger import get_logger

logger = get_logger("bigquery_connector")

def get_bigquery_client():
    logger.info("Creando cliente BigQuery con credenciales explícitas")
    
    # Construimos la ruta dinámica al JSON
    # Subimos dos niveles desde connectors/ hasta la raíz odoo_analytics/
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    cert_path = os.path.join(base_dir, "secrets", "gcp", "google_credentials.json")

    if not os.path.exists(cert_path):
        logger.error(f"No se encontró el archivo de credenciales en: {cert_path}")
        raise FileNotFoundError(f"Archivo JSON no encontrado en {cert_path}")

    # Cargamos credenciales y creamos el cliente
    credentials = service_account.Credentials.from_service_account_file(cert_path)
    return bigquery.Client(credentials=credentials, project=GCP_PROJECT_ID)