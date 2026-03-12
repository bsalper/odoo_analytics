# connectors/google_sheets.py
import os
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from utils.logger import get_logger

logger = get_logger("google_sheets_connector")

def get_gs_service():
    base_dir = os.getcwd() 
    cert_path = os.path.join(base_dir, "secrets", "gcp", "google_credentials.json")

    if not os.path.exists(cert_path):
        logger.error(f"Certificado no encontrado en: {cert_path}")
        raise FileNotFoundError(f"No existe el archivo de credenciales en {cert_path}")

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(cert_path, scopes=scopes)
    return build("sheets", "v4", credentials=creds)