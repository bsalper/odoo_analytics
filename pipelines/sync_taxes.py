import logging
import os
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

from connectors.odoo import OdooClient
from extractors.odoo.impuestos import get_taxes_raw
from utils.bigquery import load_df_to_bq

logger = logging.getLogger("sync_taxes")

PROJECT_ID = "odoo-analytics-482120"
DATASET_ANALYTICS = "odoo_analytics"
TABLE_NAME = "impuestos"

def run():
    logger.info("Iniciando sincronización de impuestos")
    
    # 1. Configuración de Credenciales
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    cert_path = os.path.join(base_dir, 'credentials_google', 'odoo-analytics-482120-4a4cd8457bc7.json')
    
    credentials = service_account.Credentials.from_service_account_file(cert_path)
    client_bq = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    odoo = OdooClient()

    # 2. Extracción de Odoo
    raw_data = get_taxes_raw(odoo, limit=100)
    if not raw_data:
        logger.warning("No se obtuvieron impuestos de Odoo")
        return
        
    df = pd.DataFrame(raw_data)

    # 3. Transformación y Filtrado
    df = df.rename(columns={
        'id': 'id_impuesto',
        'name': 'nombre_impuesto',
        'amount': 'valor_impuesto'
    })

    df['id_impuesto'] = df['id_impuesto'].astype(str)

    # Filtrar solo los 6 IDs que se requiere
    ids_permitidos = ['1', '2', '14', '15', '16', '18'] 
    df = df[df['id_impuesto'].isin(ids_permitidos)].copy()

    # Asegurar tipo numérico para el valor del impuesto
    df['valor_impuesto'] = pd.to_numeric(df['valor_impuesto'], errors='coerce').fillna(0)
    
    # Selección final de columnas
    df = df[['id_impuesto', 'nombre_impuesto', 'valor_impuesto']]

    # 4. Carga a BigQuery Analytics
    load_df_to_bq(
        df=df,
        project_id=PROJECT_ID,
        dataset=DATASET_ANALYTICS,
        table=TABLE_NAME,
        write_disposition="WRITE_TRUNCATE",
        client=client_bq
    )
    
    logger.info(f"Sync de impuestos finalizado. {len(df)} impuestos cargados correctamente.")

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s - %(name)s: %(message)s'
    )
    run()