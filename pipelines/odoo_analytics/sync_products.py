from loaders.bigquery_loader import load_dataframe
from transform.products import transform_products
from utils.logger import get_logger
from google.cloud import bigquery
import pandas as pd
import os
import pathlib

# Configuración
PROJECT_ID = "odoo-analytics-482120"
DATASET_RAW = "odoo_raw"
DATASET_ANALYTICS = "odoo_analytics"

logger = get_logger("sync_products_analytics")

def run():
    logger.info("Iniciando pipeline ANALYTICS de productos")

    # Ruta relativa: entra a la carpeta y busca el archivo
    # Esto funcionará en tu PC y en GitHub si mantienes la estructura
    import pathlib
    base_path = pathlib.Path(__file__).resolve().parent.parent.parent
    ruta_json = base_path / "credentials_google" / "odoo-analytics-482120-4a4cd8457bc7.json"

    if ruta_json.exists():
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(ruta_json)
        logger.info(f"Credenciales cargadas desde: {ruta_json}")
    else:
        logger.warning("No se encontró JSON local. Usando entorno global.")

    client_bq = bigquery.Client()
    # ----------------------------------------

    # 1. Obtener los IDs de impuestos desde BigQuery
    logger.info("Leyendo impuestos desde BQ...")
    query_impuestos = f"SELECT CAST(id_impuesto AS STRING) as id FROM `{PROJECT_ID}.{DATASET_ANALYTICS}.impuestos`"
    df_impuestos = client_bq.query(query_impuestos).to_dataframe()
    valid_tax_ids = df_impuestos['id'].tolist()
    
    logger.info(f"Impuestos cargados: {len(valid_tax_ids)}")

    # 2. Extraer de RAW
    query_raw = f"SELECT * FROM `{PROJECT_ID}.{DATASET_RAW}.products_raw`"
    df_raw = client_bq.query(query_raw).to_dataframe()
    
    if df_raw.empty:
        logger.warning("No hay datos en RAW para procesar.")
        return

    # 3. Transformar
    df_productos, df_impuestos_rel = transform_products(df_raw, valid_tax_ids)

    # 4. Cargar Tabla Principal
    load_dataframe(
        df=df_productos,
        table_id=f"{PROJECT_ID}.{DATASET_ANALYTICS}.productos",
        write_disposition="WRITE_TRUNCATE",
    )

    # 5. Cargar Tabla de Relación
    if not df_impuestos_rel.empty:
        load_dataframe(
            df=df_impuestos_rel,
            table_id=f"{PROJECT_ID}.{DATASET_ANALYTICS}.productos_impuestos",
            write_disposition="WRITE_TRUNCATE",
        )
        logger.info(f"Tabla de relación 'productos_impuestos' actualizada.")

    logger.info("Pipeline finalizado con éxito.")

if __name__ == "__main__":
    run()