from connectors.odoo import get_odoo_client
from extractors.odoo.products import get_products_raw
from transform.products import transform_products
from loaders.bigquery_loader import load_dataframe
from utils.logger import get_logger
from google.cloud import bigquery
from dotenv import load_dotenv
import os

load_dotenv(override=True)

# Configuración
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_ANALYTICS = os.getenv("BQ_DATASET_ANALYTICS")

TABLE_PRODUCTOS = "productos"
TABLE_PRODUCTO_IMPUESTO = "producto_impuesto"

logger = get_logger("sync_products_analytics")

# -------------------------
# SCHEMAS
# -------------------------

SCHEMA_PRODUCTOS = [
    bigquery.SchemaField("id_producto_variante", "INTEGER"),
    bigquery.SchemaField("id_producto_padre", "INTEGER"),
    bigquery.SchemaField("referencia_interna", "STRING"),
    bigquery.SchemaField("nombre_producto", "STRING"),
    bigquery.SchemaField("unidad_medida", "STRING"),
    bigquery.SchemaField("precio_unitario", "FLOAT"),
    bigquery.SchemaField("coste_unitario", "FLOAT"),
    bigquery.SchemaField("fecha_creacion", "DATE"),
    bigquery.SchemaField("puede_ser_vendido", "BOOLEAN"),
    bigquery.SchemaField("categoria", "STRING"),
]

SCHEMA_PRODUCTO_IMPUESTO = [
    bigquery.SchemaField("id_producto_variante", "INTEGER"),
    bigquery.SchemaField("id_impuestos", "INTEGER"),
]

# Helpers

def get_valid_tax_ids(client_bq):
    logger.info("Leyendo impuestos permitidos desde BigQuery...")

    query = f"""
        SELECT id_impuestos
        FROM `{PROJECT_ID}.{DATASET_ANALYTICS}.impuestos`
    """

    df = client_bq.query(query).to_dataframe()
    return df["id_impuestos"].dropna().astype(int).tolist()

# Pipeline principal

def run():
    logger.info("Iniciando pipeline ANALYTICS directo: Odoo -> BigQuery")

    try:
        # 1. Conexiones
        odoo_client = get_odoo_client()
        client_bq = bigquery.Client(project=PROJECT_ID)

        # 2. Datos auxiliares
        valid_tax_ids = get_valid_tax_ids(client_bq)

        # 3. Extracción
        products_raw = get_products_raw(odoo_client)

        if not products_raw:
            logger.warning("No se obtuvieron productos desde Odoo.")
            return

        logger.info(f"Productos extraídos: {len(products_raw)}")

        # 4. Transformación
        df_productos, df_producto_impuesto = transform_products(
            products_raw,
            valid_tax_ids
        )

        if df_productos.empty:
            logger.warning("Después de la transformación no quedaron productos válidos.")
            return

        logger.info(f"Productos transformados: {len(df_productos)}")
        logger.info(f"Relaciones producto-impuesto: {len(df_producto_impuesto)}")

        # 5. Carga tabla productos
        table_productos_id = f"{PROJECT_ID}.{DATASET_ANALYTICS}.{TABLE_PRODUCTOS}"

        load_dataframe(
            df=df_productos,
            table_id=table_productos_id,
            write_disposition="WRITE_TRUNCATE",
            schema=SCHEMA_PRODUCTOS,
        )

        # 6. Carga tabla puente producto_impuesto
        table_rel_id = f"{PROJECT_ID}.{DATASET_ANALYTICS}.{TABLE_PRODUCTO_IMPUESTO}"

        load_dataframe(
            df=df_producto_impuesto,
            table_id=table_rel_id,
            write_disposition="WRITE_TRUNCATE",
            schema=SCHEMA_PRODUCTO_IMPUESTO,
        )

        logger.info(
            f"Pipeline finalizado correctamente. "
            f"{len(df_productos)} productos y "
            f"{len(df_producto_impuesto)} relaciones cargadas."
        )

    except Exception as e:
        logger.exception(f"Error en pipeline sync_products: {e}")
        raise


if __name__ == "__main__":
    run()
