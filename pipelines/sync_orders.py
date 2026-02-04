from connectors.odoo import get_odoo_client
from extractors.odoo.orders import get_orders_raw, get_order_lines_raw
from transform.orders import transform_orders
from transform.invoice_lines import transform_lines
from loaders.bigquery_loader import load_dataframe
from utils.logger import get_logger
from google.cloud import bigquery

logger = get_logger("sync_orders")

def run():
    logger.info("Iniciando pipeline: sync_orders hacia odoo_analytics")

    # 1. Conexión a Odoo
    odoo_client = get_odoo_client()
    client_bq = bigquery.Client() # Cliente para leer de BigQuery

    # 2. Obtener los IDs de vendedores válidos desde tu nueva tabla fija
    logger.info("Leyendo vendedores desde la tabla fija...")
    query_vendedores = "SELECT id_vendedor FROM `odoo-analytics-482120.odoo_analytics.vendedores`"
    vendedores_df = client_bq.query(query_vendedores).to_dataframe()
    lista_vendedores_validos = vendedores_df["id_vendedor"].astype(str).tolist()

    # 3. Extracción de Odoo
    orders_raw = get_orders_raw(client=odoo_client)
    order_lines_raw = get_order_lines_raw(client=odoo_client)

    # 4. Transformación
    # Ahora pasamos la lista de vendedores que trajimos de BigQuery
    df_orders = transform_orders(
        orders_raw=orders_raw,
        valid_vendedor_ids=lista_vendedores_validos, 
        valid_client_ids=[] # Si esto te sigue vaciando el DF, asegúrate que transform_orders lo ignore si está vacío
    )

    valid_order_ids = df_orders["id_pedido"].astype(str).tolist()

    df_lines = transform_lines(
        lines_raw=order_lines_raw,
        valid_product_ids=[], 
        valid_order_ids=valid_order_ids
    )

    schema_orders = [
        bigquery.SchemaField("id_pedido", "STRING"),
        bigquery.SchemaField("referencia_pedido", "STRING"),
        bigquery.SchemaField("fecha_creacion", "STRING"),
        bigquery.SchemaField("fecha_pedido", "STRING"),
        bigquery.SchemaField("monto_neto", "FLOAT"),
        bigquery.SchemaField("monto_impuesto", "FLOAT"),
        bigquery.SchemaField("total_pedido", "FLOAT"),
        bigquery.SchemaField("estado", "STRING"),
        bigquery.SchemaField("estado_factura", "STRING"),
        bigquery.SchemaField("nota", "STRING"),
        bigquery.SchemaField("id_cliente", "STRING"),
        bigquery.SchemaField("id_vendedor", "STRING"),
    ]

    schema_lines = [
        bigquery.SchemaField("id_linea", "STRING"),
        bigquery.SchemaField("id_pedido", "STRING"),
        bigquery.SchemaField("id_producto", "STRING"),
        bigquery.SchemaField("cantidad", "FLOAT"),
        bigquery.SchemaField("precio_unitario", "FLOAT"),
        bigquery.SchemaField("subtotal", "FLOAT"),
    ]

    # 5. Carga a BigQuery (Capa odoo_analytics)
    # Cambiamos el dataset de "odoo_raw" a "odoo_analytics"
    
    logger.info("Cargando Cabecera a odoo_analytics.pedido_cabecera...")
    load_dataframe(
        df=df_orders,
        table_id="odoo-analytics-482120.odoo_analytics.pedido_cabecera",
        write_disposition="WRITE_TRUNCATE",
        schema=schema_orders
    )

    logger.info("Cargando Detalle a odoo_analytics.pedidos_detalle...")
    load_dataframe(
        df=df_lines,
        table_id="odoo-analytics-482120.odoo_analytics.pedidos_detalle",
        write_disposition="WRITE_TRUNCATE",
        schema=schema_lines
    )

    logger.info("Pipeline sync_orders finalizado con éxito en odoo_analytics")

if __name__ == "__main__":
    run()