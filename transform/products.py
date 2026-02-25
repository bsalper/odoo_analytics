import pandas as pd
from google.cloud import bigquery
from utils.logger import get_logger

logger = get_logger("transform_products")

def get_valid_product_ids():
    """Trae los IDs únicos del Excel subido a BigQuery"""
    client = bigquery.Client()
    query = """
        SELECT DISTINCT CAST(id_producto AS INT64) as id_producto 
        FROM `odoo-analytics-482120.odoo_analytics.productos_atributos` 
        WHERE id_producto IS NOT NULL
    """
    df = client.query(query).to_dataframe()
    return set(df["id_producto"].tolist())

def transform_products(products_raw, valid_tax_ids):
    if not products_raw:
        return pd.DataFrame(), pd.DataFrame()

    # 1. Convertir datos de Odoo a DataFrame
    df = pd.DataFrame(products_raw).copy()
    
    # 2. Obtener IDs del Excel (Match)
    ids_permitidos = get_valid_product_ids()
    
    # 3. Preparar ID de Odoo para el cruce
    df["id_producto"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
    
    # --- MATCH CON EXCEL ---
    df = df[df["id_producto"].isin(ids_permitidos)]

    # 4. Extracción de nombres Many2one
    def extract_many2one_name(value):
        if isinstance(value, list) and len(value) > 1:
            return value[1]
        return None

    df["unidad_medida"] = df["uom_id"].apply(extract_many2one_name)
    df["categoria"] = df["categ_id"].apply(extract_many2one_name)

    # 5. Renombrado y Formateo (Sin filtros de texto)
    df = df.rename(columns={
        "default_code": "referencia_interna",
        "name": "nombre_producto",
        "list_price": "precio_unitario",
        "standard_price": "coste_unitario",
        "create_date": "fecha_creacion",
        "sale_ok": "puede_ser_vendido"
    })

    df["precio_unitario"] = pd.to_numeric(df["precio_unitario"], errors="coerce").fillna(0).astype(float)
    df["coste_unitario"] = pd.to_numeric(df["coste_unitario"], errors="coerce").fillna(0).astype(float)
    df["fecha_creacion"] = pd.to_datetime(df["fecha_creacion"], errors="coerce").dt.date
    df["puede_ser_vendido"] = df["puede_ser_vendido"].fillna(False).astype(bool)
    df["referencia_interna"] = df["referencia_interna"].fillna("").astype(str)

    # --- RESULTADO 1: TABLA PRODUCTOS ---
    columnas_finales = [
        "id_producto", "referencia_interna", "nombre_producto", "unidad_medida",
        "precio_unitario", "coste_unitario", "fecha_creacion", "puede_ser_vendido", "categoria"
    ]
    df_productos = df[columnas_finales].reset_index(drop=True)

    # --- RESULTADO 2: TABLA PUENTE IMPUESTOS ---
    # Solo impuestos de los productos que pasaron el match
    valid_ids_finales = set(df_productos["id_producto"])
    df_rel = pd.DataFrame(products_raw).copy()
    df_rel["id_producto"] = pd.to_numeric(df_rel["id"], errors="coerce").astype("Int64")
    
    df_rel = df_rel[df_rel["id_producto"].isin(valid_ids_finales)]
    df_rel = df_rel.explode("taxes_id")
    df_rel["id_impuestos"] = pd.to_numeric(df_rel["taxes_id"], errors="coerce")
    
    valid_tax_set = set(map(int, valid_tax_ids))
    df_producto_impuesto = df_rel[df_rel["id_impuestos"].isin(valid_tax_set)]
    df_producto_impuesto = df_producto_impuesto[["id_producto", "id_impuestos"]].dropna().reset_index(drop=True)

    logger.info(f"Match exitoso: {len(df_productos)} productos encontrados en ambas fuentes.")
    return df_productos, df_producto_impuesto