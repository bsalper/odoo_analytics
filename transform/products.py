import pandas as pd
from google.cloud import bigquery
from utils.logger import get_logger

logger = get_logger("transform_products")

def get_valid_product_ids():
    """Trae los IDs únicos de la tabla de atributos en BigQuery"""
    client = bigquery.Client()
    query = """
        SELECT DISTINCT CAST(id_producto AS INT64) as id_producto 
        FROM `odoo-analytics-482120.odoo_analytics.productos_atributos` 
        WHERE id_producto IS NOT NULL
    """
    df = client.query(query).to_dataframe()
    return df

def transform_products(products_raw, valid_tax_ids):
    if not products_raw:
        return pd.DataFrame(), pd.DataFrame()

    # 1. Preparar datos de Odoo
    df_odoo = pd.DataFrame(products_raw).copy()
    df_odoo["id_odoo_variante"] = pd.to_numeric(df_odoo["id"], errors="coerce").astype("Int64")
    df_odoo["id_odoo_padre"] = df_odoo["product_tmpl_id"].apply(
        lambda x: x[0] if isinstance(x, list) else None
    ).astype("Int64")

    # 2. Obtener los 400 IDs de tu tabla Maestra en BigQuery
    df_maestro = get_valid_product_ids()

    # 3. MATCH MAESTRO (LEFT JOIN)
    # Unimos para que el resultado sea IGUAL a los IDs del maestro
    df = pd.merge(
        df_maestro,
        df_odoo,
        left_on="id_producto",
        right_on="id_odoo_padre",
        how="left"
    )

    # Si un producto de tu maestro tiene varias variantes en Odoo,
    # nos quedamos solo con la primera para mantener los 400.
    df = df.drop_duplicates(subset=["id_producto"], keep="first")

    # 4. Extracción de nombres Many2one
    def extract_many2one_name(value):
        if isinstance(value, list) and len(value) > 1:
            return value[1]
        return None

    df["unidad_medida"] = df["uom_id"].apply(extract_many2one_name)
    df["categoria"] = df["categ_id"].apply(extract_many2one_name)

    # 5. Renombrado y Limpieza
    df = df.rename(columns={
        "default_code": "referencia_interna",
        "name": "nombre_producto",
        "list_price": "precio_unitario",
        "standard_price": "coste_unitario",
        "create_date": "fecha_creacion",
        "sale_ok": "puede_ser_vendido"
    })

    # Formateo de tipos
    df["precio_unitario"] = pd.to_numeric(df["precio_unitario"], errors="coerce").fillna(0).astype(float)
    df["coste_unitario"] = pd.to_numeric(df["coste_unitario"], errors="coerce").fillna(0).astype(float)
    df["fecha_creacion"] = pd.to_datetime(df["fecha_creacion"], errors="coerce").dt.date
    df["puede_ser_vendido"] = df["puede_ser_vendido"].fillna(False).astype(bool)
    df["referencia_interna"] = df["referencia_interna"].fillna("").astype(str)

    # --- RESULTADO 1: TABLA PRODUCTOS (Tus 9 columnas solicitadas) ---
    columnas_finales = [
        "id_producto", "referencia_interna", "nombre_producto", "unidad_medida",
        "precio_unitario", "coste_unitario", "fecha_creacion", "puede_ser_vendido", "categoria"
    ]
    df_productos = df[columnas_finales].reset_index(drop=True)

    # --- RESULTADO 2: TABLA PUENTE IMPUESTOS ---
    # Usamos los datos de Odoo originales filtrados por los IDs de tu maestro
    df_rel = df_odoo[df_odoo["id_odoo_padre"].isin(df_maestro["id_producto"])].copy()
    df_rel = df_rel.explode("taxes_id")
    df_rel["id_impuestos"] = pd.to_numeric(df_rel["taxes_id"], errors="coerce")
    
    valid_tax_set = set(map(int, valid_tax_ids))
    df_producto_impuesto = df_rel[df_rel["id_impuestos"].isin(valid_tax_set)]
    df_producto_impuesto = df_producto_impuesto.rename(columns={"id_odoo_padre": "id_producto"})
    df_producto_impuesto = df_producto_impuesto[["id_producto", "id_impuestos"]].dropna().reset_index(drop=True)

    logger.info(f"Match exitoso: {len(df_productos)} productos únicos basados en el maestro.")
    return df_productos, df_producto_impuesto