import pandas as pd
from utils.logger import get_logger

logger = get_logger("transform_products")

PATRON_REFERENCIA = r'^[A-Z]{4}\d{3}$'


def transform_products(products_raw, valid_tax_ids):

    if not products_raw:
        return pd.DataFrame(), pd.DataFrame()

    df = pd.DataFrame(products_raw).copy()
    logger.info(f"Transformando {len(df)} registros...")

    valid_set = set(map(int, valid_tax_ids))

    # -----------------------------
    # MANY2ONE (nombre descriptivo)
    # -----------------------------
    def extract_many2one_name(value):
        if isinstance(value, list) and len(value) > 1:
            return value[1]
        return None

    df["unidad_medida"] = df["uom_id"].apply(extract_many2one_name)
    df["categoria"] = df["categ_id"].apply(extract_many2one_name)

    # -----------------------------
    # RENOMBRADO
    # -----------------------------
    df = df.rename(columns={
        "id": "id_producto",
        "default_code": "referencia_interna",
        "name": "nombre_producto",
        "list_price": "precio_unitario",
        "standard_price": "coste_unitario",
        "create_date": "fecha_creacion",
        "sale_ok": "puede_ser_vendido"
    })

    # -----------------------------
    # TIPOS
    # -----------------------------
    df["id_producto"] = pd.to_numeric(df["id_producto"], errors="coerce").astype("Int64")

    df["precio_unitario"] = (
        pd.to_numeric(df["precio_unitario"], errors="coerce")
        .fillna(0)
        .astype(float)
    )

    df["coste_unitario"] = (
        pd.to_numeric(df["coste_unitario"], errors="coerce")
        .fillna(0)
        .astype(float)
    )

    df["puede_ser_vendido"] = (
        df["puede_ser_vendido"]
        .map({"True": True, "False": False, True: True, False: False})
        .fillna(False)
        .astype(bool)
    )

    df["fecha_creacion"] = pd.to_datetime(df["fecha_creacion"], errors="coerce")

    df["referencia_interna"] = df["referencia_interna"].fillna("").astype(str)

    # -----------------------------
    # FILTROS NEGOCIO
    # -----------------------------
    df = df[df["referencia_interna"].str.match(PATRON_REFERENCIA, na=False)]
    df = df[~df["referencia_interna"].str.upper().str.startswith("INSU")]

    # =============================
    # TABLA PRODUCTOS
    # =============================
    columnas_finales = [
        "id_producto",
        "referencia_interna",
        "nombre_producto",
        "unidad_medida",
        "precio_unitario",
        "coste_unitario",
        "fecha_creacion",
        "puede_ser_vendido",
        "categoria"
    ]

    df_productos = df[columnas_finales].reset_index(drop=True)

    # =============================
    # TABLA PUENTE PRODUCTO_IMPUESTO
    # =============================
    df_rel = pd.DataFrame(products_raw)[["id", "taxes_id"]].copy()

    df_rel["id_producto"] = pd.to_numeric(df_rel["id"], errors="coerce").astype("Int64")

    # Explode lista de impuestos
    df_rel = df_rel.explode("taxes_id")

    df_rel["id_impuestos"] = pd.to_numeric(df_rel["taxes_id"], errors="coerce")

    # Filtrar impuestos válidos
    df_rel = df_rel[df_rel["id_impuestos"].isin(valid_set)]

    df_producto_impuesto = df_rel[
        ["id_producto", "id_impuestos"]
    ].dropna().reset_index(drop=True)

    logger.info(f"Transformación finalizada: {len(df_productos)} productos válidos")
    logger.info(f"Relaciones producto-impuesto generadas: {len(df_producto_impuesto)}")

    return df_productos, df_producto_impuesto
