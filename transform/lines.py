import pandas as pd
from utils.logger import get_logger
from .utils import normalize_many2one_field, clean_and_serialize_dates, normalize_ids_to_string

logger = get_logger("transform_lines")

def transform_lines(lines_raw, valid_product_ids, valid_order_ids):
    if not lines_raw:
        return pd.DataFrame()

    df = pd.DataFrame(lines_raw)

    # 1. Normalización
    df["id_pedido"] = normalize_many2one_field(df.get("order_id", pd.Series()))
    df["id_producto"] = normalize_many2one_field(df.get("product_id", pd.Series()))

    # 2. Renombrar
    df = df.rename(columns={
        "id": "id_linea",
        "price_subtotal": "subtotal",
        "price_unit": "precio_unitario",
        "product_uom_qty": "cantidad"
    })

    # 3. ASEGURAR TIPOS NUMÉRICOS (Importante para BigQuery)
    # Esto convierte cualquier False o basura en 0 o NaN, evitando el error de PyArrow
    cols_numericas = ["cantidad", "precio_unitario", "subtotal"]
    for col in cols_numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # 4. Fechas e IDs
    df = clean_and_serialize_dates(df, ["create_date"])
    df = normalize_ids_to_string(df, ["id_pedido", "id_producto", "id_linea"])

    # 5. Filtros
    if valid_product_ids:
        df = df[df["id_producto"].isin(valid_product_ids)]
    if valid_order_ids:
        df = df[df["id_pedido"].isin(valid_order_ids)]

    # 6. Selección final
    df = df[
        [
            "id_linea",
            "id_pedido",
            "id_producto",
            "cantidad",
            "precio_unitario",
            "subtotal",
        ]
    ]

    logger.info(f"Lineas transformadas finales: {len(df)}")
    return df