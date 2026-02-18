import pandas as pd
from utils.logger import get_logger
from .utils import normalize_many2one_id, clean_and_serialize_dates

logger = get_logger("transform_pedido_detalle")


def transform_pedido_detalle(lines_raw, valid_product_ids=None, valid_order_ids=None):
    if not lines_raw:
        return pd.DataFrame()

    df = pd.DataFrame(lines_raw)

    # --- 1. Normalización many2one ---
    df["id_pedido"] = normalize_many2one_id(df.get("order_id", pd.Series()))
    df["id_producto"] = normalize_many2one_id(df.get("product_id", pd.Series()))
    df["id_cliente"] = normalize_many2one_id(df.get("order_partner_id", pd.Series()))


    # --- 2. Renombrar columnas ---
    df = df.rename(columns={
        "id": "id_linea",
        "create_date": "fecha_creacion",
        "product_uom_qty": "cantidad",
        "price_unit": "precio_unitario",
        "price_subtotal": "subtotal"
    })

    # --- 3. Asegurar tipos numéricos ---
    cols_numericas = ["cantidad", "precio_unitario", "subtotal"]
    for col in cols_numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # --- 4. Asegurar IDs como enteros ---
    cols_ids = ["id_linea", "id_pedido", "id_producto", "id_cliente"]
    for col in cols_ids:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # --- 5. Formatear fechas ---
    df = clean_and_serialize_dates(df, ["fecha_creacion"])

    # --- 6. Filtros opcionales ---
    if valid_product_ids:
        df = df[df["id_producto"].isin(valid_product_ids)]

    if valid_order_ids:
        df = df[df["id_pedido"].isin(valid_order_ids)]

    # --- 7. Selección final ---
    df = df[
        [
            "id_linea",
            "id_pedido",
            "fecha_creacion",
            "id_cliente",
            "id_producto",
            "cantidad",
            "precio_unitario",
            "subtotal"
        ]
    ]

    logger.info(f"Lineas de pedido transformadas finales: {len(df)}")

    return df
