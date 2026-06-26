import pandas as pd
from utils.logger import get_logger
from .utils import extract_many2one_id

logger = get_logger("transform_pedido_detalle")


def transform_pedido_detalle(lines_raw, valid_product_ids=None, valid_order_ids=None):
    if not lines_raw:
        return pd.DataFrame()

    df = pd.DataFrame(lines_raw)

    # 1. Normalización many2one
    if "order_id" in df.columns:
        df["id_pedido"] = df["order_id"].apply(extract_many2one_id)
    else:
        df["id_pedido"] = None

    if "product_id" in df.columns:
        df["id_producto"] = df["product_id"].apply(extract_many2one_id)
    else:
        df["id_producto"] = None

    if "order_partner_id" in df.columns:
        df["id_cliente"] = df["order_partner_id"].apply(extract_many2one_id)
    else:
        df["id_cliente"] = None

    # 2. Renombrar columnas
    df = df.rename(columns={
        "id": "id_linea",
        "discount": "descuento",
        "product_uom_qty": "cantidad",
        "price_unit": "precio_unitario",
        "price_subtotal": "subtotal"
    })

    # 3. Asegurar tipos numéricos
    cols_numericas = ["cantidad", "precio_unitario", "subtotal", "descuento"]
    for col in cols_numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # 4. Asegurar IDs como enteros
    cols_ids = ["id_linea", "id_pedido", "id_producto", "id_cliente"]
    for col in cols_ids:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # 5. Filtros opcionales
    if valid_product_ids:
        df = df[df["id_producto"].isin(valid_product_ids)]

    if valid_order_ids:
        df = df[df["id_pedido"].isin(valid_order_ids)]

    # 6. Selección final
    df = df[
        [
            "id_linea",
            "id_pedido",
            "id_cliente",
            "id_producto",
            "cantidad",
            "precio_unitario",
            "descuento",
            "subtotal"
        ]
    ]

    logger.info(f"Lineas de pedido transformadas finales: {len(df)}")

    return df
