import pandas as pd
from utils.logger import get_logger
from .utils import (
    normalize_many2one_field,
    clean_and_serialize_dates,
    normalize_ids_to_string
)

logger = get_logger("transform_invoice_lines")

def transform_invoice_lines(invoice_lines_raw, valid_invoice_ids=None, valid_product_ids=None):
    if not invoice_lines_raw:
        return pd.DataFrame()

    df = pd.DataFrame(invoice_lines_raw)

    # --- 1. Normalización many2one ---
    df["id_factura"] = normalize_many2one_field(df.get("move_id", pd.Series()))
    df["id_producto"] = normalize_many2one_field(df.get("product_id", pd.Series()))

    # --- 2. Renombrar columnas ---
    df = df.rename(columns={
        "id": "id_linea_factura",
        "name": "descripcion",
        "quantity": "cantidad",
        "price_unit": "precio_unitario",
        "price_subtotal": "subtotal",
        "price_total": "total",
        "create_date": "fecha_creacion",
    })

    # --- 3. Corrección crítica (False → NULL) ---
    if "descripcion" in df.columns:
        df["descripcion"] = df["descripcion"].apply(
            lambda x: None if x is False else str(x) if x is not None else None
        )

    # --- 4. Fechas e IDs ---
    df = clean_and_serialize_dates(df, ["fecha_creacion"])
    df = normalize_ids_to_string(
        df,
        ["id_linea_factura", "id_factura", "id_producto"]
    )

    # --- 5. Filtros opcionales ---
    if valid_invoice_ids:
        df = df[df["id_factura"].isin(valid_invoice_ids)]

    if valid_product_ids:
        df = df[df["id_producto"].isin(valid_product_ids)]

    # --- 6. Selección final ---
    final_cols = [
        "id_linea_factura",
        "id_factura",
        "id_producto",
        "descripcion",
        "cantidad",
        "precio_unitario",
        "subtotal",
        "total",
        "fecha_creacion",
    ]

    df = df[[c for c in final_cols if c in df.columns]]


    logger.info(f"Líneas de factura transformadas finales: {len(df)}")

    return df