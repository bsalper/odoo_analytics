import pandas as pd
from utils.logger import get_logger
from .utils import (
    extract_many2one_id,
    clean_and_serialize_dates
)

logger = get_logger("transform_invoice_lines")

def transform_invoice_lines(invoice_lines_raw, valid_invoice_ids=None):
    if not invoice_lines_raw:
        return pd.DataFrame()

    df = pd.DataFrame(invoice_lines_raw).copy()
    logger.info(f"DEBUG 1: Filas recibidas de Odoo: {len(df)}")

    # 1. Renombrar (Usamos fecha_filtro como puente temporal)
    df = df.rename(columns={
        "id": "id_linea_factura",
        "quantity": "cantidad",
        "price_unit": "precio_unitario",
        "price_subtotal": "subtotal",
        "date": "fecha_filtro",
        "discount": "descuento",
        "cost_price": "costo_unitario"
    })

    # 2. Manejo de 'total'
    if "price_total" in df.columns:
        df = df.rename(columns={"price_total": "total"})
    else:
        df["total"] = df["subtotal"]

    # 3. Extraer IDs
    df["id_factura"] = df["move_id"].apply(extract_many2one_id)
    df["id_producto"] = df["product_id"].apply(extract_many2one_id)

    # 4. Normalizar numéricos
    cols_numericas = ["cantidad", "precio_unitario", "costo_unitario", "subtotal", "total", "descuento"]
    for col in cols_numericas:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    # 5. Tipado de IDs
    df["id_factura"] = pd.to_numeric(df["id_factura"], errors="coerce").astype("Int64")
    df["id_producto"] = pd.to_numeric(df["id_producto"], errors="coerce").astype("Int64")
    df["id_linea_factura"] = pd.to_numeric(df["id_linea_factura"], errors="coerce").astype("Int64")

    # 6. Limpieza
    df = df.dropna(subset=["id_producto", "id_factura"])
    df = df[df["total"] != 0]

    # 7. Preparar la fecha para el filtro del pipeline
    df["fecha_filtro"] = pd.to_datetime(df["fecha_filtro"], errors="coerce")

    # 8. Selección final (SIN fecha_creacion, pero CON fecha_filtro para el pipeline)
    final_cols = [
        "id_linea_factura", "id_factura", "id_producto", "cantidad", "costo_unitario",
        "precio_unitario", "subtotal", "descuento", "total", "fecha_filtro"
    ]
    df = df.reindex(columns=final_cols)

    logger.info(f"Líneas procesadas: {len(df)}")
    return df