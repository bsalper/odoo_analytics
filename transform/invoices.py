import pandas as pd
from utils.logger import get_logger
from .utils import (
    normalize_many2one_id,
    clean_and_serialize_dates
)

logger = get_logger("transform_invoices")


def transform_invoices(invoices_raw, valid_vendedor_ids=None, valid_client_ids=None):
    if not invoices_raw:
        return pd.DataFrame()

    df = pd.DataFrame(invoices_raw).copy()

    logger.info(f"Transformando {len(df)} facturas...")

    def normalize_many2one_name(series):
        return series.apply(
            lambda x: x[1] if isinstance(x, (list, tuple)) and len(x) > 1 else None
        )

    # --- 1. Normalización Many2one ---
    df["id_cliente"] = normalize_many2one_id(df.get("partner_id"))
    df["id_vendedor"] = normalize_many2one_id(df.get("invoice_user_id"))
    df["tipo_documento_raw"] = normalize_many2one_name(df.get("l10n_latam_document_type_id"))

    # --- 2. Renombrado ---
    df = df.rename(columns={
        "id": "id_factura",
        "name": "numero_factura",
        "l10n_latam_document_number": "folio_document",
        "tipo_documento_raw": "tipo_documento",
        "state": "estado",
        "invoice_date": "fecha_factura",
        "create_date": "fecha_creacion",
        "amount_untaxed": "monto_neto",
        "amount_tax": "monto_impuesto",
        "amount_residual": "monto_residual",
        "invoice_date_due": "fecha_vencimiento",
        "amount_total": "total_factura",
        "payment_state": "estado_pago",
        "invoice_origin": "origen",
    })

    # --- 3. Limpieza de texto ---
    text_fields = ["numero_factura", "estado", "estado_pago", "origen", "tipo_documento"]

    for field in text_fields:
        if field in df.columns:
            df[field] = df[field].replace({False: None}).astype("string")

    # --- 4. Conversión numérica ---
    numeric_fields = [
        "monto_neto", "monto_impuesto",
        "monto_residual", "total_factura"
    ]

    for field in numeric_fields:
        if field in df.columns:
            df[field] = pd.to_numeric(df[field], errors="coerce").fillna(0.0)

    # --- 5. Fechas ---
    date_fields = ["fecha_factura", "fecha_creacion", "fecha_vencimiento"]
    df = clean_and_serialize_dates(df, [f for f in date_fields if f in df.columns])

    # --- 6. Normalizar IDs ---
    id_fields = ["id_factura", "id_cliente", "id_vendedor"]

    for field in id_fields:
        if field in df.columns:
            df[field] = pd.to_numeric(df[field], errors="coerce").astype("Int64")


    # --- 7. Filtros por vendedor / cliente ---
    if valid_vendedor_ids:
        df = df[df["id_vendedor"].isin(valid_vendedor_ids)]

    if valid_client_ids:
        df = df[df["id_cliente"].isin(valid_client_ids)]

    # --- 8. Eliminar duplicados por id_factura ---
    if "id_factura" in df.columns:
        df = df.drop_duplicates(subset=["id_factura"])

    # --- 9. Selección final ---
    columnas_finales = [
        "id_factura", "numero_factura", "folio_document", "tipo_documento",
        "estado", "fecha_creacion", "fecha_factura", "fecha_vencimiento",
        "monto_neto", "monto_impuesto", "monto_residual", "total_factura",
        "estado_pago", "origen", "id_cliente", "id_vendedor"
    ]

    df = df[[c for c in columnas_finales if c in df.columns]].reset_index(drop=True)

    logger.info(f"Facturas transformadas finales: {len(df)}")

    return df
