import pandas as pd
from utils.logger import get_logger
from .utils import (
    normalize_many2one_field,
    clean_and_serialize_dates,
    normalize_ids_to_string,
)

logger = get_logger("transform_invoices")


def transform_invoices(invoices_raw, valid_vendedor_ids=None, valid_client_ids=None):
    if not invoices_raw:
        return pd.DataFrame()

    df = pd.DataFrame(invoices_raw)

    # --- 1. Normalización many2one (Aplanar listas [id, name]) para que no sea una lista y BigQuery lo acepte ---
    df["id_cliente"] = normalize_many2one_field(df.get("partner_id", pd.Series()))
    df["id_vendedor"] = normalize_many2one_field(df.get("invoice_user_id", pd.Series()))
    df["tipo_documento_raw"] = normalize_many2one_field(df.get("l10n_latam_document_type_id", pd.Series()))

    # --- 2. Renombrar columnas ---
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

    # --- 3. Limpieza crítica (False → NULL) ---
    text_fields = ["numero_factura", "estado", "estado_pago", "origen", "tipo_documento"]

    for field in text_fields:
        if field in df.columns:
            df[field] = df[field].apply(
                lambda x: None if x is False else str(x) if x is not None else None
            )

    # --- 4. Fechas e IDs ---
    date_fields = ["fecha_factura", "fecha_creacion"]
    if "fecha_vencimiento" in df.columns:
        date_fields.append("fecha_vencimiento")

    df = clean_and_serialize_dates(df, date_fields)

    df = normalize_ids_to_string(
        df,
        ["id_factura", "id_cliente", "id_vendedor"]
    )

    # --- 5. Filtros opcionales ---
    if valid_vendedor_ids:
        df = df[df["id_vendedor"].isin(valid_vendedor_ids)]

    if valid_client_ids:
        df = df[df["id_cliente"].isin(valid_client_ids)]

    # --- 6. Selección final ---
    columnas_finales = [
        "id_factura", "numero_factura", "folio_document", "tipo_documento",
        "estado", "fecha_creacion", "fecha_factura", "monto_neto",
        "monto_impuesto", "total_factura", "estado_pago", "origen",
        "id_cliente", "id_vendedor"
    ]
    
    df = df[[c for c in columnas_finales if c in df.columns]]

    logger.info(f"Facturas transformadas finales: {len(df)}")

    return df