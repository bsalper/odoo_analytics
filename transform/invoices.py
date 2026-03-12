import pandas as pd
import re  # Importamos para la limpieza de texto
from utils.logger import get_logger
from .utils import (
    extract_many2one_id,
    clean_and_serialize_dates
)

logger = get_logger("transform_facturas")

def transform_invoices(invoices_raw, valid_vendedor_ids=None, valid_client_ids=None):
    if not invoices_raw:
        return pd.DataFrame()

    df = pd.DataFrame(invoices_raw).copy()
    
    # Función para extraer el nombre
    def get_m2o_name(value):
        if isinstance(value, (list, tuple)) and len(value) > 1:
            return str(value[1])
        return None

    # --- 1. Extracción de IDs y Nombres ---
    df["id_cliente"] = df["partner_id"].apply(extract_many2one_id)
    df["id_vendedor"] = df["invoice_user_id"].apply(extract_many2one_id)
    df["id_direccion_entrega"] = df["partner_shipping_id"].apply(extract_many2one_id)
    df["tipo_documento_str"] = df["l10n_latam_document_type_id"].apply(get_m2o_name)
    df["condicion_pago_str"] = df["invoice_payment_term_id"].apply(get_m2o_name)

    # --- 2. Renombrado ---
    df = df.rename(columns={
        "id": "id_factura",
        "name": "numero_factura",
        "condicion_pago_str": "condicion_pago",
        "l10n_latam_document_number": "folio_document",
        "tipo_documento_str": "tipo_documento",
        "state": "estado",
        "invoice_date": "fecha_factura",
        "amount_untaxed": "monto_neto",
        "amount_tax": "monto_impuesto",
        "invoice_date_due": "fecha_vencimiento",
        "amount_total": "total_factura",
        "payment_state": "estado_pago",
        "invoice_origin": "origen",
    })

    # --- 3. LIMPIEZA DE TIPO_DOCUMENTO (Quitar "(#) ") ---
    if "tipo_documento" in df.columns:
        # Usamos regex para quitar cualquier cosa entre paréntesis al inicio y el espacio siguiente
        # Ejemplo: "(39) Electronic Receipt" -> "Electronic Receipt"
        df["tipo_documento"] = df["tipo_documento"].apply(
            lambda x: re.sub(r'^\(\d+\)\s*', '', str(x)) if x and x is not False else ""
        )

    # --- 4. Limpieza de Texto General ---
    text_fields = ["numero_factura", "estado", "estado_pago", "origen", "tipo_documento", "folio_document", "condicion_pago"]
    for field in text_fields:
        if field in df.columns:
            df[field] = df[field].apply(lambda x: "" if x is False or x is None else str(x)).str.strip()

    # --- 5. Conversión Numérica ---
    numeric_fields = ["monto_neto", "monto_impuesto", "total_factura"]
    for field in numeric_fields:
        if field in df.columns:
            df[field] = pd.to_numeric(df[field], errors="coerce").fillna(0.0)

    # --- 6. Tipado de IDs ---
    id_fields = ["id_factura", "id_cliente", "id_vendedor", "id_direccion_entrega"]
    for field in id_fields:
        if field in df.columns:
            df[field] = pd.to_numeric(df[field], errors="coerce").astype("Int64")

    # --- 7. Fechas (Solo DATE) ---
    date_cols = ["fecha_factura", "fecha_vencimiento"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    # --- 8. Limpieza de Datos Final ---
    df = df.dropna(subset=["id_factura", "id_cliente", "fecha_factura"])
    if "estado" in df.columns:
        df = df[df["estado"] == "posted"]
    
    df = df.drop_duplicates(subset=["id_factura"])

    # --- 9. Selección Final ---
    columnas_finales = [
        "id_factura", "numero_factura", "folio_document", "tipo_documento",
        "condicion_pago", "estado", "fecha_factura", "fecha_vencimiento",
        "monto_neto", "monto_impuesto", "total_factura",
        "estado_pago", "origen", "id_cliente", "id_vendedor", "id_direccion_entrega"
    ]
    
    df = df.reindex(columns=columnas_finales)

    return df