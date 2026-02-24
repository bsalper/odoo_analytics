import pandas as pd
from utils.logger import get_logger
from .utils import clean_and_serialize_dates, extract_many2one_id

logger = get_logger("transform_orders")


def transform_orders(orders_raw, valid_vendedor_ids=None, valid_client_ids=None):
    if not orders_raw:
        return pd.DataFrame()

    df = pd.DataFrame(orders_raw)

    # --- 1. Normalización many2one (extraer ID entero) ---
    df["id_cliente"] = df.get("partner_id", pd.Series()).apply(extract_many2one_id)
    df["id_vendedor"] = df.get("user_id", pd.Series()).apply(extract_many2one_id)
    df["id_direccion_entrega"] = df.get("partner_shipping_id", pd.Series()).apply(extract_many2one_id)
    
    # --- 2. Manejo Crítico de la Excepción ---
    def safe_exception(x):
        """Extrae el nombre de la excepción si es un many2one [id, name] o string"""
        if isinstance(x, list) and len(x) > 1:
            return str(x[1])
        if isinstance(x, list) and len(x) == 1:
            return str(x[0])
        if x is False or x is None or str(x).lower() in ["false", "none", "nan"]:
            return ""
        return str(x)

    # Creamos la columna 'excepcion' procesando el campo original de Odoo
    if "main_exception_id" in df.columns:
        df["excepcion"] = df["main_exception_id"].apply(safe_exception)
    else:
        df["excepcion"] = ""

    # --- 2. Renombrado de columnas ---
    df = df.rename(columns={
        "id": "id_pedido",
        "name": "referencia_pedido",
        "create_date": "fecha_creacion",
        "date_order": "fecha_pedido",
        "amount_untaxed": "base_imponible",
        "amount_tax": "impuestos",
        "amount_total": "total",
        "note_new": "comentarios",
        "state": "estado_pedido",
        "invoice_status": "estado_facturacion"
    })

    # --- 3. Conversión de tipos ---
    for col in ["id_pedido", "id_cliente", "id_vendedor", "id_direccion_entrega"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in ["base_imponible", "impuestos", "total"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    # --- 4. Limpieza de strings ---
    str_cols = ["referencia_pedido", "excepcion", "comentarios", "estado_pedido", "estado_facturacion"]
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).replace(["None", "False", "nan", "<NA>"], "")

    # --- 5. Fechas (Solo DATE) ---
    date_cols = ["fecha_creacion", "fecha_pedido"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    # --- 6. Filtros ---
    if valid_vendedor_ids:
        df = df[df["id_vendedor"].isin(valid_vendedor_ids)]
    if valid_client_ids:
        df = df[df["id_cliente"].isin(valid_client_ids)]

    # --- 7. Selección final ---
    columnas_finales = [
        "id_pedido",
        "referencia_pedido",
        "fecha_creacion",
        "fecha_pedido",
        "id_cliente",
        "id_vendedor",
        "base_imponible",
        "impuestos",
        "total",
        "excepcion",
        "comentarios",
        "estado_pedido",
        "estado_facturacion",
        "id_direccion_entrega"
    ]
    
    df = df[[c for c in columnas_finales if c in df.columns]].reset_index(drop=True)

    logger.info(f"Pedidos transformados finales: {len(df)}")
    return df
