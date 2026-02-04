import pandas as pd
from utils.logger import get_logger
from .utils import normalize_many2one_field, clean_and_serialize_dates, normalize_ids_to_string

logger = get_logger("transform_orders")

def transform_orders(orders_raw, valid_vendedor_ids, valid_client_ids):
    if not orders_raw:
        return pd.DataFrame()

    df = pd.DataFrame(orders_raw)

    # --- 1. Normalización de campos ---
    df["id_cliente"] = normalize_many2one_field(df.get("partner_id", pd.Series()))
    df["id_vendedor"] = normalize_many2one_field(df.get("user_id", pd.Series()))

    # --- 2. Renombrar columnas ---
    df = df.rename(columns={
        "id": "id_pedido",
        "name": "referencia_pedido",
        "create_date": "fecha_creacion",
        "date_order": "fecha_pedido",
        "amount_untaxed": "monto_neto",
        "amount_tax": "monto_impuesto",
        "amount_total": "total_pedido",
        "state": "estado",
        "invoice_status": "estado_factura",
        "note": "nota"
    })

    # --- 3. CORRECCIÓN CRÍTICA PARA BIGQUERY ---
    # Odoo envía False en lugar de "" o None en campos de texto vacíos.
    # Esto convierte los False en None (NULL en BigQuery) y asegura que todo sea String.
    if "nota" in df.columns:
        df["nota"] = df["nota"].apply(lambda x: None if x is False else str(x) if x is not None else None)

    # --- 4. Formateo de fechas e IDs ---
    # Asegúrate de incluir 'fecha_creacion' si la vas a usar, ya que la renombraste
    df = clean_and_serialize_dates(df, ["fecha_pedido", "fecha_creacion"])
    df = normalize_ids_to_string(df, ["id_pedido", "id_cliente", "id_vendedor"])

    # --- 5. Filtros condicionales ---
    if valid_vendedor_ids:
        df = df[df["id_vendedor"].isin(valid_vendedor_ids)]

    if valid_client_ids:
        df = df[df["id_cliente"].isin(valid_client_ids)]

    # --- 6. Selección de columnas finales ---
    df = df[
        [
            "id_pedido",
            "referencia_pedido",
            "fecha_creacion",
            "fecha_pedido",
            "monto_neto",
            "monto_impuesto",
            "total_pedido",
            "estado",
            "estado_factura",
            "nota",
            "id_cliente",
            "id_vendedor",
        ]
    ]

    logger.info(f"Pedidos transformados finales: {len(df)}")

    return df