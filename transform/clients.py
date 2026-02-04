import pandas as pd


def transform_clients(clients_raw, valid_vendedor_ids):
    """
    Limpia y transforma clientes (res.partner) para BigQuery
    """

    if not clients_raw:
        return pd.DataFrame()

    df = pd.DataFrame(clients_raw)

    # =========================
    # Normalización Odoo (many2one / many2many)
    # =========================
    df["id_vendedor"] = df["user_id"].apply(
        lambda x: str(x[0]) if isinstance(x, list) and x else ""
    )

    df["etiquetas"] = df["category_id"].apply(
        lambda x: ",".join(map(str, x)) if isinstance(x, list) else ""
    )

    # =========================
    # Renombrar columnas
    # =========================
    df = df.rename(columns={
        "id": "id_cliente",
        "name": "nombre_cliente",
        "vat": "rut",
        "email": "correo",
        "phone": "telefono",
        "city": "ciudad",
        "create_date": "fecha_creacion"
    })

    # =========================
    # Filtrar solo vendedores válidos
    # =========================
    df["id_vendedor"] = df["id_vendedor"].astype(str)
    df = df[df["id_vendedor"].isin(valid_vendedor_ids)]

    # =========================
    # Seleccionar columnas finales
    # =========================
    df = df[
        [
            "id_cliente",
            "nombre_cliente",
            "rut",
            "correo",
            "telefono",
            "ciudad",
            "fecha_creacion",
            "id_vendedor",
            "etiquetas"
        ]
    ]

    # =========================
    # LIMPIEZA PYARROW SAFE
    # =========================
    # Forzar strings SEGURAS para BigQuery
    for col in df.columns:
        if col != "fecha_creacion":
            df[col] = df[col].fillna("").astype(str)

    # Fecha como STRING limpia
    df["fecha_creacion"] = (
        df["fecha_creacion"]
        .fillna("")
        .astype(str)
    )

    return df