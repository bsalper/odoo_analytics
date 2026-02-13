import pandas as pd

def transform_clients(clients_raw, valid_vendedor_ids):
    """
    Limpia y transforma clientes (res.partner) para BigQuery.
    Funciona tanto con datos directos de Odoo como con datos desde BigQuery RAW.
    """

    if not clients_raw:
        return pd.DataFrame()

    df = pd.DataFrame(clients_raw)

    # 1. Función robusta para extraer IDs (maneja "[ID, 'Nombre']" o [ID, "Nombre"])
    def extraer_id(x):
        x_str = str(x).strip()
        if not x_str or x_str in ["None", "False", "[]"]:
            return ""
        # Si viene como string de BigQuery: "[14, 'Vendedor']"
        if "[" in x_str:
            return x_str.split(',')[0].replace('[', '').replace("'", "").strip()
        return x_str

    # Aplicar limpieza a vendedor y etiquetas
    df["id_vendedor"] = df["user_id"].apply(extraer_id)
    
    df["etiquetas"] = df["category_id"].apply(
        lambda x: str(x).replace('[', '').replace(']', '').replace(' ', '') 
        if x and str(x) != "[]" else ""
    )

    # 2. Renombrar columnas
    df = df.rename(columns={
        "id": "id_cliente",
        "company_type": "tipo_compania",
        "type": "tipo_direccion",
        "name": "nombre_cliente",
        "vat": "rut",
        "visit_day": "dia_visita",
        "street": "calle",
        "city": "ciudad",
        "email": "correo",
        "phone": "telefono",
        "create_date": "fecha_creacion",
        "property_payment_term_id": "plazo_pago",
        "credit_limit": "credito_limite",
        "property_product_pricelist": "tarifa",
        "partner_latitude": "geo_latitud",
        "partner_longitude": "geo_longitud",
        "category_id": "etiquetas",
        "user_id": "id_vendedor"
    })

    # 3. Filtrar solo vendedores válidos
    # Convertimos ambos a string para asegurar el match
    valid_vendedor_ids = [str(v) for v in valid_vendedor_ids]
    df = df[df["id_vendedor"].isin(valid_vendedor_ids)]

    # 4. Seleccionar columnas finales
    cols_finales = [
        "id_cliente", "nombre_cliente", "rut", "correo", 
        "telefono", "ciudad", "fecha_creacion", "id_vendedor", "etiquetas"
    ]
    
    # Solo tomamos las columnas que existen para evitar errores
    df = df[[c for c in cols_finales if c in df.columns]]

    # 5. Limpieza final para BigQuery (Evitar valores nulos que rompan PyArrow)
    for col in df.columns:
        df[col] = df[col].fillna("").astype(str)

    return df