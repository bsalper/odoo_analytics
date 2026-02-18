import pandas as pd
import numpy as np

def transform_clients(clients_raw, valid_vendedor_ids=None):
    if not clients_raw:
        return pd.DataFrame()

    df = pd.DataFrame(clients_raw)

    # --- 1. Función para extraer IDs de campos Many2one [ID, "Nombre"]
    def extraer_id(x):
        if not x or x in [False, "None", "False", "[]", ""]:
            return None
        if isinstance(x, (list, tuple)) and len(x) > 0:
            return x[0]
        x_str = str(x).strip()
        if x_str.startswith("["):
            try:
                return int(x_str.replace("[", "").replace("]", "").split(",")[0])
            except:
                return None
        try:
            return int(float(x))
        except:
            return None

    # --- 2. Normalización de campos complejos ---
    df["id_vendedor"] = df.get("user_id").apply(extraer_id)
    df["id_plazo_pago"] = df.get("property_payment_term_id").apply(extraer_id)
    df["id_tarifa"] = df.get("property_product_pricelist").apply(extraer_id)

    # Etiquetas: Convertimos lista de IDs [1,2] a "1,2"
    df["etiquetas"] = df.get("category_id").apply(lambda x: ",".join(map(str, x)) if isinstance(x, list) else "")

    # --- 3. Renombrado de columnas ---
    rename_map = {
        "id": "id_cliente",
        "company_type": "tipo_compania",
        "type": "tipo_direccion",
        "commercial_company_name": "nombre_cliente",
        "vat": "rut",
        "visit_day": "dia_visita",
        "street": "calle",
        "city": "ciudad",
        "email": "correo",
        "phone": "telefono",
        "create_date": "fecha_creacion",
        "credit_limit": "credito_limite",
        "partner_latitude": "geo_latitud",
        "partner_longitude": "geo_longitud"
    }
    df = df.rename(columns=rename_map)

    # --- 4. Conversión de tipos ---
    for col in ["id_cliente", "id_vendedor", "id_plazo_pago", "id_tarifa"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in ["credito_limite", "geo_latitud", "geo_longitud"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    if "fecha_creacion" in df.columns:
        df["fecha_creacion"] = pd.to_datetime(df["fecha_creacion"], errors="coerce")

    str_cols = ["nombre_cliente", "rut", "correo", "telefono", "ciudad", "calle",
                "tipo_compania", "tipo_direccion", "dia_visita"]
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).replace(["None", "False", "nan"], "")

    # --- 5. Filtrar clientes archivados ---
    if "active" in df.columns:
        df["active"] = (
            df["active"]
            .map({"True": True, "False": False, True: True, False: False})
            .fillna(False)
            .astype(bool)
        )

        df = df[df["active"]]

    # --- 6. Filtrado de individuo/contacto ---
    df = df[~((df["tipo_compania"] == "person") & (df["tipo_direccion"] == "contact"))]

    # --- 7. Eliminar duplicados por rut + calle + vendedor ---
    df = df.drop_duplicates(subset=["rut", "calle", "id_vendedor"], keep="first")

    # --- 8. Filtrar vendedores válidos ---
    if valid_vendedor_ids:
        valid_set = set(pd.Series(valid_vendedor_ids).astype("Int64").dropna())
        df = df[df["id_vendedor"].isin(valid_set)]

    # --- 9. Selección final de columnas ---
    cols_finales = [
        "id_cliente", "tipo_compania", "tipo_direccion", "nombre_cliente",
        "rut", "dia_visita", "calle", "ciudad", "correo", "telefono",
        "fecha_creacion", "id_plazo_pago", "credito_limite", "id_tarifa",
        "geo_latitud", "geo_longitud", "id_vendedor", "etiquetas"
    ]
    return df[[c for c in cols_finales if c in df.columns]].reset_index(drop=True)
