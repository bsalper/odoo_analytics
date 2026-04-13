import pandas as pd
import ast
from .utils import extract_many2one_name
from utils.logger import get_logger

logger = get_logger("transform_clientes")

def transform_clients(clients_raw, tag_map=None, valid_vendedor_ids=None):
    if not clients_raw:
        return pd.DataFrame()

    df = pd.DataFrame(clients_raw)

    if tag_map is None:
        tag_map = {}

    # 1. Función para extraer ID de campos Many2one
    def extraer_id(x):
        if not x or x in [False, "None", "False", "[]", ""]:
            return None

        if isinstance(x, (list, tuple)) and len(x) > 0:
            return x[0]

        x_str = str(x).strip()

        if x_str.startswith("["):
            try:
                parsed = ast.literal_eval(x_str)
                if isinstance(parsed, (list, tuple)) and len(parsed) > 0:
                    return parsed[0]
            except:
                return None

        try:
            return int(float(x_str))
        except:
            return None


    # 2. Normalización Many2one
    df["id_vendedor"] = df.get("user_id", pd.Series()).apply(extraer_id)
    df["id_plazo_pago"] = df.get("property_payment_term_id", pd.Series()).apply(extract_many2one_name)
    df["id_tarifa"] = df.get("property_product_pricelist", pd.Series()).apply(extract_many2one_name)
    df["comuna"] = df.get("city_id", pd.Series()).apply(extract_many2one_name)

    # 3. Normalización Many2many (ETIQUETAS)
    def map_tags(val):
        if not val:
            return ""

        if isinstance(val, str) and val.startswith("["):
            try:
                val = ast.literal_eval(val)
            except:
                return ""

        if isinstance(val, list):
            names = [tag_map.get(tag_id, "") for tag_id in val]
            return ", ".join([n for n in names if n])

        return ""

    df["etiquetas"] = df.get("category_id", pd.Series()).apply(map_tags)

    # 4. Renombrado columnas
    df = df.rename(columns={
        "id": "id_cliente",
        "company_type": "tipo_compania",
        "type": "tipo_direccion",
        "commercial_company_name": "nombre_cliente",
        "vat": "rut",
        "visit_day": "dia_visita",
        "street": "calle",
        "street2": "calle2",
        "city": "ciudad",
        "email": "correo",
        "phone": "telefono",
        "create_date": "fecha_creacion",
        "credit_limit": "credito_limite",
        "partner_latitude": "geo_latitud",
        "partner_longitude": "geo_longitud",
    })


    # 5. Conversión de tipos
    for col in ["id_cliente", "id_vendedor"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in ["credito_limite", "geo_latitud", "geo_longitud"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
            if col in ["geo_latitud", "geo_longitud"]:
                df[col] = df[col].round(6)

    if "fecha_creacion" in df.columns:
        df["fecha_creacion"] = pd.to_datetime(df["fecha_creacion"], errors="coerce")

    # 6. Normalización strings
    str_cols = [
        "nombre_cliente", "rut", "correo", "telefono",
        "ciudad", "calle", "tipo_compania", "id_tarifa",
        "tipo_direccion", "dia_visita", "id_plazo_pago",
        "calle2", "comuna"
    ]

    for col in str_cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .fillna("")
                .astype(str)
                .replace(["None", "False", "nan", "<NA>"], "")
            )

    # 7. Filtros de lógica de negocio (Personas vs Compañías)
    if "tipo_compania" in df.columns and "tipo_direccion" in df.columns:
        df = df[~(
            (df["tipo_compania"] == "person") &
            (df["tipo_direccion"] == "contact")
        )]

    # 8. Deduplicación
    if "calle" in df.columns:
        df["calle_normalizada"] = (
            df["calle"]
            .str.lower()
            .str.strip()
            .str.replace(r"\s+", " ", regex=True)
        )

        df = df.drop_duplicates(
            subset=["rut", "calle_normalizada", "id_vendedor"],
            keep="first"
        )

        df = df.drop(columns=["calle_normalizada"])

    # 9. Filtrar vendedores válidos
    if valid_vendedor_ids:
        valid_set = set(pd.Series(valid_vendedor_ids).astype("Int64").dropna())
        df = df[df["id_vendedor"].isin(valid_set)]

    # 10. Selección final
    cols_finales = [
        "id_cliente", "tipo_compania", "tipo_direccion",
        "nombre_cliente", "rut", "dia_visita",
        "calle", "calle2", "comuna", "ciudad",
        "correo", "telefono", "fecha_creacion",
        "id_plazo_pago", "credito_limite",
        "id_tarifa", "geo_latitud",
        "geo_longitud", "id_vendedor", "etiquetas"
    ]

    return df[[c for c in cols_finales if c in df.columns]].reset_index(drop=True)