import pandas as pd
import ast
from utils.logger import get_logger
from transform.utils import (
    normalize_many2one_field,
    clean_and_serialize_dates,
    normalize_ids_to_string
)

logger = get_logger("transform_products")
PATRON_REFERENCIA = r'^[A-Z]{4}\d{3}$'

def transform_products(df_raw: pd.DataFrame, valid_impuestos_ids: list[str]):
    logger.info(f"Iniciando transformación: {len(df_raw)} registros")
    if df_raw.empty:
        return df_raw, pd.DataFrame()

    df = df_raw.copy()

    # 1. Normalización básica
    df["unidad_medida"] = normalize_many2one_field(df.get("uom_id"))
    df["id_categoria_odoo"] = normalize_many2one_field(df.get("categ_id"))

    # 2. Renombrado
    df = df.rename(columns={
        "id": "id_producto",
        "default_code": "referencia_interna",
        "name": "nombre_producto",
        "list_price": "precio_unitario",
        "standard_price": "coste_unitario",
        "create_date": "fecha_creacion",
        "taxes_id": "id_impuestos",
        "sale_ok": "puede_ser_vendido"
    })

    # 3. Conversión de Tipos de Datos (ESTRICTO)
    # VARCHAR -> String
    df["id_producto"] = df["id_producto"].astype(str)
    df["referencia_interna"] = df["referencia_interna"].fillna("").astype(str)
    
    # NUMERIC(22,8) -> To Numeric
    df["precio_unitario"] = pd.to_numeric(df["precio_unitario"], errors='coerce').fillna(0.0)
    df["coste_unitario"] = pd.to_numeric(df["coste_unitario"], errors='coerce').fillna(0.0)
    
    # BOOL
    # Odoo a veces envía strings 'True'/'False'. Esto asegura booleano puro.
    df["puede_ser_vendido"] = df["puede_ser_vendido"].map(
        {'True': True, 'False': False, True: True, False: False}
    ).fillna(False)

    # 4. Filtrado de productos
    # Solo productos con referencia válida y que no empiecen con 'INSU'
    df = df[df["referencia_interna"].str.match(PATRON_REFERENCIA, na=False)]
    df = df[~df["referencia_interna"].str.upper().str.startswith("INSU")]

    # 5. Lógica de Impuestos (Relación intermedia)
    relaciones = []
    valid_set = set(str(i) for i in valid_impuestos_ids)

    for _, row in df.iterrows():
        raw_taxes = row.get('id_impuestos') # Ya renombrado
        if pd.notna(raw_taxes) and raw_taxes not in ["[]", "False", False]:
            try:
                lista_ids = ast.literal_eval(raw_taxes) if isinstance(raw_taxes, str) else raw_taxes
                if isinstance(lista_ids, list):
                    for t_id in lista_ids:
                        t_id_str = str(t_id)
                        if t_id_str in valid_set:
                            relaciones.append({
                                'id_producto': str(row['id_producto']),
                                'id_impuesto': t_id_str
                            })
            except Exception as e:
                logger.warning(f"Error en impuestos de {row['nombre_producto']}: {e}")

    df_relacion = pd.DataFrame(relaciones)

    # 6. Selección final de columnas (Orden de la imagen)
    columnas_finales = [
        "id_producto", "referencia_interna", "nombre_producto", "unidad_medida",
        "precio_unitario", "coste_unitario", "fecha_creacion", "id_impuestos",
        "puede_ser_vendido", "categoria"
    ]
    df = df[[c for c in columnas_finales if c in df.columns]]

    # 7. Limpieza PyArrow Safe
    df = clean_and_serialize_dates(df, ["fecha_creacion"])
    
    # IMPORTANTE: No normalizamos id_producto a string aquí si ya lo hicimos arriba,
    # pero aseguramos consistencia.
    df = normalize_ids_to_string(df, ["id_producto"])

    return df, df_relacion