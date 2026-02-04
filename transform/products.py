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
        "name": "nombre_producto",
        "default_code": "referencia_interna",
        "standard_price": "coste_unitario",
        "list_price": "precio_unitario",
        "create_date": "fecha_creacion",
        "sale_ok": "puede_ser_vendido"
    })

    # 3. Filtrado de productos (Referencia interna, etc.)
    df = df[df["referencia_interna"].notna()]
    df = df[df["referencia_interna"].astype(str).str.match(PATRON_REFERENCIA, na=False)]
    df = df[~df["referencia_interna"].astype(str).str.upper().str.startswith("INSU")]

    # 4. CREACIÓN DE TABLA INTERMEDIA (Many-to-Many)
    relaciones = []
    # Convertimos tus 6 impuestos a un set de strings para comparación rápida
    valid_set = set(str(i) for i in valid_impuestos_ids)

    for _, row in df.iterrows():
        # Obtenemos los impuestos originales de la columna taxes_id
        raw_taxes = row.get('taxes_id')
        
        if pd.notna(raw_taxes) and raw_taxes != "[]" and raw_taxes is not False:
            try:
                # Convertir "[1, 14, 41...]" a lista [1, 14, 41...]
                lista_ids = ast.literal_eval(raw_taxes) if isinstance(raw_taxes, str) else raw_taxes
                
                if isinstance(lista_ids, list):
                    for t_id in lista_ids:
                        t_id_str = str(t_id)
                        # Si el ID de Odoo está en tu lista de 6 impuestos permitidos, lo guardamos
                        if t_id_str in valid_set:
                            relaciones.append({
                                'id_producto': str(row['id_producto']),
                                'id_impuesto': t_id_str
                            })
            except Exception as e:
                logger.error(f"Error procesando impuestos para {row['nombre_producto']}: {e}")
                continue

    df_relacion = pd.DataFrame(relaciones)

    # 5. Selección final de columnas para la tabla 'products'
    columnas_finales = [
        "id_producto", "referencia_interna", "nombre_producto",
        "unidad_medida", "precio_unitario", "coste_unitario",
        "fecha_creacion", "puede_ser_vendido"
    ]
    df = df[[c for c in columnas_finales if c in df.columns]]

    # Limpieza final
    df = clean_and_serialize_dates(df, ["fecha_creacion"])
    df = normalize_ids_to_string(df, ["id_producto", "unidad_medida"])

    return df, df_relacion