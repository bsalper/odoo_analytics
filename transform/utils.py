import pandas as pd
import re

def normalize_many2one_field(series):
    """
    Extrae el NOMBRE (el segundo elemento) de un campo Many2one de Odoo.
    Maneja tanto listas [ID, "Nombre"] como strings "[ID, 'Nombre']".
    """
    def extract_name(x):
        if not x or x in ["False", "None", "[]"]:
            return None
        
        x_str = str(x)
        if "[" in x_str and "," in x_str:
            # Buscamos lo que está después de la primera coma
            # Ejemplo: "[15, 'Unidades']" -> " 'Unidades']"
            parts = x_str.split(',', 1)
            if len(parts) > 1:
                name_part = parts[1]
                # Limpiamos comillas, corchetes y espacios
                return name_part.replace("'", "").replace('"', "").replace("]", "").strip()
        return x_str

    return series.apply(extract_name) if series is not None else None

def clean_and_serialize_dates(df, date_cols):
    for col in date_cols:
        if col in df.columns:
            # Reemplazamos falsos de Odoo por nulos reales
            df[col] = df[col].astype(str).replace(["False", "false", "None", "nan"], None)
            df[col] = pd.to_datetime(df[col], errors="coerce")
            df[col] = df[col].apply(
                lambda x: x.isoformat() if pd.notna(x) else None
            )
    return df

def normalize_ids_to_string(df, id_cols):
    for col in id_cols:
        if col in df.columns:
            # Aseguramos que el ID sea un string limpio sin el .0 de los floats
            df[col] = df[col].astype(str).replace(["<NA>", "nan", "False", "None"], None)
            df[col] = df[col].apply(lambda x: x.split('.')[0] if x and '.' in x else x)
    return df