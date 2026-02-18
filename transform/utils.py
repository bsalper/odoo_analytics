import pandas as pd

def normalize_many2one_id(series):
    def extract_id(x):
        if not x or x in ["False", "None", "[]"]:
            return None

        if isinstance(x, (list, tuple)) and len(x) > 0:
            return x[0]

        x_str = str(x)
        if "[" in x_str and "," in x_str:
            parts = x_str.strip("[]").split(",", 1)
            return parts[0].strip()

        return None

    return series.apply(extract_id) if series is not None else None


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