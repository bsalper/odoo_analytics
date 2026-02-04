import pandas as pd

def normalize_many2one_field(series):
    return series.apply(lambda x: x[0] if isinstance(x, list) and x else None)

def clean_and_serialize_dates(df, date_cols):
    for col in date_cols:
        if col in df.columns:
            df[col] = df[col].replace("false", None)
            df[col] = pd.to_datetime(df[col], errors="coerce")
            df[col] = df[col].apply(
                lambda x: x.isoformat() if pd.notna(x) else None
            )
    return df

def normalize_ids_to_string(df, id_cols):
    for col in id_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).replace("<NA>", None)
    return df