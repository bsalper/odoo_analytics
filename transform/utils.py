import pandas as pd
import ast


def extract_many2one_id(value):
    if not value or value in [False, "None", "False", "[]", ""]:
        return None

    # Si ya es lista/tupla real
    if isinstance(value, (list, tuple)) and len(value) > 0:
        try:
            return int(value[0])
        except:
            return None

    # Si es string que parece lista
    if isinstance(value, str):
        x_str = value.strip()

        if x_str.startswith("["):
            try:
                parsed = ast.literal_eval(x_str)
                if isinstance(parsed, (list, tuple)) and len(parsed) > 0:
                    return int(parsed[0])
            except:
                return None

        try:
            return int(float(x_str))
        except:
            return None

    return None


def clean_and_serialize_dates(df, date_cols):
    for col in date_cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .replace(["False", "false", "None", "nan"], None)
            )

            df[col] = pd.to_datetime(df[col], errors="coerce")

            df[col] = df[col].apply(
                lambda x: x.isoformat() if pd.notna(x) else None
            )

    return df


def normalize_ids_to_string(df, id_cols):
    for col in id_cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .replace(["<NA>", "nan", "False", "None"], None)
            )

            df[col] = df[col].apply(
                lambda x: x.split(".")[0] if x and "." in x else x
            )

    return df


def extract_many2one_name(value):
    if not value or value in [False, "None", "False", "[]", ""]:
        return None

    # Si ya es lista real
    if isinstance(value, (list, tuple)) and len(value) > 1:
        return str(value[1])

    # Si es string que parece lista
    if isinstance(value, str) and value.startswith("["):
        try:
            parsed = ast.literal_eval(value)
            if isinstance(parsed, (list, tuple)) and len(parsed) > 1:
                return str(parsed[1])
        except:
            return None

    return None


def extract_many2many_names(value):
    if not value or value in [False, "None", "False", "[]", ""]:
        return None

    # Si ya es lista real
    if isinstance(value, list):
        nombres = [
            str(item[1])
            for item in value
            if isinstance(item, (list, tuple)) and len(item) > 1
        ]
        return ",".join(nombres) if nombres else None

    # Si es string que parece lista de listas
    if isinstance(value, str) and value.startswith("["):
        try:
            parsed = ast.literal_eval(value)
            if isinstance(parsed, list):
                nombres = [
                    str(item[1])
                    for item in parsed
                    if isinstance(item, (list, tuple)) and len(item) > 1
                ]
                return ",".join(nombres) if nombres else None
        except:
            return None

    return None