import os
import json
import pandas as pd
import numpy as np
import pyodbc
import requests
from datetime import datetime
from urllib3.exceptions import InsecureRequestWarning

# Importamos las librerías oficiales de Google
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Desactivar advertencias de SSL de manera segura
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

# --- FUNCIÓN GLOBAL AUXILIAR ---
def get_column_letter(n):
    string = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        string = chr(65 + remainder) + string
    return string


# 1. Configurar conexión a tu SQL Server de Mobiliza
conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=srv-mobiliza\\SQLEXPRESS;"
    "DATABASE=Mobiliza-3.6;"
    "UID=movingfood;"
    "PWD=mf2026#;"
)

conn = pyodbc.connect(conn_str)

# 2. Leer la vista limpia (Para el Reporte Final)
query = "SELECT * FROM dbo.v_Reporte_Gestion_Mobiliza;"
df_base = pd.read_sql(query, conn)

# --- PROCESO 1: REPORTE FINAL ---
df = df_base.copy()

# Normalizamos la fecha creacion para ordenar bien en memoria
df["Fecha Creacion"] = pd.to_datetime(df["Fecha Creacion"], dayfirst=True, errors='coerce')

# Ordenar por Fecha, Vendedor y Hora Creacion (Descendente)
df = df.sort_values(
    by=["Fecha Creacion", "Vendedor", "Hora Creacion"], 
    ascending=[False, False, False]
)

# Mantenemos formato tradicional (Día/Mes/Año)
df["Fecha Creacion"] = df["Fecha Creacion"].dt.strftime('%d/%m/%Y')

# Forzar a que la hora se quede SOLO con Hora y Minuto (HH:MM)
df["Hora Creacion"] = df["Hora Creacion"].astype(str).str.slice(0, 5)

# Cálculo de Dispersión en Metros (Fórmula de Haversine)
def calcular_dispersion(row):
    try:
        coord_ficha = str(row["Coordenadas Ficha"]).split(",")
        coord_real = str(row["Coordenadas Real"]).split(",")
        if len(coord_ficha) < 2 or len(coord_real) < 2:
            return 0.0
        lat1, lon1 = float(coord_ficha[0]), float(coord_ficha[1])
        lat2, lon2 = float(coord_real[0]), float(coord_real[1])
        lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
        c = 2 * np.arcsin(np.sqrt(a))
        metros = 6371000 * c
        return round(metros, 1)
    except:
        return 0.0

df["Dispersion"] = df.apply(calcular_dispersion, axis=1)

# Convertir ID Pedido y Total $ a números enteros limpios
if "ID Pedido" in df.columns:
    df["ID Pedido"] = pd.to_numeric(df["ID Pedido"], errors='coerce').fillna(0).astype(int)

if "Total $" in df.columns:
    df["Total $"] = pd.to_numeric(df["Total $"], errors='coerce').fillna(0).astype(int)

# Formatear Dispersión con COMA para evitar errores de región
if "Dispersion" in df.columns:
    df["Dispersion"] = pd.to_numeric(df["Dispersion"], errors='coerce').fillna(0.0)
    df["Dispersion"] = df["Dispersion"].round(1).astype(str).str.replace('.', ',', regex=False)

# Sanitizar tipos de datos para la API de Google
df = df.astype(str)
df = df.replace(["None", "NaT", "NaN", "<NA>"], "")
df = df.fillna("")


### MAESTRO DE CLIENTES CON VENDEDORES REALES ASIGNADOS ###
print("Cargando el universo completo de clientes desde SQL...")

# 1. Traemos el mapa de rutas limpio (Días de visita) directamente de SQL como texto
query_rutas = """
    SELECT c.idCliente, r.nombre AS nombreRuta, r.diasVisita
    FROM [Mobiliza-3.6].[dbo].[Clientes] c
    LEFT JOIN [Mobiliza-3.6].[dbo].[Rutas] r ON c.idRuta = r.idRuta
"""
df_rutas_raw = pd.read_sql(query_rutas, conn)
df_rutas_raw['idCliente'] = df_rutas_raw['idCliente'].astype(str).str.strip()
# Eliminamos duplicados del mapa de rutas basándonos en el ID como texto
df_rutas_map = df_rutas_raw.groupby('idCliente').first().reset_index()

# 2. Traemos el universo total de clientes
query_clientes_completo = """
    SELECT 
        CAST(idCliente AS VARCHAR(50)) AS [ID Cliente Raw],
        nombre AS [Nombre Cliente],
        direccion AS [Direccion]
    FROM [Mobiliza-3.6].[dbo].[Clientes]
"""
df_maestro_raw = pd.read_sql(query_clientes_completo, conn)

# Tratamos el ID como texto limpio y rellenamos los nulos absolutos de SQL para no perder la fila
df_maestro_raw['ID Cliente Raw'] = df_maestro_raw['ID Cliente Raw'].fillna("0").astype(str).str.strip()
# Si por algún motivo el ID quedó vacío o literal 'None' tras el strip, aseguramos un valor
df_maestro_raw['ID Cliente Raw'] = df_maestro_raw['ID Cliente Raw'].replace(['', 'None'], '0')

# Agrupamos por este ID de texto para consolidar y eliminar duplicados reales de la tabla base
df_maestro = df_maestro_raw.groupby('ID Cliente Raw').first().reset_index()

# Creamos una columna limpia para mostrar en la columna B (si es numérico queda como entero, si no, mantiene su texto original)
def limpiar_id_visual(val):
    try:
        return str(int(float(val)))
    except:
        return str(val)

df_maestro["ID Cliente"] = df_maestro["ID Cliente Raw"].apply(limpiar_id_visual)

# 3. EXTRAER VENDEDORES REALES: Usamos la vista de gestión histórica
df_vendedores_visitas = df_base.dropna(subset=['ID Cliente', 'Vendedor']).copy()
df_vendedores_visitas['ID Cliente Link'] = df_vendedores_visitas['ID Cliente'].astype(str).str.strip()
df_mapa_vendedores = df_vendedores_visitas.groupby('ID Cliente Link')['Vendedor'].last().reset_index()

# 4. Cruzamos toda la información hacia nuestra matriz de 4627 filas
df_maestro = df_maestro.merge(df_mapa_vendedores, left_on="ID Cliente Raw", right_on="ID Cliente Link", how="left")
df_maestro = df_maestro.merge(df_rutas_map, left_on="ID Cliente Raw", right_on="idCliente", how="left")

# Rellenamos nulos de vendedores que no registran visitas para evitar que se rompa el formato
df_maestro["Vendedor"] = df_maestro["Vendedor"].fillna("Ruta General")

# 5. Generar los campos calculados exigidos por tu formato
df_maestro["Clave"] = df_maestro["ID Cliente"].astype(str) + " - " + df_maestro["Vendedor"]
df_maestro["Dia Visita"] = df_maestro["nombreRuta"].fillna(df_maestro["diasVisita"]).fillna("")

# Seleccionar y ordenar las 6 columnas exactas de tu formato final
columnas_maestro = ["Clave", "ID Cliente", "Nombre Cliente", "Direccion", "Vendedor", "Dia Visita"]
df_maestro = df_maestro[columnas_maestro]

# Sanitizar strings finales para el traspaso a la API de Google Sheets
df_maestro = df_maestro.astype(str)
df_maestro = df_maestro.replace(["None", "NaT", "NaN", "<NA>"], "")
df_maestro = df_maestro.fillna("")


# 3. Autenticarse en Google usando el Secreto de GitHub
secret_info = os.environ["GOOGLE_CREDENTIALS"]
secret_creds_dict = json.loads(secret_info, strict=False)

scopes = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_info(secret_creds_dict, scopes=scopes)

# Construcción segura del cliente API
service = build('sheets', 'v4', credentials=creds)
spreadsheet = service.spreadsheets()

# ID del archivo compartido de Google Sheets
spreadsheet_id = '1hOkh28ZXJBjgra3HWV1Oe2Av6qOXHX0cteNRshWWR_c' 


# 4. CARGA DE DATOS - PESTAÑA: Reporte_Final
sheet_name_1 = 'Reporte_Final'
data_to_upload_1 = [df.columns.values.tolist()] + df.values.tolist()

last_column_letter = get_column_letter(df.shape[1])
data_range_1 = f"{sheet_name_1}!A1:{last_column_letter}60000"

# Limpieza y carga de Reporte_Final
spreadsheet.values().clear(spreadsheetId=spreadsheet_id, range=data_range_1).execute()
spreadsheet.values().update(
    spreadsheetId=spreadsheet_id,
    range=data_range_1,
    valueInputOption="USER_ENTERED",
    body={"values": data_to_upload_1}
).execute()
print(f"Sincronización de registros lista en {sheet_name_1}.")


### CARGA DE DATOS - PESTAÑA: Maestro_Clientes ###
sheet_name_2 = 'Maestro_Clientes'
data_to_upload_2 = [df_maestro.columns.values.tolist()] + df_maestro.values.tolist()

# El maestro usa las 6 columnas completas (de la A a la F)
data_range_2 = f"{sheet_name_2}!A1:F80000"

# Limpieza y carga dirigida en Maestro_Clientes
spreadsheet.values().clear(spreadsheetId=spreadsheet_id, range=data_range_2).execute()
spreadsheet.values().update(
    spreadsheetId=spreadsheet_id,
    range=data_range_2,
    valueInputOption="USER_ENTERED",
    body={"values": data_to_upload_2}
).execute()
print(f"Sincronización exitosa. ¡{len(df_maestro)} clientes totales cargados en {sheet_name_2}!")