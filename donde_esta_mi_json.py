import os
import json
import gspread
from google.oauth2.service_account import Credentials

def test_simple():
    cert_path = r"C:\Users\MF\Documents\odoo_analytics\credentials_google\odoo-analytics-482120-4a4cd8457bc7.json"
    
    print(f"--- Probando archivo: {cert_path} ---")
    
    if not os.path.exists(cert_path):
        print("ERROR: El archivo físicamente no existe en esa ruta.")
        return

    try:
        with open(cert_path, 'r') as f:
            info = json.load(f)
        
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(info, scopes=scopes)
        client = gspread.authorize(creds)
        
        print("✅ CONEXIÓN EXITOSA: El archivo JSON es válido y Python lo lee bien.")
        
    except Exception as e:
        print(f"❌ FALLÓ LA PRUEBA: {e}")

if __name__ == "__main__":
    test_simple()