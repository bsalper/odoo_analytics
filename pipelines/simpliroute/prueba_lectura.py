import os
import requests
from dotenv import load_dotenv

# 1. Configuración
load_dotenv()
API_TOKEN = os.getenv("SIMPLIROUTE_API_TOKEN")
BASE_URL = "https://api.simpliroute.com/v1/routes/visits/" # Aqui esta el endpoint 

headers = {
    "Authorization": f"Token {API_TOKEN}",
    "Content-Type": "application/json"
}

def leer_terminal(fecha):
    params = {"planned_date": fecha} # Los parametros 
    print(f"Consultando SimpliRoute para el día: {fecha}...")
    
    try:
        #Aqui se lanza la peticion a internet combinando la URL, las lllaves de acceso y el filtro de fecha
        response = requests.get(BASE_URL, headers=headers, params=params)
        
        if response.status_code == 200:
            data = response.json()
            
            # Ajuste para el error: Si es lista, la usamos directamente
            visitas = data if isinstance(data, list) else data.get('results', [])
            
            if not visitas:
                print("No se encontraron visitas para esta fecha.")
                return

            print(f"\n DATOS RECUPERADOS: {len(visitas)} visitas")
            print(f"{'ID':<10} | {'ESTADO':<12} | {'REFERENCIA':<20} | {'TÍTULO'}")
            print("-" * 75)
            
            # Mostramos las primeras 15 visitas en la terminal
            """"
            for v in visitas[:15]:
                v_id = v.get('id', 'N/A')
                status = v.get('status', 'N/A')
                ref = str(v.get('reference', 'S/R'))[:20]
                title = str(v.get('title', 'Sin nombre'))[:25]
                
                # Iconos para scannear rápido con la vista
                icon = "🟢" if status == "success" else "🔴" if status == "failed" else "🟡"
                
                print(f"{v_id:<10} | {icon} {status:<9} | {ref:<20} | {title}")
            """

            # Mostramos todos los datos
            if visitas:
                print("---MOSTRANDO TODO EL CONTENIDO DE LA PRIMERA VISITA")
                import json
                print(json.dumps(visitas[0], indent=4))
                
        else:
            print(f"Error {response.status_code}: {response.text}")

    except Exception as e:
        print(f"Ocurrió un error al procesar los datos: {e}")

if __name__ == "__main__":
    # Prueba con la fecha de hoy
    leer_terminal("2026-01-12")