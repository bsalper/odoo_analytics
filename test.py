from connectors.odoo import get_odoo_client

def test_relation():
    try:
        # 1. Conectamos usando tu función
        client = get_odoo_client()
        
        # 2. Definimos qué buscar
        variante_id = 875 
        dominio = [["id", "=", variante_id]]
        campos = ["id", "display_name", "product_tmpl_id"]
        
        print(f"Buscando relación para el ID {variante_id}...")

        # 3. Usamos tu método search_read
        resultado = client.search_read('product.product', dominio, campos)

        if resultado:
            p = resultado[0]
            tmpl_info = p.get('product_tmpl_id')
            
            if tmpl_info:
                # El formato de un Many2one en Odoo es [ID, "Nombre"]
                tmpl_id = tmpl_info[0]
                tmpl_name = tmpl_info[1]
                
                print("\n" + "="*40)
                print("RESULTADO DE LA PRUEBA")
                print("="*40)
                print(f"ID Variante (Factura): {p['id']}")
                print(f"Nombre Variante:       {p['display_name']}")
                print(f"ID Template (Padre):   {tmpl_id}  <-- ¿ES EL 900?")
                print(f"Nombre Template:       {tmpl_name}")
                print("="*40)
            else:
                print("El producto se encontró, pero no tiene Template asociado.")
        else:
            print(f"No se encontró ningún producto con ID {variante_id}")

    except Exception as e:
        print(f"Error durante la ejecución: {e}")

if __name__ == "__main__":
    test_relation()