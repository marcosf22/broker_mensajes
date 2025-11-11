# admin.py

import requests
import sys

BROKER_URL = "http://localhost:5000"

def listar_colas():
    """
    Llama al endpoint GET /colas del broker.
    """
    try:
        r = requests.get(f"{BROKER_URL}/colas")
        r.raise_for_status()
        data = r.json()
        
        print("\n--- Colas existentes en el Broker ---")
        if not data["colas"]:
            print("     (Ninguna)")
        else:
            for cola in data["colas"]:
                print(f"  - {cola}")
        print("-------------------------------------\n")
        
    except requests.exceptions.RequestException as e:
        print(f"\n[Error] No se pudo contactar al broker: {e}")

def borrar_cola(nombre_cola):
    """
    Llama al endpoint DELETE /colas/<nombre> del broker.
    """
    try:
        r = requests.delete(f"{BROKER_URL}/colas/{nombre_cola}")
        r.raise_for_status() # Lanza error si es 4xx o 5xx
        
        data = r.json()
        print(f"\n[Éxito] {data.get('status')}: '{data.get('cola')}'\n")
        
    except requests.exceptions.RequestException as e:
        if e.response and e.response.status_code == 404:
            print(f"\n[Error] La cola '{nombre_cola}' no fue encontrada en el broker.\n")
        else:
            print(f"\n[Error] No se pudo contactar al broker: {e}\n")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso:")
        print("  python admin.py list            -> Lista todas las colas")
        print("  python admin.py delete [nombre] -> Borra una cola específica")
        sys.exit(1)

    comando = sys.argv[1].lower()

    if comando == "list":
        listar_colas()
    elif comando == "delete":
        if len(sys.argv) < 3:
            print("Error: Debes especificar el nombre de la cola a borrar.")
            print("Uso: python admin.py delete [nombre_cola]")
            sys.exit(1)
        
        nombre = sys.argv[2]
        print(f"Intentando borrar la cola: '{nombre}'...")
        borrar_cola(nombre)
    else:
        print(f"Error: Comando '{comando}' no reconocido.")
        print("Comandos válidos: 'list', 'delete'")