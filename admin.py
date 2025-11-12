import requests, sys


def listar_colas():
    """
    Solicita la lista de colas del broker.
    """
    try:
        r = requests.get(f"{BROKER_URL}/colas")
        r.raise_for_status()
        data = r.json()
        
        print("\nColas existentes en el Broker:")
        if not data["colas"]:
            print("     (Ninguna)")
        else:
            for cola in data["colas"]:
                print(f"  - {cola}")
        
    except requests.exceptions.RequestException as e:
        print(f"\nNo se pudo contactar al broker: {e}")

def borrar_cola(nombre_cola):
    """
    Solicita el borrado de una cola del broker.
    """
    try:
        r = requests.delete(f"{BROKER_URL}/colas/{nombre_cola}")
        r.raise_for_status() # Lanza error si es 4xx o 5xx
        
        data = r.json()
        print(f"\nÉxito {data.get('status')}: '{data.get('cola')}'\n")
        
    except requests.exceptions.RequestException as e:
        if e.response and e.response.status_code == 404:
            print(f"\nLa cola '{nombre_cola}' no fue encontrada en el broker.\n")
        else:
            print(f"\nNo se pudo contactar al broker: {e}\n")

if __name__ == "__main__":

    print("\nBienvenido al Admin.\n")

    ip = input("Introduce la IP del broker: ").strip()
    BROKER_URL = "http://" + ip + ":5000"

    opcion = "0"
    while opcion != "5":

        print("\nOpciones:\n")
        print("     1. Listar colas existentes.")
        print("     2. Borrar cola.")
        print("     3. Salir.\n")

        opcion = input("Seleccione una opcion: ").strip()
        
        if opcion == '1':
            listar_colas()

        elif opcion == '2':
            nombre_cola = input("\nElige el nombre de la cola a borrar: ")
            borrar_cola(nombre_cola)

        elif opcion == '3':
            break

    print("Administrador finalizado.")