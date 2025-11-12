import requests, time

def declarar_cola(nombre_cola, durable):
        
    # Declarar la cola
    try:
        r = requests.post(
            f"{BROKER_URL}/declarar_cola", 
            json={"nombre": nombre_cola, "durable": durable}
        )
        r.raise_for_status()
        print(f"Cola '{nombre_cola}' declarada (Durable: {durable}).")
    except requests.exceptions.RequestException as e:
        print(f"Productor: Error al declarar cola: {e}")
        exit(1)

def enviar_mensajes(nombre_cola, durable, numero):

    # Enviar mensajes
    i = 0
    while i < numero:
        try:
            mensaje = f"Mensaje duradero={durable} ({i})"
            r = requests.post(
                f"{BROKER_URL}/publicar", 
                json={
                    "nombre": nombre_cola, 
                    "mensaje": mensaje,
                    "durable": durable
                }
            )
            r.raise_for_status()
            
            print(f"Mensaje '{mensaje}' enviado a la cola '{nombre_cola}'.")
            
            i += 1
            time.sleep(2)
            
        except requests.exceptions.RequestException as e:
            print(f"Error al publicar: {e}")
            time.sleep(5)
        except KeyboardInterrupt:
            print("\nProductor: Detenido.")
            break

if __name__ == '__main__':

    # ip = input("Introduce la IP del broker: ")
    # puerto = input("Introduce el puerto del broker: ")
    # BROKER_URL = "http://" + ip + ":" + puerto   
    BROKER_URL = "http://localhost:5000" 

    print("\nBienvenido al Productor.\n")
    opcion = "0"
    while opcion not in ["1", "2", "3", "4", "5"]:

        print("Opciones:\n")
        print("     1. Declarar cola duradera.")
        print("     2. Declarar cola NO duradera.")
        print("     3. Iniciar envio de mensajes duraderos.")
        print("     4. Iniciar envio de mensajes NO duraderos.")
        print("     5. Salir.\n")

        opcion = input("Seleccione una opcion: ").strip()
        
        if opcion == '1':
            nombre_cola = input("\nElige un nombre para la cola duradera: ")
            declarar_cola(nombre_cola, durable=True)

        elif opcion == '2':
            nombre_cola = input("\nElige un nombre para la cola NO duradera: ")
            declarar_cola(nombre_cola, durable=False)

        elif opcion == '3':
            nombre_cola = input("\nElige el nombre de la cola duradera para enviar mensajes: ")
            num = int(input("Numero de mensajes a enviar: "))
            enviar_mensajes(nombre_cola, durable=True, numero=num)

        elif opcion == '4':
            nombre_cola = input("\nElige el nombre de la cola NO duradera para enviar mensajes: ")
            num = int(input("Numero de mensajes a enviar: "))
            enviar_mensajes(nombre_cola, durable=False, numero=num)

        elif opcion == '5':
            print("\nSaliendo...")
            break

    print("Productor finalizado.")