# productor.py

import requests
import time

BROKER_URL = "http://localhost:5000"
COLA_NOMBRE = "cola_de_prueba"

# 1. Declarar la cola (MODIFICADO: durable=True)
try:
    r = requests.post(
        f"{BROKER_URL}/declarar_cola", 
        json={"nombre": COLA_NOMBRE, "durable": True} # <-- NUEVO
    )
    r.raise_for_status()
    print(f"Productor: Cola '{COLA_NOMBRE}' declarada (Durable: True).")
except requests.exceptions.RequestException as e:
    print(f"Productor: Error al declarar cola: {e}")
    exit(1)

# 2. Enviar mensajes
print("Productor: Enviando mensajes duraderos. Presiona CTRL+C para parar.")
i = 0
while True:
    try:
        mensaje = f"Mensaje duradero {i}"
        r = requests.post(
            f"{BROKER_URL}/publicar", 
            json={
                "nombre": COLA_NOMBRE, 
                "mensaje": mensaje,
                "durable": True # <-- NUEVO
            }
        )
        r.raise_for_status()
        
        print(f"Productor: Mensaje '{mensaje}' enviado.")
        
        i += 1
        time.sleep(2)
        
    except requests.exceptions.RequestException as e:
        print(f"Productor: Error al publicar: {e}")
        time.sleep(5)
    except KeyboardInterrupt:
        print("\nProductor: Detenido.")
        break