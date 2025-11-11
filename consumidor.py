# consumidor.py

import requests
from flask import Flask, request, jsonify
import threading
import time
import random

# --- Configuración del Consumidor ---
BROKER_URL = "http://localhost:5000"
COLA_NOMBRE = "cola_de_prueba" # Debe coincidir con el productor

MI_PUERTO = 5001
MI_URL_CALLBACK = f"http://localhost:{MI_PUERTO}/callback"


# --- Parte Servidor (sin cambios) ---
app_consumidor = Flask(__name__)

def procesar_mensaje_y_enviar_ack(message_id, mensaje):
    # (Sin cambios... sigue procesando y enviando ACK)
    try:
        print(f"CONSUMIDOR ({MI_PUERTO}): --> RECIBIDO: '{mensaje}' (ID: {message_id}). Procesando...")
        time.sleep(2)
        
        if random.random() < 0.3:
            print(f"CONSUMIDOR ({MI_PUERTO}): X-- FALLO SIMULADO para {message_id}. No se enviará ACK.")
            return 

        print(f"CONSUMIDOR ({MI_PUERTO}): <-- Procesado OK. Enviando ACK para {message_id}.")
        requests.post(
            f"{BROKER_URL}/ack",
            json={"message_id": message_id, "nombre_cola": COLA_NOMBRE},
            timeout=2
        )
    except Exception as e:
        print(f"CONSUMIDOR ({MI_PUERTO}): Error: {e}")

@app_consumidor.route('/callback', methods=['POST'])
def recibir_mensaje():
    # (Sin cambios)
    data = request.json
    mensaje = data.get('mensaje')
    message_id = data.get('message_id')
    
    if not message_id:
        return jsonify({"status": "error", "reason": "no message_id"}), 400
    
    threading.Thread(
        target=procesar_mensaje_y_enviar_ack,
        args=(message_id, mensaje)
    ).start()
    
    return jsonify({"status": "ok, mensaje recibido y procesando"}), 200

def iniciar_servidor_consumidor():
    print(f"\nConsumidor: Escuchando callbacks en {MI_URL_CALLBACK}")
    app_consumidor.run(port=MI_PUERTO)

# --- Parte Cliente (MODIFICADA) ---
def suscribirse_al_broker():
    # 1. Declarar la cola (MODIFICADO: durable=True)
    try:
        requests.post(
            f"{BROKER_URL}/declarar_cola", 
            json={"nombre": COLA_NOMBRE, "durable": True} # <-- NUEVO
        )
        print(f"Consumidor: Cola '{COLA_NOMBRE}' declarada (Durable: True).")
    except requests.exceptions.RequestException as e:
        print(f"Consumidor: No se pudo conectar al broker para declarar: {e}")
        return

    # 2. Suscribirse (sin cambios)
    try:
        r = requests.post(
            f"{BROKER_URL}/consumir", 
            json={"nombre": COLA_NOMBRE, "callback_url": MI_URL_CALLBACK}
        )
        r.raise_for_status()
        print(f"Consumidor: Suscrito a '{COLA_NOMBRE}' con callback {MI_URL_CALLBACK}")
    except requests.exceptions.RequestException as e:
        print(f"Consumidor: Error al suscribirse: {e}")

# --- Arranque (sin cambios) ---
if __name__ == '__main__':
    servidor_thread = threading.Thread(target=iniciar_servidor_consumidor, daemon=True)
    servidor_thread.start()
    time.sleep(1) 
    suscribirse_al_broker()
    print("Consumidor iniciado. Presiona CTRL+C para parar.")
    try:
        while True: time.sleep(10)
    except KeyboardInterrupt:
        print("\nConsumidor: Detenido.")