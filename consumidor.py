import requests, threading, time, random
from flask import Flask, request, jsonify

# --- Configuración del Consumidor ---
BROKER_URL = "http://10.1.66.41:5000"

MI_PUERTO = 5001
MI_URL_CALLBACK = f"http://localhost:{MI_PUERTO}/callback"


# --- Parte Servidor (sin cambios) ---
app_consumidor = Flask(__name__)

def procesar_mensaje_y_enviar_ack(message_id, mensaje):
    """Simula el procesamiento del mensaje y envía el ACK al broker."""
    try:
        print(f"CONSUMIDOR ({MI_PUERTO}): --> RECIBIDO: '{mensaje}' (ID: {message_id}). Procesando...")
        time.sleep(2)
        
        if random.random() < 0.3:
            print(f"CONSUMIDOR ({MI_PUERTO}): X-- FALLO SIMULADO para {message_id}. No se enviará ACK.")
            return 

        print(f"CONSUMIDOR ({MI_PUERTO}): <-- Procesado OK. Enviando ACK para {message_id}.")
        requests.post(
            f"{BROKER_URL}/ack",
            json={"message_id": message_id, "nombre_cola": nombre_cola},
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

def suscribirse_al_broker():
    # Declarar la cola
    try:
        requests.post(
            f"{BROKER_URL}/declarar_cola", 
            json={"nombre": nombre_cola, "durable": True} # <-- NUEVO
        )
        print(f"Consumidor: Cola '{nombre_cola}' declarada (Durable: True).")
    except requests.exceptions.RequestException as e:
        print(f"Consumidor: No se pudo conectar al broker para declarar: {e}")
        return

    # Suscribirse (sin cambios)
    try:
        r = requests.post(
            f"{BROKER_URL}/consumir", 
            json={"nombre": nombre_cola, "callback_url": MI_URL_CALLBACK}
        )
        r.raise_for_status()
        print(f"Consumidor: Suscrito a '{nombre_cola}' con callback {MI_URL_CALLBACK}")
    except requests.exceptions.RequestException as e:
        print(f"Consumidor: Error al suscribirse: {e}")

if __name__ == '__main__':
    servidor_thread = threading.Thread(target=iniciar_servidor_consumidor, daemon=True)
    servidor_thread.start()
    time.sleep(1) 
    nombre_cola = input("Consumidor: Introduce el nombre de la cola a consumir: ").strip()
    suscribirse_al_broker()
    print("Consumidor iniciado. Presiona CTRL+C para parar.")
    try:
        while True: time.sleep(10)
    except KeyboardInterrupt:
        print("\nConsumidor: Detenido.")