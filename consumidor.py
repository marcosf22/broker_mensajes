import requests, threading, time, random, psutil, socket
from flask import Flask, request, jsonify


app_consumidor = Flask(__name__)

def procesar_mensaje_y_enviar_ack(message_id, mensaje):
    """
    Procesamos el mensaje y envía el ACK al broker.
    """
    try:
        print(f"Mensaje recibido: '{mensaje}' (ID: {message_id}). Procesando...")
        time.sleep(2)
        
        # Aqui podemos simular un fallo en el envío del ACK.
        # if random.random() < 0.3:
        #     print(f"FALLO SIMULADO para {message_id}. No se enviará ACK.")
        #     return 

        # Envíamos el ACK al broker.
        print(f"Enviando ACK para {message_id}.")
        requests.post(
            f"{BROKER_URL}/ack",
            json={"message_id": message_id, "nombre_cola": nombre_cola},
            timeout=2
        )
    except Exception as e:
        print(f"Error: {e}")


@app_consumidor.route('/callback', methods=['POST'])
def recibir_mensaje():
    """
    Recibimos los mensajes enviados por el broker.
    """
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
    print(f"\nEscuchando callbacks en {CALLBACK_URL}")
    app_consumidor.run(host=dir,port=PUERTO)

def suscribirse_al_broker():
    """
    Nos suscribimos a la cola deseada en el broker.
    """
    
    # Por si acaso no existe la cola, creamos una que sea duradera por defecto.
    try:        
        requests.post(
            f"{BROKER_URL}/declarar_cola", 
            json={"nombre": nombre_cola, "durable": True} # <-- NUEVO
        )
        print(f"\nCola '{nombre_cola}' declarada (Durable: True).")
    except requests.exceptions.RequestException as e:
        print(f"No se pudo conectar al broker para declarar: {e}")
        return

    # Nos suscribimos a la cola.
    try:
        r = requests.post(
            f"{BROKER_URL}/consumir", 
            json={"nombre": nombre_cola, "callback_url": CALLBACK_URL}
        )
        r.raise_for_status()
        print(f"Suscrito a '{nombre_cola}' con callback {CALLBACK_URL}")
    except requests.exceptions.RequestException as e:
        print(f"Error al suscribirse: {e}")

if __name__ == '__main__':

    print("\nBienvenido al Consumidor.\n")

    ip = input("Introduce la IP del broker: ").strip()
    BROKER_URL = "http://" + ip + ":5000"

    # Softcodeamos la IP local del broker.
    for iface_name, iface_addrs in psutil.net_if_addrs().items():
        if 'wi-fi' in iface_name.lower():
            for addr in iface_addrs:
                if addr.family == socket.AF_INET:
                    dir = addr.address
    
    PUERTO = input("\nIntroduce el puerto del consumidor: ").strip()
    CALLBACK_URL = f"http://{dir}:{PUERTO}/callback"

    nombre_cola = input("\nIntroduce el nombre de la cola a consumir: ").strip()

    servidor_thread = threading.Thread(target=iniciar_servidor_consumidor, daemon=True)
    servidor_thread.start()
    time.sleep(1) 
    suscribirse_al_broker()
    print("Consumidor iniciado. Presiona CTRL+C para parar.")
    try:
        while True: time.sleep(10)
    except KeyboardInterrupt:
        print("\nDetenido.")