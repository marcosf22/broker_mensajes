import threading, time, uuid, json, os, requests, socket, psutil

from datetime import datetime, timedelta
from collections import deque
from flask import Flask, request, jsonify


# Esto es lo que nos crea el servidor web.
app = Flask(__name__)

ARCHIVO_JSON = "broker.json"

g_colas = {} # Esto es la memoria RAM del broker.
g_lock = threading.Lock() 
TIMEOUT_ACK = 10
PREFETCH_COUNT = 1 # Número máximo de mensajes mandados al consumidos a la vez.


def estado_a_json_serializable(diccionario):
    """
    Preparamos un formato válido para guardar el estado actual (almacenado en g_colas)
    en un archivo JSON.
    """
    estado_serializable = {}
    for name_cola, datos_cola in diccionario.items():

        if not datos_cola.get("durable", False):
            continue
        
        # Convertimos los mensajes en una cola serializable.
        serializable_mensajes = []
        for mens in datos_cola["mensajes"]:

            if not mens.get("is_durable", False):
                continue

            serializable_msg = mens.copy()
            serializable_msg["timestamp"] = mens["timestamp"].isoformat()
            serializable_mensajes.append(serializable_msg)

        estado_serializable[name_cola] = {
            "mensajes": serializable_mensajes, 
            "consumidores": datos_cola["consumidores"], 
            "indice_rr": datos_cola["indice_rr"],
            "durable": datos_cola.get("durable", False),
            "unacked": {} 
        }
        
        # Convertimos los mensajes sin ACK en una cola serializable.
        for mens, datos_sinACK in datos_cola["unacked"].items():
            msg_obj = datos_sinACK["mensaje_obj"]

            if not msg_obj.get("is_durable", False):
                continue

            serializable_msg_obj = msg_obj.copy()
            serializable_msg_obj["timestamp"] = msg_obj["timestamp"].isoformat()
            
            estado_serializable[name_cola]["unacked"][mens] = {
                "mensaje_obj": serializable_msg_obj, 
                "timestamp_envio": datos_sinACK["timestamp_envio"].isoformat(),
                "consumer_url": datos_sinACK["consumer_url"]
            }
            
    return estado_serializable


def json_a_estado(json_data):
    """
    Convierte el JSON en el formato de g_colas.
    """
    estado = {}
    for name_cola, datos_cola in json_data.items():
        if not datos_cola.get("durable", False):
            continue 

        # Convertimos los mensajes a objetos con datetime.
        mensajes_con_datetime = deque()
        for mens in datos_cola["mensajes"]:
            mensajes_con_datetime.append({
                "id": mens["id"],
                "payload": mens["payload"],
                "timestamp": datetime.fromisoformat(mens["timestamp"]),
                "is_durable": mens.get("is_durable", False)
            })
        
        # Convertimos los consumidores y reseteamos los contadores.
        consumidores_con_reset = {}
        for url, data in datos_cola.get("consumidores", {}).items():
            consumidores_con_reset[url] = {"unacked_count": 0} 

        estado[name_cola] = {
            "mensajes": mensajes_con_datetime, 
            "consumidores": consumidores_con_reset, # <-- Usar la lista reseteada
            "indice_rr": datos_cola["indice_rr"],
            "durable": datos_cola["durable"],
            "unacked": {} # Iniciar siempre vacío
        }
        
        # Re-encolamos los mensajes sin ACK.
        for mens_id, unacked_data in datos_cola.get("unacked", {}).items():
            mens = unacked_data["mensaje_obj"]
            mensaje_obj = {
                "id": mens["id"],
                "payload": mens["payload"],
                "timestamp": datetime.fromisoformat(mens["timestamp"]),
                "is_durable": mens.get("is_durable", False)
            }
            print(f"Re-encolando {mens_id} de {name_cola} tras reinicio.")
            estado[name_cola]["mensajes"].appendleft(mensaje_obj)

    return estado


def guardar_JSON():
    """
    Guarda el estado de g_colas en el archivo JSON.
    Esta función la tenemos que llamar con el lock adquirido.
    """
    try:
        estado_serializable = estado_a_json_serializable(g_colas)
        
        archivo_temporal = ARCHIVO_JSON + ".tmp"
        
        with open(archivo_temporal, 'w') as f:
            json.dump(estado_serializable, f, indent=4)
            
        os.replace(archivo_temporal, ARCHIVO_JSON)
        
    except Exception as e:
        print(f"Error al guardar el estado. {e}")

def cargar_estado_desde_JSON():
    """
    Carga el estado desde el archivo JSON al arrancar.
    """
    global g_colas
    try:
        with open(ARCHIVO_JSON, 'r') as f:
            json_data = json.load(f)
        
        print(f"Cargando estado desde {ARCHIVO_JSON}...")
        g_colas = json_a_estado(json_data)
        print("Datos cargados correctamente.\n")
        
    except FileNotFoundError:
        print(f"No se ha encontardo estado: {ARCHIVO_JSON}. Empezando nuevo estado.")
        g_colas = {}
    except Exception as e:
        print(f"Error al cargar {ARCHIVO_JSON}: {e}. Empezando nuevo estado.\n")
        g_colas = {}


def enviar_mensaje_callback(url_callback, mensaje):
    """
    Función llamada en un hilo que manda el mensaje y su id al consumidor.
    """
    try:
        requests.post(url_callback, json={
            "mensaje": mensaje["payload"],
            "message_id": mensaje["id"]
        }, timeout=3)
        print(f"Mensaje {mensaje['id']} enviado a {url_callback}")
    except requests.exceptions.RequestException as e:
        print(f"Error al enviar {mensaje['id']} a {url_callback}: {e}")


def intentar_entrega(nombre_cola):
    """
    Implementamos Fair Dispatch. Llama a guardar_JSON si es duradera.
    """
    cambios_durables = False
    
    with g_lock:
        if nombre_cola not in g_colas:
            return
        
        cola = g_colas[nombre_cola]
        
        while cola["mensajes"] and cola["consumidores"]:
            
            consumidores_lista = list(cola["consumidores"].items())
            if not consumidores_lista:
                break 
                
            start_index = cola["indice_rr"] % len(consumidores_lista)
            
            consumidor_disponible = None
            indice_encontrado = -1

            for i in range(len(consumidores_lista)):
                idx = (start_index + i) % len(consumidores_lista)
                url, estado = consumidores_lista[idx]
                
                if estado["unacked_count"] < PREFETCH_COUNT:
                    consumidor_disponible = (url, estado)
                    indice_encontrado = idx
                    break 
            
            if not consumidor_disponible:
                print("Todos los consumidores están ocupados. Esperando...")
                break 
            
            cola["indice_rr"] = (indice_encontrado + 1) % len(consumidores_lista)
            
            url_callback, estado_consumidor = consumidor_disponible
            mensaje_obj = cola["mensajes"].popleft()
            
            timestamp_envio = datetime.now()
            
            cola["unacked"][mensaje_obj["id"]] = {
                "mensaje_obj": mensaje_obj,
                "timestamp_envio": timestamp_envio,
                "consumer_url": url_callback
            }
            estado_consumidor["unacked_count"] += 1
            
            # Solo marcar para guardar si el mensaje es duradero.
            if mensaje_obj.get("is_durable", False):
                cambios_durables = True 
            
            threading.Thread(
                target=enviar_mensaje_callback, 
                args=(url_callback, mensaje_obj)
            ).start()
            print(f"Mensaje {mensaje_obj['id']} asignado a {url_callback} (unacked: {estado_consumidor['unacked_count']})")
            
        if cambios_durables:
            guardar_JSON() 


def limpiar_y_reencolar():
    """
    Llama a guardar_JSON si hay cambios.
    """
    while True:
        time.sleep(10)
        
        ahora = datetime.now()
        colas_con_novedades = set()
        cambios_en_duraderos = False 

        with g_lock:
            for nombre_cola, cola in list(g_colas.items()):

                # Limpiamos mensajes que lleven más de 5 minutos sin ser consumidos.
                if not cola["consumidores"]:
                    mensajes_activos = deque()
                    while cola["mensajes"]:
                        mensaje_obj = cola["mensajes"].popleft()
                        if ahora - mensaje_obj["timestamp"] < timedelta(minutes=5):
                            mensajes_activos.append(mensaje_obj)
                        else:
                            print(f"Mensaje {mensaje_obj['id']} eliminado de {nombre_cola} por caducidad (5 min).")
                            
                            if mensaje_obj.get("is_durable", False):
                                cambios_en_duraderos = True
                    cola["mensajes"] = mensajes_activos

                # Re-encolamos mensajes sin ACK que hay superado el timeout.
                for msg_id, datos_sinACK in list(cola["unacked"].items()):
                    if ahora - datos_sinACK["timestamp_envio"] > timedelta(seconds=TIMEOUT_ACK):
                        
                        consumer_url = datos_sinACK["consumer_url"]
                        mensaje_obj = datos_sinACK["mensaje_obj"]
                        
                        print(f"TIMEOUT en ACK para {msg_id}. Re-encolando.")
                        
                        cola["mensajes"].appendleft(mensaje_obj)
                        if consumer_url in cola["consumidores"]:
                            cola["consumidores"][consumer_url]["unacked_count"] -= 1
                        
                        del cola["unacked"][msg_id] 
                        
                        if mensaje_obj.get("is_durable", False):
                            cambios_en_duraderos = True
                        
                        colas_con_novedades.add(nombre_cola)
            
            if cambios_en_duraderos:
                guardar_JSON() 

        for nombre_cola in colas_con_novedades:
            intentar_entrega(nombre_cola)


@app.route('/declarar_cola', methods=['POST'])
def declarar_cola():
    """
    Declara una cola con un nombre y si es duradera o no.
    """
    data = request.json
    nombre_cola = data.get('nombre')
    durable = bool(data.get('durable', False)) 
    
    if not nombre_cola:
        return jsonify({"error": "Falta 'nombre'"}), 400
        
    with g_lock:
        if nombre_cola not in g_colas:
            g_colas[nombre_cola] = {
                "mensajes": deque(),
                "consumidores": {},
                "indice_rr": 0,
                "unacked": {},
                "durable": durable
            }
            if durable:
                guardar_JSON() 
            print(f"\nCola '{nombre_cola}' (Durable: {durable}) creada.\n")
        else:
            print(f"\nCola '{nombre_cola}' ya existe (idempotente).\n")
            
    return jsonify({"status": "ok", "cola": nombre_cola}), 200


@app.route('/publicar', methods=['POST'])
def publicar():
    """
    Publicamos un mensaje en una cola y si es duradero (tanto cola como mensaje) 
    lo guardamos en el JSON.
    """
    data = request.json
    nombre_cola = data.get('nombre')
    mensaje = data.get('mensaje')
    durable_msg = bool(data.get('durable', False))
    
    if not nombre_cola or mensaje is None:
        return jsonify({"error": "Faltan 'nombre' o 'mensaje'"}), 400
    
    with g_lock:
        if nombre_cola not in g_colas:
            print(f"Mensaje para cola '{nombre_cola}' (inexistente) perdido.")
            return jsonify({"status": "mensaje perdido (cola no existe)"}), 404
        
        cola = g_colas[nombre_cola]
        cola_es_duradera = cola.get("durable", False)
        
        # Calcular la durabilidad real (mensaje Y cola)
        mensaje_es_duradero = durable_msg and cola_es_duradera
        
        mensaje_obj_ram = {
            "id": str(uuid.uuid4()),
            "payload": mensaje,
            "timestamp": datetime.now(),
            "is_durable": mensaje_es_duradero
        }
        
        cola["mensajes"].append(mensaje_obj_ram)
        
        # El guardado solo depende de 'mensaje_es_duradero'
        if mensaje_es_duradero:
            guardar_JSON() 

        print(f"Mensaje {mensaje_obj_ram['id']} (Durable: {mensaje_es_duradero}) recibido para '{nombre_cola}'")
    
    intentar_entrega(nombre_cola)
    return jsonify({"status": "mensaje publicado"}), 200


@app.route('/consumir', methods=['POST'])
def consumir():
    """
    Suscribe un consumidor a una cola.
    """
    data = request.json
    nombre_cola = data.get('nombre')
    url_callback = data.get('callback_url')
    
    if not nombre_cola or not url_callback:
        return jsonify({"error": "Faltan 'nombre' o 'callback_url'"}), 400

    with g_lock:
        if nombre_cola not in g_colas:
            return jsonify({"error": "Cola no existe. Declárala primero."}), 404
        
        if url_callback not in g_colas[nombre_cola]["consumidores"]:
            g_colas[nombre_cola]["consumidores"][url_callback] = {"unacked_count": 0}
            print(f"Nuevo consumidor {url_callback} suscrito a '{nombre_cola}'\n")
        else:
            print(f"Consumidor {url_callback} ya estaba suscrito a '{nombre_cola}'\n")

    intentar_entrega(nombre_cola)
    return jsonify({"status": "suscrito correctamente"}), 200


@app.route('/ack', methods=['POST'])
def ack_mensaje():
    """
    Recibe ACK por parte del consumidor y borra el mensaje del JSON (si era durable) y del estado.
    """
    data = request.json
    message_id = data.get('message_id')
    nombre_cola = data.get('nombre_cola')
    
    if not message_id or not nombre_cola:
        return jsonify({"error": "Faltan 'message_id' o 'nombre_cola'"}), 400

    ack_exitoso = False
    cambios_en_duraderos = False
    
    with g_lock:
        if nombre_cola in g_colas:
            cola = g_colas[nombre_cola]
            
            mensaje_ackeado = cola["unacked"].pop(message_id, None)
            
            if mensaje_ackeado:
                print(f"ACK recibido para {message_id} en {nombre_cola}.")
                ack_exitoso = True
                
                if mensaje_ackeado["mensaje_obj"].get("is_durable", False):
                    cambios_en_duraderos = True
                
                consumer_url = mensaje_ackeado["consumer_url"]
                if consumer_url in cola["consumidores"]:
                    cola["consumidores"][consumer_url]["unacked_count"] -= 1
                else:
                    print(f"Consumidor {consumer_url} que envió ACK ya no está suscrito.")
            else:
                print(f"ACK recibido para {message_id} (pero no estaba en 'unacked').")
                
            if cambios_en_duraderos:
                guardar_JSON() 
                
    if ack_exitoso:
        intentar_entrega(nombre_cola)
        return jsonify({"status": "ack recibido"}), 200
    else:
        return jsonify({"status": "ack no válido o duplicado"}), 404


@app.route('/colas', methods=['GET'])
def listar_colas():
    """
    Devuelve la lista de colas existentes.
    """
    with g_lock:
        nombres_colas = list(g_colas.keys())
    print(f"\nSolicitud de listar colas. Total: {len(nombres_colas)}")
    return jsonify({"colas": nombres_colas}), 200

@app.route('/colas/<string:nombre_cola>', methods=['DELETE'])
def borrar_cola(nombre_cola):
    """
    Borra la cola del estado y del JSON (si es durable).
    """
    print(f"\nSolicitud de borrado para cola: '{nombre_cola}'")
    
    with g_lock:
        cola_eliminada = g_colas.pop(nombre_cola, None)
    
        if cola_eliminada:
            if cola_eliminada.get("durable", False):
                guardar_JSON() 
            
            print(f"Cola '{nombre_cola}' eliminada exitosamente.\n")
            return jsonify({"status": "cola eliminada", "cola": nombre_cola}), 200
        else:
            print(f"Intento de borrar cola inexistente '{nombre_cola}'.\n")
            return jsonify({"error": "cola no encontrada"}), 404


if __name__ == '__main__':
    # Cargamos estado desde JSON.
    cargar_estado_desde_JSON()
    
    # Iniciamos el hilo de limpieza.
    hilo_limpieza = threading.Thread(target=limpiar_y_reencolar, daemon=True)
    hilo_limpieza.start()
    
    # Intentamos entregar los mensajes que no han recibido ACK.
    print("Realizando intento de entrega inicial tras reinicio.\n")
    with g_lock: 
        colas_a_revisar = list(g_colas.keys())

    for nombre_cola in colas_a_revisar:
        print(f"Intentando entrega para cola '{nombre_cola}'.")
        intentar_entrega(nombre_cola)
    
    # Softcodeamos la IP local del broker.
    for iface_name, iface_addrs in psutil.net_if_addrs().items():
        if 'wi-fi' in iface_name.lower():
            for addr in iface_addrs:
                if addr.family == socket.AF_INET:
                    dir = addr.address

    # Iniciamos el servidor web
    print(f"Broker iniciado en http://{dir}:5000\n")

    app.run(host=dir, port=5000, debug=True, use_reloader=False)