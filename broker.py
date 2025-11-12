# broker.py

import threading
import time
import uuid
import json 
import os   
from datetime import datetime, timedelta
from collections import deque
from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

# --- Configuración de la Base de Datos JSON ---
JSON_DB_FILE = "broker.db.json"

# --- Estructura de Datos (Estado del Broker) ---
g_colas = {} # Se carga desde el JSON al inicio
g_lock = threading.Lock() 
ACK_TIMEOUT_SEC = 30
PREFETCH_COUNT = 1

# -----------------------------------------------
# --- Helpers de Serialización JSON ---
# -----------------------------------------------

def _state_to_json_serializable(state_dict):
    """
    Convierte el estado de g_colas (con deques y datetimes)
    a un diccionario que json.dump puede manejar (con listas y strings ISO).
    """
    serializable_state = {}
    for queue_name, queue_data in state_dict.items():
        
        # 1. Convertir mensajes en la cola 'mensajes'
        serializable_mensajes = []
        for msg_obj in queue_data["mensajes"]:
            serializable_msg = msg_obj.copy()
            serializable_msg["timestamp"] = msg_obj["timestamp"].isoformat()
            serializable_mensajes.append(serializable_msg)

        serializable_state[queue_name] = {
            "mensajes": serializable_mensajes, 
            "consumidores": queue_data["consumidores"], 
            "indice_rr": queue_data["indice_rr"],
            "durable": queue_data.get("durable", False),
            "unacked": {} 
        }
        
        # 2. Convertir mensajes anidados en 'unacked'
        for msg_id, unacked_data in queue_data["unacked"].items():
            
            msg_obj = unacked_data["mensaje_obj"]
            serializable_msg_obj = msg_obj.copy()
            serializable_msg_obj["timestamp"] = msg_obj["timestamp"].isoformat()
            
            serializable_state[queue_name]["unacked"][msg_id] = {
                "mensaje_obj": serializable_msg_obj, 
                "timestamp_envio": unacked_data["timestamp_envio"].isoformat(),
                "consumer_url": unacked_data["consumer_url"]
            }
            
    return serializable_state

def _json_to_state(json_data):
    """
    (CORREGIDO) Convierte el diccionario cargado de JSON de nuevo al formato
    que g_colas espera (con deques y datetimes).
    """
    state = {}
    for queue_name, queue_data in json_data.items():
        if not queue_data.get("durable", False):
            continue 

        # 1. Convertir 'mensajes' (de string a datetime)
        mensajes_con_datetime = deque()
        for msg_obj_raw in queue_data["mensajes"]:
            mensajes_con_datetime.append({
                "id": msg_obj_raw["id"],
                "payload": msg_obj_raw["payload"],
                "timestamp": datetime.fromisoformat(msg_obj_raw["timestamp"])
            })
        
        # 2. (CORREGIDO) Convertir 'consumidores' y FORZAR unacked_count a 0
        consumidores_con_reset = {}
        for url, data in queue_data.get("consumidores", {}).items():
            # FORZAR el reseteo del contador al cargar
            consumidores_con_reset[url] = {"unacked_count": 0} 

        state[queue_name] = {
            "mensajes": mensajes_con_datetime, 
            "consumidores": consumidores_con_reset, # <-- Usar la lista reseteada
            "indice_rr": queue_data["indice_rr"],
            "durable": queue_data["durable"],
            "unacked": {} # Iniciar siempre vacío
        }
        
        # 3. (CORREGIDO) Re-encolar 'unacked' viejos (no reconstruirlos)
        for msg_id, unacked_data in queue_data.get("unacked", {}).items():
            msg_obj_raw = unacked_data["mensaje_obj"]
            mensaje_obj = {
                "id": msg_obj_raw["id"],
                "payload": msg_obj_raw["payload"],
                "timestamp": datetime.fromisoformat(msg_obj_raw["timestamp"])
            }
            print(f"Broker: Re-encolando mensaje 'unacked' {msg_id} de {queue_name} tras reinicio.")
            # Añadir a la cola de mensajes, no a 'unacked'
            state[queue_name]["mensajes"].appendleft(mensaje_obj)

    return state

# -----------------------------------------------
# --- Funciones de Carga/Guardado de DB ---
# -----------------------------------------------

def _save_state_to_json():
    """
    Guarda el estado de g_colas en el archivo JSON de forma atómica.
    IMPORTANTE: Esta función DEBE ser llamada DESPUÉS de
    obtener g_lock.
    """
    try:
        serializable_state = _state_to_json_serializable(g_colas)
        
        tmp_file = JSON_DB_FILE + ".tmp"
        
        with open(tmp_file, 'w') as f:
            json.dump(serializable_state, f, indent=4)
            
        os.replace(tmp_file, JSON_DB_FILE)
        
    except Exception as e:
        print(f"Broker: ¡¡ERROR CRÍTICO AL GUARDAR ESTADO!! {e}")

def load_state_from_json():
    """
    Carga el estado desde el archivo JSON al arrancar.
    """
    global g_colas # Asegurarnos de modificar la variable global
    try:
        with open(JSON_DB_FILE, 'r') as f:
            json_data = json.load(f)
        
        print(f"Broker: Cargando estado desde {JSON_DB_FILE}...")
        g_colas = _json_to_state(json_data)
        print("Broker: Carga de estado completada.")
        
    except FileNotFoundError:
        print(f"Broker: No se encontró {JSON_DB_FILE}. Empezando con estado vacío.")
        g_colas = {}
    except Exception as e:
        print(f"Broker: Error al cargar {JSON_DB_FILE}: {e}. Empezando con estado vacío.")
        g_colas = {}

# -----------------------------------------------
# --- Lógica de Entrega ---
# -----------------------------------------------

def enviar_mensaje_callback(url_callback, mensaje_obj):
    """
    Función en hilo para enviar el mensaje Y SU ID al consumidor.
    """
    try:
        requests.post(url_callback, json={
            "mensaje": mensaje_obj["payload"],
            "message_id": mensaje_obj["id"]
        }, timeout=3)
        print(f"Broker: Mensaje {mensaje_obj['id']} enviado a {url_callback}")
    except requests.exceptions.RequestException as e:
        print(f"Broker: Error al enviar {mensaje_obj['id']} a {url_callback}: {e}")

def intentar_entrega(nombre_cola):
    """
    Implementa Fair Dispatch.
    Llama a _save_state_to_json si es durable.
    """
    changes_made_to_durable = False
    
    with g_lock:
        if nombre_cola not in g_colas:
            return
        
        cola = g_colas[nombre_cola]
        is_durable = cola.get("durable", False)
        
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
                print("Broker: Todos los consumidores están ocupados. Esperando...")
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
            
            if is_durable:
                changes_made_to_durable = True 
            
            threading.Thread(
                target=enviar_mensaje_callback, 
                args=(url_callback, mensaje_obj)
            ).start()
            print(f"Broker: Mensaje {mensaje_obj['id']} asignado a {url_callback} (unacked: {estado_consumidor['unacked_count']})")
            
        if changes_made_to_durable:
            _save_state_to_json() 

# -----------------------------------------------
# --- Hilo de Limpieza ---
# -----------------------------------------------

def limpiar_y_reencolar():
    """
    Llama a _save_state_to_json si hay cambios.
    """
    while True:
        time.sleep(10)
        
        ahora = datetime.now()
        colas_con_novedades = set()
        changes_made_to_durable = False 

        with g_lock:
            for nombre_cola, cola in list(g_colas.items()):
                is_durable = cola.get("durable", False)

                # --- Tarea 1: Limpiar mensajes viejos (5 min) ---
                if not cola["consumidores"]:
                    mensajes_activos = deque()
                    while cola["mensajes"]:
                        mensaje_obj = cola["mensajes"].popleft()
                        if ahora - mensaje_obj["timestamp"] < timedelta(minutes=5):
                            mensajes_activos.append(mensaje_obj)
                        else:
                            print(f"Broker: Mensaje {mensaje_obj['id']} eliminado de {nombre_cola} por caducidad (5 min).")
                            if is_durable:
                                changes_made_to_durable = True
                    cola["mensajes"] = mensajes_activos

                # --- Tarea 2: Re-encolar 'unacked' vencidos ---
                for msg_id, unacked_data in list(cola["unacked"].items()):
                    if ahora - unacked_data["timestamp_envio"] > timedelta(seconds=ACK_TIMEOUT_SEC):
                        
                        consumer_url = unacked_data["consumer_url"]
                        mensaje_obj = unacked_data["mensaje_obj"]
                        
                        print(f"Broker: TIMEOUT en ACK para {msg_id}. Re-encolando.")
                        
                        cola["mensajes"].appendleft(mensaje_obj)
                        if consumer_url in cola["consumidores"]:
                            cola["consumidores"][consumer_url]["unacked_count"] -= 1
                        
                        del cola["unacked"][msg_id] 
                        
                        if is_durable:
                            changes_made_to_durable = True
                        
                        colas_con_novedades.add(nombre_cola)
            
            if changes_made_to_durable:
                _save_state_to_json() 

        for nombre_cola in colas_con_novedades:
            intentar_entrega(nombre_cola)

# -----------------------------------------------
# --- Endpoints de la API del Broker ---
# -----------------------------------------------

@app.route('/declarar_cola', methods=['POST'])
def declarar_cola():
    """
    Acepta 'durable' y lo guarda en JSON.
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
                _save_state_to_json() 
            print(f"Broker: Cola '{nombre_cola}' (Durable: {durable}) creada.")
        else:
            print(f"Broker: Cola '{nombre_cola}' ya existe (idempotente).")
            
    return jsonify({"status": "ok", "cola": nombre_cola}), 200

@app.route('/publicar', methods=['POST'])
def publicar():
    """
    Acepta 'durable' y lo guarda en JSON.
    """
    data = request.json
    nombre_cola = data.get('nombre')
    mensaje = data.get('mensaje')
    durable = bool(data.get('durable', False))
    
    if not nombre_cola or mensaje is None:
        return jsonify({"error": "Faltan 'nombre' o 'mensaje'"}), 400
    
    is_msg_durable = False
    
    with g_lock:
        if nombre_cola not in g_colas:
            print(f"Broker: Mensaje para cola '{nombre_cola}' (inexistente) perdido.")
            return jsonify({"status": "mensaje perdido (cola no existe)"}), 404
        
        cola = g_colas[nombre_cola]
        is_queue_durable = cola.get("durable", False)
        
        mensaje_obj_ram = {
            "id": str(uuid.uuid4()),
            "payload": mensaje,
            "timestamp": datetime.now() 
        }
        
        cola["mensajes"].append(mensaje_obj_ram)
        
        is_msg_durable = durable and is_queue_durable
        if is_msg_durable:
            _save_state_to_json() 

        print(f"Broker: Mensaje {mensaje_obj_ram['id']} (Durable: {is_msg_durable}) recibido para '{nombre_cola}'")
    
    intentar_entrega(nombre_cola)
    return jsonify({"status": "mensaje publicado"}), 200

@app.route('/consumir', methods=['POST'])
def consumir():
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
            print(f"Broker: Nuevo consumidor {url_callback} suscrito a '{nombre_cola}'")
        else:
            print(f"Broker: Consumidor {url_callback} ya estaba suscrito a '{nombre_cola}'")

    intentar_entrega(nombre_cola)
    return jsonify({"status": "suscrito correctamente"}), 200

@app.route('/ack', methods=['POST'])
def ack_mensaje():
    """
    Ahora borra el mensaje del estado y guarda en JSON.
    """
    data = request.json
    message_id = data.get('message_id')
    nombre_cola = data.get('nombre_cola')
    
    if not message_id or not nombre_cola:
        return jsonify({"error": "Faltan 'message_id' o 'nombre_cola'"}), 400

    ack_exitoso = False
    changes_made_to_durable = False
    
    with g_lock:
        if nombre_cola in g_colas:
            cola = g_colas[nombre_cola]
            is_durable = cola.get("durable", False)
            
            mensaje_ackeado = cola["unacked"].pop(message_id, None)
            
            if mensaje_ackeado:
                print(f"Broker: ACK recibido para {message_id} en {nombre_cola}.")
                ack_exitoso = True
                if is_durable:
                    changes_made_to_durable = True
                
                consumer_url = mensaje_ackeado["consumer_url"]
                if consumer_url in cola["consumidores"]:
                    cola["consumidores"][consumer_url]["unacked_count"] -= 1
                else:
                    print(f"Broker: Consumidor {consumer_url} que envió ACK ya no está suscrito.")
            else:
                print(f"Broker: ACK recibido para {message_id} (pero no estaba en 'unacked').")
                
            if changes_made_to_durable:
                _save_state_to_json() 
                
    if ack_exitoso:
        intentar_entrega(nombre_cola)
        return jsonify({"status": "ack recibido"}), 200
    else:
        return jsonify({"status": "ack no válido o duplicado"}), 404

# -----------------------------------------------
# --- Endpoints de Administración ---
# -----------------------------------------------

@app.route('/colas', methods=['GET'])
def listar_colas():
    with g_lock:
        nombres_colas = list(g_colas.keys())
    print(f"Admin: Solicitud de listar colas. Total: {len(nombres_colas)}")
    return jsonify({"colas": nombres_colas}), 200

@app.route('/colas/<string:nombre_cola>', methods=['DELETE'])
def borrar_cola(nombre_cola):
    """
    Borra la cola de la RAM y guarda el estado en JSON.
    """
    print(f"Admin: Solicitud de borrado para cola: '{nombre_cola}'")
    
    with g_lock:
        cola_eliminada = g_colas.pop(nombre_cola, None)
    
        if cola_eliminada:
            if cola_eliminada.get("durable", False):
                _save_state_to_json() 
            
            print(f"Admin: Cola '{nombre_cola}' eliminada exitosamente.")
            return jsonify({"status": "cola eliminada", "cola": nombre_cola}), 200
        else:
            print(f"Admin: Intento de borrar cola inexistente '{nombre_cola}'.")
            return jsonify({"error": "cola no encontrada"}), 404

# -----------------------------------------------
# --- Arranque del Servidor ---
# -----------------------------------------------
if __name__ == '__main__':
    # Cargar estado desde JSON ANTES de arrancar
    load_state_from_json()
    
    hilo_limpieza = threading.Thread(target=limpiar_y_reencolar, daemon=True)
    hilo_limpieza.start()
    
    print(f"Broker iniciado en http://localhost:5000 (DB: {JSON_DB_FILE})")
    app.run(port=5000, debug=True, use_reloader=False)