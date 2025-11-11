# broker.py

import threading
import time
import uuid
import sqlite3 # <-- NUEVO
from datetime import datetime, timedelta
from collections import deque
from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

# --- Configuración de la Base de Datos ---
DB_NAME = "broker.db"

def get_db_conn():
    """Crea una nueva conexión a la DB."""
    conn = sqlite3.connect(DB_NAME, check_same_thread=False) # check_same_thread=False es necesario
    conn.row_factory = sqlite3.Row # Para acceder a columnas por nombre
    return conn

def init_db():
    """
    Crea las tablas de la DB si no existen al arrancar.
    """
    with get_db_conn() as conn:
        cursor = conn.cursor()
        
        # Tabla para las colas
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS queues (
            name TEXT PRIMARY KEY NOT NULL,
            durable INTEGER NOT NULL DEFAULT 0
        )
        """)
        
        # Tabla para los mensajes
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id TEXT PRIMARY KEY NOT NULL,
            queue_name TEXT NOT NULL,
            payload TEXT NOT NULL,
            timestamp DATETIME NOT NULL,
            durable INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'queued', -- 'queued' o 'unacked'
            unacked_by_consumer TEXT,
            unacked_timestamp DATETIME,
            FOREIGN KEY (queue_name) REFERENCES queues (name) ON DELETE CASCADE
        )
        """)
        conn.commit()
    print("Broker: Base de datos inicializada.")

def load_state_from_db():
    """
    (NUEVO) Carga el estado duradero de la DB a la memoria (g_colas).
    Se llama solo al arrancar el broker.
    """
    print("Broker: Cargando estado desde la base de datos...")
    with get_db_conn() as conn:
        cursor = conn.cursor()
        
        # 1. Cargar colas duraderas
        cursor.execute("SELECT name FROM queues WHERE durable = 1")
        for row in cursor.fetchall():
            nombre_cola = row['name']
            if nombre_cola not in g_colas:
                g_colas[nombre_cola] = {
                    "mensajes": deque(),
                    "consumidores": {},
                    "indice_rr": 0,
                    "unacked": {},
                    "durable": True # <-- Marcamos como duradera
                }
                print(f"Broker: Cola duradera '{nombre_cola}' recuperada.")

        # 2. Cargar mensajes duraderos (los 'unacked' se re-encolan)
        cursor.execute("SELECT * FROM messages WHERE durable = 1")
        for row in cursor.fetchall():
            nombre_cola = row['queue_name']
            if nombre_cola in g_colas: # Solo si la cola es duradera
                mensaje_obj = {
                    "id": row['id'],
                    "payload": row['payload'],
                    "timestamp": datetime.fromisoformat(row['timestamp'])
                }
                
                # (NUEVO) Reconstruir el estado 'unacked'
                if row['status'] == 'unacked':
                    consumer_url = row['unacked_by_consumer']
                    unacked_data = {
                        "mensaje_obj": mensaje_obj,
                        "timestamp_envio": datetime.fromisoformat(row['unacked_timestamp']),
                        "consumer_url": consumer_url
                    }
                    g_colas[nombre_cola]["unacked"][mensaje_obj["id"]] = unacked_data
                    
                    # (NUEVO) Reconstruir el contador del consumidor
                    # Esto asume que el consumidor se volverá a suscribir.
                    # Si no lo hace, el 'limpiar_y_reencolar' lo arreglará.
                    if consumer_url not in g_colas[nombre_cola]["consumidores"]:
                         g_colas[nombre_cola]["consumidores"][consumer_url] = {"unacked_count": 0}
                    g_colas[nombre_cola]["consumidores"][consumer_url]["unacked_count"] += 1
                    
                    print(f"Broker: Mensaje 'unacked' {mensaje_obj['id']} recuperado para {nombre_cola}")
                else:
                    g_colas[nombre_cola]["mensajes"].append(mensaje_obj)
                    print(f"Broker: Mensaje 'queued' {mensaje_obj['id']} recuperado para {nombre_cola}")
            
    print("Broker: Carga de estado completada.")


# --- Estructura de Datos (Estado del Broker) ---
g_colas = {} # Ahora se rellena desde la DB al inicio
g_lock = threading.Lock()
ACK_TIMEOUT_SEC = 30
PREFETCH_COUNT = 1

# --- Lógica de Entrega (MODIFICADA) ---

def enviar_mensaje_callback(url_callback, mensaje_obj):
    # (Sin cambios)
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
    (MODIFICADO) Ahora actualiza la DB cuando un mensaje pasa a 'unacked'.
    """
    with g_lock:
        if nombre_cola not in g_colas:
            return
        
        cola = g_colas[nombre_cola]
        
        while cola["mensajes"] and cola["consumidores"]:
            # ... (Lógica de Fair Dispatch idéntica para encontrar consumidor) ...
            consumidores_lista = list(cola["consumidores"].items())
            if not consumidores_lista: break
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
            
            # 1. Actualizar RAM
            cola["unacked"][mensaje_obj["id"]] = {
                "mensaje_obj": mensaje_obj,
                "timestamp_envio": timestamp_envio,
                "consumer_url": url_callback
            }
            estado_consumidor["unacked_count"] += 1

            # 2. (NUEVO) Actualizar DB (solo si la cola es duradera)
            if cola.get("durable", False):
                with get_db_conn() as conn:
                    conn.execute(
                        """
                        UPDATE messages 
                        SET status = 'unacked', unacked_by_consumer = ?, unacked_timestamp = ?
                        WHERE id = ?
                        """,
                        (url_callback, timestamp_envio, mensaje_obj["id"])
                    )
                    conn.commit()
            
            # 3. Enviar
            threading.Thread(
                target=enviar_mensaje_callback, 
                args=(url_callback, mensaje_obj)
            ).start()
            print(f"Broker: Mensaje {mensaje_obj['id']} asignado a {url_callback} (unacked: {estado_consumidor['unacked_count']})")

# --- Hilo de Limpieza (MODIFICADO) ---

def limpiar_y_reencolar():
    """
    (MODIFICADO) Ahora actualiza la DB en timeouts y re-encolados.
    """
    while True:
        time.sleep(10)
        ahora = datetime.now()
        colas_con_novedades = set()

        # (NUEVO) Hacemos todo el trabajo de DB en una sola conexión
        # para el hilo de background.
        with get_db_conn() as conn:
            with g_lock: # Bloqueamos el estado en RAM
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
                                # (NUEVO) Borrar de DB
                                if is_durable:
                                    conn.execute("DELETE FROM messages WHERE id = ?", (mensaje_obj["id"],))
                        cola["mensajes"] = mensajes_activos

                    # --- Tarea 2: Re-encolar 'unacked' vencidos ---
                    mensajes_a_reencolar_ids = []
                    for msg_id, unacked_data in list(cola["unacked"].items()):
                        if ahora - unacked_data["timestamp_envio"] > timedelta(seconds=ACK_TIMEOUT_SEC):
                            
                            consumer_url = unacked_data["consumer_url"]
                            mensaje_obj = unacked_data["mensaje_obj"]
                            
                            print(f"Broker: TIMEOUT en ACK para {msg_id}. Re-encolando.")
                            
                            # Actualizar RAM
                            cola["mensajes"].appendleft(mensaje_obj)
                            if consumer_url in cola["consumidores"]:
                                cola["consumidores"][consumer_url]["unacked_count"] -= 1

                            # (NUEVO) Actualizar DB
                            if is_durable:
                                conn.execute(
                                    """
                                    UPDATE messages 
                                    SET status = 'queued', unacked_by_consumer = NULL, unacked_timestamp = NULL
                                    WHERE id = ?
                                    """,
                                    (msg_id,)
                                )
                            
                            mensajes_a_reencolar_ids.append(msg_id)
                    
                    for msg_id in mensajes_a_reencolar_ids:
                        del cola["unacked"][msg_id]
                        colas_con_novedades.add(nombre_cola)
            
            # (NUEVO) Hacemos commit de todos los cambios del hilo al final
            conn.commit()

        # --- Fuera del Lock, intentamos entregar ---
        for nombre_cola in colas_con_novedades:
            intentar_entrega(nombre_cola)

# -----------------------------------------------
# --- Endpoints de la API del Broker (MODIFICADOS) ---
# -----------------------------------------------

@app.route('/declarar_cola', methods=['POST'])
def declarar_cola():
    """
    (MODIFICADO) Acepta 'durable' y lo guarda en DB.
    """
    data = request.json
    nombre_cola = data.get('nombre')
    durable = bool(data.get('durable', False)) # <-- NUEVO
    
    if not nombre_cola:
        return jsonify({"error": "Falta 'nombre'"}), 400
        
    with g_lock:
        if nombre_cola not in g_colas:
            # 1. Actualizar RAM
            g_colas[nombre_cola] = {
                "mensajes": deque(),
                "consumidores": {},
                "indice_rr": 0,
                "unacked": {},
                "durable": durable # <-- NUEVO
            }
            
            # 2. (NUEVO) Actualizar DB
            if durable:
                with get_db_conn() as conn:
                    # INSERT OR IGNORE para idempotencia
                    conn.execute("INSERT OR IGNORE INTO queues (name, durable) VALUES (?, 1)", (nombre_cola,))
                    conn.commit()
            print(f"Broker: Cola '{nombre_cola}' (Durable: {durable}) creada.")
        else:
            print(f"Broker: Cola '{nombre_cola}' ya existe (idempotente).")
            
    return jsonify({"status": "ok", "cola": nombre_cola}), 200

@app.route('/publicar', methods=['POST'])
def publicar():
    """
    (MODIFICADO) Acepta 'durable' y lo guarda en DB.
    """
    data = request.json
    nombre_cola = data.get('nombre')
    mensaje = data.get('mensaje')
    durable = bool(data.get('durable', False)) # <-- NUEVO
    
    if not nombre_cola or mensaje is None:
        return jsonify({"error": "Faltan 'nombre' o 'mensaje'"}), 400
    
    with g_lock:
        if nombre_cola not in g_colas:
            print(f"Broker: Mensaje para cola '{nombre_cola}' (inexistente) perdido.")
            return jsonify({"status": "mensaje perdido (cola no existe)"}), 404
        
        cola = g_colas[nombre_cola]
        is_queue_durable = cola.get("durable", False)
        
        mensaje_obj = {
            "id": str(uuid.uuid4()),
            "payload": mensaje,
            "timestamp": datetime.now()
        }
        
        # 1. Actualizar RAM
        cola["mensajes"].append(mensaje_obj)
        
        # 2. (NUEVO) Actualizar DB
        # Un mensaje solo es duradero si ÉL es durable Y la cola es durable
        is_msg_durable = durable and is_queue_durable
        if is_msg_durable:
            with get_db_conn() as conn:
                conn.execute(
                    """
                    INSERT INTO messages (id, queue_name, payload, timestamp, durable, status) 
                    VALUES (?, ?, ?, ?, 1, 'queued')
                    """,
                    (mensaje_obj["id"], nombre_cola, mensaje_obj["payload"], mensaje_obj["timestamp"])
                )
                conn.commit()

        print(f"Broker: Mensaje {mensaje_obj['id']} (Durable: {is_msg_durable}) recibido para '{nombre_cola}'")
    
    intentar_entrega(nombre_cola)
    return jsonify({"status": "mensaje publicado"}), 200

@app.route('/consumir', methods=['POST'])
def consumir():
    # (Sin cambios de lógica, solo la estructura de datos que ya teníamos)
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
    (MODIFICADO) Ahora borra el mensaje de la DB.
    """
    data = request.json
    message_id = data.get('message_id')
    nombre_cola = data.get('nombre_cola')
    
    if not message_id or not nombre_cola:
        return jsonify({"error": "Faltan 'message_id' o 'nombre_cola'"}), 400

    ack_exitoso = False
    
    with g_lock:
        if nombre_cola in g_colas:
            cola = g_colas[nombre_cola]
            is_durable = cola.get("durable", False)
            
            # 1. Actualizar RAM
            mensaje_ackeado = cola["unacked"].pop(message_id, None)
            
            if mensaje_ackeado:
                print(f"Broker: ACK recibido para {message_id} en {nombre_cola}.")
                ack_exitoso = True
                
                consumer_url = mensaje_ackeado["consumer_url"]
                if consumer_url in cola["consumidores"]:
                    cola["consumidores"][consumer_url]["unacked_count"] -= 1

                # 2. (NUEVO) Actualizar DB
                if is_durable:
                    with get_db_conn() as conn:
                        conn.execute("DELETE FROM messages WHERE id = ?", (message_id,))
                        conn.commit()
            else:
                print(f"Broker: ACK recibido para {message_id} (pero no estaba en 'unacked').")
                
    if ack_exitoso:
        intentar_entrega(nombre_cola)
        return jsonify({"status": "ack recibido"}), 200
    else:
        return jsonify({"status": "ack no válido o duplicado"}), 404

# --- Endpoints de Administración (MODIFICADOS) ---

@app.route('/colas', methods=['GET'])
def listar_colas():
    # (Sin cambios)
    with g_lock:
        nombres_colas = list(g_colas.keys())
    return jsonify({"colas": nombres_colas}), 200

@app.route('/colas/<string:nombre_cola>', methods=['DELETE'])
def borrar_cola(nombre_cola):
    """
    (MODIFICADO) Borra la cola de la DB.
    """
    print(f"Admin: Solicitud de borrado para cola: '{nombre_cola}'")
    
    with g_lock:
        # 1. Borrar de RAM
        cola_eliminada = g_colas.pop(nombre_cola, None)
    
    if cola_eliminada:
        # 2. (NUEVO) Borrar de DB
        if cola_eliminada.get("durable", False):
            with get_db_conn() as conn:
                # El "ON DELETE CASCADE" debería borrar los mensajes,
                # pero lo hacemos explícito por si acaso.
                conn.execute("DELETE FROM messages WHERE queue_name = ?", (nombre_cola,))
                conn.execute("DELETE FROM queues WHERE name = ?", (nombre_cola,))
                conn.commit()
        
        print(f"Admin: Cola '{nombre_cola}' eliminada exitosamente.")
        return jsonify({"status": "cola eliminada", "cola": nombre_cola}), 200
    else:
        print(f"Admin: Intento de borrar cola inexistente '{nombre_cola}'.")
        return jsonify({"error": "cola no encontrada"}), 404

# -----------------------------------------------
# --- Arranque del Servidor (MODIFICADO) ---
# -----------------------------------------------
if __name__ == '__main__':
    # (NUEVO) Inicializar y cargar DB ANTES de arrancar
    init_db()
    load_state_from_db()
    
    hilo_limpieza = threading.Thread(target=limpiar_y_reencolar, daemon=True)
    hilo_limpieza.start()
    
    print("Broker iniciado en http://localhost:5000")
    app.run(port=5000, debug=True, use_reloader=False)