import os
import sqlite3
import threading
import time
import json
import requests
import socketio
import paho.mqtt.client as mqtt
from flask import Flask, jsonify, request

app = Flask(__name__)

# --- Konfiguration aus Umgebungsvariablen ---
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_USER = os.getenv("MQTT_USER", "")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "")
ZIGBEE_TOPIC = os.getenv("ZIGBEE2MQTT_BASE_TOPIC", "zigbee2mqtt")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", 60))

UPTIME_KUMA_URL = os.getenv("UPTIME_KUMA_URL", "http://localhost:3001")
UK_USER = os.getenv("UPTIME_KUMA_USER", "")
UK_PASS = os.getenv("UPTIME_KUMA_PASS", "")

ONLINE_GROUP_NAME = os.getenv("ONLINE_GROUP_NAME", "Zigbee Online Status")
BATTERY_GROUP_NAME = os.getenv("BATTERY_GROUP_NAME", "Zigbee Batteriestand")
BATTERY_THRESHOLD = int(os.getenv("BATTERY_THRESHOLD", 20))

DEBUG = os.getenv("DEBUG", "false").lower() in ("true", "1", "yes")

DB_PATH = "/app/data/devices.db"

def log_debug(msg):
    if DEBUG:
        print(f"[DEBUG] {msg}")

# --- SQLite Setup ---
def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS devices (
            ieee_address TEXT PRIMARY KEY,
            friendly_name TEXT,
            online INTEGER,
            battery REAL,
            has_battery INTEGER,
            online_monitor_id INTEGER,
            battery_monitor_id INTEGER
        )
    """)
    conn.commit()
    conn.close()
    log_debug("SQLite Datenbank initialisiert.")

init_db()

# --- Uptime Kuma Socket.io Client ---
sio = socketio.Client()
uk_connected = threading.Event()

@sio.event
def connect():
    print("Mit Uptime Kuma verbunden via Socket.io")
    if UK_USER and UK_PASS:
        try:
            log_debug(f"Versuche Uptime Kuma Login mit Benutzer: {UK_USER}")
            # Uptime Kuma erwartet für den Login oft ein Dictionary mit token oder user/pass
            response = sio.call("login", {"username": UK_USER, "password": UK_PASS}, timeout=10)
            log_debug(f"Uptime Kuma Login Antwort: {response}")
            
            # Manchmal gibt Kuma ein Dictionary mit {"ok": true} zurück
            if isinstance(response, dict) and response.get("ok") == False:
                print(f"Uptime Kuma Login vom Server abgelehnt: {response.get('msg', 'Unbekannter Fehler')}")
            else:
                print("Uptime Kuma Login erfolgreich.")
                uk_connected.set()
        except Exception as e:
            print(f"Uptime Kuma Login fehlgeschlagen (Timeout oder Fehler): {e}")
            # Optional: Falls kein Login zwingend nötig ist oder du testen willst, 
            # ob der Sync ohne Login klappt, kannst du uk_connected.set() hier testweise setzen:
            # uk_connected.set()
    else:
        print("Keine Uptime Kuma Zugangsdaten hinterlegt, überspringe Login.")
        uk_connected.set()

@sio.event
def disconnect():
    print("Verbindung zu Uptime Kuma getrennt.")
    uk_connected.clear()

def connect_uptime_kuma():
    while True:
        try:
            if not sio.connected:
                log_debug(f"Verbinde zu Uptime Kuma unter {UPTIME_KUMA_URL}...")
                sio.connect(UPTIME_KUMA_URL, transports=["websocket", "polling"])
        except Exception as e:
            print(f"Konnte nicht mit Uptime Kuma verbinden: {e}")
        time.sleep(10)

threading.Thread(target=connect_uptime_kuma, daemon=True).start()

def get_or_create_tag_id(tag_name, color="#00df9a"):
    if not uk_connected.is_set():
        log_debug("Socket.io nicht verbunden, kann Tag nicht abrufen/erstellen.")
        return None
    try:
        log_debug(f"Frage Tags von Uptime Kuma ab für Gruppe: {tag_name}")
        tags = sio.call("getTags")
        for tag in tags:
            if tag.get("name") == tag_name:
                log_debug(f"Tag '{tag_name}' gefunden mit ID {tag.get('id')}")
                return tag.get("id")
        
        log_debug(f"Tag '{tag_name}' existiert nicht. Erstelle neuen Tag...")
        res = sio.call("addTag", {"name": tag_name, "color": color})
        if res.get("ok"):
            tag_id = res.get("tagID")
            log_debug(f"Tag '{tag_name}' erfolgreich erstellt mit ID {tag_id}")
            return tag_id
    except Exception as e:
        print(f"Fehler beim Verwalten der Gruppe {tag_name}: {e}")
    return None

def sync_monitor_with_kuma(ieee, name, is_battery_monitor=False):
    if not uk_connected.is_set():
        log_debug("Socket.io nicht verbunden, Monitor-Sync übersprungen.")
        return None

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    col_name = "battery_monitor_id" if is_battery_monitor else "online_monitor_id"
    cursor.execute(f"SELECT {col_name}, friendly_name FROM devices WHERE ieee_address = ?", (ieee,))
    row = cursor.fetchone()
    
    monitor_id = row[0] if row else None
    group_name = BATTERY_GROUP_NAME if is_battery_monitor else ONLINE_GROUP_NAME
    tag_id = get_or_create_tag_id(group_name)
    
    app_host_url = os.getenv("PUBLIC_APP_URL", "http://localhost:5000")
    endpoint_type = "battery" if is_battery_monitor else "online"
    default_url = f"{app_host_url}/api/device/{ieee}/{endpoint_type}"
    
    monitor_title = f"{name} (Batterie)" if is_battery_monitor else f"{name} (Online)"

    try:
        if not monitor_id:
            log_debug(f"Erstelle neuen Uptime Kuma Monitor: {monitor_title}")
            payload = {
                "type": "http",
                "name": monitor_title,
                "url": default_url,
                "interval": 60,
                "maxretries": 3,
                "retryInterval": 60,
                "tags": [{"tagId": tag_id}] if tag_id else []
            }
            res = sio.call("add", payload)
            if res.get("ok"):
                monitor_id = res.get("monitorID")
                cursor.execute(f"UPDATE devices SET {col_name} = ? WHERE ieee_address = ?", (monitor_id, ieee))
                conn.commit()
                print(f"Monitor erstellt für {monitor_title} (ID: {monitor_id})")
        else:
            log_debug(f"Aktualisiere bestehenden Uptime Kuma Monitor ID {monitor_id}: {monitor_title}")
            payload = {
                "id": monitor_id,
                "type": "http",
                "name": monitor_title,
                "tags": [{"tagId": tag_id}] if tag_id else []
            }
            sio.call("edit", payload)
    except Exception as e:
        print(f"Fehler beim Sync mit Uptime Kuma für {monitor_title}: {e}")
    
    conn.close()
    return monitor_id

# --- MQTT Integration ---
def on_connect(client, userdata, flags, rc, properties=None):
    print(f"Verbunden mit MQTT Broker mit Code {rc}")
    client.subscribe(f"{ZIGBEE_TOPIC}/bridge/devices")
    client.subscribe(f"{ZIGBEE_TOPIC}/#")

def on_message(client, userdata, msg):
    try:
        topic = msg.topic
        payload_str = msg.payload.decode("utf-8")
        log_debug(f"MQTT Nachricht empfangen auf Topic: {topic}")
        
        if topic == f"{ZIGBEE_TOPIC}/bridge/devices":
            devices = json.loads(payload_str)
            log_debug(f"Bridge-Devices empfangen. Anzahl Geräte: {len(devices)}")
            for d in devices:
                if d.get("type") == "Coordinator":
                    continue
                ieee = d.get("ieee_address")
                friendly_name = d.get("definition") and d.get("friendly_name") or ieee
                has_battery = 0
                for ex in d.get("definition", {}).get("exposes", []):
                    if "battery" in str(ex).lower():
                        has_battery = 1
                        break
                
                log_debug(f"Gerät verarbeitet: IEEE={ieee}, Name={friendly_name}, Batterie={has_battery}")
                with sqlite3.connect(DB_PATH) as conn:
                    cursor = conn.cursor()
                    cursor.execute("""
                        INSERT INTO devices (ieee_address, friendly_name, has_battery, online)
                        VALUES (?, ?, ?, 1)
                        ON CONFLICT(ieee_address) DO UPDATE SET
                        friendly_name = excluded.friendly_name,
                        has_battery = excluded.has_battery
                    """, (ieee, friendly_name, has_battery))
                    conn.commit()
                
                sync_monitor_with_kuma(ieee, friendly_name, is_battery_monitor=False)
                if has_battery:
                    sync_monitor_with_kuma(ieee, friendly_name, is_battery_monitor=True)

        else:
            parts = topic.split("/")
            if len(parts) == 2 and parts[0] == ZIGBEE_TOPIC:
                dev_name = parts[1]
                data = json.loads(payload_str)
                log_debug(f"Gerätestatus empfangen für '{dev_name}': {data}")
                
                with sqlite3.connect(DB_PATH) as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT ieee_address, has_battery FROM devices WHERE friendly_name = ?", (dev_name,))
                    row = cursor.fetchone()
                    if row:
                        ieee, has_battery = row
                        online = 1 if data.get("linkquality") is not None or data.get("state") is not None else None
                        battery = data.get("battery")
                        
                        log_debug(f"Aktualisiere DB für IEEE {ieee}: online={online}, battery={battery}")
                        cursor.execute("""
                            UPDATE devices SET online = COALESCE(?, online), battery = COALESCE(?, battery)
                            WHERE ieee_address = ?
                        """, (online, battery, ieee))
                        conn.commit()
                    else:
                        log_debug(f"Gerät mit Name '{dev_name}' nicht in DB gefunden.")

    except Exception as e:
        print(f"Fehler bei MQTT Nachrichtenverarbeitung: {e}")

def start_mqtt():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    if MQTT_USER and MQTT_PASSWORD:
        client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message
    
    while True:
        try:
            log_debug(f"Verbinde zu MQTT Broker {MQTT_BROKER}:{MQTT_PORT}...")
            client.connect(MQTT_BROKER, MQTT_PORT, 60)
            client.loop_start()
            break
        except Exception as e:
            print(f"MQTT Verbindungsfehler: {e}. Neuer Versuch in 5s...")
            time.sleep(5)

threading.Thread(target=start_mqtt, daemon=True).start()

# --- HTTP Endpunkte für Uptime Kuma ---

@app.route("/api/device/<ieee>/online", methods=["GET"])
def check_online(ieee):
    log_debug(f"HTTP Anfrage /online für IEEE: {ieee}")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT online, friendly_name FROM devices WHERE ieee_address = ?", (ieee,))
    row = cursor.fetchone()
    conn.close()
    
    if not row:
        log_debug(f"Gerät {ieee} nicht gefunden.")
        return jsonify({"error": "Device not found"}), 404
    
    online, name = row
    log_debug(f"Gerät {name} ({ieee}) Online-Status: {online}")
    if online is None or online == 0:
        return jsonify({"status": "offline", "device": name}), 503
    
    return jsonify({"status": "online", "device": name}), 200

@app.route("/api/device/<ieee>/battery", methods=["GET"])
def check_battery(ieee):
    log_debug(f"HTTP Anfrage /battery für IEEE: {ieee} mit Args: {request.args}")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT battery, has_battery, friendly_name FROM devices WHERE ieee_address = ?", (ieee,))
    row = cursor.fetchone()
    conn.close()
    
    if not row:
        log_debug(f"Gerät {ieee} nicht gefunden.")
        return jsonify({"error": "Device not found"}), 404
    
    battery, has_battery, name = row
    
    if not has_battery:
        log_debug(f"Gerät {name} hat keine Batterie.")
        return jsonify({"error": "Device is not battery powered"}), 400
        
    if battery is None:
        log_debug(f"Für Gerät {name} liegen noch keine Batteriedaten vor.")
        return jsonify({"error": "No battery data available yet"}), 503
    
    threshold = BATTERY_THRESHOLD
    threshold_param = request.args.get("threshold")
    if threshold_param is not None:
        try:
            threshold = float(threshold_param)
        except ValueError:
            log_debug(f"Ungültiger Threshold-Parameter: {threshold_param}")
            return jsonify({"error": "Invalid threshold parameter"}), 400
        
    log_debug(f"Gerät {name} Batterie: {battery}% (Schwellenwert: {threshold}%)")
    if battery >= threshold:
        return jsonify({"status": "ok", "battery_percent": battery, "threshold_used": threshold, "device": name}), 200
    else:
        return jsonify({"status": "low_battery", "battery_percent": battery, "threshold_used": threshold, "device": name}), 422

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy"}), 200

if __name__ == "__main__":
    print(f"Starte Flask-App (Debug-Modus: {DEBUG})...")
    app.run(host="0.0.0.0", port=5000)