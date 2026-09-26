import os
import sqlite3
import threading
import time
import json
import requests
import socketio
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

DB_PATH = "/app/data/devices.db"

# Globaler Cache für Gerätedaten
devices_cache = {}
cache_lock = threading.Lock()

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

init_db()

# --- Uptime Kuma Socket.io Client ---
sio = socketio.Client()
uk_connected = threading.Event()

@sio.event
def connect():
    print("Mit Uptime Kuma verbunden via Socket.io")
    if UK_USER and UK_PASS:
        try:
            sio.call("login", {"username": UK_USER, "password": UK_PASS})
            print("Uptime Kuma Login erfolgreich.")
            uk_connected.set()
        except Exception as e:
            print(f"Uptime Kuma Login fehlgeschlagen: {e}")
    else:
        uk_connected.set()

@sio.event
def disconnect():
    print("Verbindung zu Uptime Kuma getrennt.")
    uk_connected.clear()

def connect_uptime_kuma():
    while True:
        try:
            if not sio.connected:
                sio.connect(UPTIME_KUMA_URL, transports=["websocket"])
        except Exception as e:
            print(f"Konnte nicht mit Uptime Kuma verbinden: {e}")
        time.sleep(10)

threading.Thread(target=connect_uptime_kuma, daemon=True).start()

def get_or_create_tag_id(tag_name, color="#00df9a"):
    """Erstellt oder holt die ID einer Monitor-Gruppe (Tag) in Uptime Kuma."""
    if not uk_connected.is_set():
        return None
    try:
        tags = sio.call("getTags")
        for tag in tags:
            if tag.get("name") == tag_name:
                return tag.get("id")
        
        res = sio.call("addTag", {"name": tag_name, "color": color})
        if res.get("ok"):
            return res.get("tagID")
    except Exception as e:
        print(f"Fehler beim Verwalten der Gruppe {tag_name}: {e}")
    return None

def sync_monitor_with_kuma(ieee, name, is_battery_monitor=False):
    """Erstellt oder aktualisiert einen Monitor in Uptime Kuma."""
    if not uk_connected.is_set():
        return None

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    col_name = "battery_monitor_id" if is_battery_monitor else "online_monitor_id"
    cursor.execute(f"SELECT {col_name}, friendly_name FROM devices WHERE ieee_address = ?", (ieee,))
    row = cursor.fetchone()
    
    monitor_id = row[0] if row else None
    group_name = BATTERY_GROUP_NAME if is_battery_monitor else ONLINE_GROUP_NAME
    tag_id = get_or_create_tag_id(group_name)
    
    app_host_url = os.getenv("PUBLIC_APP_URL", f"http://localhost:5000")
    endpoint_type = "battery" if is_battery_monitor else "online"
    default_url = f"{app_host_url}/api/device/{ieee}/{endpoint_type}"
    
    monitor_title = f"{name} (Batterie)" if is_battery_monitor else f"{name} (Online)"

    try:
        if not monitor_id:
            # Neuen Monitor erstellen
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
            # Bestehenden Monitor aktualisieren (Nur Name & Tags anpassen, URL bewahren um manuelle Query-Parameter nicht zu überschreiben)
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

# --- Zigbee2MQTT / MQTT Integration ---
import paho.mqtt.client as mqtt

def on_connect(client, userdata, flags, rc, properties=None):
    print(f"Verbunden mit MQTT Broker mit Code {rc}")
    client.subscribe(f"{ZIGBEE_TOPIC}/bridge/devices")
    client.subscribe(f"{ZIGBEE_TOPIC}/#")

def on_message(client, userdata, msg):
    try:
        topic = msg.topic
        payload_str = msg.payload.decode("utf-8")
        
        if topic == f"{ZIGBEE_TOPIC}/bridge/devices":
            devices = json.loads(payload_str)
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
                
                with sqlite3.connect(DB_PATH) as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT ieee_address, has_battery FROM devices WHERE friendly_name = ?", (dev_name,))
                    row = cursor.fetchone()
                    if row:
                        ieee, has_battery = row
                        online = 1 if data.get("linkquality") is not None or data.get("state") is not None else None
                        battery = data.get("battery")
                        
                        cursor.execute("""
                            UPDATE devices SET online = COALESCE(?, online), battery = COALESCE(?, battery)
                            WHERE ieee_address = ?
                        """, (online, battery, ieee))
                        conn.commit()

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
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT online, friendly_name FROM devices WHERE ieee_address = ?", (ieee,))
    row = cursor.fetchone()
    conn.close()
    
    if not row:
        return jsonify({"error": "Device not found"}), 404
    
    online, name = row
    if online is None or online == 0:
        return jsonify({"status": "offline", "device": name}), 503
    
    return jsonify({"status": "online", "device": name}), 200

@app.route("/api/device/<ieee>/battery", methods=["GET"])
def check_battery(ieee):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT battery, has_battery, friendly_name FROM devices WHERE ieee_address = ?", (ieee,))
    row = cursor.fetchone()
    conn.close()
    
    if not row:
        return jsonify({"error": "Device not found"}), 404
    
    battery, has_battery, name = row
    
    if not has_battery:
        return jsonify({"error": "Device is not battery powered"}), 400
        
    if battery is None:
        return jsonify({"error": "No battery data available yet"}), 503
    
    # Schwellenwert aus Query-Parameter ermitteln (überschreibt Standard)
    threshold = BATTERY_THRESHOLD
    threshold_param = request.args.get("threshold")
    if threshold_param is not None:
        try:
            threshold = float(threshold_param)
        except ValueError:
            return jsonify({"error": "Invalid threshold parameter"}), 400
        
    if battery >= threshold:
        return jsonify({"status": "ok", "battery_percent": battery, "threshold_used": threshold, "device": name}), 200
    else:
        return jsonify({"status": "low_battery", "battery_percent": battery, "threshold_used": threshold, "device": name}), 422

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy"}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)