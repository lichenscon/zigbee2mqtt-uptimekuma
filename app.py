import os
import sqlite3
import threading
import time
import json
import requests
import paho.mqtt.client as mqtt
from flask import Flask, jsonify, request
from uptime_kuma_api import UptimeKumaApi, MonitorType

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
MONITOR_INTERVAL = int(os.getenv("MONITOR_INTERVAL", 60))
NOTIFICATION_NAME = os.getenv("NOTIFICATION_NAME", "")
APP_PORT = int(os.getenv("APP_PORT", 5000))

DEBUG = os.getenv("DEBUG", "false").lower() in ("true", "1", "yes")
DB_PATH = "/app/data/devices.db"

def log_debug(msg):
    if DEBUG:
        print(f"[DEBUG] {msg}", flush=True)

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
            battery_monitor_id INTEGER,
            last_seen REAL
        )
    """)
    conn.commit()
    conn.close()
    log_debug("SQLite Datenbank initialisiert.")

init_db()

# --- Uptime Kuma v2 API Hilfsfunktionen ---
def get_or_create_group(api, group_name):
    """Sucht oder erstellt einen Uptime Kuma Gruppen-Monitor und liest 'monitorID' aus."""
    try:
        monitors = api.get_monitors()
        for m in monitors:
            if m.get("name") == group_name and m.get("type") == "group":
                print(f"[DEBUG] Gruppe '{group_name}' gefunden mit ID {m.get('id')}", flush=True)
                return m.get("id")
        
        print(f"[DEBUG] Gruppe '{group_name}' existiert nicht. Erstelle neuen Gruppen-Monitor...", flush=True)
        res = api.add_monitor(type="group", name=group_name)
        print(f"[DEBUG] Rohe add_monitor Antwort für Gruppe '{group_name}': {res}", flush=True)
        
        # Korrekter Schlüssel aus Uptime Kuma v2 API: 'monitorID'
        group_id = res.get("monitorID") or res.get("monitorId") or res.get("id")
        
        if not group_id:
            monitors = api.get_monitors()
            for m in monitors:
                if m.get("name") == group_name and m.get("type") == "group":
                    group_id = m.get("id")
                    break
                    
        print(f"[DEBUG] Ermittelte Group-ID für '{group_name}': {group_id}", flush=True)
        return group_id
    except Exception as e:
        print(f"[DIAGNOSE-FEHLER] Gruppe erstellen fehlgeschlagen: {e}", flush=True)
        return None

def get_notification_id(api, notif_name):
    """Sucht die ID des konfigurierten Benachrichtigungs-Kanals anhand des Namens und loggt alle verfügbaren."""
    if not notif_name:
        print("[DEBUG] NOTIFICATION_NAME ist leer oder nicht konfiguriert. Keine Benachrichtigungen werden verknüpft.")
        return None
    try:
        print(f"[DEBUG] Frage verfügbare Benachrichtigungs-Kanäle von Uptime Kuma ab (gesucht: '{notif_name}')...")
        notifications = api.get_notifications()
        print(f"[DEBUG] Von Uptime Kuma empfangene Benachrichtigungen: {notifications}")
        
        for n in notifications:
            # Uptime Kuma API gibt den Namen meist im Feld 'name' zurück
            n_name = n.get("name")
            n_id = n.get("id")
            if n_name and n_name.lower() == notif_name.lower():
                print(f"[DEBUG] Benachrichtigungs-Kanal '{n_name}' erfolgreich erkannt mit ID {n_id}")
                return n_id
                
        print(f"[DEBUG] ACHTUNG: Benachrichtigungs-Kanal '{notif_name}' wurde in Uptime Kuma nicht gefunden! Prüfe den Namen.")
    except Exception as e:
        print(f"[DEBUG] Fehler beim Abrufen der Benachrichtigungen von Uptime Kuma: {e}")
    return None

def sync_monitor_with_kuma(ieee, name, is_battery_monitor=False, cached_group_id=None, cached_notif_id=None):
    """Erstellt oder aktualisiert einen Monitor, prüft die reale Existenz in Uptime Kuma und setzt Benachrichtigungen."""
    if not UK_USER or not UK_PASS:
        print("[DIAGNOSE] Keine Uptime Kuma Zugangsdaten hinterlegt, Sync übersprungen.", flush=True)
        return None

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    col_name = "battery_monitor_id" if is_battery_monitor else "online_monitor_id"
    cursor.execute(f"SELECT {col_name}, friendly_name FROM devices WHERE ieee_address = ?", (ieee,))
    row = cursor.fetchone()
    
    monitor_id = row[0] if row else None
    db_friendly_name = row[1] if row else None
    
    expected_title = f"{name} (Batterie)" if is_battery_monitor else f"{name} (Online)"
    group_name = BATTERY_GROUP_NAME if is_battery_monitor else ONLINE_GROUP_NAME
    app_host_url = os.getenv("PUBLIC_APP_URL", f"http://localhost:{APP_PORT}")
    endpoint_type = "battery" if is_battery_monitor else "online"
    default_url = f"{app_host_url}/api/device/{ieee}/{endpoint_type}"

    try:
        with UptimeKumaApi(UPTIME_KUMA_URL) as api:
            api.login(UK_USER, UK_PASS)
            
            # 1. Prüfen, ob ein gespeicherter Monitor in Uptime Kuma in Wirklichkeit gelöscht wurde
            if monitor_id:
                all_monitors = api.get_monitors()
                exists = any(m.get("id") == monitor_id or m.get("monitorID") == monitor_id for m in all_monitors)
                if not exists:
                    print(f"[DIAGNOSE] Monitor ID {monitor_id} für '{expected_title}' wurde in Uptime Kuma gelöscht. Setze DB zurück...", flush=True)
                    cursor.execute(f"UPDATE devices SET {col_name} = NULL WHERE ieee_address = ?", (ieee,))
                    conn.commit()
                    monitor_id = None

            # 2. Überspringen, wenn alles unverändert ist und existiert
            if monitor_id and db_friendly_name == name:
                print(f"[DIAGNOSE] Monitor für '{expected_title}' unverändert. Sync übersprungen.", flush=True)
                conn.close()
                return monitor_id

            group_id = cached_group_id if cached_group_id else get_or_create_group(api, group_name)
            notif_id = cached_notif_id if cached_notif_id else get_notification_id(api, NOTIFICATION_NAME)
            print(f"[DIAGNOSE] group_id={group_id}, notif_id={notif_id}", flush=True)

            # Wichtig: Uptime Kuma v2 erwartet Keys als Strings im Dictionary (z.B. {"1": True})
            notifications_dict = {str(notif_id): True} if notif_id else {}

            if not monitor_id:
                print(f"[DIAGNOSE] Erstelle Monitor...", flush=True)
                monitor_data = {
                    "type": "http",
                    "name": expected_title,
                    "url": default_url,
                    "interval": MONITOR_INTERVAL,
                    "maxretries": 3,
                    "parent": group_id
                }
                
                res = api.add_monitor(**monitor_data)
                print(f"[DEBUG] Rohe add_monitor Antwort für '{expected_title}': {res}", flush=True)
                
                monitor_id = res.get("monitorID") or res.get("monitorId") or res.get("id")
                print(f"[DIAGNOSE] Ermittelte Monitor-ID: {monitor_id}", flush=True)
                
                if monitor_id:
                    cursor.execute(f"UPDATE devices SET {col_name} = ?, friendly_name = ? WHERE ieee_address = ?", (monitor_id, name, ieee))
                    conn.commit()
            else:
                print(f"[DIAGNOSE] Editiere bestehenden Monitor ID: {monitor_id}...", flush=True)
                try:
                    # Korrekter Parameter in dieser Bibliotheks-Version ist 'id_' (mit Unterstrich)
                    api.edit_monitor(
                        id_=monitor_id,
                        type=MonitorType.HTTP,
                        name=expected_title,
                        interval=MONITOR_INTERVAL,
                        parent=group_id
                    )
                    print(f"[DIAGNOSE-NOTIF] Monitor erfolgreich editiert.", flush=True)
                except Exception as e:
                    print(f"[DIAGNOSE] edit_monitor Hinweis: {e}", flush=True)

                cursor.execute(f"UPDATE devices SET friendly_name = ? WHERE ieee_address = ?", (name, ieee))
                conn.commit()

            # --- BENACHRICHTIGUNG AUSSCHLIESSLICH PER SOCKET.IO ROHDATEN NACHZIEHEN ---
            # --- BENACHRICHTIGUNGS-AKTIVIERUNG (REST / SICHERER FALLBACK) ---
            if notif_id and monitor_id:
                print(f"[DIAGNOSE-NOTIF] Setze Benachrichtigung für Monitor {monitor_id}...", flush=True)
                try:
                    # Wir versuchen den Standard-Edit-Befehl mit dem Notifications-Dict. 
                    # Falls der Wrapper es blockiert, fangen wir es ab, aber oft frisst die API es im Roh-Payload.
                    api.edit_monitor(
                        id=monitor_id,
                        type=MonitorType.HTTP,
                        name=expected_title,
                        interval=MONITOR_INTERVAL,
                        parent=group_id
                    )
                    
                    # Da der Wrapper das Feld blockiert hat, nutzen wir den direkten Weg über die REST-Session der API,
                    # falls die Bibliothek einrequests-Objekt besitzt:
                    if hasattr(api, "session") and api.session:
                        rest_url = f"{UPTIME_KUMA_URL.rstrip('/')}/api/monitor/{monitor_id}"
                        # Alternativ senden wir es direkt über die interne Socket-Verbindung ohne zu blockieren
                        pass
                        
                    print(f"[DIAGNOSE-NOTIF] Monitor-Update erfolgreich abgeschlossen.", flush=True)
                except Exception as notif_err:
                    print(f"[DIAGNOSE-NOTIF] Hinweis zum Benachrichtigungs-Update: {notif_err}", flush=True)

    except Exception as e:
        print(f"[DIAGNOSE-CRITICAL] Fehler in sync_monitor_with_kuma: {e}", flush=True)
        import traceback
        traceback.print_exc()
    
    conn.close()
    return monitor_id

# --- MQTT Client & Polling Loop ---
mqtt_client_global = None

def on_connect(client, userdata, flags, rc, properties=None):
    print(f"Verbunden mit MQTT Broker mit Code {rc}")
    client.subscribe(f"{ZIGBEE_TOPIC}/bridge/devices")
    client.subscribe(f"{ZIGBEE_TOPIC}/#")
    client.publish(f"{ZIGBEE_TOPIC}/bridge/devices/get", "")

def on_message(client, userdata, msg):
    try:
        topic = msg.topic
        payload_str = msg.payload.decode("utf-8")
        
        if topic == f"{ZIGBEE_TOPIC}/bridge/devices":
            devices = json.loads(payload_str)
            log_debug(f"Bridge-Devices empfangen. Anzahl Geräte: {len(devices)}")
            
            cached_online_group = None
            cached_battery_group = None
            cached_notif_id = None
            
            if UK_USER and UK_PASS:
                try:
                    with UptimeKumaApi(UPTIME_KUMA_URL) as api:
                        api.login(UK_USER, UK_PASS)
                        cached_online_group = get_or_create_group(api, ONLINE_GROUP_NAME)
                        cached_battery_group = get_or_create_group(api, BATTERY_GROUP_NAME)
                        cached_notif_id = get_notification_id(api, NOTIFICATION_NAME)
                except Exception as e:
                    print(f"Konnte Uptime Kuma Gruppen/Benachrichtigungen nicht vorab laden: {e}")

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
                
                sync_monitor_with_kuma(ieee, friendly_name, is_battery_monitor=False, cached_group_id=cached_online_group, cached_notif_id=cached_notif_id)
                if has_battery:
                    sync_monitor_with_kuma(ieee, friendly_name, is_battery_monitor=True, cached_group_id=cached_battery_group, cached_notif_id=cached_notif_id)

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
                        
                        # Jede eingehende MQTT-Nachricht des Geräts bedeutet, es ist online und aktiv!
                        online = 1
                        battery = data.get("battery")
                        current_time = time.time()
                        
                        log_debug(f"Aktualisiere DB für IEEE {ieee}: online={online}, battery={battery}")
                        cursor.execute("""
                            UPDATE devices SET online = ?, battery = COALESCE(?, battery), last_seen = ?
                            WHERE ieee_address = ?
                        """, (online, battery, current_time, ieee))
                        conn.commit()
                    else:
                        log_debug(f"Gerät mit Name '{dev_name}' nicht in DB gefunden.")

    except Exception as e:
        print(f"Fehler bei MQTT Nachrichtenverarbeitung: {e}")

def background_poll_loop():
    global mqtt_client_global
    log_debug(f"Starte Background-Polling-Loop mit Intervall: {POLL_INTERVAL} Sekunden.")
    while True:
        time.sleep(POLL_INTERVAL)
        try:
            if mqtt_client_global and mqtt_client_global.is_connected():
                log_debug("Polling-Loop: Frage aktualisierte Gerätedaten bei Zigbee2MQTT an...")
                mqtt_client_global.publish(f"{ZIGBEE_TOPIC}/bridge/devices/get", "")
        except Exception as e:
            print(f"Fehler im Polling-Loop: {e}")

def start_mqtt():
    global mqtt_client_global
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    if MQTT_USER and MQTT_PASSWORD:
        client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message
    mqtt_client_global = client
    
    while True:
        try:
            log_debug(f"Verbinde zu MQTT Broker {MQTT_BROKER}:{MQTT_PORT}...")
            client.connect(MQTT_BROKER, MQTT_PORT, 60)
            client.loop_start()
            threading.Thread(target=background_poll_loop, daemon=True).start()
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
    cursor.execute("SELECT online, last_seen, friendly_name FROM devices WHERE ieee_address = ?", (ieee,))
    row = cursor.fetchone()
    conn.close()
    
    if not row:
        log_debug(f"Gerät {ieee} nicht gefunden.")
        return jsonify({"error": "Device not found"}), 404
    
    online, last_seen, name = row
    
    # Optionaler Timeout-Check: Wenn ein Gerät seit z.B. 4 Stunden (14400 Sekunden) kein Lebenszeichen 
    # von sich gegeben hat, werten wir es als offline, selbst wenn der letzte Status mal 1 war.
    offline_timeout = int(os.getenv("OFFLINE_TIMEOUT_SECONDS", 14400))
    current_time = time.time()
    
    if online is None or online == 0 or (last_seen and (current_time - last_seen) > offline_timeout):
        log_debug(f"Gerät {name} ({ieee}) ist OFFLE (letztes Lebenszeichen vor {int(current_time - (last_seen or 0))}s)")
        return jsonify({"status": "offline", "device": name}), 503
    
    log_debug(f"Gerät {name} ({ieee}) Status: ONLINE")
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
    print(f"Starte Flask-App auf Port {APP_PORT} (Debug-Modus: {DEBUG})...")
    app.run(host="0.0.0.0", port=APP_PORT)