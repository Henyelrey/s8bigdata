import json
import os
import time
import uuid
import psutil
import paho.mqtt.client as mqtt

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "41883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "lambda26/red/llsw3/telemetria")
INTERVAL_MS = int(os.getenv("AGENTE_INTERVAL_MS", "1000"))
AGENTE_INTERFACES = os.getenv("AGENTE_INTERFACES", "")

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"agente-red-{uuid.uuid4().hex[:8]}")

def on_connect(client, userdata, flags, reason_code, properties=None):
    print(f"MQTT conectado: {reason_code}")

client.on_connect = on_connect

print(f"Conectando a broker MQTT en {MQTT_HOST}:{MQTT_PORT}...")
try:
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
except Exception as e:
    print(f"Error conectando a MQTT: {e}")
    exit(1)

client.loop_start()

interfaces_permitidas = []
if AGENTE_INTERFACES:
    interfaces_permitidas = [i.strip() for i in AGENTE_INTERFACES.split(",")]

last_io = psutil.net_io_counters(pernic=True)
last_time = time.time()

print(f"Iniciando lecturas de red cada {INTERVAL_MS} ms...")

while True:
    time.sleep(INTERVAL_MS / 1000.0)
    
    current_time = time.time()
    current_io = psutil.net_io_counters(pernic=True)
    if_stats = psutil.net_if_stats()
    
    dt = current_time - last_time
    if dt <= 0:
        dt = 0.001
        
    for nic_name, io in current_io.items():
        if interfaces_permitidas and nic_name not in interfaces_permitidas:
            continue
            
        stats = if_stats.get(nic_name)
        if stats and not stats.isup:
            continue
            
        if "Loopback" in nic_name or "Pseudo-Interface" in nic_name:
            continue

        prev_io = last_io.get(nic_name)
        if not prev_io:
            continue
            
        speed_mbps = stats.speed if stats and stats.speed > 0 else 1000
            
        bytes_sent_sec = (io.bytes_sent - prev_io.bytes_sent) / dt
        bytes_recv_sec = (io.bytes_recv - prev_io.bytes_recv) / dt
        pkts_sent_sec = (io.packets_sent - prev_io.packets_sent) / dt
        pkts_recv_sec = (io.packets_recv - prev_io.packets_recv) / dt
        
        event = {
            "tipoEvento": "red.lectura",
            "sensorId": nic_name,
            "bytesSentPerSec": bytes_sent_sec,
            "bytesRecvPerSec": bytes_recv_sec,
            "pktsSentPerSec": pkts_sent_sec,
            "pktsRecvPerSec": pkts_recv_sec,
            "errin": io.errin,
            "errout": io.errout,
            "dropin": io.dropin,
            "dropout": io.dropout,
            "velocidadEnlaceMbps": speed_mbps,
            "origen": "agente-windows",
            "timestamp": int(current_time * 1000)
        }
        
        payload = json.dumps(event)
        client.publish(MQTT_TOPIC, payload)
        print(f"Publicado {nic_name} -> {bytes_recv_sec:.1f} B/s recv, {bytes_sent_sec:.1f} B/s sent")
        
    last_io = current_io
    last_time = current_time
