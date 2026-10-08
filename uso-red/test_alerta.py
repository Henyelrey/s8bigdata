import paho.mqtt.publish as publish
import json
import time

topic = "lambda26/red/llsw3/telemetria"
payload_dict = {
    "tipoEvento": "red.lectura",
    "sensorId": "Ethernet",
    "bytesSentPerSec": 999999999.0,
    "bytesRecvPerSec": 999999999.0,
    "pktsSentPerSec": 50.0,
    "pktsRecvPerSec": 50.0,
    "errin": 0,
    "errout": 0,
    "dropin": 0,
    "dropout": 0,
    "velocidadEnlaceMbps": 1000,
    "origen": "agente-windows",
    "timestamp": int(time.time() * 1000)
}

payload = json.dumps(payload_dict)
publish.single(topic, payload=payload, hostname="localhost", port=41883)
print("Mensaje 'alerta' (999 999 999 B/s) enviado correctamente a Mosquitto.")
