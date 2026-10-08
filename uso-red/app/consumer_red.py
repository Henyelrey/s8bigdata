import json
import os
import time
from kafka import KafkaConsumer

TOPIC_RED = os.getenv("KAFKA_TOPIC_RED", "red-telemetria")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "uso-red-group")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
UMBRAL_SATURACION_PCT = float(os.getenv("UMBRAL_SATURACION_PCT", "70"))

def deserialize_message(value):
    text = value.decode("utf-8")
    try:
        return {
            "payload": json.loads(text),
            "raw": text,
            "isJson": True,
            "decodeError": None,
        }
    except json.JSONDecodeError as ex:
        return {
            "payload": None,
            "raw": text,
            "isJson": False,
            "decodeError": str(ex),
        }

consumer = KafkaConsumer(
    TOPIC_RED,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=GROUP_ID,
    value_deserializer=deserialize_message,
    key_deserializer=lambda key: key.decode("utf-8") if key is not None else None,
)

print(json.dumps({
    "service": "uso-red",
    "component": "consumer",
    "topic": TOPIC_RED,
    "groupId": GROUP_ID,
    "bootstrapServers": BOOTSTRAP_SERVERS,
    "status": "listening",
}))

def procesar_lectura(event, raw_payload, decode_error, is_json):
    timestamp = event.get("timestamp") if is_json and isinstance(event, dict) else None
    processed_at = int(time.time() * 1000)
    latency_ms = processed_at - timestamp if timestamp is not None else None

    required_fields = [
        "tipoEvento", "sensorId", "bytesSentPerSec", "bytesRecvPerSec", 
        "pktsSentPerSec", "pktsRecvPerSec", "errin", "errout", "dropin", 
        "dropout", "velocidadEnlaceMbps", "timestamp"
    ]
    
    is_valid = is_json and isinstance(event, dict) and all(event.get(f) is not None for f in required_fields)
    
    status = "invalid"
    if is_valid:
        capacidad_bytes_sec = event["velocidadEnlaceMbps"] * 125000.0
        umbral_alerta = capacidad_bytes_sec * 1.05
        
        rates = [event["bytesSentPerSec"], event["bytesRecvPerSec"], event["pktsSentPerSec"], event["pktsRecvPerSec"]]
        
        is_alerta = False
        for rate in rates:
            if rate < 0:
                is_alerta = True
        
        if event["bytesSentPerSec"] > umbral_alerta or event["bytesRecvPerSec"] > umbral_alerta:
            is_alerta = True
            
        if is_alerta:
            status = "alerta"
        else:
            umbral_saturacion = capacidad_bytes_sec * (UMBRAL_SATURACION_PCT / 100.0)
            if event["bytesSentPerSec"] > umbral_saturacion or event["bytesRecvPerSec"] > umbral_saturacion:
                status = "saturacion"
            else:
                status = "consumed"

    log = {
        "service": "uso-red",
        "component": "consumer",
        "topic": TOPIC_RED,
        "groupId": GROUP_ID,
        "eventType": event.get("tipoEvento") if is_json and isinstance(event, dict) else None,
        "sensorId": event.get("sensorId") if is_json and isinstance(event, dict) else None,
        "timestamp": timestamp,
        "isValid": is_valid,
        "processedAt": processed_at,
        "latencyMs": latency_ms,
        "rawPayload": raw_payload,
        "isJson": is_json,
        "decodeError": decode_error,
        "status": status,
    }
    print(json.dumps(log))


for msg in consumer:
    decoded = msg.value
    event = decoded["payload"] if decoded["isJson"] else {}
    procesar_lectura(event, decoded["raw"], decoded["decodeError"], decoded["isJson"])
