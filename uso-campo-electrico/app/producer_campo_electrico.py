import json
import os
import time

from kafka import KafkaProducer

from simulador_campo_electrico import INTERVALO_S, estado_inicial, siguiente_lectura

TOPIC_CAMPO_ELECTRICO = os.getenv("KAFKA_TOPIC_CAMPO_ELECTRICO", "campo-electrico-eventos")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
INTERVAL_MS = int(os.getenv("ESTACION_INTERVAL_MS", str(INTERVALO_S * 1000)))
ESTACION_IDS = os.getenv("ESTACION_IDS", "estacion-ns-01").split(",")

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    key_serializer=lambda key: key.encode("utf-8"),
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

estado = {estacion_id: estado_inicial() for estacion_id in ESTACION_IDS}

while True:
    for estacion_id in ESTACION_IDS:
        estado[estacion_id] = siguiente_lectura(estado[estacion_id])
        data = {
            "tipoEvento": "estacion.lectura",
            "estacionId": estacion_id,
            **estado[estacion_id],
            "origen": "uso-campo-electrico",
            "timestamp": int(time.time() * 1000),
        }
        meta = producer.send(TOPIC_CAMPO_ELECTRICO, key=estacion_id, value=data).get(timeout=10)
        print(json.dumps({"service": "uso-campo-electrico", "estacionId": estacion_id, "CE": data["CE"],
                          "partition": meta.partition, "offset": meta.offset, "status": "published"}), flush=True)
    time.sleep(INTERVAL_MS / 1000)
