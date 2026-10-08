# uso-red

Ingesta IoT del Proyecto Sello (tráfico de red del campus, equipo LLSW3) para
la actividad autónoma de S7. El "sensor" es **la tarjeta de red física de esta
PC**: un agente lee cada segundo sus contadores reales (bytes y paquetes
recibidos y enviados, errores, descartes) y publica la lectura por MQTT. Desde
ahí sigue el mismo patrón de S7: dispositivo → MQTT → puente → Kafka → consumer
con validación de esquema y de rango físico.

```text
NIC física (Ethernet) ─┐
NIC virtuales ─────────┤  agente/agente_red.py   (Windows, fuera de Docker)
                       ▼  MQTT  lambda26/red/llsw3/telemetria
               mosquitto (contenedor, puerto 41883 del host)
                       ▼
               app/bridge_mqtt_kafka.py  → Kafka: red-telemetria (3 particiones, key = sensorId)
                       ▼
               app/consumer_red.py       → consumed | invalid | alerta | saturacion
```

El agente corre en Windows y no en un contenedor porque Docker Desktop no ve
las NIC físicas del host, solo su red virtual.

## Uso

Requiere que `kafka/compose.yml` esté arriba, porque crea la red `lambda26-kafka-net`.

```powershell
docker compose -f kafka/compose.yml exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic red-telemetria --partitions 3 --replication-factor 1 --bootstrap-server kafka:9092
docker compose -f uso-red/compose.yml up -d --build
python -m pip install --user -r uso-red/agente/requirements.txt
```

Usa tres terminales:

```powershell
docker compose -f uso-red/compose.yml exec uso-red python /app/consumer_red.py
docker compose -f uso-red/compose.yml exec uso-red python /app/bridge_mqtt_kafka.py
python -u uso-red/agente/agente_red.py
```

## Estados del consumer

| `status` | Significa |
|---|---|
| `consumed` | Lectura válida y dentro de rango. |
| `invalid` | Falla de **esquema**: no es JSON, no es un objeto, falta un campo o un campo tiene el tipo equivocado. |
| `alerta` | Falla de **rango físico**: tasas negativas (contador reiniciado) o tráfico por encima de la capacidad del enlace (`velocidadEnlaceMbps` × 125 000 B/s, con 5 % de tolerancia). |
| `saturacion` | Lectura físicamente posible, pero que supera el umbral operativo de uso del enlace (`UMBRAL_SATURACION_PCT`, 70 % por defecto). |

## Variables de entorno

| Variable | Por defecto | Aplica a |
|---|---|---|
| `MQTT_HOST` / `MQTT_PORT` | `localhost` / `41883` (agente), `mosquitto` / `1883` (bridge) | agente y bridge |
| `MQTT_TOPIC` | `lambda26/red/llsw3/telemetria` | agente y bridge |
| `AGENTE_INTERVAL_MS` | `1000` | agente |
| `AGENTE_INTERFACES` | *(vacío = todas las activas, salvo loopback/teredo)* | agente |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | bridge y consumer |
| `KAFKA_TOPIC_RED` | `red-telemetria` | bridge y consumer |
| `KAFKA_GROUP_ID` | `uso-red-group` | consumer |
| `UMBRAL_SATURACION_PCT` | `70` | consumer |

El contrato completo de `red.lectura` está en el informe S07 (Tablas 1 y 2).
