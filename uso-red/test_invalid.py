import paho.mqtt.publish as publish

topic = "lambda26/red/llsw3/telemetria"
payload = "esto no es JSON"

publish.single(topic, payload=payload, hostname="localhost", port=41883)
print("Mensaje 'invalid' enviado correctamente a Mosquitto.")
