from flask import Flask, request
import threading
import time
import json
import os
import paho.mqtt.client as mqtt

# === Configurazione ===
MQTT_BROKER = os.getenv("MQTT_BROKER", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
TOPIC = os.getenv("MQTT_TOPIC", "iot/aula/gateway")

# === App Flask ===
app = Flask(__name__)

# ✅ Buffer locale
queue = []

# === MQTT Client ===
client = mqtt.Client()
client.connect(MQTT_BROKER, MQTT_PORT)
client.loop_start()

print(f"[GATEWAY] Connesso a MQTT {MQTT_BROKER}:{MQTT_PORT}", flush=True)

# === Endpoint HTTP ===
@app.route('/data', methods=['POST'])
def receive_data():
    data = request.json

    # ✅ aggiungi timestamp ricezione (debug + analisi)
    data["gateway_ts"] = time.time()

    queue.append(data)

    print(f"[GATEWAY][RECEIVE] Dato ricevuto: {data}", flush=True)
    print(f"[GATEWAY][QUEUE] Lunghezza coda: {len(queue)}", flush=True)

    return {"status": "ok"}, 200

# === Thread invio MQTT ===
def sender_loop():
    global queue

    while True:
        if queue:
            msg = queue.pop(0)

            try:
                start_time = time.time()

                result = client.publish(TOPIC, json.dumps(msg), qos=1)
                result.wait_for_publish()

                latency = time.time() - start_time

                print(
                    f"[GATEWAY][SEND OK] → MQTT {msg} "
                    f"(latency={round(latency, 3)}s)",
                    flush=True
                )

            except Exception as e:
                print(
                    f"[GATEWAY][ERROR] Invio fallito, reinserisco in coda: {e}",
                    flush=True
                )

                # ✅ reinserisce in testa (NON perdi dati)
                queue.insert(0, msg)

        time.sleep(1)

# === Avvio thread ===
threading.Thread(target=sender_loop, daemon=True).start()

# === Avvio server ===
if __name__ == "__main__":
    print("[GATEWAY] Avvio server HTTP porta 5000", flush=True)
    app.run(host="0.0.0.0", port=5000)