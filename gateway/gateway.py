import json
import os
import queue
import socket
import threading
import time

from flask import Flask, request
import paho.mqtt.client as mqtt


MQTT_BROKER = os.getenv("MQTT_BROKER", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")
MQTT_TOPIC_TEMPLATE = os.getenv(
    "MQTT_TOPIC_TEMPLATE",
    "sensors/{sensor_id}/temperature",
)

HTTP_HOST = os.getenv("GATEWAY_HTTP_HOST", "0.0.0.0")
HTTP_PORT = int(os.getenv("GATEWAY_HTTP_PORT", "5000"))

app = Flask(__name__)
message_queue = queue.Queue()


def create_mqtt_client():
    """Create the MQTT client used by the gateway to publish sensor data."""

    client_id = f"sensor-gateway-{socket.gethostname()}"

    try:
        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
        )
    except AttributeError:
        client = mqtt.Client(client_id=client_id)

    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    return client


def connect_mqtt_with_retry(client):
    """Connect to Mosquitto and retry until the broker is ready."""

    while True:
        try:
            client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
            client.loop_start()
            print(
                f"[GATEWAY] Connected to MQTT {MQTT_BROKER}:{MQTT_PORT}",
                flush=True,
            )
            return
        except OSError as error:
            print(
                f"[GATEWAY][WAIT] MQTT unavailable ({error}), retrying...",
                flush=True,
            )
            time.sleep(2)


def normalize_sensor_payload(data):
    """Validate the HTTP payload and convert it to the MQTT payload shape."""

    sensor = data.get("sensor")
    if not sensor:
        raise ValueError("missing sensor")

    raw_temperature = data.get("temperatura")
    temperature = float(raw_temperature)

    timestamp = data.get("timestamp", time.time())

    return {
        "sensor": sensor,
        "type": "temperature",
        "unit": "celsius",
        "temperatura": round(temperature, 2),
        "timestamp": timestamp,
    }


def topic_for(sensor_id):
    """Build the MQTT topic for a specific sensor."""

    return MQTT_TOPIC_TEMPLATE.format(sensor_id=sensor_id)


@app.route("/data", methods=["POST"])
def receive_data():
    """Receive one sensor reading over HTTP and enqueue it for MQTT publish."""

    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return {"status": "error", "message": "invalid JSON body"}, 400

    try:
        message = normalize_sensor_payload(data)
    except (TypeError, ValueError) as error:
        return {"status": "error", "message": str(error)}, 400

    message_queue.put(message)

    print(f"[GATEWAY][RECEIVE] {message}", flush=True)
    print(f"[GATEWAY][QUEUE] length={message_queue.qsize()}", flush=True)

    return {"status": "ok", "sensor": message["sensor"]}, 200


def sender_loop(client):
    """Continuously publish queued HTTP readings to Mosquitto."""

    while True:
        message = message_queue.get()
        topic = topic_for(message["sensor"])

        try:
            publish_started_at = time.perf_counter()
            result = client.publish(topic, json.dumps(message), qos=1)
            result.wait_for_publish()
            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError(f"MQTT publish returned result code {result.rc}")

            publish_latency = time.perf_counter() - publish_started_at

            print(
                f"[GATEWAY][SEND OK] {topic} -> {message} "
                f"(latency={publish_latency:.4f}s, result_code={result.rc})",
                flush=True,
            )
            message_queue.task_done()

        except Exception as error:
            print(
                f"[GATEWAY][ERROR] Publish failed, requeueing message: {error}",
                flush=True,
            )
            message_queue.put(message)
            message_queue.task_done()
            time.sleep(2)


mqtt_client = create_mqtt_client()
connect_mqtt_with_retry(mqtt_client)
threading.Thread(target=sender_loop, args=(mqtt_client,), daemon=True).start()


if __name__ == "__main__":
    print(f"[GATEWAY] Starting HTTP server on {HTTP_HOST}:{HTTP_PORT}", flush=True)
    app.run(host=HTTP_HOST, port=HTTP_PORT)
