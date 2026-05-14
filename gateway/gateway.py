import json
import os
import socket
import sqlite3
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
DB_PATH = os.getenv("GATEWAY_DB_PATH", "/data/gateway_queue.db")
MQTT_RETRY_SECONDS = float(os.getenv("MQTT_RETRY_SECONDS", "2"))
PUBLISH_IDLE_SECONDS = float(os.getenv("PUBLISH_IDLE_SECONDS", "1"))

app = Flask(__name__)


def open_database():
    """Open a short-lived SQLite connection for one operation."""

    connection = sqlite3.connect(DB_PATH, timeout=30)
    connection.row_factory = sqlite3.Row
    return connection


def initialize_database():
    """Create the persistent queue table used while Mosquitto is unavailable."""

    database_dir = os.path.dirname(DB_PATH)
    if database_dir:
        os.makedirs(database_dir, exist_ok=True)

    with open_database() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at REAL NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_messages_created_at
            ON messages (created_at)
            """
        )


def create_mqtt_client():
    """Create the MQTT client used by the gateway publisher worker."""

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


def connect_mqtt(client):
    """Try to connect to Mosquitto once and return whether it worked."""

    try:
        result_code = client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
        if result_code != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f"MQTT connect returned result code {result_code}")

        client.loop_start()
        print(f"[GATEWAY] Connected to MQTT {MQTT_BROKER}:{MQTT_PORT}", flush=True)
        return True
    except Exception as error:
        print(
            f"[GATEWAY][WAIT] MQTT unavailable ({error}), retrying later",
            flush=True,
        )
        return False


def normalize_sensor_payload(data):
    """Validate an incoming sensor message and keep only the allowed fields."""

    sensor = data.get("sensor")
    if not sensor:
        raise ValueError("missing sensor")

    raw_temperature = data.get("temperatura")
    temperature = float(raw_temperature)

    return {
        "sensor": sensor,
        "type": "temperature",
        "unit": "celsius",
        "temperatura": round(temperature, 2),
        "timestamp": data.get("timestamp", time.time()),
    }


def topic_for(sensor):
    """Build the MQTT topic for a specific sensor."""

    return MQTT_TOPIC_TEMPLATE.format(sensor_id=sensor)


def enqueue_message(topic, payload):
    """Persist one message before acknowledging the sensor request."""

    with open_database() as connection:
        cursor = connection.execute(
            """
            INSERT INTO messages (topic, payload, created_at)
            VALUES (?, ?, ?)
            """,
            (topic, json.dumps(payload), time.time()),
        )
        message_id = cursor.lastrowid
        queued_count = connection.execute(
            "SELECT COUNT(*) FROM messages"
        ).fetchone()[0]

    return message_id, queued_count


def get_oldest_message():
    """Read the oldest queued message without removing it."""

    with open_database() as connection:
        row = connection.execute(
            """
            SELECT id, topic, payload
            FROM messages
            ORDER BY id
            LIMIT 1
            """
        ).fetchone()

    if row is None:
        return None

    return {
        "id": row["id"],
        "topic": row["topic"],
        "payload": row["payload"],
    }


def delete_message(message_id):
    """Remove a message only after MQTT publish confirmation."""

    with open_database() as connection:
        connection.execute("DELETE FROM messages WHERE id = ?", (message_id,))


def queued_message_count():
    """Return how many messages are still waiting in the persistent queue."""

    with open_database() as connection:
        return connection.execute("SELECT COUNT(*) FROM messages").fetchone()[0]


@app.route("/data", methods=["POST"])
def receive_data():
    """Receive one sensor reading, store it, then respond to the sensor."""

    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return {"status": "error", "message": "invalid JSON body"}, 400

    try:
        payload = normalize_sensor_payload(data)
    except (TypeError, ValueError) as error:
        return {"status": "error", "message": str(error)}, 400

    topic = topic_for(payload["sensor"])
    message_id, queued_count = enqueue_message(topic, payload)

    print(
        f"[GATEWAY][QUEUED] id={message_id} topic={topic} "
        f"queue_length={queued_count} payload={payload}",
        flush=True,
    )

    return {
        "status": "queued",
        "id": message_id,
        "queue_length": queued_count,
    }, 200


def publish_queued_messages():
    """Publish persisted messages in order whenever Mosquitto is reachable."""

    client = None

    while True:
        if client is None or not client.is_connected():
            client = create_mqtt_client()
            if not connect_mqtt(client):
                client = None
                time.sleep(MQTT_RETRY_SECONDS)
                continue

        queued_message = get_oldest_message()
        if queued_message is None:
            time.sleep(PUBLISH_IDLE_SECONDS)
            continue

        try:
            publish_started_at = time.perf_counter()
            result = client.publish(
                queued_message["topic"],
                queued_message["payload"],
                qos=1,
            )
            result.wait_for_publish()
            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError(f"MQTT publish returned result code {result.rc}")

            delete_message(queued_message["id"])
            publish_latency = time.perf_counter() - publish_started_at

            print(
                f"[GATEWAY][PUBLISHED] id={queued_message['id']} "
                f"topic={queued_message['topic']} "
                f"latency={publish_latency:.4f}s "
                f"queue_length={queued_message_count()}",
                flush=True,
            )

        except Exception as error:
            print(
                f"[GATEWAY][MQTT ERROR] keeping queued data: {error}",
                flush=True,
            )
            try:
                client.loop_stop()
                client.disconnect()
            except Exception:
                pass
            client = None
            time.sleep(MQTT_RETRY_SECONDS)


initialize_database()
threading.Thread(target=publish_queued_messages, daemon=True).start()


if __name__ == "__main__":
    print(f"[GATEWAY] Persistent queue database: {DB_PATH}", flush=True)
    print(f"[GATEWAY] Starting HTTP server on {HTTP_HOST}:{HTTP_PORT}", flush=True)
    app.run(host=HTTP_HOST, port=HTTP_PORT)
