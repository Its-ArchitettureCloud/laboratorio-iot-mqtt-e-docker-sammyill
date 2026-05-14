import os
import random
import socket
import time

import requests


# Runtime settings come from Docker Compose environment variables.
SERVER = os.getenv("HTTP_SERVER", "nodered")
PORT = int(os.getenv("HTTP_PORT", "1880"))
ENDPOINT = os.getenv("HTTP_ENDPOINT", "/sensor")
INTERVAL = int(os.getenv("SEND_INTERVAL", "5"))
SENSOR_ID = os.getenv("SENSOR_ID", "sensore01")

URL = f"http://{SERVER}:{PORT}{ENDPOINT}"

print(f"[BOOT] Sensore {SENSOR_ID} avviato", flush=True)
print(f"[BOOT] HTTP endpoint: {URL}", flush=True)
print(f"[BOOT] Intervallo invio: {INTERVAL}s", flush=True)


def wait_for_server(host, port):
    """Wait until Node-RED accepts TCP connections."""

    print("[WAIT] Attendo il server HTTP...", flush=True)
    while True:
        try:
            # create_connection only checks reachability; it does not send sensor data.
            socket.create_connection((host, port), timeout=5)
            print("[WAIT] Server HTTP raggiungibile", flush=True)
            return
        except OSError as error:
            print(f"[WAIT] Server non disponibile ({error}), ritento...", flush=True)
            time.sleep(2)


def build_payload(temperature):
    """Build the JSON body that will be sent to Node-RED."""

    return {
        "sensor": SENSOR_ID,
        "type": "temperature",
        "unit": "celsius",
        "temperatura": round(temperature, 2),
        "timestamp": time.time(), 
    }


def send_payload(payload):
    """Send one HTTP POST request and return the response plus client latency."""

    # perf_counter is best for measuring short durations because it is monotonic.
    request_started_at = time.perf_counter()
    response = requests.post(URL, json=payload, timeout=5)
    request_latency_seconds = time.perf_counter() - request_started_at

    return response, request_latency_seconds


wait_for_server(SERVER, PORT)

print("[HTTP] Pronto per invio dati", flush=True)

# Main loop: generate one reading, send it to Node-RED, then wait.
while True:
    # Generate a simulated temperature value for this sensor.
    temperatura = round(random.uniform(15, 35), 1)
    payload = build_payload(temperatura)

    try:
        response, client_latency = send_payload(payload)

        print(
            f"[POST] {URL} -> {payload} (status={response.status_code})",
            flush=True,
        )
        print(f"[HTTP LATENCY] {client_latency:.4f}s", flush=True)

    except requests.exceptions.RequestException as error:
        print(f"[ERROR] Invio fallito: {error}", flush=True)

    time.sleep(INTERVAL)
