import os
import random
import socket
import time

import requests


GATEWAY_HOST = os.getenv("GATEWAY_HOST", "gateway")
GATEWAY_PORT = int(os.getenv("GATEWAY_PORT", "5000"))
INTERVAL = float(os.getenv("SEND_INTERVAL", "5"))
SENSOR_ID = os.getenv("SENSOR_ID", "gateway_temperature_01")
MIN_VALUE = float(os.getenv("MIN_VALUE", "20"))
MAX_VALUE = float(os.getenv("MAX_VALUE", "30"))

URL = f"http://{GATEWAY_HOST}:{GATEWAY_PORT}/data"

print(f"[BOOT] Sensor {SENSOR_ID} started", flush=True)
print(f"[BOOT] Gateway endpoint: {URL}", flush=True)
print(f"[BOOT] Send interval: {INTERVAL}s", flush=True)


def wait_for_gateway(host, port):
    """Wait until the gateway HTTP server is reachable."""

    print("[WAIT] Waiting for gateway...", flush=True)
    while True:
        try:
            socket.create_connection((host, port), timeout=5)
            print("[WAIT] Gateway reachable", flush=True)
            return
        except OSError as error:
            print(f"[WAIT] Gateway unavailable ({error}), retrying...", flush=True)
            time.sleep(2)


def build_payload(temperature):
    """Create the JSON body sent by the sensor to the gateway."""

    return {
        "sensor": SENSOR_ID,
        "type": "temperature",
        "unit": "celsius",
        "temperatura": round(temperature, 2),
        "timestamp": time.time(),
    }


def send_payload(payload):
    """Send one reading to the gateway and measure HTTP client latency."""

    request_started_at = time.perf_counter()
    response = requests.post(URL, json=payload, timeout=5)
    request_latency = time.perf_counter() - request_started_at
    response.raise_for_status()

    return response, request_latency


if MIN_VALUE > MAX_VALUE:
    raise ValueError("MIN_VALUE must be less than or equal to MAX_VALUE")

if INTERVAL <= 0:
    raise ValueError("SEND_INTERVAL must be greater than 0")

wait_for_gateway(GATEWAY_HOST, GATEWAY_PORT)

print("[HTTP] Ready to send data to gateway", flush=True)

while True:
    temperature = random.uniform(MIN_VALUE, MAX_VALUE)
    payload = build_payload(temperature)

    try:
        response, http_latency = send_payload(payload)

        print(
            f"[SEND -> GATEWAY] {payload} (status={response.status_code})",
            flush=True,
        )
        print(f"[HTTP LATENCY SENSOR->GATEWAY] {http_latency:.4f}s", flush=True)

    except requests.exceptions.RequestException as error:
        print(f"[ERROR] Gateway send failed: {error}", flush=True)

    time.sleep(INTERVAL)
