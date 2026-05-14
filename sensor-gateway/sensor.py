import time
import random
import os
import socket
import requests

# === Parametri configurabili via environment ===
GATEWAY_HOST = os.getenv("GATEWAY_HOST", "gateway")
GATEWAY_PORT = int(os.getenv("GATEWAY_PORT", "5000"))
INTERVAL = int(os.getenv("SEND_INTERVAL", "5"))
SENSOR_ID = os.getenv("SENSOR_ID", "sensore01")

URL = f"http://{GATEWAY_HOST}:{GATEWAY_PORT}/data"

print(f"[BOOT] Sensore {SENSOR_ID} avviato", flush=True)
print(f"[BOOT] Gateway endpoint: {URL}", flush=True)
print(f"[BOOT] Intervallo invio: {INTERVAL}s", flush=True)

# === Attesa attiva del gateway ===
def wait_for_gateway(host, port):
    print("[WAIT] Attendo il gateway...", flush=True)
    while True:
        try:
            socket.create_connection((host, port), timeout=5)
            print("[WAIT] Gateway raggiungibile ✅", flush=True)
            return
        except OSError as e:
            print(f"[WAIT] Gateway non disponibile ({e}), ritento...", flush=True)
            time.sleep(2)

wait_for_gateway(GATEWAY_HOST, GATEWAY_PORT)

print("[HTTP] Pronto per invio dati al gateway", flush=True)

# === Loop principale del sensore ===
while True:
    temperatura = round(random.uniform(15, 35), 1)

    # ✅ timestamp lato sensore
    timestamp = time.time()

    payload = {
        "sensor_id": SENSOR_ID,
        "temperatura": temperatura,
        "timestamp": timestamp
    }

    try:
        # ✅ misura latenza HTTP (sensore → gateway)
        start_time = time.time()

        response = requests.post(URL, json=payload, timeout=5)

        http_latency = time.time() - start_time

        print(
            f"[SEND → GATEWAY] {payload} "
            f"(status={response.status_code})",
            flush=True
        )

        print(
            f"[HTTP LATENCY SENSORE→GATEWAY] {round(http_latency, 3)}s",
            flush=True
        )

    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Invio al gateway fallito: {e}", flush=True)

    time.sleep(INTERVAL)