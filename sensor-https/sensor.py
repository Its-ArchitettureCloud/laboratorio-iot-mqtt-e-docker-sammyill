import time
import random
import os
import socket
import requests

# === Parametri configurabili via environment ===
SERVER = os.getenv("HTTP_SERVER", "nodered")
PORT = int(os.getenv("HTTP_PORT", "1880"))
ENDPOINT = os.getenv("HTTP_ENDPOINT", "/sensor")
INTERVAL = int(os.getenv("SEND_INTERVAL", "5"))
SENSOR_ID = os.getenv("SENSOR_ID", "sensore01")

URL = f"http://{SERVER}:{PORT}{ENDPOINT}"

print(f"[BOOT] Sensore {SENSOR_ID} avviato", flush=True)
print(f"[BOOT] HTTP endpoint: {URL}", flush=True)
print(f"[BOOT] Intervallo invio: {INTERVAL}s", flush=True)

# === Attesa attiva del server HTTP ===
def wait_for_server(host, port):
    print("[WAIT] Attendo il server HTTP...", flush=True)
    while True:
        try:
            socket.create_connection((host, port), timeout=5)
            print("[WAIT] Server HTTP raggiungibile", flush=True)
            return
        except OSError as e:
            print(f"[WAIT] Server non disponibile ({e}), ritento...", flush=True)
            time.sleep(2)

wait_for_server(SERVER, PORT)

print("[HTTP] Pronto per invio dati", flush=True)

# === Loop principale del sensore ===
while True:
    temperatura = round(random.uniform(15, 35), 1)

    payload = {
        "sensor_id": SENSOR_ID,
        "temperatura": temperatura,
        "timestamp": time.time()
    }

    try:
        response = requests.post(URL, json=payload, timeout=5)

        print(
            f"[POST] {URL} → {payload} (status={response.status_code})",
            flush=True
        )

    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Invio fallito: {e}", flush=True)

    time.sleep(INTERVAL)