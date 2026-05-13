import json
import os
import random
import socket
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import paho.mqtt.client as mqtt


@dataclass(frozen=True)
class SensorConfig:
    sensor_name: str
    min_value: float
    max_value: float
    guard_level: float
    mqtt_host: str
    mqtt_port: int
    mqtt_username: str | None
    mqtt_password: str | None
    mqtt_topic: str
    interval_seconds: float


def read_float_env(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default

    try:
        return float(value)
    except ValueError as error:
        raise ValueError(f"{name} must be a number, got {value!r}") from error


def read_int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default

    try:
        return int(value)
    except ValueError as error:
        raise ValueError(f"{name} must be an integer, got {value!r}") from error


def read_config() -> SensorConfig:
    sensor_name = os.getenv("SENSOR_NAME", "sensor-temperature")

    config = SensorConfig(
        sensor_name=sensor_name,
        min_value=read_float_env("MIN_VALUE", 10.0),
        max_value=read_float_env("MAX_VALUE", 40.0),
        guard_level=read_float_env("GUARD_LEVEL", 30.0),
        mqtt_host=os.getenv("MQTT_HOST", "localhost"),
        mqtt_port=read_int_env("MQTT_PORT", 883),
        mqtt_username=os.getenv("MQTT_USERNAME"),
        mqtt_password=os.getenv("MQTT_PASSWORD"),
        mqtt_topic=os.getenv("MQTT_TOPIC", f"sensors/{sensor_name}/temperature"),
        interval_seconds=read_float_env("INTERVAL_SECONDS", 5.0),
    )

    if config.min_value > config.max_value:
        raise ValueError("MIN_VALUE must be less than or equal to MAX_VALUE")

    if config.interval_seconds <= 0:
        raise ValueError("INTERVAL_SECONDS must be greater than 0")

    return config


def create_client(config: SensorConfig) -> mqtt.Client:
    client_id = f"{config.sensor_name}-{socket.gethostname()}"

    try:
        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
        )
    except AttributeError:
        client = mqtt.Client(client_id=client_id)

    if config.mqtt_username:
        client.username_pw_set(config.mqtt_username, config.mqtt_password)

    return client


def connect_with_retry(client: mqtt.Client, config: SensorConfig) -> None:
    while True:
        try:
            client.connect(config.mqtt_host, config.mqtt_port, keepalive=60)
            client.loop_start()
            print(
                f"Connected to MQTT broker at {config.mqtt_host}:{config.mqtt_port}",
                flush=True,
            )
            return
        except OSError as error:
            print(f"MQTT connection failed: {error}. Retrying in 5 seconds.", flush=True)
            time.sleep(5)


def build_message(config: SensorConfig, temperature: float) -> str:
    payload = {
        "sensor": config.sensor_name,
        "type": "temperature",
        "value": round(temperature, 2),
        "unit": "celsius",
        "guard_level": config.guard_level,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    return json.dumps(payload)


def run_sensor() -> None:
    config = read_config()
    client = create_client(config)
    connect_with_retry(client, config)

    print(
        f"{config.sensor_name} running: range={config.min_value}-{config.max_value}, "
        f"guard_level={config.guard_level}, topic={config.mqtt_topic}",
        flush=True,
    )

    try:
        while True:
            temperature = random.uniform(config.min_value, config.max_value)
            print(f"Generated temperature: {temperature:.2f} C", flush=True)

            if temperature > config.guard_level:
                message = build_message(config, temperature)
                result = client.publish(config.mqtt_topic, message, qos=1)
                result.wait_for_publish()
                print(f"Published alert: {message}", flush=True)

            time.sleep(config.interval_seconds)
    except KeyboardInterrupt:
        print("Stopping temperature sensor.", flush=True)
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    run_sensor()
