import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer


def build_event(device_id: str) -> dict:
    temperature = round(random.uniform(18.0, 95.0), 2)
    anomaly_score = round(min(1.0, max(0.0, random.gauss(0.08, 0.15))), 4)
    status = "ok" if anomaly_score < 0.4 else "warn" if anomaly_score < 0.7 else "error"

    return {
        "event_id": str(uuid.uuid4()),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "device_id": device_id,
        "user_id": f"user-{random.randint(1, 50000)}",
        "temperature": temperature,
        "metric_value": temperature,
        "status": status,
        "anomaly_score": anomaly_score,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="IoT telemetry producer")
    parser.add_argument("--brokers", default="redpanda:9092")
    parser.add_argument("--topic", default="iot.telemetry.raw.v1")
    parser.add_argument("--rate", type=int, default=10000, help="events per second")
    parser.add_argument("--devices", type=int, default=2000)
    args = parser.parse_args()

    producer = Producer(
        {
            "bootstrap.servers": args.brokers,
            "acks": "all",
            "compression.type": "lz4",
            "linger.ms": 5,
            "batch.num.messages": 10000,
            "enable.idempotence": True,
            "retries": 20,
        }
    )

    device_ids = [f"device-{i:06d}" for i in range(1, args.devices + 1)]
    sent_total = 0

    while True:
        start = time.time()
        for _ in range(args.rate):
            device_id = random.choice(device_ids)
            event = build_event(device_id)
            producer.produce(
                args.topic,
                key=device_id,
                value=json.dumps(event, separators=(",", ":")),
            )
        producer.poll(0)
        producer.flush(timeout=0.2)

        sent_total += args.rate
        elapsed = time.time() - start
        sleep_for = max(0.0, 1.0 - elapsed)
        if sleep_for > 0:
            time.sleep(sleep_for)

        print(f"produced={sent_total} rate={args.rate}/s cycle={elapsed:.3f}s")


if __name__ == "__main__":
    main()
