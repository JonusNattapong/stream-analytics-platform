import json
import os
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone

import clickhouse_connect
from confluent_kafka import Consumer, KafkaError


@dataclass
class MinuteState:
    total_events: int = 0
    metric_sum: float = 0.0
    unique_users: set = field(default_factory=set)
    anomaly_events: int = 0

    def update(self, event: dict) -> None:
        self.total_events += 1
        self.metric_sum += float(event.get("metric_value", 0.0))
        user_id = event.get("user_id")
        if user_id:
            self.unique_users.add(user_id)
        if float(event.get("anomaly_score", 0.0)) >= 0.7 or event.get("status") == "error":
            self.anomaly_events += 1


def floor_to_minute(epoch_seconds: float) -> datetime:
    minute_epoch = int(epoch_seconds // 60) * 60
    return datetime.fromtimestamp(minute_epoch, tz=timezone.utc)


def parse_event_time(event: dict) -> float:
    raw = event.get("event_time")
    if not raw:
        return time.time()
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return time.time()


def flush_to_clickhouse(ch_client, table: str, windows: dict) -> None:
    now = datetime.now(timezone.utc)
    rows = []
    for window_start, state in windows.items():
        if state.total_events == 0:
            continue
        avg_metric = state.metric_sum / state.total_events
        rows.append(
            [
                window_start,
                state.total_events,
                round(avg_metric, 4),
                len(state.unique_users),
                state.anomaly_events,
                now,
            ]
        )

    if rows:
        ch_client.insert(
            table,
            rows,
            column_names=[
                "window_start",
                "total_events",
                "avg_metric",
                "unique_users",
                "anomaly_events",
                "updated_at",
            ],
        )


def main() -> None:
    brokers = os.getenv("BROKERS", "redpanda:9092")
    topic = os.getenv("TOPIC", "iot.telemetry.raw.v1")
    group_id = os.getenv("GROUP_ID", "iot-minute-aggregation-v1")
    clickhouse_host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    clickhouse_port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    clickhouse_db = os.getenv("CLICKHOUSE_DB", "stream_analytics")
    clickhouse_user = os.getenv("CLICKHOUSE_USER", "default")
    clickhouse_pass = os.getenv("CLICKHOUSE_PASSWORD", "")
    agg_table = os.getenv("AGG_TABLE", "iot_minute_agg")
    flush_interval_secs = int(os.getenv("FLUSH_INTERVAL_SECS", "5"))
    allowed_lateness_secs = int(os.getenv("ALLOWED_LATENESS_SECS", "15"))

    consumer = Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
            "max.poll.interval.ms": 300000,
            "session.timeout.ms": 10000,
        }
    )
    consumer.subscribe([topic])

    ch_client = clickhouse_connect.get_client(
        host=clickhouse_host,
        port=clickhouse_port,
        username=clickhouse_user,
        password=clickhouse_pass,
        database=clickhouse_db,
    )

    windows = defaultdict(MinuteState)
    last_flush = time.time()

    print("aggregator started")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                pass
            elif msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"consume error: {msg.error()}")
            else:
                try:
                    event = json.loads(msg.value())
                    evt_ts = parse_event_time(event)
                    window_start = floor_to_minute(evt_ts)
                    windows[window_start].update(event)
                except Exception as exc:
                    print(f"bad message: {exc}")

            now = time.time()
            if now - last_flush >= flush_interval_secs:
                flush_to_clickhouse(ch_client, agg_table, windows)
                cutoff = floor_to_minute(now - allowed_lateness_secs - 60)
                expired = [w for w in windows if w <= cutoff]
                for w in expired:
                    del windows[w]

                active_windows = len(windows)
                active_events = sum(s.total_events for s in windows.values())
                print(f"flush windows={active_windows} buffered_events={active_events}")
                last_flush = now
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
