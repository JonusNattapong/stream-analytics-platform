# Real-Time Data Reporting Architecture (Redpanda + ClickHouse + Grafana)

```mermaid
flowchart LR
    A[IoT Apps / Devices\nJSON Events: temperature,status,anomaly] -->|Produce keyed by device_id| B[(Redpanda Topic\niot.telemetry.raw.v1)]
    B --> C[Minute Aggregator\nConsumer Group: iot-minute-aggregation-v1\n1-minute tumbling windows\nflush every 5s]
    C --> D[(ClickHouse\nstream_analytics.iot_minute_agg)]
    D --> E[Grafana Dashboard\nrefresh 5s]

    B --> F[Redpanda Console]
    B --> G[Monitoring\nlag, throughput, errors]
```

## Topic design
- `iot.telemetry.raw.v1`: primary inbound telemetry stream.
- `iot.telemetry.minute-agg.v1`: reserved for downstream pre-aggregated stream if a second-stage consumer is added.
- `iot.telemetry.dlq.v1`: malformed/unprocessable events.

## Partitioning strategy
- Key by `device_id` to preserve per-device ordering and spread load.
- Local dev: 24 partitions for `iot.telemetry.raw.v1`.
- Production baseline for 10k events/sec: start at 48 partitions and adjust to keep per-partition ingress under ~5 MB/s and consumer lag stable.

## Replication and retention
- Local dev uses replication factor `1`.
- Production: replication factor `3`, rack-aware placement, min ISR `2`.
- `iot.telemetry.raw.v1` retention: 7 days.
- `iot.telemetry.minute-agg.v1` retention: 30 days.
- `iot.telemetry.dlq.v1` retention: 14 days.
