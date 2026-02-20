# stream-analytics-platform

Production-ready reference implementation for real-time IoT reporting with Redpanda, ClickHouse, and Grafana.

## What this delivers

- JSON telemetry ingestion at high throughput (default producer target: 10k events/sec).
- Redpanda streaming layer with topic strategy, partitioning, and retention settings.
- Window-based stream processing (1-minute tumbling window, flush every 5s).
- Aggregated analytical storage in ClickHouse optimized for time-series reads.
- Grafana dashboard with auto-refresh (`5s`) and pre-provisioned ClickHouse datasource.

## Architecture diagram

See `docs/architecture.md`.

## Workflow Diagram

```text
                                (10k ops/sec)
+-----------------+ JSON Events  +-----------------+  Consume  +------------------+
|   Producer      | -----------> | Redpanda        | --------> |   Aggregator     |
| (app/producer)  |              | iot.telemetry   |           | (app/aggregator) |
+-----------------+              +-----------------+           +------------------+
                                                                         |
                                                                         | 1-Min Tumbling Window
                                                                         | Flush every 5s
                                                                         v
+-----------------+  Auto refresh 5s  +--------------------+         +------------------+
| Dashboard       | <---------------- | ClickHouse (DB)    | <------ | SQL Insert       |
| (Grafana)       |                   | iot_minute_agg     |         +------------------+
+-----------------+                   +--------------------+
```

## Event model

Each event includes:

- `event_id`
- `event_time` (UTC ISO-8601)
- `device_id`
- `user_id`
- `temperature`
- `metric_value`
- `status`
- `anomaly_score`

## Streaming design

- Topic naming strategy:
  - `iot.telemetry.raw.v1`
  - `iot.telemetry.minute-agg.v1`
  - `iot.telemetry.dlq.v1`
- Partitioning:
  - Key by `device_id` to preserve per-device ordering.
  - Dev default: 24 partitions on raw topic.
- Replication/retention:
  - Dev RF=1.
  - Prod RF=3, min ISR=2.
  - Raw retention 7d, aggregate 30d, DLQ 14d.

## ClickHouse schema

Schema is auto-created from `clickhouse/init/01_schema.sql`.

Table: `stream_analytics.iot_minute_agg`

- `window_start`: minute bucket timestamp
- `total_events`: count per minute
- `avg_metric`: average metric per minute
- `unique_users`: unique users per minute
- `anomaly_events`: anomalies per minute
- `updated_at`: version column for `ReplacingMergeTree`

## Run locally

```bash
docker compose up -d --build
```

Services:

- Redpanda Console: <http://localhost:8080>
- ClickHouse HTTP: <http://localhost:8123>
- Grafana: <http://localhost:6363> (`admin` / `admin`)

## Producer and Aggregator examples

- Producer: `app/producer.py`
  - Default command (already in compose):
    - `python producer.py --brokers redpanda:9092 --topic iot.telemetry.raw.v1 --rate 10000 --devices 2000`
- Aggregator: `app/aggregator.py`
  - Consumes `iot.telemetry.raw.v1`
  - Performs 1-minute tumbling aggregation
  - Flushes snapshots every 5s to ClickHouse

## Dashboard SQL queries

Query examples are in `docs/dashboard_queries.sql` and embedded in `grafana/dashboards/iot-streaming-overview.json`.

## Latency target (< 5 seconds)

- Aggregator flush interval is set to `5s` (`FLUSH_INTERVAL_SECS=5`).
- Dashboard refresh is `5s`.
- End-to-end visibility is typically a few seconds under normal load.

## Production scaling advice

1. Redpanda cluster

- Use 3+ brokers (RF=3, min ISR=2).
- Enable rack awareness across AZs.
- Start with 48 raw partitions for 10k eps and tune with observed lag/CPU/disk.

1. Stream processing

- Run multiple aggregator instances in the same consumer group.
- Keep consumer count <= partition count.
- Use static group membership and idempotent writes for smoother failover.

1. ClickHouse

- Deploy replicated shards (`ReplicatedMergeTree`) with at least 2 replicas per shard.
- Keep hot partitions on fast SSD; apply TTL to move/cold-store older data.

1. Reliability and data quality

- Route parse/validation failures to `iot.telemetry.dlq.v1`.
- Use schema validation (JSON Schema or protobuf/Avro with schema registry in production).

1. Monitoring recommendations

- Consumer lag (per topic/partition and per group).
- Ingress/egress throughput (bytes/s, messages/s).
- Error rate (producer delivery failures, consumer parse failures, DLQ rate).
- Processing latency (event_time -> persisted_time in ClickHouse).
- Resource saturation (broker disk, network, CPU; ClickHouse merge backlog).

## Useful commands

```bash
# Follow aggregator logs
docker compose logs -f aggregator

# Follow producer logs
docker compose logs -f producer

# List topics
docker compose exec redpanda rpk topic list
```

## Quick troubleshooting & helpers ✅

If Grafana shows "No data":

1) Start the data pipeline (aggregator + producer):

```powershell
# Windows PowerShell
docker-compose up -d --build aggregator producer
```

1) Insert a single test row into ClickHouse (quick test):

```powershell
# PowerShell (or run scripts/insert_test_row.ps1)
./scripts/insert_test_row.ps1
# or (bash)
./scripts/insert_test_row.sh
```

1) Confirm ClickHouse has rows:

```powershell
docker exec -it clickhouse clickhouse-client --query "SELECT count() FROM stream_analytics.iot_minute_agg"
```

1) If aggregator isn't writing, tail its logs to see errors:

```powershell
docker-compose logs -f aggregator
```

1) Grafana datasource & dashboard:

- Grafana is provisioned from `grafana/provisioning` and the dashboard JSON lives at `grafana/dashboards/iot-streaming-overview.json`.
- Open Grafana: <http://localhost:6363> (admin/admin)
- If panels still show no data after ClickHouse has rows: refresh the dashboard time range to last 24 hours and check `Explore` → choose `ClickHouse` datasource and run the panel SQL.

If you run the commands above and paste any failing logs or errors here, I'll analyze and provide the fix.
