#!/usr/bin/env bash
# Insert a test row into ClickHouse iot_minute_agg
set -euo pipefail

CONTAINER_NAME=clickhouse
TABLE=stream_analytics.iot_minute_agg

docker exec -i "$CONTAINER_NAME" clickhouse-client --query "INSERT INTO $TABLE (window_start,total_events,avg_metric,unique_users,anomaly_events,updated_at) VALUES (now(), 123, 45.67, 12, 3, now())"

echo "Inserted test row into $TABLE (container: $CONTAINER_NAME)"