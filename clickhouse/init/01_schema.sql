CREATE DATABASE IF NOT EXISTS stream_analytics;

CREATE TABLE IF NOT EXISTS stream_analytics.iot_minute_agg
(
    window_start DateTime,
    total_events UInt64,
    avg_metric Float64,
    unique_users UInt64,
    anomaly_events UInt64,
    updated_at DateTime64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toDate(window_start)
ORDER BY (window_start)
SETTINGS index_granularity = 8192;
