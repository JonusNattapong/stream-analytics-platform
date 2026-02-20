-- AMR Oil Pipeline Sensor Schema
-- This schema stores AMR sensor telemetry data

CREATE TABLE IF NOT EXISTS stream_analytics.amr_telemetry
(
    event_time DateTime,
    event_id String,
    device_id String,
    device_type String,
    meter_serial String,
    pipeline_id String,
    location String,
    flow_rate Float64,
    flow_rate_unit String,
    flow_direction String,
    cumulative_flow Float64,
    cumulative_flow_unit String,
    gross_volume Float64,
    net_volume Float64,
    volume_unit String,
    inlet_pressure Float64,
    outlet_pressure Float64,
    differential_pressure Float64,
    pressure_unit String,
    temperature Float64,
    temperature_unit String,
    viscosity Float64,
    viscosity_unit String,
    density Float64,
    density_unit String,
    api_gravity Float64,
    water_content Float64,
    water_content_unit String,
    sulfur_content Float64,
    sulfur_content_unit String,
    correction_factor Float64,
    pump_speed Int32,
    pump_speed_unit String,
    valve_status String,
    valve_open_percent Float64,
    leak_detected UInt8,
    leak_sensitivity String,
    battery_level Float64,
    battery_unit String,
    signal_strength Int32,
    signal_unit String,
    status String,
    anomaly_score Float64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toDate(event_time)
ORDER BY (device_id, event_time)
SETTINGS index_granularity = 8192;

-- Minute aggregation for AMR sensors
CREATE TABLE IF NOT EXISTS stream_analytics.amr_minute_agg
(
    window_start DateTime,
    device_id String,
    avg_flow_rate Float64,
    min_flow_rate Float64,
    max_flow_rate Float64,
    avg_inlet_pressure Float64,
    avg_outlet_pressure Float64,
    avg_differential_pressure Float64,
    avg_temperature Float64,
    avg_density Float64,
    avg_viscosity Float64,
    avg_water_content Float64,
    avg_sulfur_content Float64,
    valve_status String,
    valve_open_percent Float64,
    pump_speed Int32,
    leak_detected UInt8,
    total_events UInt64,
    anomaly_events UInt64,
    normal_count UInt64,
    warn_count UInt64,
    error_count UInt64,
    updated_at DateTime
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toDate(window_start)
ORDER BY (device_id, window_start)
SETTINGS index_granularity = 8192;
