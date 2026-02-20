# Insert a test row into ClickHouse (PowerShell)
$container = 'clickhouse'
$table = 'stream_analytics.iot_minute_agg'

docker exec -i $container clickhouse-client --query "INSERT INTO $table (window_start,total_events,avg_metric,unique_users,anomaly_events,updated_at) VALUES (now(), 123, 45.67, 12, 3, now())"
Write-Host "Inserted test row into $table (container: $container)"