-- 1) Total events per minute
SELECT
  window_start AS time,
  argMax(total_events, updated_at) AS total_events
FROM stream_analytics.iot_minute_agg
WHERE window_start >= now() - INTERVAL 2 HOUR
GROUP BY window_start
ORDER BY time;

-- 2) Average metric value per minute
SELECT
  window_start AS time,
  argMax(avg_metric, updated_at) AS avg_metric
FROM stream_analytics.iot_minute_agg
WHERE window_start >= now() - INTERVAL 2 HOUR
GROUP BY window_start
ORDER BY time;

-- 3) Unique users per minute
SELECT
  window_start AS time,
  argMax(unique_users, updated_at) AS unique_users
FROM stream_analytics.iot_minute_agg
WHERE window_start >= now() - INTERVAL 2 HOUR
GROUP BY window_start
ORDER BY time;

-- 4) Anomaly rate per minute
SELECT
  window_start AS time,
  if(argMax(total_events, updated_at) = 0, 0,
     argMax(anomaly_events, updated_at) / argMax(total_events, updated_at)) AS anomaly_rate
FROM stream_analytics.iot_minute_agg
WHERE window_start >= now() - INTERVAL 2 HOUR
GROUP BY window_start
ORDER BY time;
