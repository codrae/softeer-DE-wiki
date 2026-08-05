SELECT
    period_start_date,
    period_end_date,
    station_id,
    station_name,
    total_usage_count,
    total_distance_m,
    total_duration_min
FROM station_period_usage
WHERE period_start_date = '2026-06-27'
  AND period_end_date = '2026-06-28'
ORDER BY total_usage_count DESC;
