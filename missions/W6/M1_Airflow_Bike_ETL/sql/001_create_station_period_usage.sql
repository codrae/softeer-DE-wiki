CREATE TABLE IF NOT EXISTS station_period_usage (
    station_id           VARCHAR(64)  NOT NULL,
    period_start_date    DATE         NOT NULL,
    period_end_date      DATE         NOT NULL,
    station_name         VARCHAR(255) NOT NULL,
    total_usage_count    DOUBLE       NOT NULL,
    total_distance_m     DOUBLE       NOT NULL,
    total_duration_min   DOUBLE       NOT NULL,
    PRIMARY KEY (station_id, period_start_date, period_end_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
