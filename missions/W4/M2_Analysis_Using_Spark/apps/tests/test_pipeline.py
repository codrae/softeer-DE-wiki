import pandas as pd
from datetime import datetime

import pipeline


def test_load_trips_reads_parquet_glob(spark, tmp_path):
    pdf = pd.DataFrame(
        {
            "tpep_pickup_datetime": pd.to_datetime(["2024-01-01 00:00:00"]),
            "tpep_dropoff_datetime": pd.to_datetime(["2024-01-01 00:10:00"]),
            "trip_distance": [2.5],
            "fare_amount": [12.0],
        }
    )
    df = spark.createDataFrame(pdf)
    df.write.parquet(str(tmp_path / "part-0.parquet"), mode="overwrite")

    df = pipeline.load_trips(spark, str(tmp_path / "*.parquet"))

    assert df.count() == 1
    assert "trip_distance" in df.columns


def test_load_weather_reads_csv_and_casts_timestamp(spark, tmp_path):
    csv_path = tmp_path / "weather.csv"
    csv_path.write_text(
        "datetime,temperature_2m,precipitation\n"
        "2024-01-01T00:00,1.0,0.0\n"
    )

    df = pipeline.load_weather(spark, str(csv_path))

    row = df.collect()[0]
    assert row["temperature_2m"] == 1.0
    assert row["datetime"] is not None


def test_clean_trips_filters_invalid_rows_and_adds_duration(spark):
    columns = [
        "tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "fare_amount",
    ]
    rows = [
        (datetime(2024, 1, 1, 0, 0, 0), datetime(2024, 1, 1, 0, 10, 0), 2.5, 12.0),  # valid
        (datetime(2024, 1, 1, 1, 0, 0), datetime(2024, 1, 1, 0, 50, 0), 2.0, 10.0),  # negative duration
        (datetime(2024, 1, 1, 2, 0, 0), datetime(2024, 1, 1, 2, 5, 0), 0.0, 5.0),    # zero distance
        (datetime(2024, 1, 1, 3, 0, 0), datetime(2024, 1, 1, 3, 5, 0), 150.0, 5.0),  # distance too far
        (datetime(2024, 1, 1, 4, 0, 0), datetime(2024, 1, 1, 4, 5, 0), None, 5.0),   # null distance
        (datetime(2024, 1, 1, 5, 0, 0), datetime(2024, 1, 1, 5, 5, 0), 2.0, -1.0),   # negative fare
    ]
    df = spark.createDataFrame(rows, columns)

    cleaned = pipeline.clean_trips(df)
    result = cleaned.collect()

    assert len(result) == 1
    assert result[0]["trip_distance"] == 2.5
    assert abs(result[0]["trip_duration_min"] - 10.0) < 1e-6


def _cleaned_sample(spark):
    columns = [
        "tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "fare_amount",
    ]
    rows = [
        (datetime(2024, 1, 1, 0, 0, 0), datetime(2024, 1, 1, 0, 10, 0), 2.0, 10.0),
        (datetime(2024, 1, 1, 0, 30, 0), datetime(2024, 1, 1, 0, 40, 0), 4.0, 10.0),
        (datetime(2024, 1, 1, 5, 0, 0), datetime(2024, 1, 1, 5, 5, 0), 3.0, 10.0),
    ]
    return pipeline.clean_trips(spark.createDataFrame(rows, columns))


def test_compute_summary_metrics_returns_averages(spark):
    cleaned = _cleaned_sample(spark)

    result = pipeline.compute_summary_metrics(cleaned).collect()[0]

    assert result["trip_count"] == 3
    assert abs(result["avg_trip_distance_mi"] - 3.0) < 1e-6
    assert abs(result["avg_trip_duration_min"] - 8.333333) < 1e-3


def test_compute_hourly_trip_counts_groups_by_hour(spark):
    cleaned = _cleaned_sample(spark)

    result = {
        row["pickup_hour"]: row["trip_count"]
        for row in pipeline.compute_hourly_trip_counts(cleaned).collect()
    }

    assert result == {0: 2, 5: 1}


def test_compute_hourly_trip_series_groups_by_date_and_hour(spark):
    cleaned = _cleaned_sample(spark)

    rows = {
        row["dt_hour"]: row["trip_count"]
        for row in pipeline.compute_hourly_trip_series(cleaned).collect()
    }

    assert len(rows) == 2  # 2024-01-01 00시, 2024-01-01 05시


def test_join_hourly_weather_matches_on_truncated_hour(spark):
    hourly_series = spark.createDataFrame(
        [(datetime(2024, 1, 1, 0, 0, 0), 10), (datetime(2024, 1, 1, 1, 0, 0), 4)],
        ["dt_hour", "trip_count"],
    )
    weather = spark.createDataFrame(
        [
            (datetime(2024, 1, 1, 0, 30, 0), 5.0, 0.0),
            (datetime(2024, 1, 1, 1, 15, 0), -2.0, 3.0),
        ],
        ["datetime", "temperature_2m", "precipitation"],
    )

    joined = pipeline.join_hourly_weather(hourly_series, weather).orderBy("dt_hour").collect()

    assert len(joined) == 2
    assert joined[0]["trip_count"] == 10 and joined[0]["temperature_2m"] == 5.0
    assert joined[1]["trip_count"] == 4 and joined[1]["precipitation"] == 3.0


def test_compute_correlations_perfect_positive_relationship():
    pdf = pd.DataFrame({
        "trip_count": [10, 20, 30, 40],
        "temperature_2m": [1, 2, 3, 4],
        "precipitation": [4, 3, 2, 1],
    })

    result = pipeline.compute_correlations(pdf)

    assert abs(result["temperature_r"] - 1.0) < 1e-6
    assert abs(result["precipitation_r"] - (-1.0)) < 1e-6


def test_compute_weather_condition_stats_detects_lower_trips_on_rainy_hours():
    pdf = pd.DataFrame({
        "trip_count": [100, 105, 98, 20, 25, 18],
        "precipitation": [0, 0, 0, 5, 6, 4],
    })

    result = pipeline.compute_weather_condition_stats(pdf)

    assert result["dry_mean_trip_count"] > result["rainy_mean_trip_count"]
    assert result["p_value"] < 0.05


def test_write_output_table_creates_parquet_and_csv(spark, tmp_path):
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "label"])

    pipeline.write_output_table(df, str(tmp_path), "sample_table")

    parquet_dir = tmp_path / "sample_table"
    csv_dir = tmp_path / "sample_table_csv"
    assert parquet_dir.exists()
    assert csv_dir.exists()

    read_back = spark.read.parquet(str(parquet_dir))
    assert read_back.count() == 2

    csv_files = list(csv_dir.glob("*.csv"))
    assert len(csv_files) == 1
    assert "id,label" in csv_files[0].read_text()


def test_stats_to_dataframe_wraps_dict_as_single_row(spark):
    df = pipeline.stats_to_dataframe(spark, {"a": 1.0, "b": 2.0})

    row = df.collect()[0]
    assert row["a"] == 1.0
    assert row["b"] == 2.0
