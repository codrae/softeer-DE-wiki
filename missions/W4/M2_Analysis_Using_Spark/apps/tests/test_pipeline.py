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
