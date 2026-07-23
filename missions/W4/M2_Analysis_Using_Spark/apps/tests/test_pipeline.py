import pandas as pd

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
