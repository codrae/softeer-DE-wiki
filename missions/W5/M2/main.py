from pathlib import Path

import requests
from pyspark.sql import DataFrame, SparkSession, functions as F

ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

MAX_TRIP_DURATION_MIN = 180
MAX_TRIP_DISTANCE_MI = 100
MIN_PASSENGER_COUNT = 1
MAX_PASSENGER_COUNT = 6


def download_zone_lookup(dest_path, url: str = ZONE_LOOKUP_URL, timeout: int = 60) -> Path:
    dest_path = Path(dest_path)
    if dest_path.exists():
        return dest_path
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    response = requests.get(url, timeout=timeout)
    if response.status_code != 200:
        raise RuntimeError(f"Failed to download {url}: HTTP {response.status_code}")
    dest_path.write_bytes(response.content)
    return dest_path


def load_trips(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path)


def load_zone_lookup(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.option("header", True).option("inferSchema", True).csv(path)


def clean_trips(df: DataFrame) -> DataFrame:
    return (
        df.dropna(subset=[
            "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
            "trip_distance", "fare_amount", "PULocationID", "DOLocationID",
        ])
        .withColumn(
            "trip_duration_min",
            (
                F.col("tpep_dropoff_datetime").cast("long")
                - F.col("tpep_pickup_datetime").cast("long")
            ) / 60.0,
        )
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .filter(
            (F.col("trip_duration_min") > 0)
            & (F.col("trip_duration_min") <= MAX_TRIP_DURATION_MIN)
            & (F.col("trip_distance") > 0)
            & (F.col("trip_distance") <= MAX_TRIP_DISTANCE_MI)
            & (F.col("passenger_count") >= MIN_PASSENGER_COUNT)
            & (F.col("passenger_count") <= MAX_PASSENGER_COUNT)
            & (F.col("fare_amount") >= 0)
        )
    )


def filter_multi_passenger(df: DataFrame) -> DataFrame:
    return df.filter(F.col("passenger_count") > 1)
