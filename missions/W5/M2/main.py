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


def compute_daily_summary(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("pickup_date")
        .agg(
            F.count(F.lit(1)).alias("trip_count"),
            F.avg("trip_distance").alias("avg_trip_distance_mi"),
            F.sum("fare_amount").alias("total_fare_amount"),
        )
        .orderBy("pickup_date")
    )


def compute_hourly_counts(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("pickup_hour")
        .count()
        .withColumnRenamed("count", "trip_count")
        .orderBy("pickup_hour")
    )


def compute_borough_summary(df: DataFrame, zone_lookup_df: DataFrame) -> DataFrame:
    zone_broadcast = F.broadcast(
        zone_lookup_df.select(
            F.col("LocationID").alias("PULocationID"),
            F.col("Borough").alias("pickup_borough"),
        )
    )
    return (
        df.join(zone_broadcast, on="PULocationID", how="inner")
        .groupBy("pickup_borough")
        .agg(
            F.count(F.lit(1)).alias("trip_count"),
            F.avg("fare_amount").alias("avg_fare_amount"),
        )
        .orderBy(F.col("trip_count").desc())
    )


def write_output_table(df: DataFrame, output_dir: str, name: str) -> None:
    df.write.mode("overwrite").parquet(f"{output_dir}/{name}")
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{output_dir}/{name}_csv")
