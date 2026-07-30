import time
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


def run_pipeline(spark: SparkSession, trips_path: str, zone_lookup_path: str, output_dir: str) -> dict:
    log = []

    def emit(message: str) -> None:
        log.append(message)
        print(message)

    trips_df = load_trips(spark, trips_path)
    zone_df = load_zone_lookup(spark, zone_lookup_path)

    raw_count = trips_df.count()
    emit(f"[action] raw row count = {raw_count}")

    cleaned_df = clean_trips(trips_df)
    cleaned_count = cleaned_df.count()
    emit(f"[action] cleaned row count = {cleaned_count} (dropped {raw_count - cleaned_count})")

    cleaned_df = cleaned_df.cache()
    cleaned_df.count()  # materialize the cache before it's reused below
    emit("[action] cache materialized on cleaned_df")

    multi_passenger_df = filter_multi_passenger(cleaned_df)
    daily_summary_df = compute_daily_summary(cleaned_df)
    hourly_counts_df = compute_hourly_counts(cleaned_df)
    borough_summary_df = compute_borough_summary(cleaned_df, zone_df)

    emit(
        f"[lazy] transformations defined at {time.time():.3f} "
        "-- no Spark job has run for these DataFrames yet"
    )
    emit("[lazy] daily_summary_df physical plan (explain() does not trigger a job):")
    daily_summary_df.explain(mode="extended")

    emit(f"[action] collect() called at {time.time():.3f}")
    sample_rows = [row.asDict() for row in multi_passenger_df.limit(20).collect()]
    emit(f"[action] collect() returned at {time.time():.3f}, {len(sample_rows)} rows")

    for name, result_df in [
        ("daily_summary", daily_summary_df),
        ("hourly_counts", hourly_counts_df),
        ("borough_summary", borough_summary_df),
    ]:
        emit(f"[action] write({name}) called at {time.time():.3f}")
        write_output_table(result_df, output_dir, name)
        emit(f"[action] write({name}) finished at {time.time():.3f}")

    return {
        "raw_count": raw_count,
        "cleaned_count": cleaned_count,
        "sample_rows": sample_rows,
        "daily_summary": daily_summary_df,
        "hourly_counts": hourly_counts_df,
        "borough_summary": borough_summary_df,
        "log": log,
    }
