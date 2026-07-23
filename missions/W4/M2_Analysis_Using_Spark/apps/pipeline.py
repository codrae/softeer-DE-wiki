from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

MAX_TRIP_DURATION_MIN = 180
MAX_TRIP_DISTANCE_MI = 100


def load_trips(spark: SparkSession, path_glob: str) -> DataFrame:
    return spark.read.parquet(path_glob)


def load_weather(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.option("header", True).option("inferSchema", True).csv(path)
        .withColumn("datetime", F.to_timestamp("datetime"))
    )


def clean_trips(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("tpep_pickup_datetime", F.to_timestamp("tpep_pickup_datetime"))
        .withColumn("tpep_dropoff_datetime", F.to_timestamp("tpep_dropoff_datetime"))
        .dropna(subset=[
            "tpep_pickup_datetime", "tpep_dropoff_datetime",
            "trip_distance", "fare_amount",
        ])
        .withColumn(
            "trip_duration_min",
            (
                F.col("tpep_dropoff_datetime").cast("long")
                - F.col("tpep_pickup_datetime").cast("long")
            ) / 60.0,
        )
        .filter(
            (F.col("trip_duration_min") > 0)
            & (F.col("trip_duration_min") <= MAX_TRIP_DURATION_MIN)
            & (F.col("trip_distance") > 0)
            & (F.col("trip_distance") <= MAX_TRIP_DISTANCE_MI)
            & (F.col("fare_amount") >= 0)
        )
    )
