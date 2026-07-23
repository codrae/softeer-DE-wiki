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
