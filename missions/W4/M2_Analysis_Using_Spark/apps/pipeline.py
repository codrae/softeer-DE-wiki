from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from scipy import stats as scipy_stats

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


def compute_summary_metrics(df: DataFrame) -> DataFrame:
    return df.agg(
        F.avg("trip_duration_min").alias("avg_trip_duration_min"),
        F.avg("trip_distance").alias("avg_trip_distance_mi"),
        F.count(F.lit(1)).alias("trip_count"),
    )


def compute_hourly_trip_counts(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .groupBy("pickup_hour")
        .count()
        .withColumnRenamed("count", "trip_count")
        .orderBy("pickup_hour")
    )


def compute_hourly_trip_series(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("dt_hour", F.date_trunc("hour", "tpep_pickup_datetime"))
        .groupBy("dt_hour")
        .count()
        .withColumnRenamed("count", "trip_count")
        .orderBy("dt_hour")
    )


def join_hourly_weather(hourly_series_df: DataFrame, weather_df: DataFrame) -> DataFrame:
    weather_hourly = weather_df.withColumn("dt_hour", F.date_trunc("hour", "datetime"))
    return hourly_series_df.join(weather_hourly, on="dt_hour", how="inner").select(
        "dt_hour", "trip_count", "temperature_2m", "precipitation"
    )


def compute_correlations(pdf) -> dict:
    temp_r, temp_p = scipy_stats.pearsonr(pdf["trip_count"], pdf["temperature_2m"])
    precip_r, precip_p = scipy_stats.pearsonr(pdf["trip_count"], pdf["precipitation"])
    return {
        "temperature_r": float(temp_r),
        "temperature_p": float(temp_p),
        "precipitation_r": float(precip_r),
        "precipitation_p": float(precip_p),
    }


def compute_weather_condition_stats(pdf) -> dict:
    rainy = pdf[pdf["precipitation"] > 0]["trip_count"]
    dry = pdf[pdf["precipitation"] == 0]["trip_count"]
    t_stat, p_value = scipy_stats.ttest_ind(rainy, dry, equal_var=False)
    return {
        "rainy_mean_trip_count": float(rainy.mean()),
        "dry_mean_trip_count": float(dry.mean()),
        "t_stat": float(t_stat),
        "p_value": float(p_value),
    }
