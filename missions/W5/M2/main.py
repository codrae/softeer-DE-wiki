from pathlib import Path

import requests
from pyspark.sql import DataFrame, SparkSession

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
