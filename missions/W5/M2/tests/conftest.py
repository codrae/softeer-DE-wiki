import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("w5m2-pipeline-tests")
        .getOrCreate()
    )
    yield session
    session.stop()
