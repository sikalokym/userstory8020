import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope='session')
def spark() -> SparkSession:
    spark = SparkSession.builder \
                        .remote("sc://localhost:15002") \
                        .getOrCreate()
    yield spark
    spark.stop()
