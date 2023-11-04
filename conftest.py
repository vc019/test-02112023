import pytest
from pyspark.sql import SparkSession


@pytest.fixture()
def spark_session() -> SparkSession:
    """
    Creates a local spark session for test
    :return:  SparkSession
    """

    return SparkSession.builder \
        .appName("UnitTestApp") \
        .master("local[2]") \
        .getOrCreate()
