"""
Tests unitaires ETL
"""

import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .appName("Tests") \
        .master("local[2]") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_spark_session(spark):
    """Teste que Spark fonctionne"""
    assert spark is not None
    df = spark.range(10)
    assert df.count() == 10

def test_data_filtering(spark):
    """Teste le filtrage des données"""
    data = [
        {"code": "123", "name": "Product A"},
        {"code": None, "name": "Product B"},
        {"code": "456", "name": "Product C"}
    ]
    df = spark.createDataFrame(data)
    df_filtered = df.filter(df.code.isNotNull())
    assert df_filtered.count() == 2
