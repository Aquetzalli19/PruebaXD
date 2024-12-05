# tests/conftest.py
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("TestAitTribu").getOrCreate()


@pytest.fixture(scope="session")
def sample_vuelos_df(spark):
    schema = StructType([
        StructField("Vuelo", StringType(), True),
        StructField("Origen", StringType(), True),
        StructField("Destino", StringType(), True)
    ])
    data = [
        ("6005", "MEX", "ESP"),
        ("2021", "PER", "MEX"),
        ("5528", "ARG", "COL"),
        ("5529", "MEX", "COL")
    ]
    return spark.createDataFrame(data, schema)

@pytest.fixture(scope="session")
def sample_paises_df(spark):
    schema = StructType([
        StructField("COD_pais", StringType(), True),
        StructField("Pais", StringType(), True)
    ])
    data = [
        ("MEX", "México"),
        ("PER", "Perú"),
        ("ESP", "España"),
        ("ARG", "Argentina"),
        ("COL", "Colombia")
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture(scope="session")
def sample_fecha_df(spark):
    schema = StructType([
        StructField("Vuelo", StringType(), True),
        StructField("Dia", IntegerType(), True)
    ])
    data = [
        ("6005", 2),
        ("2021", 2),
        ("5528", 25),
        ("5529", 2)  # Added for testing day with most flights
    ]
    return spark.createDataFrame(data, schema)

@pytest.fixture(scope="session")
def sample_retrasos_df(spark):
    schema = StructType([
        StructField("Vuelo", StringType(), True),
        StructField("Retraso", IntegerType(), True)
    ])
    data = [
        ("6005", 52),
        ("2021", 27),
        ("5528", 36),
        ("5529", 45)
    ]
    return spark.createDataFrame(data, schema)