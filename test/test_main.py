# test_main.py
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from analysis import (
    pais_mayor_despegues,
    pais_mayor_aterrizajes,
    dia_mayor_menor_actividad,
    dia_mayor_menor_retrasos,
    retrasos_acumulados,
    crear_dataframe_vip,
    filtrar_vuelos_peru_mexico
)

def test_pais_mayor_despegues(sample_vuelos_df, sample_paises_df):
    result = pais_mayor_despegues(sample_vuelos_df, sample_paises_df)
    assert result.collect()[0]["Pais"] == "México"
    assert result.collect()[0]["count"] == 2

def test_pais_mayor_aterrizajes(sample_vuelos_df, sample_paises_df):
    result = pais_mayor_aterrizajes(sample_vuelos_df, sample_paises_df)
    assert result.collect()[0]["Pais"] == "Colombia"
    assert result.collect()[0]["count"] == 2

def test_dia_mayor_menor_actividad(sample_fecha_df):
    mayor, menor = dia_mayor_menor_actividad(sample_fecha_df)
    assert mayor.collect()[0]["Dia"] == 2
    assert mayor.collect()[0]["count"] == 3
    assert menor.collect()[0]["Dia"] == 25
    assert menor.collect()[0]["count"] == 1

def test_dia_mayor_menor_retrasos(sample_retrasos_df, sample_fecha_df):
    mayor, menor = dia_mayor_menor_retrasos(sample_retrasos_df, sample_fecha_df)
    assert mayor.collect()[0]["Dia"] == 2
    assert mayor.collect()[0]["count"] == 3
    assert menor.collect()[0]["Dia"] == 25
    assert menor.collect()[0]["count"] == 1

def test_retrasos_acumulados(sample_vuelos_df, sample_retrasos_df, sample_fecha_df):
    result = retrasos_acumulados(sample_vuelos_df, sample_retrasos_df, sample_fecha_df)
    assert "Retraso_Acumulado" in result.columns
    assert result.count() > 0

def test_crear_dataframe_vip(sample_vuelos_df, sample_paises_df):
    result = crear_dataframe_vip(sample_vuelos_df, sample_paises_df)
    # México y Perú son VIP
    vip_count = result.filter(result.Pais_VIP == "VIP").count()
    assert vip_count == 3  # 2 MEX + 1 PER
    assert "Pais_VIP" in result.columns

def test_filtrar_vuelos_peru_mexico(sample_vuelos_df, sample_paises_df):
    result = filtrar_vuelos_peru_mexico(sample_vuelos_df, sample_paises_df)
    assert result.count() == 3  # 2 MEX + 1 PER
    origins = set([row["Origen"] for row in result.select("Origen").distinct().collect()])
    assert origins == {"MEX", "PER"}