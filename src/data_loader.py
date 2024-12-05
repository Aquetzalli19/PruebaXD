from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring
import os

def init_spark():
    """Inicializa la sesion de Spark"""
    return SparkSession.builder\
        .appName("AitTribu Data Processing")\
        .getOrCreate()

def load_data(file_path : str, format : str = "csv", delimiter : str = None, header: bool = True) -> "DataFrame":
    """
    Carga datos de un archivo en un DataFrame de PySpark.
    Los formatos pueden ser 'csv', 'json', 'parquet', etc.
    Si se trata de un archivo con delimitador personalizado, se puede especificar.
    
    :param file_path: Ruta del archivo.
    :param format: Formato del archivo (por ejemplo, 'csv', 'json').
    :param delimiter: Delimitador en caso de archivos CSV.
    :param header: Indica si el archivo tiene encabezado.
    :return: DataFrame cargado.
    """

    spark = init_spark()

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"El archivo {file_path} no existe.")

    if format == "csv" or format == "dat" or format == "ada":
        if delimiter is None:
            delimiter = "," # Delimitador por defecto
        return spark.read.option("delimiter", delimiter).option("header", header).csv(file_path, inferSchema=True)
    
    if format == "json":
        return spark.read.json(file_path)
    
    if format == "parquet":
        return spark.read.parquet(file_path)
    #Se pueden añadir mas opciones de formatos

def load_fecha(file_path :str) -> "DataFrame":
    """ Cargar el archivo de fecha.dat en un DataFrame de PySpark """
    df = load_data(file_path, format="dat", delimiter="_", header=False)
    df = df.withColumnRenamed("_c0", "Vuelo").withColumnRenamed("_c1", "Dia")
    return df

def load_paises(file_path :str) -> "DataFrame":
    """ Cargar el archivo de paises.dat en un DataFrame de PySpark """
    df = load_data(file_path, format="ada", delimiter=";", header=False)
    df = df.withColumnRenamed("_c0", "COD_pais").withColumnRenamed("_c1", "Pais")
    return df

def load_vuelos(file_path :str) -> "DataFrame":
    """ Cargar el archivo de vuelos.dat en un DataFrame de PySpark """
    df = load_data(file_path, format="dat", delimiter="|", header=False)
    df = df.withColumnRenamed("_c0", "Vuelo").withColumnRenamed("_c1", "Origen").withColumnRenamed("_c2", "Destino")
    return df

def load_retrasos(file_path :str) -> "DataFrame":
    """ Cargar el archivo de retrasos.dat en un DataFrame de PySpark y separar los datos """
    df = load_data(file_path, format="dat", delimiter="_", header=False)
    df = df.withColumnRenamed("_c0", "value")
    df = df.withColumn("first_four", substring(col("value"), 1, 4))
    df = df.withColumn("last_two", substring(col("value"), -2, 2))
    df = df.withColumnRenamed("first_four", "Vuelo").withColumnRenamed("last_two", "Retraso")
    return df.select("Vuelo", "Retraso")