# analysis.py
# analysis.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, sum as _sum, udf, when
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from data_loader import load_fecha, load_paises, load_retrasos, load_vuelos


def pais_mayor_despegues(vuelos_df, paises_df):
    """Determina el país con el mayor número de despegues"""
    despegues_df = vuelos_df.groupBy("Origen").count().orderBy(desc("count")).limit(1)
    despegues_df = despegues_df.join(paises_df, despegues_df.Origen == paises_df.COD_pais).select("Pais", "count")
    return despegues_df

def pais_mayor_aterrizajes(vuelos_df, paises_df):
    """Determina el país con la mayor cantidad de aterrizajes"""
    aterrizajes_df = vuelos_df.groupBy("Destino").count().orderBy(desc("count")).limit(1)
    aterrizajes_df = aterrizajes_df.join(paises_df, aterrizajes_df.Destino == paises_df.COD_pais).select("Pais", "count")
    return aterrizajes_df

def dia_mayor_menor_actividad(fecha_df):
    """Identifica el día del mes con mayor y menor actividad de vuelos"""
    actividad_df = fecha_df.groupBy("Dia").count().orderBy(desc("count"))
    mayor_actividad_df = actividad_df.limit(1)
    menor_actividad_df = actividad_df.orderBy("count").limit(1)
    return mayor_actividad_df, menor_actividad_df

def dia_mayor_menor_retrasos(retrasos_df, fecha_df):
    """Determina el día con la mayor y menor cantidad de retrasos"""
    retrasos_df = retrasos_df.join(fecha_df, "Vuelo")
    retrasos_actividad_df = retrasos_df.groupBy("Dia").count().orderBy(desc("count"))
    mayor_retrasos_df = retrasos_actividad_df.limit(1)
    menor_retrasos_df = retrasos_actividad_df.orderBy("count").limit(1)
    return mayor_retrasos_df, menor_retrasos_df

def retrasos_acumulados(vuelos_df, retrasos_df, fecha_df):
    """Crea un DataFrame con la vista acumulativa de los retrasos por país de origen y por día"""
    # Unir los DataFrames para obtener la información completa
    retrasos_completos_df = retrasos_df.join(fecha_df, "Vuelo").join(vuelos_df, "Vuelo")
    
    # Seleccionar las columnas necesarias
    retrasos_completos_df = retrasos_completos_df.select("Origen", "Destino", "Dia", "Retraso")
    
    # Definir la ventana para calcular el retraso acumulado
    window_spec = Window.partitionBy("Origen").orderBy("Dia").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    # Calcular el retraso acumulado
    retrasos_completos_df = retrasos_completos_df.withColumn("Retraso_Acumulado", _sum("Retraso").over(window_spec))
    
    return retrasos_completos_df

def es_pais_vip(pais):
    """Función para identificar si un país es VIP"""
    paises_vip = ["España", "Perú", "México"]
    return "VIP" if pais in paises_vip else "No VIP"

# Registrar la UDF
es_pais_vip_udf = udf(es_pais_vip, StringType())

def crear_dataframe_vip(vuelos_df, paises_df):
    """Crear un DataFrame con los valores de origen, destino y país, y añadir la columna Pais_VIP"""
    # Unir los DataFrames para obtener la información completa
    vuelos_paises_df = vuelos_df.join(paises_df, vuelos_df.Origen == paises_df.COD_pais, "left").select("Origen", "Destino", "Pais")
    
    # Usar when() en lugar de UDF para mejor rendimiento y evitar problemas de serialización
    vuelos_paises_df = vuelos_paises_df.withColumn(
        "Pais_VIP",
        when(col("Pais").isin(["España", "Perú", "México"]), "VIP")
        .otherwise("No VIP")
    )
    
    return vuelos_paises_df

def guardar_resultados(df, path="output/resultados.csv"):
    #Guardamos el resultado en un archivo .parquet
    df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(path)


def guardar_archivo_particionado(df, num_partitions=10, path="output/resultados_particionados.csv"):
    #Guardamos el resultado en múltiples archivos CSV particionados
    df.repartition(num_partitions).write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(path)

def filtrar_vuelos_por_origen(vuelos_df, paises_df, codigos_pais):
    """
    Filtra vuelos por códigos de país de origen
    :param vuelos_df: DataFrame de vuelos
    :param paises_df: DataFrame de países
    :param codigos_pais: Lista de códigos de país (ej: ['MEX', 'PER'])
    :return: DataFrame filtrado
    """
    # Filtrar directamente por códigos de país en Origen
    return vuelos_df.filter(col("Origen").isin(codigos_pais))


def filtrar_vuelos_peru_mexico(vuelos_df, paises_df):
    """
    Filtra y combina vuelos de Perú y México
    """
    # Usar códigos de país en lugar de nombres completos
    codigos_pais = ["MEX", "PER"]
    
    # Obtener DataFrame filtrado
    df_filtrado = filtrar_vuelos_por_origen(vuelos_df, paises_df, codigos_pais)
    
    # Añadir información del país para mejor visualización
    df_final = df_filtrado.join(paises_df, df_filtrado.Origen == paises_df.COD_pais)
    
    # Debug: mostrar conteos
    print(f"Total de vuelos encontrados: {df_final.count()}")
    
    return df_final

def run_analysis():
    # Cargar los datos de los archivos
    df_vuelos_por_fecha = load_fecha("data/fecha.dat")
    df_catalogo_paises = load_paises("data/paises.ada")
    df_vuelos_retrasados = load_retrasos("data/retrasos.dat")
    df_registro_vuelos = load_vuelos("data/vuelos.dat")
    
   # Mostrar las primeras filas de cada DataFrame
    print("DataFrame de Vuelos por Fecha:")
    df_vuelos_por_fecha.show()
    
    print("Catálogo de Países:")
    df_catalogo_paises.show()
    
    print("Registro de Retrasos:")
    df_vuelos_retrasados.show()
    
    print("Registro de Vuelos:")
    df_registro_vuelos.show()

    # Análisis básico
    df_pais_max_despegues = pais_mayor_despegues(df_registro_vuelos, df_catalogo_paises)
    df_pais_max_aterrizajes = pais_mayor_aterrizajes(df_registro_vuelos, df_catalogo_paises)
    df_max_actividad, df_min_actividad = dia_mayor_menor_actividad(df_vuelos_por_fecha)
    df_max_retrasos, df_min_retrasos = dia_mayor_menor_retrasos(df_vuelos_retrasados, df_vuelos_por_fecha)

    # Mostrar resultados
    print("País con el mayor número de despegues:")
    df_pais_max_despegues.show()

    print("País con la mayor cantidad de aterrizajes:")
    df_pais_max_aterrizajes.show()

    print("Día del mes con mayor actividad de vuelos:")
    df_max_actividad.show()

    print("Día del mes con menor actividad de vuelos:")
    df_min_actividad.show()

    print("Día con la mayor cantidad de retrasos:")
    df_max_retrasos.show()

    print("Día con la menor cantidad de retrasos:")
    df_min_retrasos.show()

    # Retrasos acumulados
    retrasos_acumulados_df = retrasos_acumulados(df_registro_vuelos, df_vuelos_retrasados, df_vuelos_por_fecha)
    print("Retrasos acumulados por país de origen y por día:")
    retrasos_acumulados_df.show()

    # DataFrame con columna Pais_VIP
    vuelos_paises_vip_df = crear_dataframe_vip(df_registro_vuelos, df_catalogo_paises)
    print("DataFrame con columna Pais_VIP:")
    vuelos_paises_vip_df.show()

    #Guardar los resultados
    print("Guardando los resultados en archivos")

    #opcion 1
    guardar_resultados(vuelos_paises_vip_df )

    #opcion 2
    guardar_archivo_particionado(vuelos_paises_vip_df)

    print("Resultados guardados exitosamente.")

    # Debug: mostrar algunos datos de entrada
    print("Muestra de vuelos originales:")
    df_registro_vuelos.show(5)
    
    print("Muestra de países:")
    df_catalogo_paises.show(5)
    
    # Filtrar vuelos
    vuelos_filtrados_df = filtrar_vuelos_peru_mexico(df_registro_vuelos, df_catalogo_paises)
    print("Vuelos con origen en Perú y México:")
    vuelos_filtrados_df.show()

    #almacenamos resultados
    guardar_resultados(vuelos_filtrados_df, path="output/vuelos_peru_mexico.csv")
    print("Resultados guardados exitosamente.")

if __name__ == "__main__":
    run_analysis()