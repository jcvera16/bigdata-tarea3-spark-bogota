from pyspark.sql import SparkSession

# Crear sesion de Spark
spark = SparkSession.builder \
    .appName("AnalisisAccidentesBogota") \
    .master("local[2]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Cargar el archivo CSV
df = spark.read.csv(
    "/home/vboxuser/osb_evento_transporte.csv",
    header=True,
    sep=";",
    inferSchema=True
)

# Mostrar estructura del archivo
print("\n=== ESTRUCTURA DEL DATASET ===")
df.printSchema()

# Contar registros
print("\n=== TOTAL DE REGISTROS ===")
print(df.count())

# Mostrar primeros registros
print("\n=== PRIMEROS 5 REGISTROS ===")
df.show(5, truncate=False)

print("\n=== REGISTROS POR ANO ===")

df.groupBy("ANO") \
    .count() \
    .orderBy("ANO") \
    .show(30, truncate=False)

print("\n=== REGISTROS POR LOCALIDAD ===")

df.groupBy("LOCALIDAD") \
    .count() \
    .orderBy("count", ascending=False) \
    .show(25, truncate=False)

print("\n=== REGISTROS POR SEXO ===")

df.groupBy("Sexo") \
    .count() \
    .orderBy("count", ascending=False) \
    .show(truncate=False)
print("\n=== REGISTROS POR TIPO DE ACCIDENTE ===")

df.groupBy("CLASE_O_TIPO_DE_ACCIDENTE_DE_TRANSPORTE") \
    .count() \
    .orderBy("count", ascending=False) \
    .show(20, truncate=False)
print("\n=== VALIDACION DE LA COLUMNA CASOS ===")

df.groupBy("casos") \
    .count() \
    .orderBy("casos") \
    .show(30, truncate=False)
# Exportar resultados del analisis batch
print("\n=== EXPORTANDO RESULTADOS CSV ===")

resultados = {
    "por_anio": df.groupBy("ANO").count(),
    "por_localidad": df.groupBy("LOCALIDAD").count(),
    "por_sexo": df.groupBy("Sexo").count(),
    "por_tipo_accidente": df.groupBy(
        "CLASE_O_TIPO_DE_ACCIDENTE_DE_TRANSPORTE"
    ).count()
}

for nombre, tabla in resultados.items():
    tabla.coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header", True) \
        .csv("/home/vboxuser/resultados_accidentes/" + nombre)

    print("Exportado:", nombre)

print("=== EXPORTACION FINALIZADA ===")

spark.stop()
