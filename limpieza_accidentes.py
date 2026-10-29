from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, sum as spark_sum, when

spark = SparkSession.builder \
    .appName("LimpiezaAccidentesBogota") \
    .master("local[2]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv(
    "/home/vboxuser/osb_evento_transporte.csv",
    header=True,
    sep=";",
    inferSchema=True
)

columnas = [
    "ANO",
    "LOCALIDAD",
    "Sexo",
    "CLASE_O_TIPO_DE_ACCIDENTE_DE_TRANSPORTE"
]

print("\n=== VALORES NULOS Y VACIOS ===")

df.select([
    spark_sum(
        when(
            col(c).isNull() |
            (trim(col(c).cast("string")) == ""),
            1
        ).otherwise(0)
    ).alias(c)
    for c in columnas
]).show(truncate=False)

print("\n=== REGISTROS DUPLICADOS EXACTOS ===")
print(df.count() - df.dropDuplicates().count())

print("\n=== COMBINACIONES DE REGISTROS REPETIDOS ===")

df.groupBy(df.columns) \
    .count() \
    .filter(col("count") > 1) \
    .orderBy(col("count").desc()) \
    .select("ANO", "LOCALIDAD", "Sexo", "casos", "count") \
    .show(15, truncate=False)

spark.stop()
