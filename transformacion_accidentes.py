from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
from pyspark.sql.types import StringType

spark = SparkSession.builder \
    .appName("TransformacionAccidentesBogota") \
    .master("local[2]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Cargar datos originales
df = spark.read.csv(
    "/home/vboxuser/osb_evento_transporte.csv",
    header=True,
    sep=";",
    inferSchema=True
)

print("\n=== REGISTROS ORIGINALES ===")
print(df.count())

# Limpiar espacios en columnas de texto
df_limpio = df

for campo in df_limpio.schema.fields:
    if isinstance(campo.dataType, StringType):
        df_limpio = df_limpio.withColumn(
            campo.name,
            trim(col(campo.name))
        )

print("\n=== REGISTROS DESPUES DE TRANSFORMACION ===")
print(df_limpio.count())

# Guardar datos transformados
df_limpio.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", True) \
    .option("delimiter", ";") \
    .csv("/home/vboxuser/resultados_accidentes/datos_transformados")

print("\n=== TRANSFORMACION FINALIZADA ===")

spark.stop()
