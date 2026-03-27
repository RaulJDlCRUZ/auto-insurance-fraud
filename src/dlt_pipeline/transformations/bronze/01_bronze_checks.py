import pyspark.sql.functions as F
# import dlt as dp # Uncomment this line if you are using Databricks Delta Live Tables

# Also uncomment/toogle the @dp.table decorators if you are using Delta Live Tables
# and cloudFiles for auto ingestion. If you are running this as a standalone Spark job, you can leave the decorators commented out.

from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, current_timestamp, regexp_extract
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder.appName("bronze_ingestion")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

tables = ["claims", "labels", "claims_buffer", "policies"]

print(f"===\nVerificando carga de datos en tablas: {tables}\n===")
for t in tables:
    df = spark.read.format("delta").load(f"pipelines/bronze/{t}")
    # Ver si el conteo de filas es mayor a 0, lo que indicaría que se han cargado datos
    print(t, df.count())

# Verificar columnas auditoría
print(f"===\nVerificando columnas de auditoría\n===")
spark.read.format("delta").load("pipelines/bronze/claims").printSchema()

# Mostrar algunas filas en busca de metadatos de auditoría
# en pos de validar paths correctos y timestamps razonables
print(f"===\nVerificando metadata real\n===")
spark.read.format("delta").load("pipelines/bronze/claims").select(
    "source_file",
    "ingestion_timestamp"
).show(5, False)

# Verificamos particionado físico
print(f"===\nVerificando particionado físico en tabla: claims\n===")
spark.read.format("delta").load("pipelines/bronze/claims").groupBy("year", "month").count().show()

# delayed labels
# si el recuento es distinto de cero, dataset entrenable valido
print(f"===\nVerificando delayed labels\n===")
claims = spark.read.format("delta").load("pipelines/bronze/claims")
labels = spark.read.format("delta").load("pipelines/bronze/labels")

# claims.join(labels, "claim_id").count()
print(f"{claims.sample(0.01).join(labels, 'claim_id').count()}")

# timestamps disponibles, para chequear:
# temporal consistency training
# label leakage prevention
print(f"===\nVerificando timestamps\n===")
claims.select("timestamp").show(5)
labels.select("label_available_date").show(5)

spark.read.format("delta").load("pipelines/bronze/claims").columns

