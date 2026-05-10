"""
export_inference_spine.py
=========================
Exporta las transacciones de data/source_buffer/claims/2025 al formato
que espera fraud_inference_spine, para subirlas a raw_uploads y hacer
MERGE en Databricks.

Transformaciones aplicadas:
  - 'timestamp' -> 'claim_timestamp'
  - 'label_available_date' = None  (desconocida en el momento de prediccion)
  - 'year' y 'month' derivados de claim_timestamp
  - 'month' como string con cero a la izquierda ("01", "02", ...)

Ejecucion:
    python export_inference_spine.py

Salida:
    pipelines/export/inference_spine_2025/   <- Parquet listo para subir
"""

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = configure_spark_with_delta_pip(
    SparkSession.builder
    .appName("export-inference-spine")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.driver.memory", "4g")
).getOrCreate()

BUFFER_PATH = "data/source_buffer/claims/2025"
EXPORT_PATH = "pipelines/export/inference_spine_2025"

df = (
    spark.read
         .option("recursiveFileLookup", "true")
         .json(BUFFER_PATH)
         .withColumnRenamed("timestamp", "claim_timestamp")
         .withColumn("claim_timestamp", F.to_timestamp("claim_timestamp"))
         .withColumn("label_available_date", F.lit(None).cast("timestamp"))
         .withColumn("year",  F.year("claim_timestamp").cast("string"))
         .withColumn("month", F.lpad(F.month("claim_timestamp").cast("string"), 2, "0"))
)

total = df.count()
ts_min, ts_max = df.agg(F.min("claim_timestamp"), F.max("claim_timestamp")).first()
print(f"Rows          : {total:,}")
print(f"Range         : {ts_min}  ->  {ts_max}")
df.printSchema()

(
    df.write
      .format("parquet")
      .mode("overwrite")
      .partitionBy("year", "month")
      .save(EXPORT_PATH)
)

import os
size = sum(
    os.path.getsize(os.path.join(r, f))
    for r, _, files in os.walk(EXPORT_PATH)
    for f in files
    if f.endswith(".parquet")
)
print(f"Tamanio       : {size / 1024**2:.1f} MB")
print(f"\n✓ Listo para subir {EXPORT_PATH} a raw_uploads")

spark.stop()
