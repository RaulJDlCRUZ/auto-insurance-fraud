"""
export_claims_for_inference.py
==============================
Construye el dataset base para gold_customer_aggregations_inference
uniendo silver/claims_enriched (2015-2025) con data/source_buffer/claims/2025,
recalcula las ventanas deslizantes y exporta a Parquet para subir a Databricks.

Ejecución:
    python export_claims_for_inference.py

Salida:
    pipelines/export/claims_inference_base/   <- Parquet listo para subir
"""

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.window import WindowSpec

# ─────────────────────────────────────────────────────────────────────────────
# Sesión Spark
# ─────────────────────────────────────────────────────────────────────────────

spark = configure_spark_with_delta_pip(
    SparkSession.builder
    .appName("export-claims-inference")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.driver.memory", "6g")
    .config("spark.sql.shuffle.partitions", "24")
).getOrCreate()

SILVER_PATH = "pipelines/silver"
BUFFER_PATH = "data/source_buffer/claims/2025"
EXPORT_PATH = "pipelines/export/claims_inference_base"

# Columnas minimas necesarias para recalcular las ventanas deslizantes.
# El perfil del cliente (edad, genero, region...) no interviene en ninguna
# agregacion de ventana y no se necesita aqui.
WINDOW_COLS = [
    "claim_id",
    "policy_id",
    "claim_timestamp",
    "claimed_amount_eur",
    "telematics_anomaly",
    "body_shop_id",
    "is_fraud",
    "year",
    "month",
]

# ─────────────────────────────────────────────────────────────────────────────
# 1. Leer silver/claims_enriched — seleccionar solo columnas de ventana
# ─────────────────────────────────────────────────────────────────────────────

silver = (
    spark.read.format("delta")
         .load(f"{SILVER_PATH}/claims_enriched")
         .select(WINDOW_COLS)
)

silver_count = silver.count()
print(f"[silver/claims_enriched]  rows: {silver_count:,}")

# ─────────────────────────────────────────────────────────────────────────────
# 2. Leer source_buffer/claims/2025
#    - Renombrar 'timestamp' -> 'claim_timestamp'
#    - is_fraud = 0 (etiqueta no disponible en el momento de prediccion)
#    - Derivar year / month desde claim_timestamp
# ─────────────────────────────────────────────────────────────────────────────

buffer = (
    spark.read
         .option("recursiveFileLookup", "true")
         .json(BUFFER_PATH)
         .withColumnRenamed("timestamp", "claim_timestamp")
         .withColumn("claim_timestamp", F.to_timestamp("claim_timestamp"))
         .withColumn("is_fraud", F.lit(0))
         .withColumn("year",  F.year("claim_timestamp").cast("string"))
         .withColumn("month", F.month("claim_timestamp").cast("string"))
         .select(WINDOW_COLS)
)

buffer_count = buffer.count()
print(f"[source_buffer/claims]    rows: {buffer_count:,}")

# ─────────────────────────────────────────────────────────────────────────────
# 3. Union y deduplicacion
#    Las 3 filas de 2025 que ya estan en silver son las mismas que las primeras
#    del buffer — eliminamos duplicados por claim_id conservando la version
#    de silver (que puede tener is_fraud real si fue procesada).
# ─────────────────────────────────────────────────────────────────────────────

silver_marked = silver.withColumn("_src", F.lit(0))  # 0 = silver (prioritario)
buffer_marked = buffer.withColumn("_src", F.lit(1))  # 1 = buffer

union = silver_marked.unionByName(buffer_marked)

dedup = (
    union
    .withColumn(
        "_rn",
        F.row_number().over(
            Window.partitionBy("claim_id").orderBy("_src")
        )
    )
    .filter(F.col("_rn") == 1)
    .drop("_src", "_rn")
)

total = dedup.count()
print(f"[union deduplicada]       rows: {total:,}")

ts_min, ts_max = dedup.agg(F.min("claim_timestamp"), F.max("claim_timestamp")).first()
print(f"Rango temporal            : {ts_min}  ->  {ts_max}")

# ─────────────────────────────────────────────────────────────────────────────
# 4. Recalcular ventanas deslizantes
#    Misma logica que 03_gold_customer_aggregations.py:
#    rangeBetween(-X, -1) sobre ts_epoch particionado por policy_id.
# ─────────────────────────────────────────────────────────────────────────────

_1H  = 3_600
_24H = 86_400
_7D  = 7  * 86_400
_30D = 30 * 86_400

def rolling(seconds: int) -> WindowSpec:
    return (
        Window
        .partitionBy("policy_id")
        .orderBy("ts_epoch")
        .rangeBetween(-seconds, -1)
    )

enriched = dedup.withColumn("ts_epoch", F.col("claim_timestamp").cast("long"))
enriched = enriched.repartition(24, "policy_id").cache()

agg = enriched.select(
    "claim_id",
    "policy_id",
    "claim_timestamp",
    "is_fraud",
    "year",
    "month",

    # 1h
    F.count("claim_id")          .over(rolling(_1H)) .alias("num_claims_1h"),
    F.sum("claimed_amount_eur")  .over(rolling(_1H)) .alias("total_amount_1h"),
    F.mean("claimed_amount_eur") .over(rolling(_1H)) .alias("avg_amount_1h"),
    F.sum("telematics_anomaly")  .over(rolling(_1H)) .alias("num_telematics_1h"),

    # 24h
    F.count("claim_id")          .over(rolling(_24H)).alias("num_claims_24h"),
    F.sum("claimed_amount_eur")  .over(rolling(_24H)).alias("total_amount_24h"),
    F.mean("claimed_amount_eur") .over(rolling(_24H)).alias("avg_amount_24h"),
    F.sum("telematics_anomaly")  .over(rolling(_24H)).alias("num_telematics_24h"),

    # 7d
    F.count("claim_id")          .over(rolling(_7D)) .alias("num_claims_7d"),
    F.sum("claimed_amount_eur")  .over(rolling(_7D)) .alias("total_amount_7d"),
    F.mean("claimed_amount_eur") .over(rolling(_7D)) .alias("avg_amount_7d"),
    F.sum("telematics_anomaly")  .over(rolling(_7D)) .alias("num_telematics_7d"),
    F.sum("is_fraud")            .over(rolling(_7D)) .alias("num_fraud_confirmed_7d"),

    # 30d
    F.count("claim_id")                          .over(rolling(_30D)).alias("num_claims_30d"),
    F.sum("claimed_amount_eur")                  .over(rolling(_30D)).alias("total_amount_30d"),
    F.mean("claimed_amount_eur")                 .over(rolling(_30D)).alias("avg_amount_30d"),
    F.sum("telematics_anomaly")                  .over(rolling(_30D)).alias("num_telematics_30d"),
    F.sum("is_fraud")                            .over(rolling(_30D)).alias("num_fraud_confirmed_30d"),
    F.approx_count_distinct("body_shop_id", 0.05).over(rolling(_30D)).alias("num_unique_shops_30d"),
)

agg = agg.withColumns({
    "amount_ratio_24h_vs_30d": F.round(
        F.col("total_amount_24h") / F.nullif(F.col("avg_amount_30d"), F.lit(0)),
        4,
    ),
    "fraud_rate_30d": F.round(
        F.col("num_fraud_confirmed_30d") / F.nullif(F.col("num_claims_30d"), F.lit(0)),
        4,
    ),
})

# ─────────────────────────────────────────────────────────────────────────────
# 5. Exportar a Parquet
#    Se excluye is_fraud del payload final — no forma parte del contrato
#    de inferencia y evita cualquier riesgo de leakage accidental.
# ─────────────────────────────────────────────────────────────────────────────

(
    agg
    .drop("is_fraud")
    .write
    .format("parquet")
    .mode("overwrite")
    .partitionBy("year", "month")
    .save(EXPORT_PATH)
)

# ─────────────────────────────────────────────────────────────────────────────
# 6. Verificacion
# ─────────────────────────────────────────────────────────────────────────────

result = spark.read.parquet(EXPORT_PATH)
total_out = result.count()
rows_2025 = result.filter(F.col("year") == "2025").count()

print("\n" + "=" * 55)
print("VERIFICACION EXPORT")
print("=" * 55)
print(f"Total filas exportadas  : {total_out:,}")
print(f"Filas 2025 (inferencia) : {rows_2025:,}")

result.select(
    F.min("claim_timestamp").alias("min_ts"),
    F.max("claim_timestamp").alias("max_ts"),
).show(truncate=False)

import os
size_bytes = sum(
    os.path.getsize(os.path.join(r, f))
    for r, _, files in os.walk(EXPORT_PATH)
    for f in files
    if f.endswith(".parquet")
)
print(f"Tamanio en disco        : {size_bytes / 1024**2:.1f} MB")
print("\nEsquema exportado:")
result.printSchema()
print("\n Listo para subir pipelines/export/claims_inference_base/ a raw_uploads")

spark.stop()
