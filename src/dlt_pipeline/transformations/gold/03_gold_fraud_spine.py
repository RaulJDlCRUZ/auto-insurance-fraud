# Genera las dos tablas spine (columna vertebral) para el pipeline de fraude:
#
#   gold/fraud_spine          → base de entrenamiento
#       - Claims con etiqueta confirmada (is_fraud IS NOT NULL)
#       - Fuente: silver/claims_enriched
#       - Incluye: identificadores + timestamp + label + features en tiempo real
#
#   gold/fraud_inference_spine → base de inferencia en producción
#       - Todos los claims sin esperar filtros de calidad ni etiqueta
#       - Fuente: bronze/claims (cobertura total)
#       - Excluye: is_fraud, label_available_date
#
# Las features de perfil (customer_profile) y agregaciones históricas
# (customer_aggregations) NO se unen aquí — las inyecta el feature store
# en la fase de modelado mediante point-in-time lookup.
#
# Integración feature store (local → Databricks):
#   - CDF habilitado en ambas tablas
#   - tblproperties con primary_key y timestamp_key para registro
#     automático en Unity Catalog al migrar
# =============================================================================

from pathlib import Path

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ─────────────────────────────────────────────────────────────────────────────
# Sesión Spark
# ─────────────────────────────────────────────────────────────────────────────

spark = configure_spark_with_delta_pip(
    SparkSession.builder
    .appName("gold-fraud-spine")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.driver.memory", "5g")
    .config("spark.sql.shuffle.partitions", "24")
).getOrCreate()

SILVER_PATH = "pipelines/silver"
BRONZE_PATH = "pipelines/bronze"
GOLD_PATH   = "pipelines/gold"

# ─────────────────────────────────────────────────────────────────────────────
# Columnas de features en tiempo real
# Son las que llegan inherentemente con el parte de accidente en el momento
# de la petición — sin necesidad de lookup histórico ni feature store.
# ─────────────────────────────────────────────────────────────────────────────

REALTIME_FEATURES = [
    "claimed_amount_eur",
    "accident_type",
    "accident_location_type",
    "days_to_report",
    "n_parties_involved",
    "witnesses",
    "injury_level",
    "police_report_filed",
    "outside_business_hours",
    "has_third_party_injury",
    "third_party_same_insurer",
    "telematics_anomaly",
    "claim_channel",
    "body_shop_id",
]

# ─────────────────────────────────────────────────────────────────────────────
# Helper: escribir tabla Delta + habilitar CDF + registrar tblproperties
# ─────────────────────────────────────────────────────────────────────────────

def write_spine(df, output_path: str, primary_key: str, timestamp_key: str, description: str) -> None:
    """Escribe el DataFrame como tabla Delta con CDF y tblproperties de feature store."""

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("year", "month")
        .save(output_path)
    )

    abs_path = str(Path(output_path).resolve())

    spark.sql(f"""
        ALTER TABLE delta.`{abs_path}`
        SET TBLPROPERTIES (
            'delta.enableChangeDataFeed'      = 'true',
            'feature_store.primary_keys'      = '{primary_key}',
            'feature_store.timestamp_key'     = '{timestamp_key}',
            'feature_store.description'       = '{description}',
            'pipeline.layer'                  = 'gold',
            'pipeline.component'              = 'fraud_spine',
            'pipeline.source'                 = '{output_path}'
        )
    """)


# =============================================================================
# TABLA 1: gold/fraud_spine  (entrenamiento)
# =============================================================================
#
# Fuente: silver/claims_enriched filtrado por is_fraud IS NOT NULL
# Equivalente batch del stream-stream join con watermark de Databricks:
# todas las filas donde la etiqueta ya ha llegado y pasado calidad.
# =============================================================================

enriched = spark.read.format("delta").load(f"{SILVER_PATH}/claims_enriched")

fraud_spine = (
    enriched
    .filter(F.col("is_fraud").isNotNull())
    .select(
        # — Identificadores —
        F.col("claim_id"),
        F.col("policy_id"),

        # — Timestamp del evento (anchor point-in-time para feature store) —
        F.col("claim_timestamp"),

        # — Variable objetivo —
        F.col("is_fraud"),
        F.col("label_available_date"),

        # — Features en tiempo real —
        *[F.col(c) for c in REALTIME_FEATURES],

        # — Partición —
        F.col("year"),
        F.col("month"),

        # — Auditoría —
        F.current_timestamp().alias("gold_spine_timestamp"),
    )
)

write_spine(
    df           = fraud_spine,
    output_path  = f"{GOLD_PATH}/fraud_spine",
    primary_key  = "claim_id",
    timestamp_key= "claim_timestamp",
    description  = (
        "Training spine for fraud detection. One row per labelled claim. "
        "Real-time features only — profile and aggregations injected by feature store."
    ),
)

print("[spine] gold/fraud_spine escrita.")

# =============================================================================
# TABLA 2: gold/fraud_inference_spine  (inferencia en producción)
# =============================================================================
#
# Fuente: bronze/claims — cobertura total, sin esperar filtros de calidad
# ni etiqueta confirmada. En producción, no puntuar = aprobar sin análisis.
#
# Las etiquetas disponibles en silver/labels se unen en LEFT JOIN
# únicamente para trazabilidad (saber qué claims ya tienen resolución),
# pero NO se incluyen en el esquema de salida — en inferencia real
# is_fraud no existe en el momento de la predicción.
#
# Columnas de partición year/month derivadas del timestamp del claim.
# =============================================================================

bronze_claims = spark.read.format("delta").load(f"{BRONZE_PATH}/claims")
silver_labels = spark.read.format("delta").load(f"{SILVER_PATH}/labels") \
                     .select("claim_id", "label_available_date")

# LEFT JOIN para trazabilidad: saber si este claim ya tiene etiqueta resuelta.
# El campo label_available_date queda como metadato operacional (no es feature
# ni target) — permite al equipo monitorizar qué fracción de la inference
# spine ya tiene resolución confirmada.
bronze_with_meta = (
    bronze_claims.alias("b")
    .join(silver_labels.alias("l"), on="claim_id", how="left")
)

inference_spine = (
    bronze_with_meta
    .select(
        # — Identificadores —
        F.col("b.claim_id"),
        F.col("b.policy_id"),

        # — Timestamp del evento —
        F.to_timestamp(F.col("b.timestamp")).alias("claim_timestamp"),

        # — Features en tiempo real (mismas que fraud_spine) —
        *[F.col(f"b.{c}") for c in REALTIME_FEATURES],

        # — Metadato operacional (no es target ni feature) —
        F.col("l.label_available_date"),

        # — Partición (derivada del timestamp, no de columnas pre-existentes) —
        F.year(F.col("b.timestamp")).cast("string").alias("year"),
        F.lpad(F.month(F.col("b.timestamp")).cast("string"), 2, "0") # lpad para formato 'MM' como month=01
            .alias("month"),

        # — Auditoría —
        F.current_timestamp().alias("gold_spine_timestamp"),
    )
)

write_spine(
    df            = inference_spine,
    output_path   = f"{GOLD_PATH}/fraud_inference_spine",
    primary_key   = "claim_id",
    timestamp_key = "claim_timestamp",
    description   = (
        "Inference spine for fraud detection. One row per claim regardless of "
        "label availability or quality filters. No target column — scores assigned "
        "by model at prediction time."
    ),
)

print("[spine] gold/fraud_inference_spine escrita.")

# =============================================================================
# Verificación
# =============================================================================

spine     = spark.read.format("delta").load(f"{GOLD_PATH}/fraud_spine")
inf_spine = spark.read.format("delta").load(f"{GOLD_PATH}/fraud_inference_spine")

print("\n=== Verificación gold_fraud_spine (training) ===")
print(f"Total registros            : {spine.count():>10,}")
print(f"Nulos en is_fraud          : {spine.filter(F.col('is_fraud').isNull()).count():>10,}")
print(f"Nulos en claim_id          : {spine.filter(F.col('claim_id').isNull()).count():>10,}")

print("\nDistribución is_fraud:")
spine.groupBy("is_fraud").count().orderBy("is_fraud").show()

print("Distribución claim_channel:")
spine.groupBy("claim_channel").count().orderBy("claim_channel").show()

print("Rango temporal:")
spine.select(
    F.min("claim_timestamp").alias("min"),
    F.max("claim_timestamp").alias("max"),
).show()

print("Muestra (5 filas):")
spine.select(
    "claim_id", "policy_id", "claim_timestamp",
    "is_fraud", "claimed_amount_eur", "accident_type",
).show(5, truncate=False)

print("\n=== Verificación gold_fraud_inference_spine (inferencia) ===")
print(f"Total registros            : {inf_spine.count():>10,}")
print(f"Nulos en claim_id          : {inf_spine.filter(F.col('claim_id').isNull()).count():>10,}")
print(f"Con label_available_date   : {inf_spine.filter(F.col('label_available_date').isNotNull()).count():>10,}")
print(f"Sin label (producción real): {inf_spine.filter(F.col('label_available_date').isNull()).count():>10,}")

print("\nRango temporal:")
inf_spine.select(
    F.min("claim_timestamp").alias("min"),
    F.max("claim_timestamp").alias("max"),
).show()

print("Muestra (5 filas):")
inf_spine.select(
    "claim_id", "policy_id", "claim_timestamp",
    "claimed_amount_eur", "accident_type", "label_available_date",
).show(5, truncate=False)

# CDF en ambas tablas
print("\nCDF y tblproperties — fraud_spine:")
abs_spine = str(Path(f"{GOLD_PATH}/fraud_spine").resolve())
spark.sql(f"SHOW TBLPROPERTIES delta.`{abs_spine}`") \
     .filter(F.col("key").startswith("delta.enable") |
             F.col("key").startswith("feature_store")) \
     .show(truncate=False)

print("CDF y tblproperties — fraud_inference_spine:")
abs_inf = str(Path(f"{GOLD_PATH}/fraud_inference_spine").resolve())
spark.sql(f"SHOW TBLPROPERTIES delta.`{abs_inf}`") \
     .filter(F.col("key").startswith("delta.enable") |
             F.col("key").startswith("feature_store")) \
     .show(truncate=False)

print("\n=== gold_fraud_spine completado ===")