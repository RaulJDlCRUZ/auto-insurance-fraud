# Este script realiza un enriquecimiento de la tabla silver de claims, haciendo un point-in-time join con
# la tabla de policies SCD2 para obtener el estado de la póliza vigente en el momento del siniestro.
# Luego hace un left join con labels para agregar la etiqueta de fraude confirmada (is_fraud) cuando esté disponible.
# Finalmente, escribe el resultado en una nueva tabla silver llamada claims_enriched, particionada por año y mes del siniestro.
# Naturalmente, se ejecuta tras hacer la transformación SCD2 de policies (tras transformación principal) y antes de cualquier análisis o modelado.

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = configure_spark_with_delta_pip(
    SparkSession.builder
    .appName("silver-claims-enriched")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "16")
).getOrCreate()

SILVER_PATH = "pipelines/silver"

# ─────────────────────────────────────────────────────────────────────────────
# Lectura de tablas silver
# ─────────────────────────────────────────────────────────────────────────────

claims = spark.read.format("delta").load(f"{SILVER_PATH}/claims")
labels = spark.read.format("delta").load(f"{SILVER_PATH}/labels")
policies_scd2 = spark.read.format("delta").load(f"{SILVER_PATH}/policies_scd2")

# ─────────────────────────────────────────────────────────────────────────────
# Point-in-time join: cada claim con el estado de su póliza vigente
# en el momento del siniestro.
#
# Condición:
#   claim.timestamp >= policy.scd_start
#   AND (policy.scd_end IS NULL OR claim.timestamp < policy.scd_end)
#
# Con una sola versión por póliza (scd_end = NULL para todas),
# esto equivale a un join directo. Cuando existan versiones históricas,
# la condición seleccionará automáticamente la correcta.
# ─────────────────────────────────────────────────────────────────────────────

claims_with_policy = (
    claims.alias("c")
    .join(
        policies_scd2.alias("p"),
        on=(
            (F.col("c.policy_id") == F.col("p.policy_id")) &
            (F.col("c.timestamp") >= F.col("p.scd_start")) &
            (
                F.col("p.scd_end").isNull() |
                (F.col("c.timestamp") < F.col("p.scd_end"))
            )
        ),
        how="left",
    )
    .select(
        # — Identifiers —
        F.col("c.claim_id"),
        F.col("c.policy_id"),

        # — Claim event —
        F.col("c.timestamp").alias("claim_timestamp"),
        F.col("c.accident_type"),
        F.col("c.accident_location_type"),
        F.col("c.days_to_report"),
        F.col("c.n_parties_involved"),
        F.col("c.witnesses"),
        F.col("c.injury_level"),
        F.col("c.police_report_filed"),
        F.col("c.outside_business_hours"),
        F.col("c.claimed_amount_eur"),
        F.col("c.body_shop_id"),
        F.col("c.has_third_party_injury"),
        F.col("c.third_party_same_insurer"),
        F.col("c.telematics_anomaly"),
        F.col("c.claim_channel"),

        # — Policy state at claim time (point-in-time) —
        F.col("p.policyholder_age"),
        F.col("p.gender"),
        F.col("p.region"),
        F.col("p.region_type"),
        F.col("p.occupation"),
        F.col("p.policy_start_date"),
        F.col("p.coverage_type"),
        F.col("p.payment_frequency"),
        F.col("p.annual_premium_eur"),
        F.col("p.bonus_malus_years"),
        F.col("p.has_telematics"),
        F.col("p.multi_policy"),
        F.col("p.vehicle_make"),
        F.col("p.vehicle_type"),
        F.col("p.vehicle_year"),
        F.col("p.vehicle_value_eur"),
        F.col("p.annual_mileage_km"),
        F.col("p.is_electric"),

        # — SCD2 version used (trazabilidad) —
        F.col("p.scd_start").alias("policy_version_start"),
        F.col("p.scd_end").alias("policy_version_end"),

        # — Auditoría —
        F.col("c.year"),
        F.col("c.month"),
        F.current_timestamp().alias("enriched_timestamp"),
    )
)

# ─────────────────────────────────────────────────────────────────────────────
# LEFT JOIN con labels:
#   is_fraud = valor confirmado  → training
#   is_fraud = NULL              → inference (sin etiqueta aún)
# ─────────────────────────────────────────────────────────────────────────────

labels_slim = labels.select("claim_id", "is_fraud", "label_available_date")

enriched = (
    claims_with_policy
    .join(labels_slim, on="claim_id", how="left")
)

# ─────────────────────────────────────────────────────────────────────────────
# Escritura
# ─────────────────────────────────────────────────────────────────────────────

(
    enriched.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("year", "month")
    .save(f"{SILVER_PATH}/claims_enriched")
)

# ─────────────────────────────────────────────────────────────────────────────
# Verificación
# ─────────────────────────────────────────────────────────────────────────────

result = spark.read.format("delta").load(f"{SILVER_PATH}/claims_enriched")

print("\n=== Verificación claims_enriched ===")
print(f"Total registros:              {result.count():>10,}")
print(f"Con etiqueta (training):      {result.filter('is_fraud IS NOT NULL').count():>10,}")
print(f"Sin etiqueta (inference):     {result.filter('is_fraud IS NULL').count():>10,}")
print(f"Sin póliza matching (warning):{result.filter('policyholder_age IS NULL').count():>10,}")

print("\nDistribución is_fraud:")
result.groupBy("is_fraud").count().orderBy("is_fraud").show()

print("Rango temporal:")
result.select(
    F.min("claim_timestamp").alias("min"),
    F.max("claim_timestamp").alias("max"),
).show()

print("Muestra de columnas clave (5 filas):")
result.select(
    "claim_id", "claim_timestamp", "is_fraud",
    "coverage_type", "vehicle_value_eur",
    "policy_version_start", "policy_version_end",
).show(5, truncate=False)

print("\n=== Enriquecimiento completado ===")