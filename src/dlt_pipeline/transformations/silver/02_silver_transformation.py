from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, DoubleType, TimestampType, DateType
)
from src.dlt_pipeline.rules import get_rules_for
from pyspark.sql.functions import broadcast

# ─────────────────────────────────────────────────────────────────────────────
# Spark session
# ─────────────────────────────────────────────────────────────────────────────

spark = configure_spark_with_delta_pip(
    SparkSession.builder
    .appName("silver-transformation")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # Memoria y spill para WSL2
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .config("spark.memory.fraction", "0.6")
    .config("spark.memory.storageFraction", "0.3")
    .config("spark.sql.shuffle.partitions", "16")
    .config("spark.local.dir", "/tmp/spark-tmp")
).getOrCreate()

BRONZE_PATH = "pipelines/bronze"
SILVER_PATH = "pipelines/silver"


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _build_flag(constraint: str) -> F.Column:
    """Convierte una constraint SQL en una columna booleana."""
    return F.expr(constraint)


def apply_rules(
    df: DataFrame,
    tag: str,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """
    Aplica las reglas de calidad de un tag dado sobre un DataFrame.

    Devuelve tres DataFrames:
        clean      — registros que pasan TODAS las reglas
        quarantine — registros que fallan al menos una regla con action='quarantine'
        warn_log   — filas con las reglas 'warn' que fallaron (para logging)
    """
    rules = get_rules_for(tag)

    drop_rules = [r for r in rules if r["action"] == "drop"]
    quarantine_rules = [r for r in rules if r["action"] == "quarantine"]
    warn_rules = [r for r in rules if r["action"] == "warn"]

    # 1. Drop: eliminar silenciosamente si falla cualquier regla drop
    if drop_rules:
        drop_condition = F.lit(True)
        for rule in drop_rules:
            drop_condition = drop_condition & _build_flag(rule["constraint"])
        df = df.filter(drop_condition)

    # 2. Quarantine: separar los registros que fallan alguna regla quarantine
    if quarantine_rules:
        quarantine_condition = F.lit(False)
        failed_rule_exprs = []
        for rule in quarantine_rules:
            flag = _build_flag(rule["constraint"])
            quarantine_condition = quarantine_condition | ~flag
            failed_rule_exprs.append(
                F.when(~flag, F.lit(rule["name"]))
            )

        quarantine_df = (
            df.filter(quarantine_condition)
            .withColumn(
                "failed_rules",
                F.array(*[
                    F.when(~_build_flag(r["constraint"]), F.lit(r["name"]))
                    for r in quarantine_rules
                ])
            )
            .withColumn(
                "failed_rules",
                F.expr("filter(failed_rules, x -> x is not null)")
            )
            .withColumn("quarantine_timestamp", F.current_timestamp())
            .withColumn("entity", F.lit(tag))
        )
        clean_df = df.filter(~quarantine_condition)
    else:
        quarantine_df = spark.createDataFrame([], df.schema)
        clean_df = df

    # 3. Warn: registrar en log pero dejar pasar el registro
    if warn_rules:
        warn_condition = F.lit(False)
        for rule in warn_rules:
            warn_condition = warn_condition | ~_build_flag(rule["constraint"])
        warn_log_df = (
            clean_df.filter(warn_condition)
            .withColumn(
                "warned_rules",
                F.array(*[
                    F.when(~_build_flag(r["constraint"]), F.lit(r["name"]))
                    for r in warn_rules
                ])
            )
            .withColumn(
                "warned_rules",
                F.expr("filter(warned_rules, x -> x is not null)")
            )
            .withColumn("warn_timestamp", F.current_timestamp())
        )
    else:
        warn_log_df = spark.createDataFrame([], df.schema)

    return clean_df, quarantine_df, warn_log_df


def write_delta(df: DataFrame, path: str, partition_by: list[str] | None = None) -> None:
    writer = (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
    )
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)


# ─────────────────────────────────────────────────────────────────────────────
# 1. POLICIES
# Primero porque claims necesita has_telematics para su regla condicional
# ─────────────────────────────────────────────────────────────────────────────

raw_policies = spark.read.format("delta").load(f"{BRONZE_PATH}/policies")

policies = (
    raw_policies
    # Casting: bronce leyó todo como string desde el CSV
    .withColumn("policyholder_age",   F.col("policyholder_age").cast(IntegerType()))
    .withColumn("vehicle_year",       F.col("vehicle_year").cast(IntegerType()))
    .withColumn("vehicle_value_eur",  F.col("vehicle_value_eur").cast(DoubleType()))
    .withColumn("annual_premium_eur", F.col("annual_premium_eur").cast(DoubleType()))
    .withColumn("bonus_malus_years",  F.col("bonus_malus_years").cast(IntegerType()))
    .withColumn("annual_mileage_km",  F.col("annual_mileage_km").cast(IntegerType()))
    .withColumn("has_telematics",     F.col("has_telematics").cast(IntegerType()))
    .withColumn("multi_policy",       F.col("multi_policy").cast(IntegerType()))
    .withColumn("is_electric",        F.col("is_electric").cast(IntegerType()))
    # Fechas
    .withColumn("policy_start_date",  F.to_date("policy_start_date"))
    .withColumn("policy_updated_at",  F.to_timestamp("policy_updated_at"))
    # Columnas de auditoría silver
    .withColumn("silver_timestamp",   F.current_timestamp())
    # Quitar columnas de auditoría bronce que no pertenecen a silver
    .drop("source_file", "year", "month")
)

policies_clean, policies_quarantine, policies_warn = apply_rules(policies, "policies")

write_delta(policies_clean, f"{SILVER_PATH}/policies")
write_delta(policies_quarantine, f"{SILVER_PATH}/policies_quarantine")

if policies_warn.count() > 0:
    policies_warn.show(10, truncate=False)

print(f"[policies] clean={policies_clean.count()} | quarantine={policies_quarantine.count()}")

# ─────────────────────────────────────────────────────────────────────────────
# 2. CLAIMS
# Requiere join con policies para las reglas condicionales de telematics
# ─────────────────────────────────────────────────────────────────────────────

raw_claims = spark.read.format("delta").load(f"{BRONZE_PATH}/claims")

# Traemos solo has_telematics de policies para la regla condicional
policies_telematics = policies_clean.select("policy_id", "has_telematics")

claims = (
    raw_claims
    .withColumn("timestamp",       F.to_timestamp("timestamp"))
    .withColumn("silver_timestamp", F.current_timestamp())
    .drop("source_file", "year", "month")
    # Join para poder evaluar la regla telematics_anomaly_requires_device
    .join(policies_telematics, on="policy_id", how="left")
)

claims_clean, claims_quarantine, claims_warn = apply_rules(claims, "claims")

# Quitamos has_telematics del output final de claims (pertenece a policies)
claims_clean = claims_clean.drop("has_telematics")
claims_quarantine = claims_quarantine.drop("has_telematics")

# Reconstruimos year/month desde timestamp para el particionado
claims_clean_partitioned = (
    claims_clean
    .withColumn("year",  F.date_format("timestamp", "yyyy"))
    .withColumn("month", F.date_format("timestamp", "MM"))
)

write_delta(claims_clean_partitioned, f"{SILVER_PATH}/claims", partition_by=["year", "month"])
write_delta(claims_quarantine, f"{SILVER_PATH}/claims_quarantine")

if claims_warn.count() > 0:
    claims_warn.show(10, truncate=False)

print(f"[claims] clean={claims_clean.count()} | quarantine={claims_quarantine.count()}")

# ─────────────────────────────────────────────────────────────────────────────
# 3. LABELS
# ─────────────────────────────────────────────────────────────────────────────

raw_labels = spark.read.format("delta").load(f"{BRONZE_PATH}/labels")

labels = (
    raw_labels
    .withColumn("label_available_date", F.to_timestamp("label_available_date"))
    .withColumn("silver_timestamp",     F.current_timestamp())
    .drop("source_file", "year", "month")
)

labels_clean, labels_quarantine, labels_warn = apply_rules(labels, "labels")

write_delta(labels_clean, f"{SILVER_PATH}/labels")
write_delta(labels_quarantine, f"{SILVER_PATH}/labels_quarantine")

print(f"[labels] clean={labels_clean.count()} | quarantine={labels_quarantine.count()}")

# ─────────────────────────────────────────────────────────────────────────────
# 4. INTEGRIDAD REFERENCIAL
# Se aplica sobre las tablas silver ya limpias
# ─────────────────────────────────────────────────────────────────────────────

# 4a. claims -> policies: todo claim debe tener su policy en silver
claims_integrity = (
    claims_clean_partitioned
    .join(
        broadcast(policies_clean.select(F.col("policy_id").alias("policy_found"))),
        on=F.col("policy_id") == F.col("policy_found"),
        how="left",
    )
)

claims_integrity_clean, claims_integrity_q, _ = apply_rules(
    claims_integrity, "integrity_claims"
)
claims_final = claims_integrity_clean.drop("policy_found")
claims_orphans = claims_integrity_q.drop("policy_found")

write_delta(
    claims_final,
    f"{SILVER_PATH}/claims",
    partition_by=["year", "month"],
)
if claims_orphans.count() > 0:
    write_delta(claims_orphans, f"{SILVER_PATH}/claims_quarantine")

print(f"[integrity claims] orphans={claims_orphans.count()}")

# 4b. labels -> claims: toda label debe tener su claim en silver
labels_integrity = (
    labels_clean
    .join(
        broadcast(claims_final.select(F.col("claim_id").alias("claim_found"))),
        on=F.col("claim_id") == F.col("claim_found"),
        how="left",
    )
)

labels_integrity_clean, labels_integrity_q, _ = apply_rules(
    labels_integrity, "integrity_labels"
)
labels_final = labels_integrity_clean.drop("claim_found")
labels_orphans = labels_integrity_q.drop("claim_found")

write_delta(labels_final, f"{SILVER_PATH}/labels")
if labels_orphans.count() > 0:
    write_delta(labels_orphans, f"{SILVER_PATH}/labels_quarantine")

print(f"[integrity labels] orphans={labels_orphans.count()}")

print("\n=== Silver pipeline completado ===")