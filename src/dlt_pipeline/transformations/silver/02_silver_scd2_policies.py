# Este script implementa una transformación incremental SCD2 sobre la tabla silver de policies.
# Ejecutar tras haber corrido la transformación silver principal (02_silver_transformation.py) al menos una vez,
# para tener datos iniciales en silver_policies.

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, DateType, TimestampType

spark = configure_spark_with_delta_pip(
    SparkSession.builder
    .appName("silver-scd2-policies")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "16")
).getOrCreate()

SILVER_PATH  = "pipelines/silver"
SCD2_PATH    = f"{SILVER_PATH}/policies_scd2"

# Columnas que, si cambian, generan una nueva versión histórica.
# Excluimos deliberadamente policy_updated_at (es la clave de versión,
# no un atributo de negocio) e ingestion_timestamp / source_file (auditoría bronce).
SCD2_TRACKED_COLS = [
    "policyholder_age",
    "gender",
    "region",
    "region_type",
    "occupation",
    "coverage_type",
    "payment_frequency",
    "annual_premium_eur",
    "bonus_malus_years",
    "has_telematics",
    "multi_policy",
    "vehicle_make",
    "vehicle_type",
    "vehicle_year",
    "vehicle_value_eur",
    "annual_mileage_km",
    "is_electric",
]

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _cast_policies(df):
    """Aplica los mismos castings que el pipeline silver principal."""
    return (
        df
        .withColumn("policyholder_age",   F.col("policyholder_age").cast(IntegerType()))
        .withColumn("vehicle_year",       F.col("vehicle_year").cast(IntegerType()))
        .withColumn("vehicle_value_eur",  F.col("vehicle_value_eur").cast(DoubleType()))
        .withColumn("annual_premium_eur", F.col("annual_premium_eur").cast(DoubleType()))
        .withColumn("bonus_malus_years",  F.col("bonus_malus_years").cast(IntegerType()))
        .withColumn("annual_mileage_km",  F.col("annual_mileage_km").cast(IntegerType()))
        .withColumn("has_telematics",     F.col("has_telematics").cast(IntegerType()))
        .withColumn("multi_policy",       F.col("multi_policy").cast(IntegerType()))
        .withColumn("is_electric",        F.col("is_electric").cast(IntegerType()))
        .withColumn("policy_start_date",  F.to_date("policy_start_date"))
        .withColumn("policy_updated_at",  F.to_timestamp("policy_updated_at"))
        .drop("source_file", "year", "month", "ingestion_timestamp", "silver_timestamp")
    )


def _build_change_condition() -> str:
    """
    Condición SQL que detecta si algún atributo de negocio cambió
    entre la fila existente (prefijo 'existing') y la entrante (prefijo 'incoming').
    """
    parts = [
        f"existing.{c} <> incoming.{c} OR "
        f"(existing.{c} IS NULL AND incoming.{c} IS NOT NULL) OR "
        f"(existing.{c} IS NOT NULL AND incoming.{c} IS NULL)"
        for c in SCD2_TRACKED_COLS
    ]
    return " OR ".join(f"({p})" for p in parts)


def init_scd2_table(df):
    """
    Primera carga: construye la tabla SCD2 desde cero.
    Cada registro arranca con scd_start = policy_updated_at,
    scd_end = NULL (fila vigente) y is_current = True.
    """
    (
        df
        .withColumn("scd_start",  F.col("policy_start_date").cast(TimestampType())) # es la fecha real desde la que la póliza está vigente
        .withColumn("scd_end",    F.lit(None).cast(TimestampType()))
        .withColumn("is_current", F.lit(True))
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(SCD2_PATH)
    )
    print(f"[scd2] Tabla inicializada: {df.count()} registros.")


def apply_scd2(incoming_df):
    """
    Lógica incremental SCD2 sobre la tabla Delta existente.

    Para cada registro entrante:
      - Si la policy_id NO existe → INSERT como fila vigente.
      - Si la policy_id existe Y algún atributo cambió:
          1. UPDATE la fila vigente: scd_end = incoming.policy_updated_at, is_current = False.
          2. INSERT la nueva versión como fila vigente.
      - Si la policy_id existe Y nada cambió → no hacer nada (idempotente).
    """
    scd2_table = DeltaTable.forPath(spark, SCD2_PATH)
    change_condition = _build_change_condition()

    # Paso 1: cerrar filas vigentes que han cambiado
    (
        scd2_table.alias("existing")
        .merge(
            incoming_df.alias("incoming"),
            condition=(
                "existing.policy_id = incoming.policy_id "
                "AND existing.is_current = true "
                f"AND ({change_condition})"
            ),
        )
        .whenMatchedUpdate(set={
            "scd_end":    "incoming.policy_updated_at",
            "is_current": "false",
        })
        .execute()
    )

    # Paso 2: insertar nuevas versiones para las filas que cambiaron
    # y registros completamente nuevos
    existing_current = (
        scd2_table.toDF()
        .filter(F.col("is_current") == True)
        .select("policy_id")
    )

    new_versions = (
        incoming_df
        .join(
            existing_current.withColumnRenamed("policy_id", "existing_policy_id"),
            incoming_df.policy_id == F.col("existing_policy_id"),
            how="left",
        )
        # Insertar si: (a) es una policy nueva, o (b) ya cerramos su fila vigente
        # en el paso anterior (ya no aparece como is_current=True)
        .filter(F.col("existing_policy_id").isNull())
        .drop("existing_policy_id")
        .withColumn("scd_start",  F.col("policy_updated_at"))
        .withColumn("scd_end",    F.lit(None).cast(TimestampType()))
        .withColumn("is_current", F.lit(True))
    )

    (
        new_versions.write
        .format("delta")
        .mode("append")
        .save(SCD2_PATH)
    )

    n_new = new_versions.count()
    print(f"[scd2] Versiones nuevas insertadas: {n_new}")


# ─────────────────────────────────────────────────────────────────────────────
# Ejecución
# ─────────────────────────────────────────────────────────────────────────────

incoming = _cast_policies(
    spark.read.format("delta").load(f"{SILVER_PATH}/policies")
)

if not DeltaTable.isDeltaTable(spark, SCD2_PATH):
    print("[scd2] Primera ejecución — inicializando tabla.")
    init_scd2_table(incoming)
else:
    print("[scd2] Tabla existente detectada — aplicando merge incremental.")
    apply_scd2(incoming)

# ─────────────────────────────────────────────────────────────────────────────
# Verificación
# ─────────────────────────────────────────────────────────────────────────────

scd2 = spark.read.format("delta").load(SCD2_PATH)

print("\n=== Verificación SCD2 ===")
print(f"Total filas (todas las versiones): {scd2.count():,}")
print(f"Filas vigentes (is_current=True):  {scd2.filter('is_current = true').count():,}")
print(f"Filas históricas (is_current=False): {scd2.filter('is_current = false').count():,}")

print("\nPolicies con más de una versión histórica (muestra):")
(
    scd2
    .groupBy("policy_id")
    .count()
    .filter(F.col("count") > 1)
    .orderBy(F.desc("count"))
    .limit(5)
    .show()
)

print("\nEjemplo de evolución de una policy con varias versiones:")
policy_multiversion = (
    scd2
    .groupBy("policy_id")
    .count()
    .filter(F.col("count") > 1)
    .orderBy(F.desc("count"))
    .first()
)
if policy_multiversion:
    (
        scd2
        .filter(F.col("policy_id") == policy_multiversion["policy_id"])
        .select(
            "policy_id", "policy_updated_at",
            "coverage_type", "annual_premium_eur",
            "vehicle_value_eur", "is_electric",
            "scd_start", "scd_end", "is_current"
        )
        .orderBy("scd_start")
        .show(truncate=False)
    )

print("\n=== SCD2 completado ===")