# Este script trata una inspección de cuarentena en silver, para entender mejor qué reglas están fallando y por qué.
# Se ejecuta después de haber corrido la transformación y antes de cualquier análisis o modelado.

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = configure_spark_with_delta_pip(
    SparkSession.builder
    .appName("silver-quarantine-inspection")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "16")
).getOrCreate()

SILVER_PATH = "pipelines/silver"

# ─────────────────────────────────────────────────────────────────────────────
# 1. Volumetría general
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "="*60)
print("1. VOLUMETRÍA SILVER")
print("="*60)

tables = {
    "claims":             f"{SILVER_PATH}/claims",
    "claims_quarantine":  f"{SILVER_PATH}/claims_quarantine",
    "labels":             f"{SILVER_PATH}/labels",
    "labels_quarantine":  f"{SILVER_PATH}/labels_quarantine",
    "policies":           f"{SILVER_PATH}/policies",
}

counts = {}
for name, path in tables.items():
    try:
        n = spark.read.format("delta").load(path).count()
        counts[name] = n
        print(f"  {name:<30} {n:>10,}")
    except Exception:
        print(f"  {name:<30} {'(vacía o no existe)':>10}")

total_claims_in  = counts.get("claims", 0) + counts.get("claims_quarantine", 0)
quarantine_rate  = counts.get("claims_quarantine", 0) / total_claims_in * 100 if total_claims_in else 0
print(f"\n  Tasa de cuarentena claims: {quarantine_rate:.2f}%")

# ─────────────────────────────────────────────────────────────────────────────
# 2. Distribución de reglas fallidas en claims_quarantine
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "="*60)
print("2. REGLAS FALLIDAS — claims_quarantine")
print("="*60)

q_claims = spark.read.format("delta").load(f"{SILVER_PATH}/claims_quarantine")

rule_counts = (
    q_claims
    .select(F.explode("failed_rules").alias("rule"))
    .groupBy("rule")
    .count()
    .orderBy(F.desc("count"))
)
rule_counts.show(20, truncate=False)

# ─────────────────────────────────────────────────────────────────────────────
# 3. ¿Cuántos registros fallan UNA sola regla vs. varias?
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "="*60)
print("3. REGISTROS POR NÚMERO DE REGLAS FALLIDAS")
print("="*60)

(
    q_claims
    .withColumn("n_failed", F.size("failed_rules"))
    .groupBy("n_failed")
    .count()
    .orderBy("n_failed")
    .show()
)

# ─────────────────────────────────────────────────────────────────────────────
# 4. Inspección de la regla más frecuente
#    (muestra 5 ejemplos reales para entender el patrón)
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "="*60)
print("4. EJEMPLOS DE LA REGLA MÁS FRECUENTE")
print("="*60)

top_rule = rule_counts.first()["rule"]
print(f"  Regla: {top_rule}\n")

(
    q_claims
    .filter(F.array_contains("failed_rules", top_rule))
    .select(
        "claim_id", "policy_id", "timestamp",
        "claimed_amount_eur", "days_to_report",
        "injury_level", "witnesses", "claim_channel",
        "telematics_anomaly", # has_telematics no existe en claims_quarantine porque lo eliminamos con .drop("has_telematics") antes de escribir a cuarentena
        "n_parties_involved", "has_third_party_injury",
        "failed_rules",
    )
    .limit(5)
    .show(truncate=False)
)

# ─────────────────────────────────────────────────────────────────────────────
# 5. Distribución temporal de la cuarentena
#    (¿el problema se concentra en algún año/mes?)
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "="*60)
print("5. DISTRIBUCIÓN TEMPORAL — claims_quarantine")
print("="*60)

(
    q_claims
    .withColumn("year", F.date_format("timestamp", "yyyy"))
    .groupBy("year")
    .count()
    .orderBy("year")
    .show()
)

# ─────────────────────────────────────────────────────────────────────────────
# 6. Validación de claims_clean
#    (comprobaciones básicas sobre lo que SÍ pasó)
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "="*60)
print("6. VALIDACIÓN CLAIMS_CLEAN")
print("="*60)

clean = spark.read.format("delta").load(f"{SILVER_PATH}/claims")

print("  Nulos por columna clave:")
for col in ["claim_id", "policy_id", "timestamp", "claimed_amount_eur"]:
    n_null = clean.filter(F.col(col).isNull()).count()
    print(f"    {col:<30} nulls={n_null}")

print("\n  Rango de fechas:")
clean.select(
    F.min("timestamp").alias("min_ts"),
    F.max("timestamp").alias("max_ts"),
).show()

print("  Distribución claim_channel:")
clean.groupBy("claim_channel").count().orderBy(F.desc("count")).show()

print("  Estadísticas claimed_amount_eur:")
clean.select(
    F.min("claimed_amount_eur").alias("min"),
    F.percentile_approx("claimed_amount_eur", 0.5).alias("p50"),
    F.percentile_approx("claimed_amount_eur", 0.95).alias("p95"),
    F.max("claimed_amount_eur").alias("max"),
).show()

# ─────────────────────────────────────────────────────────────────────────────
# 7. Coherencia claims <-> labels en silver
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "="*60)
print("7. COHERENCIA CLAIMS <-> LABELS EN SILVER")
print("="*60)

labels = spark.read.format("delta").load(f"{SILVER_PATH}/labels")

from pyspark.sql.functions import broadcast

matched = clean.join(broadcast(labels), on="claim_id", how="inner").count()
claims_sin_label = clean.join(broadcast(labels), on="claim_id", how="left_anti").count()
labels_sin_claim = labels.join(broadcast(clean.select("claim_id")), on="claim_id", how="left_anti").count()

print(f"  Claims con etiqueta:         {matched:>10,}")
print(f"  Claims SIN etiqueta (yet):   {claims_sin_label:>10,}")
print(f"  Labels SIN claim en clean:   {labels_sin_claim:>10,}")

print("\n=== Inspección completada ===")