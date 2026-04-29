# Genera la tabla de agregaciones dinámicas de comportamiento por cliente
# (policy_id) para la detección de fraude.
#
# Fuente   : silver/claims_enriched (training — is_fraud IS NOT NULL)
# Salida   : pipelines/gold/customer_aggregations
#
# Estrategia: batch + rolling windows ancladas al milisegundo exacto
# de cada claim_timestamp, usando Window.partitionBy + rangeBetween
# (point-in-time correctness — sin leakage de información futura).
#
# Ventanas calculadas: 1h · 24h · 7d · 30d
#
# Métricas por ventana:
#   - num_claims_Xh/d          : conteo de claims anteriores
#   - total_amount_Xh/d        : suma de claimed_amount_eur
#   - avg_amount_Xh/d          : media de claimed_amount_eur
#   - num_telematics_Xh/d      : conteo de anomalías telemáticas
#   - num_fraud_confirmed_Xd   : conteo de fraudes confirmados (solo training)
#   - num_unique_shops_30d     : body shops distintos en 30 días
#
# Métricas derivadas (post-ventana):
#   - amount_ratio_24h_vs_30d  : ratio gasto 24h / media 30d
#   - fraud_rate_30d            : tasa de fraude confirmado en 30d
# =============================================================================

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window, WindowSpec

# ─────────────────────────────────────────────────────────────────────────────
# Sesión Spark
# ─────────────────────────────────────────────────────────────────────────────

spark = configure_spark_with_delta_pip(
    SparkSession.builder
    .appName("gold-customer-aggregations")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "24")
).getOrCreate()

SILVER_PATH = "pipelines/silver"
GOLD_PATH   = "pipelines/gold"

# ─────────────────────────────────────────────────────────────────────────────
# Constantes de ventana (en segundos)
# ─────────────────────────────────────────────────────────────────────────────

_1H  = 3_600
_24H = 86_400
_7D  = 7  * 86_400
_30D = 30 * 86_400

# ─────────────────────────────────────────────────────────────────────────────
# Lectura
# Solo claims con etiqueta confirmada → training.
# ─────────────────────────────────────────────────────────────────────────────

enriched = (
    spark.read.format("delta")
    .load(f"{SILVER_PATH}/claims_enriched")
    .filter(F.col("is_fraud").isNotNull())
    .select(
        "claim_id",
        "policy_id",
        "claim_timestamp",
        "claimed_amount_eur",
        "telematics_anomaly",
        "body_shop_id",
        "is_fraud",
        "year",
        "month",
    )
)

# ─────────────────────────────────────────────────────────────────────────────
# Columna auxiliar: epoch en segundos (base para rangeBetween)
# ─────────────────────────────────────────────────────────────────────────────

enriched = enriched.withColumn(
    "ts_epoch",
    F.col("claim_timestamp").cast("long"),  # segundos desde epoch UTC
)

# ─────────────────────────────────────────────────────────────────────────────
# Definición de ventanas deslizantes
#
# rangeBetween(-X, -1):
#   - Límite superior = -1 segundo → excluye el evento actual (no leakage)
#   - Límite inferior = -X         → X segundos hacia atrás
#   - orderBy(ts_epoch) → orden temporal estricto
#
# partitionBy(policy_id) → cada cliente tiene su propio contexto histórico
# ─────────────────────────────────────────────────────────────────────────────

def rolling(seconds: int) -> WindowSpec:
    """Ventana deslizante hacia atrás de `seconds` segundos, excluyendo el evento actual."""
    return (
        Window
        .partitionBy("policy_id")
        .orderBy("ts_epoch")
        .rangeBetween(-seconds, -1)
    )

w1h  = rolling(_1H)
w24h = rolling(_24H)
w7d  = rolling(_7D)
w30d = rolling(_30D)

# ─────────────────────────────────────────────────────────────────────────────
# Cálculo de agregaciones
#
# Todas las métricas se calculan en una sola pasada (un único select)
# para evitar múltiples shuffles sobre el mismo DataFrame.
# ─────────────────────────────────────────────────────────────────────────────

enriched = enriched.repartition(24, "policy_id").cache()

agg = enriched.select(
    # — Identificadores y partición —
    "claim_id",
    "policy_id",
    "claim_timestamp",
    "is_fraud",
    "year",
    "month",

    # ── Ventana 1 hora ─────────────────────────────────────────────────────
    F.count("claim_id")          .over(w1h).alias("num_claims_1h"),
    F.sum("claimed_amount_eur")  .over(w1h).alias("total_amount_1h"),
    F.mean("claimed_amount_eur") .over(w1h).alias("avg_amount_1h"),
    F.sum("telematics_anomaly")  .over(w1h).alias("num_telematics_1h"),

    # ── Ventana 24 horas ───────────────────────────────────────────────────
    F.count("claim_id")          .over(w24h).alias("num_claims_24h"),
    F.sum("claimed_amount_eur")  .over(w24h).alias("total_amount_24h"),
    F.mean("claimed_amount_eur") .over(w24h).alias("avg_amount_24h"),
    F.sum("telematics_anomaly")  .over(w24h).alias("num_telematics_24h"),

    # ── Ventana 7 días ─────────────────────────────────────────────────────
    F.count("claim_id")          .over(w7d).alias("num_claims_7d"),
    F.sum("claimed_amount_eur")  .over(w7d).alias("total_amount_7d"),
    F.mean("claimed_amount_eur") .over(w7d).alias("avg_amount_7d"),
    F.sum("telematics_anomaly")  .over(w7d).alias("num_telematics_7d"),
    F.sum("is_fraud")            .over(w7d).alias("num_fraud_confirmed_7d"),

    # ── Ventana 30 días ────────────────────────────────────────────────────
    F.count("claim_id")                          .over(w30d).alias("num_claims_30d"),
    F.sum("claimed_amount_eur")                  .over(w30d).alias("total_amount_30d"),
    F.mean("claimed_amount_eur")                 .over(w30d).alias("avg_amount_30d"),
    F.sum("telematics_anomaly")                  .over(w30d).alias("num_telematics_30d"),
    F.sum("is_fraud")                            .over(w30d).alias("num_fraud_confirmed_30d"),
    F.approx_count_distinct("body_shop_id", 0.05).over(w30d).alias("num_unique_shops_30d"),
)

# ─────────────────────────────────────────────────────────────────────────────
# Métricas derivadas (calculadas sobre el resultado de la ventana)
#
# amount_ratio_24h_vs_30d:
#   Cuánto supone el gasto de las últimas 24h respecto a la media de 30d.
#   Un ratio > 1 indica un spike de actividad reciente.
#   Nullif evita división por cero cuando avg_amount_30d = 0 (primer claim).
#
# fraud_rate_30d:
#   Tasa de fraude confirmado en los últimos 30d para este cliente.
#   En el primer claim, num_claims_30d = 0 → NULL (sin historial).
# ─────────────────────────────────────────────────────────────────────────────

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
# Auditoría
# ─────────────────────────────────────────────────────────────────────────────

agg = agg.withColumn("gold_agg_timestamp", F.current_timestamp())

# ─────────────────────────────────────────────────────────────────────────────
# Escritura
# Particionado por year + month — consistente con silver y el resto de gold.
# ─────────────────────────────────────────────────────────────────────────────

(
    agg.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("year", "month")
    .save(f"{GOLD_PATH}/customer_aggregations")
)

# ─────────────────────────────────────────────────────────────────────────────
# Verificación
# ─────────────────────────────────────────────────────────────────────────────

result = spark.read.format("delta").load(f"{GOLD_PATH}/customer_aggregations")

total       = result.count()
with_hist   = result.filter(F.col("num_claims_30d") > 0).count()
first_claim = total - with_hist

print("\n=== Verificación gold_customer_aggregations ===")
print(f"Total registros            : {total:>10,}")
print(f"Con historial 30d          : {with_hist:>10,}")
print(f"Sin historial (primer claim): {first_claim:>10,}")

print("\nNulos por columna clave (deben ser 0 salvo métricas de historial):")
for col in ("claim_id", "policy_id", "claim_timestamp", "is_fraud"):
    nulls = result.filter(F.col(col).isNull()).count()
    print(f"  {col:<30} nulls={nulls}")

print("\nEstadísticas num_claims_30d:")
result.select(
    F.min("num_claims_30d").alias("min"),
    F.percentile_approx("num_claims_30d", 0.50).alias("p50"),
    F.percentile_approx("num_claims_30d", 0.95).alias("p95"),
    F.max("num_claims_30d").alias("max"),
).show()

print("Estadísticas amount_ratio_24h_vs_30d (no nulos):")
result.filter(F.col("amount_ratio_24h_vs_30d").isNotNull()).select(
    F.min("amount_ratio_24h_vs_30d").alias("min"),
    F.percentile_approx("amount_ratio_24h_vs_30d", 0.50).alias("p50"),
    F.percentile_approx("amount_ratio_24h_vs_30d", 0.95).alias("p95"),
    F.max("amount_ratio_24h_vs_30d").alias("max"),
).show()

print("Distribución fraud_rate_30d (no nulos, agrupada):")
result.filter(F.col("fraud_rate_30d").isNotNull()).select(
    F.round(F.col("fraud_rate_30d"), 1).alias("rate_bucket")
).groupBy("rate_bucket").count().orderBy("rate_bucket").show()

print("Muestra (5 filas con historial):")
result.filter(F.col("num_claims_30d") > 0).select(
    "claim_id", "claim_timestamp",
    "num_claims_30d", "total_amount_30d", "avg_amount_30d",
    "num_fraud_confirmed_30d", "fraud_rate_30d",
    "amount_ratio_24h_vs_30d",
).show(5, truncate=False)

print("\n=== gold_customer_aggregations completado ===")