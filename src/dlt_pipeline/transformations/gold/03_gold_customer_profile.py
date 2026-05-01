# Genera el perfil estático/demográfico de cada cliente (policy_id)
# a partir de la vista SCD2 de la capa plata.
#
# Diseño: full-refresh (materialized view) — lectura completa de
# silver/policies_scd2 en cada ejecución, garantizando que scd_end
# se traslada intacto sin riesgo de pérdida silenciosa por inserciones.
#
# Características derivadas:
#   - age_group          : bucket etario (young / adult / senior)
#   - vehicle_age        : antigüedad del vehículo en años
#   - vehicle_age_group  : bucket de antigüedad (new / mid / old)
#   - policy_tenure_days : días desde policy_start_date hasta scd_start
#   - premium_per_day    : annual_premium_eur / 365
#   - high_mileage_flag  : 1 si annual_mileage_km > p75 del dataset
#   - risk_score_static  : combinación lineal simple de señales estáticas
#                          (proxy interpretable, no sustituye al modelo)
#
# Integración feature store (local → Databricks):
#   - CDF habilitado: delta.enableChangeDataFeed = true
#   - Metadatos de clave primaria y timeseries almacenados en
#     delta properties para facilitar el registro en Unity Catalog
#     al migrar a Databricks.
#   - Esquema exportado a docs/data_dictionary/gold_customer_profile.json
# =============================================================================

import json
from pathlib import Path

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

# ─────────────────────────────────────────────────────────────────────────────
# Sesión Spark
# ─────────────────────────────────────────────────────────────────────────────

spark = configure_spark_with_delta_pip(
    SparkSession.builder
    .appName("gold-customer-profile")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "16")
).getOrCreate()

SILVER_PATH = "pipelines/silver"
GOLD_PATH   = "pipelines/gold"
DOCS_PATH   = Path("docs/data_dictionary")

# ─────────────────────────────────────────────────────────────────────────────
# Lectura — full refresh sobre SCD2
# Incluimos todas las versiones (is_current=True y False) para preservar
# el histórico completo. Cada fila representa un período de vigencia
# de la póliza, identificado por (policy_id, scd_start, scd_end).
# ─────────────────────────────────────────────────────────────────────────────

scd2 = spark.read.format("delta").load(f"{SILVER_PATH}/policies_scd2")

# ─────────────────────────────────────────────────────────────────────────────
# Umbral de alto kilometraje (p75 del dataset)
# Se calcula sobre todas las versiones vigentes para representar
# la distribución actual de la flota.
# ─────────────────────────────────────────────────────────────────────────────

p75_mileage = (
    scd2
    .filter(F.col("is_current") == True)
    .select(
        F.percentile_approx("annual_mileage_km", 0.75).alias("p75")
    )
    .collect()[0]["p75"]
)

print(f"[profile] p75 annual_mileage_km = {p75_mileage:,} km")

# ─────────────────────────────────────────────────────────────────────────────
# Transformaciones
# ─────────────────────────────────────────────────────────────────────────────

# En Databricks se pone desde un widget:
# CURRENT_YEAR = int(dbutils.widgets.get("reference_year"))
# AQUI SE PONE MANUALMENTE PARA EJECUTAR EN LOCAL
CURRENT_YEAR = 2025  # año de referencia para vehicle_age

profile = scd2.select(

    # — Identificadores y control SCD2 —
    F.col("policy_id"),
    F.col("scd_start").alias("__START_AT"),   # convención feature store
    F.col("scd_end").alias("__END_AT"),        # NULL = versión vigente
    F.col("is_current"),

    # ── Demográficas ────────────────────────────────────────────────────────

    F.col("policyholder_age"),

    # age_group: alineado con los grupos de sesgo del generador
    F.when(F.col("policyholder_age") < 25,  F.lit("young"))
     .when(F.col("policyholder_age") <= 70, F.lit("adult"))
     .otherwise(F.lit("senior"))
     .alias("age_group"),

    F.col("gender"),
    F.col("region"),
    F.col("region_type"),
    F.col("occupation"),

    # ── Póliza ──────────────────────────────────────────────────────────────

    F.col("coverage_type"),
    F.col("payment_frequency"),
    F.col("annual_premium_eur"),
    F.col("bonus_malus_years"),
    F.col("has_telematics"),
    F.col("multi_policy"),

    # Antigüedad de la póliza en días en el momento de esta versión SCD2
    # F.datediff(
    #     F.col("scd_start").cast("date"),
    #     F.col("policy_start_date"),
    # ).alias("policy_tenure_days"),

    # Acorde a la capa silver, decidimos usar policy_start_date como scd_start porque es la fecha real desde la que la póliza está vigente
    F.datediff(
        F.lit(f"{CURRENT_YEAR}-01-01").cast("date"),
        F.col("policy_start_date"),
    ).alias("policy_tenure_days"),

    # Sin embargo, ya podemos asumir que el diff es cero al ser la misma fecha, lo cual es coherente
    # si dijimos en la capa silver que el dataset cuenta con una sola versión por póliza

    # Prima diaria normalizada (útil para comparar pólizas de distinta duración)
    F.round(F.col("annual_premium_eur") / F.lit(365.0), 4)
     .alias("premium_per_day"),

    # ── Vehículo ────────────────────────────────────────────────────────────

    F.col("vehicle_make"),
    F.col("vehicle_type"),
    F.col("vehicle_year"),
    F.col("is_electric"),
    F.col("annual_mileage_km"),

    # Antigüedad del vehículo en años respecto al año de referencia
    (F.lit(CURRENT_YEAR) - F.col("vehicle_year"))
     .alias("vehicle_age"),

    # vehicle_age_group: alineado con los grupos de sesgo del generador
    F.when((F.lit(CURRENT_YEAR) - F.col("vehicle_year")) < 4,  F.lit("new"))
     .when((F.lit(CURRENT_YEAR) - F.col("vehicle_year")) <= 10, F.lit("mid"))
     .otherwise(F.lit("old"))
     .alias("vehicle_age_group"),

    F.col("vehicle_value_eur"),

    # Flag de alto kilometraje respecto al p75 del dataset
    F.when(F.col("annual_mileage_km") > F.lit(p75_mileage), F.lit(1))
     .otherwise(F.lit(0))
     .alias("high_mileage_flag"),

    # ── Risk score estático ──────────────────────────────────────────────────
    # Combinación lineal interpretable de señales estáticas de riesgo.
    # Pesos basados en los multiplicadores de sesgo documentados en generate.py:
    #   age_group young/senior: +1 | adult: 0
    #   region_type urban: +1       | rural: -1 | suburban: 0
    #   vehicle_age_group old: +1   | new: -1   | mid: 0
    #   bonus_malus_years >= 5: -1  (conductor experimentado sin incidentes)
    #   has_telematics: -0.5        (monitorización reduce riesgo percibido)
    #
    # Rango teórico: [-3.5, +3.0]. No es un score de ML — es una feature
    # interpretable para el modelo y para auditoría regulatoria.
    F.round(
        # age
        F.when(F.col("policyholder_age") < 25,  F.lit(1.0))
         .when(F.col("policyholder_age") > 70,  F.lit(1.0))
         .otherwise(F.lit(0.0))
        # region
        + F.when(F.col("region_type") == "urban",  F.lit(1.0))
           .when(F.col("region_type") == "rural",  F.lit(-1.0))
           .otherwise(F.lit(0.0))
        # vehicle age
        + F.when((F.lit(CURRENT_YEAR) - F.col("vehicle_year")) > 10, F.lit(1.0))
           .when((F.lit(CURRENT_YEAR) - F.col("vehicle_year")) < 4,  F.lit(-1.0))
           .otherwise(F.lit(0.0))
        # bonus malus
        + F.when(F.col("bonus_malus_years") >= 5, F.lit(-1.0))
           .otherwise(F.lit(0.0))
        # telematics
        + F.when(F.col("has_telematics") == 1, F.lit(-0.5))
           .otherwise(F.lit(0.0)),
        2,
    ).alias("risk_score_static"),

    # — Auditoría —
    F.current_timestamp().alias("gold_profile_timestamp"),
)

# ─────────────────────────────────────────────────────────────────────────────
# Escritura — full overwrite + CDF + propiedades de feature store
#
# tblproperties que Databricks Unity Catalog leerá al registrar:
#   - feature_store.primary_keys  : clave de lookup del feature store
#   - feature_store.timestamp_key : columna timeseries para point-in-time
# En local estas properties quedan almacenadas en el _delta_log y se
# pueden inspeccionar con DESCRIBE EXTENDED (o DeltaTable.detail()).
# ─────────────────────────────────────────────────────────────────────────────

output_path = f"{GOLD_PATH}/customer_profile"

(
    profile.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(output_path)
)

dt = DeltaTable.forPath(spark, output_path)
dt.detail()  # warm-up para asegurar que la tabla está registrada

# Las properties se establecen una a una con el API Python
# No hay ALTER TABLE SQL — usamos el log de Delta directamente
spark.sql(f"""
    ALTER TABLE delta.`{Path(output_path).resolve()}`
    SET TBLPROPERTIES (
        'delta.enableChangeDataFeed'      = 'true',
        'feature_store.primary_keys'      = 'policy_id',
        'feature_store.timestamp_key'     = '__START_AT',
        'feature_store.description'       = 'Static demographic and vehicle profile per policy version (SCD2). Full-refresh on each pipeline run.',
        'pipeline.layer'                  = 'gold',
        'pipeline.component'              = 'customer_profile',
        'pipeline.source'                 = 'silver/policies_scd2'
    )
""")

# ─────────────────────────────────────────────────────────────────────────────
# Exportar esquema a data_dictionary
# Facilita el registro manual en Unity Catalog al migrar a Databricks.
# ─────────────────────────────────────────────────────────────────────────────

DOCS_PATH.mkdir(parents=True, exist_ok=True)

result = spark.read.format("delta").load(output_path)

schema_dict = json.loads(result.schema.json())
metadata = {
    "table": "gold.customer_profile",
    "layer": "gold",
    "source": "silver/policies_scd2",
    "refresh_strategy": "full_overwrite",
    "primary_key": "policy_id",
    "timestamp_key": "__START_AT",
    "cdf_enabled": True,
    "fields": schema_dict["fields"],
}

schema_path = DOCS_PATH / "gold_customer_profile.json"
schema_path.write_text(json.dumps(metadata, indent=2, ensure_ascii=False))
print(f"[profile] Esquema exportado → {schema_path}")

# ─────────────────────────────────────────────────────────────────────────────
# Verificación
# ─────────────────────────────────────────────────────────────────────────────

total     = result.count()
vigentes  = result.filter(F.col("is_current") == True).count()
historico = total - vigentes

print("\n=== Verificación gold_customer_profile ===")
print(f"Total filas (todas las versiones) : {total:>10,}")
print(f"Versiones vigentes (is_current)   : {vigentes:>10,}")
print(f"Versiones históricas              : {historico:>10,}")

print("\nDistribución age_group:")
result.filter(F.col("is_current") == True) \
      .groupBy("age_group").count() \
      .orderBy("age_group").show()

print("Distribución vehicle_age_group:")
result.filter(F.col("is_current") == True) \
      .groupBy("vehicle_age_group").count() \
      .orderBy("vehicle_age_group").show()

print("Distribución risk_score_static (versiones vigentes):")
result.filter(F.col("is_current") == True) \
      .groupBy("risk_score_static").count() \
      .orderBy("risk_score_static").show()

print("CDF habilitado (tblproperties):")
abs_path = str(Path(output_path).resolve())
spark.sql(f"SHOW TBLPROPERTIES delta.`{abs_path}`") \
     .filter(F.col("key").startswith("delta.enable") |
             F.col("key").startswith("feature_store")) \
     .show(truncate=False)

print("Muestra (5 filas vigentes):")
result.filter(F.col("is_current") == True).select(
    "policy_id", "__START_AT", "__END_AT",
    "age_group", "vehicle_age_group",
    "policy_tenure_days", "risk_score_static",
    "high_mileage_flag",
).show(5, truncate=False)

print("\n=== gold_customer_profile completado ===")