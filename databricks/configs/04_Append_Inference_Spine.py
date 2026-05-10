# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Append 2025 transactions to `fraud_inference_spine`
# MAGIC
# MAGIC Esta libreta es un **paso de inicialización único** que incorpora las transacciones
# MAGIC de 2025 del source buffer a `fraud_inference_spine`, tabla que el pipeline de
# MAGIC inferencia (`09_Inference_And_Label_Enrichment.py`) consulta para obtener las
# MAGIC transacciones pendientes de predicción.
# MAGIC
# MAGIC ### Por qué es necesaria
# MAGIC
# MAGIC La arquitectura medallón se construyó íntegramente en local y solo los datos
# MAGIC de capa gold (hasta 2024-12-31) se subieron manualmente a Databricks.
# MAGIC Las transacciones de 2025 del source buffer nunca pasaron por el pipeline
# MAGIC medallón en Databricks, por lo que `fraud_inference_spine` no las contiene.
# MAGIC Esta libreta cierra ese gap de forma puntual sin reconstruir el pipeline completo.
# MAGIC
# MAGIC ### Mecanismo
# MAGIC
# MAGIC Hace un `MERGE` idempotente sobre `fraud_inference_spine` usando `claim_id`
# MAGIC como clave: inserta únicamente las filas nuevas, nunca sobreescribe existentes.
# MAGIC Puede relanzarse sin riesgo de duplicados.
# MAGIC
# MAGIC ### Ejecución
# MAGIC
# MAGIC Esta libreta se ejecuta **una sola vez** antes de lanzar el pipeline de
# MAGIC inferencia. No forma parte de ningún job programado.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Importaciones y configuración

# COMMAND ----------

exec(open("07_Utils.py").read(), globals())

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

source_path  = f"/Volumes/{catalog}/{database}/raw_uploads/inference_spine_2025"
target_table = f"{catalog}.{database}.fraud_inference_spine"

print(f"Source  : {source_path}")
print(f"Target  : {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Saber el esquema exacto del target

# COMMAND ----------

spark.table("workspace.auto_insurance_fraud.fraud_inference_spine").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Lectura del Parquet

# COMMAND ----------

new_df = (
    spark.read
         .format("parquet")
         .load(source_path)
         .withColumn("year",  F.col("year") .cast("string"))
         .withColumn("month", F.col("month").cast("string"))
)

total_new = new_df.count()
ts_min, ts_max = new_df.agg(
    F.min("claim_timestamp"),
    F.max("claim_timestamp")
).first()

print(f"Rows to merge   : {total_new:,}")
print(f"Timestamp range : {ts_min}  ->  {ts_max}")
new_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verificación previa del target
# MAGIC
# MAGIC Confirma el estado de `fraud_inference_spine` antes del MERGE.

# COMMAND ----------

target_df     = spark.table(target_table)
total_before  = target_df.count()
ts_max_before = target_df.agg(F.max("claim_timestamp")).first()[0]

print(f"Rows before MERGE      : {total_before:,}")
print(f"Max timestamp before   : {ts_max_before}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. MERGE idempotente
# MAGIC
# MAGIC Inserta únicamente las filas cuyo `claim_id` no existe en el target.
# MAGIC Nunca actualiza filas existentes — garantiza idempotencia completa.

# COMMAND ----------

delta_target = DeltaTable.forName(spark, target_table)

(
    delta_target.alias("target")
    .merge(
        new_df.alias("source"),
        "target.claim_id = source.claim_id"
    )
    .whenNotMatchedInsert(values={
        "claim_id"                : "source.claim_id",
        "policy_id"               : "source.policy_id",
        "claim_timestamp"         : "source.claim_timestamp",
        "claimed_amount_eur"      : "source.claimed_amount_eur",
        "accident_type"           : "source.accident_type",
        "accident_location_type"  : "source.accident_location_type",
        "days_to_report"          : "source.days_to_report",
        "n_parties_involved"      : "source.n_parties_involved",
        "witnesses"               : "source.witnesses",
        "injury_level"            : "source.injury_level",
        "police_report_filed"     : "source.police_report_filed",
        "outside_business_hours"  : "source.outside_business_hours",
        "has_third_party_injury"  : "source.has_third_party_injury",
        "third_party_same_insurer": "source.third_party_same_insurer",
        "telematics_anomaly"      : "source.telematics_anomaly",
        "claim_channel"           : "source.claim_channel",
        "body_shop_id"            : "source.body_shop_id",
        "label_available_date"    : "source.label_available_date",
        "year"                    : "source.year",
        "month"                   : "source.month",
        "gold_spine_timestamp"    : "current_timestamp()",
    })
    .execute()
)

print("MERGE completed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verificación posterior

# COMMAND ----------

target_after  = spark.table(target_table)
total_after   = target_after.count()
ts_max_after  = target_after.agg(F.max("claim_timestamp")).first()[0]
rows_2025     = target_after.filter(F.col("year") == "2025").count()
inserted      = total_after - total_before

print("=" * 55)
print("VERIFICATION")
print("=" * 55)
print(f"Rows before MERGE  : {total_before:,}")
print(f"Rows after MERGE   : {total_after:,}")
print(f"Rows inserted      : {inserted:,}")
print(f"Rows 2025          : {rows_2025:,}")
print(f"Max timestamp after: {ts_max_after}")

assert inserted == total_new, (
    f"Expected {total_new:,} insertions, got {inserted:,}. "
    "Check for duplicate claim_ids between source and target."
)

print(f"\n✓ {target_table} ready. Proceed to 09_Inference_And_Label_Enrichment.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Conclusiones y siguientes pasos
# MAGIC
# MAGIC ### ¿Qué ha hecho esta libreta?
# MAGIC
# MAGIC 1. **Leído** el Parquet de `raw_uploads/inference_spine_2025`
# MAGIC    (300.003 transacciones, 2025-01-01 → 2025-06-30).
# MAGIC 2. **Mergeado** las transacciones en `fraud_inference_spine` usando
# MAGIC    `claim_id` como clave, sin tocar los registros existentes (2015-2024).
# MAGIC
# MAGIC ### ¿Qué sigue?
# MAGIC
# MAGIC Ejecutar `09_Inference_And_Label_Enrichment.py` con **Clear state and
# MAGIC all outputs → Run all**. El bloque 4 encontrará 300.003 transacciones
# MAGIC pendientes de predicción y el pipeline completará el ciclo completo.