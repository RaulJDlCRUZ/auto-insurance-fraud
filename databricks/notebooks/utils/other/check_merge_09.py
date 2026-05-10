# Databricks notebook source
# MAGIC %md
# MAGIC # ¿Qué columnas tiene `gold_fraud_inference_enriched`?

# COMMAND ----------

spark.table("workspace.auto_insurance_fraud.gold_fraud_inference_enriched").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # ¿Cómo está escrito el MERGE del bloque 7 en la libreta 09?

# COMMAND ----------

# Verifica que la join key es claim_id en ambas tablas
spark.table("workspace.auto_insurance_fraud.gold_fraud_inference_enriched") \
     .select("claim_id", "is_fraud", "label_available_date") \
     .filter("is_fraud IS NOT NULL") \
     .show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC # ¿`fraud_inference_spine` y `fraud_spine` tienen `claim_id` distintos para 2025?

# COMMAND ----------

enriched_ids = spark.table("workspace.auto_insurance_fraud.gold_fraud_inference_enriched") \
    .select("claim_id")

labeled = spark.table("workspace.auto_insurance_fraud.fraud_spine") \
    .filter("label_available_date >= '2025-01-01'") \
    .select("claim_id", "is_fraud")

overlap = enriched_ids.join(labeled, "claim_id").count()
print(f"Overlap: {overlap:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC Si el overlap es 0 el problema es que `fraud_inference_spine` y `fraud_spine` tienen `claim_id` distintos para 2025 — los de inferencia vienen del _source buffer_ y los de `fraud_spine` son los históricos.