# Databricks notebook source
# MAGIC %md
# MAGIC # ¿Qué métricas de rendimiento hay en profile_metrics?

# COMMAND ----------

df = spark.table("workspace.auto_insurance_fraud.gold_fraud_inference_enriched_profile_metrics")
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # ¿Qué columnas relacionadas con F1 o clasificación hay?

# COMMAND ----------

display(
    df.select([c for c in df.columns if any(x in c.lower() for x in ["f1", "precision", "recall", "accuracy", "window"])])
    .limit(5)
)

# COMMAND ----------

# MAGIC %md
# MAGIC El F1 de la clase fraude (`is_fraud = 1`) está en `f1_score.one_vs_all["1"]` y vale **0.7405 en producción**, frente al baseline de test de 0.8070 (AUC-PR)

# COMMAND ----------

# MAGIC %md
# MAGIC # Calcular F1 de la clase fraude manualmente desde las predicciones del baseline

# COMMAND ----------

from pyspark.sql import functions as F

baseline = spark.table("workspace.auto_insurance_fraud.gold_fraud_test_baseline")

tp = baseline.filter("prediction_int = 1 AND is_fraud = 1").count()
fp = baseline.filter("prediction_int = 1 AND is_fraud = 0").count()
fn = baseline.filter("prediction_int = 0 AND is_fraud = 1").count()

precision = tp / (tp + fp)
recall    = tp / (tp + fn)
f1        = 2 * precision * recall / (precision + recall)

print(f"TP: {tp:,}  FP: {fp:,}  FN: {fn:,}")
print(f"Precision : {precision:.4f}")
print(f"Recall    : {recall:.4f}")
print(f"F1 fraude : {f1:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC `F1 baseline = 0.7308`. Con el umbral de `−0.05` del enunciado, la **alerta se dispara** cuando el F1 de producción cae por debajo de `0.6808`.