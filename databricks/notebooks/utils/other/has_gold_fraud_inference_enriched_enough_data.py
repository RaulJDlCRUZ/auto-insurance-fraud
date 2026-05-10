# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

df = spark.table("workspace.auto_insurance_fraud.gold_fraud_inference_enriched")

print(f"Total rows: {df.count():,}")

# COMMAND ----------

df.agg(
    F.min("inference_timestamp"),
    F.max("inference_timestamp"),
    F.countDistinct("model_version"),
    F.sum(F.when(F.col("is_fraud").isNotNull(), 1).otherwise(0)).alias("rows_with_label"),
    F.sum(F.when(F.col("is_fraud").isNull(), 1).otherwise(0)).alias("rows_without_label"),
).show(truncate=False)