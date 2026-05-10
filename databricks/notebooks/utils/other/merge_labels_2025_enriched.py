# Databricks notebook source
# MAGIC %md
# MAGIC # Añadir columna y hacer `MERGE`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuración

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Añadir la columna si no existe

# COMMAND ----------

spark.sql("""
    ALTER TABLE workspace.auto_insurance_fraud.gold_fraud_inference_enriched
    ADD COLUMN label_available_date TIMESTAMP
""")

print("Column label_available_date added.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Leer etiquetas

# COMMAND ----------

labels = (
    spark.read.format("parquet")
         .load("/Volumes/workspace/auto_insurance_fraud/raw_uploads/labels_2025")
         .withColumn("is_fraud", F.col("is_fraud").cast("int"))
)
print(f"Labels to merge: {labels.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. `MERGE `

# COMMAND ----------

target = DeltaTable.forName(
    spark, "workspace.auto_insurance_fraud.gold_fraud_inference_enriched"
)

(
    target.alias("t")
    .merge(labels.alias("s"), "t.claim_id = s.claim_id")
    .whenMatchedUpdate(set={
        "is_fraud"             : "s.is_fraud",
        "label_available_date" : "s.label_available_date",
    })
    .execute()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verificación

# COMMAND ----------

result = spark.table(
    "workspace.auto_insurance_fraud.gold_fraud_inference_enriched"
)
total        = result.count()
with_label   = result.filter(F.col("is_fraud").isNotNull()).count()
without_label = total - with_label

print(f"\nTotal rows       : {total:,}")
print(f"With label       : {with_label:,}")
print(f"Without label    : {without_label:,}")
result.groupBy("is_fraud").count().orderBy("is_fraud").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Añadir una columna nueva `prediction_int` casteada

# COMMAND ----------

# MAGIC %md
# MAGIC Delta no permite downcast de BIGINT a INT directamente. Si el formulario exige específicamente una columna de predicción binaria que coincida en tipo con el label, la alternativa es añadir una columna nueva prediction_int casteada

# COMMAND ----------

spark.sql("""
    ALTER TABLE workspace.auto_insurance_fraud.gold_fraud_inference_enriched
    ADD COLUMN prediction_int INT
""")

DeltaTable.forName(spark, "workspace.auto_insurance_fraud.gold_fraud_inference_enriched") \
    .update(set={"prediction_int": F.col("prediction").cast("int")})



# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificación

# COMMAND ----------

spark.table("workspace.auto_insurance_fraud.gold_fraud_inference_enriched") \
     .select("prediction", "prediction_int").show(25)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hacer lo mismo con `baseline`

# COMMAND ----------

spark.sql("""
    ALTER TABLE workspace.auto_insurance_fraud.gold_fraud_test_baseline
    ADD COLUMN prediction_int INT
""")

DeltaTable.forName(spark, "workspace.auto_insurance_fraud.gold_fraud_test_baseline") \
    .update(set={"prediction_int": F.col("prediction").cast("int")})

spark.table("workspace.auto_insurance_fraud.gold_fraud_test_baseline") \
     .select("prediction", "prediction_int", "is_fraud").show(5)