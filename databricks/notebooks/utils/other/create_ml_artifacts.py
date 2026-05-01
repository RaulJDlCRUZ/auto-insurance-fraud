# Databricks notebook source
# MAGIC %md
# MAGIC # Comprobamos y creamos volumen
# MAGIC
# MAGIC > Lo hacemos para sustituir el planteamiento orientado por clústeres

# COMMAND ----------

spark.sql("""
SHOW VOLUMES IN workspace.auto_insurance_fraud
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Ahora mismo tenemos el volumen donde tenemos los datos de la capa gold subidos _tal cual_, directamente desde local, descomprimidos de un zip. Hay que crear otro volumen `ml_artifacts`.

# COMMAND ----------

spark.sql("""
CREATE VOLUME workspace.auto_insurance_fraud.ml_artifacts
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Ahora vamos a probar a poner un txt:

# COMMAND ----------

dbutils.fs.put("/Volumes/workspace/auto_insurance_fraud/ml_artifacts/test.txt", "ok", True)