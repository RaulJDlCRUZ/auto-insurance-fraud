# Databricks notebook source
# MAGIC %md
# MAGIC # Descomprimir ficheros que me traigo de local (`claims_inference_base`)

# COMMAND ----------

from zipfile import ZipFile

volume_path = "/Volumes/workspace/auto_insurance_fraud/raw_uploads"
tables = ["claims_inference_base"]

for name in tables:
    print(f"Descomprimiendo {name}...")
    with ZipFile(f"{volume_path}/{name}.zip") as z:
        z.extractall(f"{volume_path}")
    print(f"  OK → {volume_path}/{name}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Configuramos el registro de tablas

# COMMAND ----------

spark.sql("SHOW CATALOGS").show()

# COMMAND ----------

spark.sql("SHOW SCHEMAS IN workspace").show()

# COMMAND ----------

dbutils.fs.ls("/Volumes/workspace/auto_insurance_fraud/raw_uploads/claims_inference_base/")

# COMMAND ----------

path = "/Volumes/workspace/auto_insurance_fraud/raw_uploads/claims_inference_base"

df = spark.read.format("parquet").load(path)
print(f"Filas: {df.count():,}")
print(f"Filas 2025: {df.filter(df.year == 2025).count():,}")
df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Como tenemos registros, confirmamos que es **es Parquet**.