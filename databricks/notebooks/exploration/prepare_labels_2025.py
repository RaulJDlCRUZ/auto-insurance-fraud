# Databricks notebook source
# MAGIC %md
# MAGIC # Descomprimir ficheros que me traigo de local (`labels_2025`)

# COMMAND ----------

from zipfile import ZipFile

volume_path = "/Volumes/workspace/auto_insurance_fraud/raw_uploads"
tables = ["labels_2025"]

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

dbutils.fs.ls("/Volumes/workspace/auto_insurance_fraud/raw_uploads/labels_2025/")

# COMMAND ----------

path = "/Volumes/workspace/auto_insurance_fraud/raw_uploads/labels_2025"

df = spark.read.format("parquet").load(path)
print(f"Filas: {df.count():,}")
df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Como tenemos registros, confirmamos que es **es Parquet**.