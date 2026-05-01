# Databricks notebook source
# MAGIC %md
# MAGIC # Descomprimir ficheros que me traigo de local

# COMMAND ----------

from zipfile import ZipFile

# Example: /Volumes/workspace/auto_insurance_fraud/raw_uploads/customer_aggregations.zip

volume_path = "/Volumes/workspace/auto_insurance_fraud/raw_uploads"
tables = ["customer_aggregations", "customer_profile", 
          "fraud_spine", "fraud_inference_spine"]

for name in tables:
    print(f"Descomprimiendo {name}...")
    with ZipFile(f"{volume_path}/{name}.zip") as z:
        z.extractall(f"{volume_path}/{name}")
    print(f"  OK → {volume_path}/{name}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Sacar un nivel las carpetas

# COMMAND ----------

base_path = "/Volumes/workspace/auto_insurance_fraud/raw_uploads/"

folders = [
    "customer_aggregations",
    "customer_profile",
    "fraud_spine",
    "fraud_inference_spine"
]

for folder in folders:
    parent = base_path + folder
    nested = f"{parent}/{folder}"

    print(f"Procesando: {folder}")

    # comprobar si la carpeta anidada existe (robustez)
    try:
        files = dbutils.fs.ls(nested)
    except:
        print(f" ya parece limpio o no existe carpeta duplicada; saltando")
        continue

    for f in files:
        target = f"{parent}/{f.name}"

        # mover (soporta ficheros y carpetas)
        dbutils.fs.mv(f.path, target, recurse=True)
        print(f" movido: {f.name}")

    # borrar carpeta duplicada
    dbutils.fs.rm(nested, recurse=True)
    print(f" carpeta eliminada: {nested}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Configuramos el registro de tablas

# COMMAND ----------

spark.sql("SHOW CATALOGS").show()

# COMMAND ----------

spark.sql("SHOW SCHEMAS IN workspace").show()

# COMMAND ----------

path = "/Volumes/workspace/auto_insurance_fraud/raw_uploads/customer_aggregations"

spark.read.format("delta").load(path).limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Como tenemos registros, confirmamos que es **es Delta**.

# COMMAND ----------

tables = [
    "customer_aggregations",
    "customer_profile",
    "fraud_spine",
    "fraud_inference_spine"
]

volume_path = "/Volumes/workspace/auto_insurance_fraud/raw_uploads"

for name in tables:
    path = f"{volume_path}/{name}"
    table_name = f"workspace.auto_insurance_fraud.{name}"

    df = spark.read.format("delta").load(path)

    df.write \
      .format("delta") \
      .mode("overwrite") \
      .saveAsTable(table_name)

    count = spark.table(table_name).count()
    print(f"{name}: {count:,} registros")

# COMMAND ----------

spark.sql("SHOW TABLES IN workspace.auto_insurance_fraud").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Dado que tenemos conteos correctos (*6M* en `aggregations` `fraud_spine` e `inference_spine`, *300k* en `customer_profile`), y, además, podemos ver las cuatro tablas listadas, el entorno está listo para modelado. Luego, usaremos cosas como:
# MAGIC
# MAGIC ```py
# MAGIC spine = spark.table("workspace.auto_insurance_fraud.fraud_spine")
# MAGIC ```
# MAGIC
# MAGIC para cargar tablas y empezar modelado.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Esquema de las tablas

# COMMAND ----------

# MAGIC %md
# MAGIC #### `customer_aggregations`

# COMMAND ----------

spark.table("workspace.auto_insurance_fraud.customer_aggregations").printSchema()

# COMMAND ----------

spark.sql("""
DESCRIBE DETAIL workspace.auto_insurance_fraud.customer_aggregations
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### `customer_profile`

# COMMAND ----------

spark.table("workspace.auto_insurance_fraud.customer_profile").printSchema()

# COMMAND ----------

spark.sql("""
DESCRIBE DETAIL workspace.auto_insurance_fraud.customer_profile
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### `fraud_inference_spine`

# COMMAND ----------

spark.table("workspace.auto_insurance_fraud.fraud_inference_spine").printSchema()

# COMMAND ----------

spark.sql("""
DESCRIBE DETAIL workspace.auto_insurance_fraud.fraud_inference_spine
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### `fraud_spine`

# COMMAND ----------

spark.table("workspace.auto_insurance_fraud.fraud_spine").printSchema()

# COMMAND ----------

spark.sql("""
DESCRIBE DETAIL workspace.auto_insurance_fraud.fraud_spine
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Distribución del label

# COMMAND ----------

df = spark.table("workspace.auto_insurance_fraud.fraud_spine")

df.groupBy("is_fraud").count().display()