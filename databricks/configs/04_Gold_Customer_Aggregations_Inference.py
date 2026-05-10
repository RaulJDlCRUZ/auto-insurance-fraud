# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Gold — Customer Aggregations (Inference)
# MAGIC
# MAGIC Esta libreta registra `customer_aggregations_inference` en Unity Catalog
# MAGIC a partir del Parquet exportado desde local y subido a `raw_uploads`.
# MAGIC
# MAGIC ### Origen de los datos
# MAGIC
# MAGIC El Parquet en `raw_uploads/claims_inference_base` fue generado por
# MAGIC `export_claims_for_inference.py` combinando:
# MAGIC
# MAGIC * `silver/claims_enriched` — 6.000.000 filas, 2015-2024 (+ 3 de 2025)
# MAGIC * `data/source_buffer/claims/2025` — 300.000 filas, 2025
# MAGIC
# MAGIC Las ventanas deslizantes (`num_claims_Xh/d`, `total_amount_Xh/d`, etc.) ya están
# MAGIC precalculadas en el Parquet con la misma lógica `rangeBetween(-X, -1)` que
# MAGIC `03_gold_customer_aggregations.py`, garantizando idéntico contrato de features
# MAGIC con el modelo champion.
# MAGIC
# MAGIC ### Lo que hace esta libreta
# MAGIC
# MAGIC 1. Lee el Parquet particionado desde `raw_uploads`.
# MAGIC 2. Homogeneiza `year`/`month` a `string` (en el Parquet son `integer`).
# MAGIC 3. Escribe como tabla Delta gestionada en Unity Catalog (`saveAsTable`).
# MAGIC 4. Registra la tabla como Feature Table con `claim_id` como primary key
# MAGIC    y `claim_timestamp` como timestamp key, habilitando el `FeatureLookup`
# MAGIC    point-in-time de `09_Inference_And_Label_Enrichment.py`.
# MAGIC
# MAGIC ### Ejecución
# MAGIC
# MAGIC Esta libreta se ejecuta **una sola vez** antes de lanzar el pipeline de
# MAGIC inferencia. No forma parte de ningún job programado.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Importaciones y configuración

# COMMAND ----------

# MAGIC %pip install databricks-feature-engineering>=0.13.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

exec(open("07_Utils.py").read(), globals())

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient
from pyspark.sql import functions as F

# COMMAND ----------

source_path = f"/Volumes/{catalog}/{database}/raw_uploads/claims_inference_base"
dest_table  = f"{catalog}.{database}.customer_aggregations_inference"

print(f"Catalog    : {catalog}")
print(f"Database   : {database}")
print(f"Source     : {source_path}")
print(f"Dest table : {dest_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Guardia de idempotencia
# MAGIC
# MAGIC Si la tabla ya existe, la libreta aborta para evitar sobreescrituras accidentales.
# MAGIC Para reconstruirla ejecuta primero:
# MAGIC ```sql
# MAGIC DROP TABLE IF EXISTS workspace.auto_insurance_fraud.customer_aggregations_inference;
# MAGIC ```

# COMMAND ----------

if spark.catalog.tableExists(dest_table):
    count = spark.table(dest_table).count()
    raise Exception(
        f"Table {dest_table} already exists with {count:,} rows.\n"
        "Drop it manually if you need to rebuild:\n"
        f"  spark.sql('DROP TABLE IF EXISTS {dest_table}')"
    )

print(f"Table {dest_table} does not exist yet. Proceeding.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Lectura del Parquet y homogeneización de tipos
# MAGIC
# MAGIC El script local exportó `year` y `month` como `integer` (inferidos desde las
# MAGIC columnas de partición Hive). Se castean a `string` para ser consistentes con
# MAGIC el resto de tablas gold del proyecto.

# COMMAND ----------

df = (
    spark.read
         .format("parquet")
         .load(source_path)
         .withColumn("year",  F.col("year") .cast("string"))
         .withColumn("month", F.col("month").cast("string"))
)

total     = df.count()
rows_2025 = df.filter(F.col("year") == "2025").count()
ts_min, ts_max = df.agg(F.min("claim_timestamp"), F.max("claim_timestamp")).first()

print(f"Total rows           : {total:,}")
print(f"Inference rows (2025): {rows_2025:,}")
print(f"Timestamp range      : {ts_min}  ->  {ts_max}")
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Escritura como tabla Delta en Unity Catalog

# COMMAND ----------

(
    df.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .partitionBy("year", "month")
      .saveAsTable(dest_table)
)

print(f"Table written: {dest_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Registro como Feature Table
# MAGIC
# MAGIC <!-- Este paso habilita el `FeatureLookup` point-in-time de `09_Utils.py`.
# MAGIC `claim_id` es la primary key y `claim_timestamp` el timestamp key para
# MAGIC el point-in-time join. `policy_id` actua como lookup key en los
# MAGIC FeatureLookup de inferencia ya que es el identificador de cliente
# MAGIC en el dominio de seguros de automoviles. -->
# MAGIC
# MAGIC En Unity Catalog, las tablas Delta gestionadas se registran como feature tables automáticamente al escribirlas. Solo hay que añadir las claves de lookup mediante `ALTER TABLE`.

# COMMAND ----------

spark.sql(f"""
    ALTER TABLE {dest_table}
    SET TBLPROPERTIES (
        'delta.feature.allowColumnDefaults' = 'supported',
        'databricks.feature_store.primary_keys' = 'claim_id',
        'databricks.feature_store.timestamp_keys' = 'claim_timestamp'
    )
""")

print(f"Feature table registered : {dest_table}")
print(f"Primary key              : claim_id")
print(f"Timestamp key            : claim_timestamp")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verificacion

# COMMAND ----------

result    = spark.table(dest_table)
total_out = result.count()
rows_2025 = result.filter(F.col("year") == "2025").count()

print("=" * 60)
print("VERIFICATION")
print("=" * 60)
print(f"Total rows           : {total_out:,}")
print(f"Training rows (<=2024): {total_out - rows_2025:,}")
print(f"Inference rows (2025): {rows_2025:,}")

print("\nNull check (first-claim nulls expected on rolling cols):")
for col in ["num_claims_1h", "num_claims_24h", "num_claims_30d",
            "amount_ratio_24h_vs_30d", "fraud_rate_30d"]:
    nulls = result.filter(F.col(col).isNull()).count()
    print(f"  {col:<30} nulls={nulls:,}")

print("\nSample inference rows (2025, with history):")
(
    result
    .filter((F.col("year") == "2025") & (F.col("num_claims_30d") > 0))
    .select(
        "claim_id", "policy_id", "claim_timestamp",
        "num_claims_30d", "avg_amount_30d",
        "num_fraud_confirmed_30d", "fraud_rate_30d",
    )
    .show(5, truncate=False)
)

print(f"\n✓ {dest_table} ready for inference pipeline.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Conclusiones y siguientes pasos
# MAGIC
# MAGIC ### ¿Qué ha hecho esta libreta?
# MAGIC
# MAGIC 1. **Leído el Parquet** de `raw_uploads/claims_inference_base` (271 MB,
# MAGIC    6.3M filas, 2015-2025).
# MAGIC 2. **Homogeneizado** `year`/`month` de `integer` a `string`.
# MAGIC 3. **Escrito** `customer_aggregations_inference` como tabla Delta gestionada
# MAGIC    en Unity Catalog, particionada por `year/month`.
# MAGIC 4. **Registrado** la tabla como Feature Table con `claim_id` como primary key
# MAGIC    y `claim_timestamp` como timestamp key.
# MAGIC
# MAGIC ### ¿Qué sigue?
# MAGIC
# MAGIC Con la feature table registrada, el siguiente paso es reescribir `09_Utils.py`
# MAGIC para adaptar los `feature_lookups` al dominio de seguros de automoviles:
# MAGIC sustituir `customer_id` por `policy_id` como lookup key, `timestamp` por
# MAGIC `claim_timestamp`, y reemplazar las feature names del proyecto de referencia
# MAGIC (tarjetas de credito) por las columnas reales de `customer_profile` y
# MAGIC `customer_aggregations_inference`.