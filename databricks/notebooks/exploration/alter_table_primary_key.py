# Databricks notebook source
# MAGIC %md
# MAGIC # Agregar PKs
# MAGIC
# MAGIC El Feature Store de Databricks requiere que las tablas de características tengan una primary key constraint definida a nivel de metadatos en Unity Catalog — no basta con que la columna exista, hay que declararla formalmente. Las tablas se crearon en local y se subieron como Parquet/Delta sin pasar por la API del Feature Store, así que esa constraint nunca se registró.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Las columnas de la PK deben ser NOT NULL

# COMMAND ----------

spark.sql("ALTER TABLE workspace.auto_insurance_fraud.customer_profile ALTER COLUMN policy_id SET NOT NULL")
spark.sql("ALTER TABLE workspace.auto_insurance_fraud.customer_profile ALTER COLUMN __START_AT SET NOT NULL")

# COMMAND ----------

spark.sql("ALTER TABLE workspace.auto_insurance_fraud.customer_aggregations ALTER COLUMN policy_id SET NOT NULL")
spark.sql("ALTER TABLE workspace.auto_insurance_fraud.customer_aggregations ALTER COLUMN claim_timestamp SET NOT NULL")

# COMMAND ----------

# MAGIC %md
# MAGIC Ahora sí, PKs **con `TIMESERIES` dentro del paréntesis**:

# COMMAND ----------

# MAGIC %md
# MAGIC ## Primary key de `customer_profile`
# MAGIC
# MAGIC `policy_id + __START_AT` (SCD Type 2 — clave compuesta)

# COMMAND ----------

spark.sql("""
    ALTER TABLE workspace.auto_insurance_fraud.customer_profile
    ADD CONSTRAINT customer_profile_pk PRIMARY KEY (policy_id, __START_AT TIMESERIES)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Primary key de `customer_aggregations`
# MAGIC `claim_id` es único por fila

# COMMAND ----------

# Primary key de customer_aggregations: claim_id es único por fila
spark.sql("""
    ALTER TABLE workspace.auto_insurance_fraud.customer_aggregations
    ADD CONSTRAINT customer_aggregations_pk PRIMARY KEY (policy_id, claim_timestamp TIMESERIES)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ## Primary key de `customer_aggregations_inference`

# COMMAND ----------

spark.sql("ALTER TABLE workspace.auto_insurance_fraud.customer_aggregations_inference ALTER COLUMN claim_id SET NOT NULL")

# COMMAND ----------

spark.sql("""
    ALTER TABLE workspace.auto_insurance_fraud.customer_aggregations_inference
    ALTER COLUMN claim_timestamp SET NOT NULL
""")

# COMMAND ----------

spark.sql("""
    ALTER TABLE workspace.auto_insurance_fraud.customer_aggregations_inference
    ADD CONSTRAINT customer_aggregations_inference_pk
    PRIMARY KEY (claim_id, claim_timestamp TIMESERIES)
""")