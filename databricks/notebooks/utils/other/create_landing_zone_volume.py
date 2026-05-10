# Databricks notebook source
# MAGIC %md
# MAGIC # Create `/landing_zone` volume
# MAGIC
# MAGIC Esta libreta sólo sirve para crear el volumen de _landing zone_ para preparar la creación de datos simulados.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Qué `volumes` tenemos ahora

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES IN workspace.auto_insurance_fraud;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creo el volumen nuevo

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.auto_insurance_fraud.landing_zone;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verifico

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES IN workspace.auto_insurance_fraud;