# Databricks notebook source
# MAGIC %pip install databricks-feature-engineering>=0.13.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient
fe = FeatureEngineeringClient()