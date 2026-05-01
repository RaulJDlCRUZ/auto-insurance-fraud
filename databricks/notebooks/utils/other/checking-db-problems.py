# Databricks notebook source
print(spark.version)

# COMMAND ----------

from pyspark.ml.feature import Imputer
Imputer()

# COMMAND ----------

# Check which pyspark.ml.connect components are available
import pyspark.ml.connect as mlc
print(dir(mlc))

# COMMAND ----------

# Verify Spark Connect session
print(type(spark))