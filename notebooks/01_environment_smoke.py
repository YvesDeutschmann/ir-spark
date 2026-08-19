# Databricks notebook source
# Phase 1 only: confirm Spark and Splink's Spark backend import on Free Edition.
# Do not load the company dataset here (that is Phase 2).
#
# On Databricks, Splink registers its similarity JAR when DATABRICKS_RUNTIME_VERSION
# is set. Locally, use `uv run ir-spark-smoke` instead (it adds spark.jars).

# COMMAND ----------

# MAGIC %pip install splink

# COMMAND ----------

spark.range(10).show()

# COMMAND ----------

from splink import SparkAPI

db_api = SparkAPI(spark_session=spark)
print(type(db_api).__name__)
