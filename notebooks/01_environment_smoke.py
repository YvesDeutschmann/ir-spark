# Databricks notebook source
# Phase 1 only: confirm Spark and Splink's Spark backend import on Free Edition.
# Do not load the company dataset here (that is Phase 2).
#
# Free Edition default compute is serverless Spark Connect: no driver JVM, so Splink
# cannot attach its similarity JAR via sparkContext. Init skips that hook.
# Locally, use `uv run ir-spark-smoke` instead (it adds spark.jars).

# COMMAND ----------

# MAGIC %pip install splink

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

print(spark.version)
spark.range(10).show()
assert spark.range(10).count() == 10

# COMMAND ----------

import os

import splink
from splink import SparkAPI

# Serverless Spark Connect: shuffle partitions may be the string "auto", and
# enable_splink() / JAR UDFs need a driver JVM. Skip those hooks for Phase 1.
dbr_version = os.environ.pop("DATABRICKS_RUNTIME_VERSION", None)
try:
    db_api = SparkAPI(
        spark_session=spark,
        num_partitions_on_repartition=2,
        register_udfs_automatically=False,
        break_lineage_method="persist",
    )
finally:
    if dbr_version is not None:
        os.environ["DATABRICKS_RUNTIME_VERSION"] = dbr_version

print(f"splink={splink.__version__}")
print(type(db_api).__name__)

# COMMAND ----------

dbutils.notebook.exit(
    f"ok spark={spark.version} range10={spark.range(10).count()} "
    f"splink={splink.__version__} SparkAPI={type(db_api).__name__}"
)
