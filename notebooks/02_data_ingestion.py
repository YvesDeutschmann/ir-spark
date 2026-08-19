# Databricks notebook source
# Phase 2: load the public company snapshot from a UC Volume, write Delta tables,
# and capture an aggregate data-quality summary. No Splink matching here.
#
# Prerequisites (one-time, outside this notebook):
# 1. Create a Volume, e.g. /Volumes/<catalog>/default/ir_spark
# 2. Upload the full gzip (~600 MiB) from local data/raw/:
#    databricks fs cp data/raw/companies-2023-q4-sm.csv.gz \
#      dbfs:/Volumes/<catalog>/default/ir_spark/companies-2023-q4-sm.csv.gz \
#      --profile yves.deutschmann
# Do NOT use workspace files (500 MB cap) or DBFS root (disabled on Free Edition).

# COMMAND ----------

dbutils.widgets.text(
    "gzip_path",
    "/Volumes/workspace/default/ir_spark/companies-2023-q4-sm.csv.gz",
    "Path to companies gzip on a UC Volume",
)
dbutils.widgets.text("schema_name", "ir_spark", "Target schema in the current catalog")
dbutils.widgets.text("sample_n", "30000", "Sample size for companies_sample")
dbutils.widgets.text("sample_seed", "42", "Random seed for companies_sample")

gzip_path = dbutils.widgets.get("gzip_path")
schema_name = dbutils.widgets.get("schema_name")
sample_n = int(dbutils.widgets.get("sample_n"))
sample_seed = int(dbutils.widgets.get("sample_seed"))

catalog = spark.catalog.currentCatalog()
raw_table = f"{catalog}.{schema_name}.companies_raw"
sample_table = f"{catalog}.{schema_name}.companies_sample"

print(f"catalog={catalog}")
print(f"gzip_path={gzip_path}")
print(f"raw_table={raw_table}")
print(f"sample_table={sample_table}")

# COMMAND ----------

from pyspark.sql import functions as F

source_columns = [
    "handle",
    "name",
    "website",
    "industry",
    "size",
    "type",
    "founded",
    "city",
    "state",
    "country_code",
]

df = (
    spark.read.option("header", True)
    .option("inferSchema", False)
    .csv(gzip_path)
    .select(*source_columns)
)

# monotonically_increasing_id is unique across partitions without a 17M-row global sort.
# Pad to 12 digits so ids stay sortable strings (6 digits would overflow past 1M rows).
df = df.withColumn(
    "record_id",
    F.concat(F.lit("r"), F.lpad(F.monotonically_increasing_id().cast("string"), 12, "0")),
)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}")

full_count = df.count()
print(f"full_count={full_count}")

raw_written = False
try:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(raw_table)
    )
    raw_written = True
    print(f"wrote {raw_table}")
except Exception as exc:  # noqa: BLE001 - notebook fallback path
    print(f"companies_raw write skipped: {type(exc).__name__}: {exc}")

# COMMAND ----------

source_df = spark.table(raw_table) if raw_written else df

sampled = source_df.orderBy(F.rand(sample_seed)).limit(sample_n)

(
    sampled.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(sample_table)
)
sample_count = spark.table(sample_table).count()
print(f"wrote {sample_table} rows={sample_count}")

# COMMAND ----------

null_check_columns = ["name", "website", "handle", "city", "state", "country_code"]
profile_df = spark.table(raw_table) if raw_written else df

null_rates = {}
for column in null_check_columns:
    null_rates[column] = profile_df.filter(
        F.col(column).isNull() | (F.trim(F.col(column)) == "")
    ).count() / full_count

host_expr = F.regexp_replace(
    F.regexp_replace(F.lower(F.trim(F.col("website"))), r"^https?://", ""),
    r"^www\.",
    "",
)
host_expr = F.regexp_extract(host_expr, r"^([^/]+)", 1)
host_expr = F.when((host_expr == "") | host_expr.isNull(), F.lit(None)).otherwise(host_expr)

collision_df = spark.table(sample_table)
host_groups_ge_2 = (
    collision_df.withColumn("host", host_expr)
    .filter(F.col("host").isNotNull())
    .groupBy("host")
    .count()
    .filter(F.col("count") >= 2)
    .count()
)

name_norm = F.trim(
    F.regexp_replace(F.lower(F.trim(F.col("name"))), r"\s+", " ")
)
name_groups_ge_2 = (
    collision_df.withColumn("name_norm", name_norm)
    .filter(F.col("name_norm").isNotNull() & (F.col("name_norm") != ""))
    .groupBy("name_norm")
    .count()
    .filter(F.col("count") >= 2)
    .count()
)

print("null_rates", null_rates)
print(f"same_host_collision_groups_ge_2_on_sample={host_groups_ge_2}")
print(f"normalized_name_collision_groups_ge_2_on_sample={name_groups_ge_2}")

# COMMAND ----------

exit_msg = (
    f"ok catalog={catalog} full_count={full_count} sample_count={sample_count} "
    f"raw_table={raw_table if raw_written else 'skipped'} sample_table={sample_table}"
)
dbutils.notebook.exit(exit_msg)
