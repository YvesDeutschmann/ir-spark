# Databricks notebook source
# Phase 3 close-out: duplicate-enriched matching sample from companies_raw.
#
# A uniform 30k slice of this snapshot is almost entity-unique (distinct handles),
# so Splink never sees enough training matches. This notebook pulls records from
# small same-host and same-name+country groups (size 2–10), plus random filler,
# into workspace.ir_spark.companies_match_sample (≤50k). Aggregates only — no
# company-name dumps.
#
# Input:  workspace.ir_spark.companies_raw
# Output: workspace.ir_spark.companies_match_sample

# COMMAND ----------

import re

from pyspark.sql import functions as F

GENERIC_HOST_DENYLIST = (
    "linkedin.com",
    "facebook.com",
    "instagram.com",
    "twitter.com",
    "x.com",
    "youtube.com",
    "bit.ly",
    "linktr.ee",
    "google.com",
    "business.site",
    "negocio.site",
    "indiamart.com",
    "wixsite.com",
    "wordpress.com",
    "yelp.com",
    "weebly.com",
)

TABLE_NAME_PATTERN = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){0,2}$"
)
FORBIDDEN_OUTPUT_SUFFIXES = frozenset(
    {"companies_raw", "companies_sample", "companies_golden", "companies_hierarchy"}
)
MAX_OUTPUT_ROWS = 50_000
SOURCE_COLUMNS = [
    "record_id",
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


def _table_suffix(table_name: str) -> str:
    if not TABLE_NAME_PATTERN.fullmatch(table_name):
        raise ValueError(f"Invalid table identifier: {table_name!r}")
    return table_name.rsplit(".", 1)[-1]


def nullify_blank(column: str):
    expr = F.trim(F.col(column))
    return F.when(expr == "", F.lit(None)).otherwise(expr)


def prepare_keys(df):
    name_expr = F.regexp_replace(F.lower(F.trim(F.col("name"))), r"\s+", " ")
    name_expr = F.when(name_expr == "", F.lit(None)).otherwise(name_expr)
    host_expr = F.regexp_replace(
        F.regexp_replace(F.lower(F.trim(F.col("website"))), r"^https?://", ""),
        r"^www\.",
        "",
    )
    host_expr = F.regexp_extract(host_expr, r"^([^/]+)", 1)
    host_expr = F.when((host_expr == "") | host_expr.isNull(), F.lit(None)).otherwise(
        host_expr
    )
    return (
        df.withColumn("name_norm", name_expr)
        .withColumn("host", host_expr)
        .withColumn("country_norm", F.lower(nullify_blank("country_code")))
    )


# COMMAND ----------

dbutils.widgets.text(
    "source_table", "workspace.ir_spark.companies_raw", "Source Delta table"
)
dbutils.widgets.text(
    "output_table",
    "workspace.ir_spark.companies_match_sample",
    "Enriched matching sample output",
)
dbutils.widgets.text("host_target_rows", "18000", "Rows from host groups size 2-10")
dbutils.widgets.text(
    "name_country_target_rows", "12000", "Rows from name+country groups size 2-10"
)
dbutils.widgets.text("filler_rows", "10000", "Random filler rows")
dbutils.widgets.text("sample_seed", "42", "Random seed")
dbutils.widgets.text("min_group", "2", "Minimum collision group size")
dbutils.widgets.text("max_group", "10", "Maximum collision group size")

source_table = dbutils.widgets.get("source_table").strip()
output_table = dbutils.widgets.get("output_table").strip()
host_target_rows = int(dbutils.widgets.get("host_target_rows"))
name_country_target_rows = int(dbutils.widgets.get("name_country_target_rows"))
filler_rows = int(dbutils.widgets.get("filler_rows"))
sample_seed = int(dbutils.widgets.get("sample_seed"))
min_group = int(dbutils.widgets.get("min_group"))
max_group = int(dbutils.widgets.get("max_group"))

if _table_suffix(source_table) != "companies_raw":
    raise ValueError(f"Refusing source table: {source_table!r}")
if _table_suffix(output_table) in FORBIDDEN_OUTPUT_SUFFIXES:
    raise ValueError(f"Refusing output table: {output_table!r}")
if source_table == output_table:
    raise ValueError("source_table and output_table must differ")
if min_group < 2 or max_group < min_group or max_group > 80:
    raise ValueError("group size window must be 2 <= min <= max <= 80")
if host_target_rows + name_country_target_rows + filler_rows > MAX_OUTPUT_ROWS * 2:
    raise ValueError("targets are too large relative to the 50k matching cap")

catalog = spark.catalog.currentCatalog()
schema_name = output_table.split(".")[-2] if output_table.count(".") == 2 else "ir_spark"

print(f"source_table={source_table}")
print(f"output_table={output_table}")
print(
    f"host_target_rows={host_target_rows} name_country_target_rows={name_country_target_rows} "
    f"filler_rows={filler_rows} seed={sample_seed} group_window={min_group}-{max_group}"
)

# COMMAND ----------

source_df = spark.table(source_table).select(*SOURCE_COLUMNS)
raw_n = source_df.count()
print(f"raw_n={raw_n}")

keyed = prepare_keys(source_df)

host_groups = (
    keyed.filter(
        F.col("host").isNotNull() & ~F.col("host").isin(*GENERIC_HOST_DENYLIST)
    )
    .groupBy("host")
    .count()
    .filter((F.col("count") >= min_group) & (F.col("count") <= max_group))
)
name_country_groups = (
    keyed.filter(F.col("name_norm").isNotNull() & F.col("country_norm").isNotNull())
    .groupBy("name_norm", "country_norm")
    .count()
    .filter((F.col("count") >= min_group) & (F.col("count") <= max_group))
)

n_host_groups = host_groups.count()
n_name_country_groups = name_country_groups.count()
print(f"eligible_host_groups={n_host_groups}")
print(f"eligible_name_country_groups={n_name_country_groups}")

host_group_n = max(1, int(host_target_rows / 2.2))
name_country_group_n = max(1, int(name_country_target_rows / 2.2))
host_group_n = min(host_group_n, n_host_groups)
name_country_group_n = min(name_country_group_n, n_name_country_groups)

host_pick = host_groups.orderBy(F.rand(sample_seed)).limit(host_group_n).select("host")
name_country_pick = (
    name_country_groups.orderBy(F.rand(sample_seed + 1))
    .limit(name_country_group_n)
    .select("name_norm", "country_norm")
)

host_rows = keyed.join(host_pick, "host", "inner").select(*SOURCE_COLUMNS)
name_country_rows = keyed.join(
    name_country_pick, ["name_norm", "country_norm"], "inner"
).select(*SOURCE_COLUMNS)

host_n = host_rows.count()
name_country_n = name_country_rows.count()
print(f"host_collision_rows={host_n} from_groups={host_group_n}")
print(
    f"name_country_collision_rows={name_country_n} from_groups={name_country_group_n}"
)

collision = host_rows.unionByName(name_country_rows).dropDuplicates(["record_id"])
collision_n = collision.count()
print(f"collision_union_rows={collision_n}")

remaining = source_df.join(collision.select("record_id"), "record_id", "left_anti")
remaining_n = remaining.count()
filler_fraction = min(1.0, (filler_rows * 4) / max(remaining_n, 1))
filler = remaining.sample(withReplacement=False, fraction=filler_fraction, seed=sample_seed)
filler = filler.limit(filler_rows).select(*SOURCE_COLUMNS)
filler_n = filler.count()
print(f"filler_rows_written={filler_n} remaining_n={remaining_n} fraction={filler_fraction}")

match_sample = collision.unionByName(filler).dropDuplicates(["record_id"])
sample_n = match_sample.count()
print(f"match_sample_n={sample_n}")
if sample_n > MAX_OUTPUT_ROWS:
    raise ValueError(
        f"Enriched sample has {sample_n} rows (max {MAX_OUTPUT_ROWS}); lower the targets"
    )
if sample_n < 1000:
    raise ValueError(f"Enriched sample too small: {sample_n}")

# COMMAND ----------

check_df = prepare_keys(match_sample)
distinct_ids = match_sample.select("record_id").distinct().count()
distinct_handles = (
    check_df.filter(F.col("handle").isNotNull()).select("handle").distinct().count()
)
handle_nonnull = check_df.filter(F.col("handle").isNotNull()).count()
max_host_group = (
    check_df.filter(
        F.col("host").isNotNull() & ~F.col("host").isin(*GENERIC_HOST_DENYLIST)
    )
    .groupBy("host")
    .count()
    .agg(F.max("count").alias("max_host_group"))
    .collect()[0]["max_host_group"]
)
print(f"distinct_record_id={distinct_ids}")
print(f"handle_nonnull={handle_nonnull} distinct_handle={distinct_handles}")
print(f"max_non_denylisted_host_group={max_host_group}")
if sample_n != distinct_ids:
    raise ValueError(f"record_id must be unique: count={sample_n} distinct={distinct_ids}")
if max_host_group is not None and max_host_group > 80:
    raise ValueError(
        f"Largest non-denylisted host group is {max_host_group} (>80); "
        "tighten GENERIC_HOST_DENYLIST or the group window"
    )

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}")
(
    match_sample.select(*SOURCE_COLUMNS)
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(output_table)
)
written_n = spark.table(output_table).count()
print(f"wrote {output_table} rows={written_n}")

exit_msg = (
    f"ok raw_n={raw_n} sample_n={written_n} host_rows={host_n} "
    f"name_country_rows={name_country_n} collision_union={collision_n} "
    f"filler_n={filler_n} distinct_handle={distinct_handles} "
    f"max_host_group={max_host_group} output_table={output_table}"
)
dbutils.notebook.exit(exit_msg)
