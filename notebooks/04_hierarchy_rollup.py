# Databricks notebook source
# Phase 4: ultimate-parent rollup on golden entities.
#
# Parent rules (original to this repo; not a production hierarchy model):
# 1. Shared non-denylisted host — goldens with the same registrable host
#    (full host after stripping scheme/www; no public-suffix list on this PoC)
#    share an ultimate parent.
# 2. Subsidiary name prefix — if two goldens share country, the shorter name is
#    at least 8 characters, and the longer name starts with that name plus a
#    space, the longer golden rolls under the shorter one. Host grouping wins
#    when both apply. One hop only; not a full connected-components walk.
#
# Input:  companies_golden + the matching source (companies_match_sample)
# Output: workspace.ir_spark.companies_hierarchy
#         (record_id, golden_entity_id, ultimate_parent_id)

# COMMAND ----------

import hashlib
import re

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

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
FORBIDDEN_SOURCE_SUFFIXES = frozenset(
    {"companies_raw", "companies_golden", "companies_hierarchy"}
)
FORBIDDEN_GOLDEN_SUFFIXES = frozenset(
    {"companies_raw", "companies_sample", "companies_match_sample"}
)
FORBIDDEN_OUTPUT_SUFFIXES = frozenset(
    {"companies_raw", "companies_sample", "companies_match_sample", "companies_golden"}
)
MIN_PARENT_NAME_LEN = 8


def _table_suffix(table_name: str) -> str:
    if not TABLE_NAME_PATTERN.fullmatch(table_name):
        raise ValueError(f"Invalid table identifier: {table_name!r}")
    return table_name.rsplit(".", 1)[-1]


def nullify_blank(column: str):
    expr = F.trim(F.col(column))
    return F.when(expr == "", F.lit(None)).otherwise(expr)


def masked_hash(value) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()[:8]
    return f"hash:{digest}"


masked_hash_udf = F.udf(masked_hash, StringType())


def prepare_keys(df: DataFrame) -> DataFrame:
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
    "source_table",
    "workspace.ir_spark.companies_match_sample",
    "Matching source (attributes)",
)
dbutils.widgets.text(
    "golden_table",
    "workspace.ir_spark.companies_golden",
    "Golden entity ID table",
)
dbutils.widgets.text(
    "hierarchy_table",
    "workspace.ir_spark.companies_hierarchy",
    "Hierarchy output table",
)
dbutils.widgets.dropdown("unmask_review", "false", ["false", "true"], "Show unmasked review rows")

source_table = dbutils.widgets.get("source_table").strip()
golden_table = dbutils.widgets.get("golden_table").strip()
hierarchy_table = dbutils.widgets.get("hierarchy_table").strip()
unmask_review = dbutils.widgets.get("unmask_review").strip().lower() == "true"

if _table_suffix(source_table) in FORBIDDEN_SOURCE_SUFFIXES:
    raise ValueError(f"Refusing source table: {source_table!r}")
if _table_suffix(golden_table) in FORBIDDEN_GOLDEN_SUFFIXES:
    raise ValueError(f"Refusing golden table: {golden_table!r}")
if _table_suffix(hierarchy_table) in FORBIDDEN_OUTPUT_SUFFIXES:
    raise ValueError(f"Refusing hierarchy output: {hierarchy_table!r}")
if len({source_table, golden_table, hierarchy_table}) < 3:
    raise ValueError("source_table, golden_table, and hierarchy_table must differ")

catalog = spark.catalog.currentCatalog()
schema_name = (
    hierarchy_table.split(".")[-2] if hierarchy_table.count(".") == 2 else "ir_spark"
)
print(f"source_table={source_table}")
print(f"golden_table={golden_table}")
print(f"hierarchy_table={hierarchy_table}")

# COMMAND ----------

source_df = spark.table(source_table)
golden_df = spark.table(golden_table)
source_n = source_df.count()
golden_n = golden_df.count()
if source_n != golden_n:
    raise ValueError(
        f"source and golden row counts differ: source={source_n} golden={golden_n}"
    )
if golden_df.select("record_id").distinct().count() != golden_n:
    raise ValueError("golden record_id must be unique")

joined = golden_df.join(
    prepare_keys(source_df.select("record_id", "name", "website", "country_code")),
    "record_id",
    "inner",
)
joined_n = joined.count()
if joined_n != golden_n:
    raise ValueError(
        f"golden/source join dropped rows: joined={joined_n} golden={golden_n}"
    )

rep_window = Window.partitionBy("golden_entity_id").orderBy("record_id")
reps = (
    joined.withColumn("rn", F.row_number().over(rep_window))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .select(
        "golden_entity_id",
        "record_id",
        "name_norm",
        "host",
        "country_norm",
    )
)
golden_entity_n = reps.count()
print(f"record_n={golden_n} golden_entity_n={golden_entity_n}")

# COMMAND ----------

host_eligible = F.col("host").isNotNull() & ~F.col("host").isin(*GENERIC_HOST_DENYLIST)
host_parent = reps.withColumn(
    "host_parent_id",
    F.when(
        host_eligible,
        F.concat(F.lit("p"), F.substring(F.sha2(F.col("host"), 256), 1, 16)),
    ),
).withColumn("name_first3", F.substring(F.col("name_norm"), 1, 3))

child = host_parent.alias("c")
parent = host_parent.alias("p")
name_links = (
    child.join(
        parent,
        (F.col("c.country_norm").isNotNull())
        & (F.col("c.country_norm") == F.col("p.country_norm"))
        & (F.col("c.name_first3") == F.col("p.name_first3"))
        & (F.col("c.golden_entity_id") != F.col("p.golden_entity_id"))
        & (F.length(F.col("p.name_norm")) >= MIN_PARENT_NAME_LEN)
        & F.col("c.name_norm").startswith(F.concat(F.col("p.name_norm"), F.lit(" "))),
        "inner",
    )
    .filter(F.col("c.host_parent_id").isNull())
    .select(
        F.col("c.golden_entity_id").alias("child_golden_id"),
        F.col("p.golden_entity_id").alias("name_parent_id"),
        F.length(F.col("p.name_norm")).alias("parent_name_len"),
    )
)
name_parent_window = Window.partitionBy("child_golden_id").orderBy("parent_name_len")
name_parent = (
    name_links.withColumn("rn", F.row_number().over(name_parent_window))
    .filter(F.col("rn") == 1)
    .select("child_golden_id", "name_parent_id")
)
name_link_n = name_parent.count()
print(f"name_prefix_child_goldens={name_link_n}")

rolled = (
    host_parent.alias("hp")
    .join(
        name_parent.alias("np"),
        F.col("hp.golden_entity_id") == F.col("np.child_golden_id"),
        "left",
    )
    .withColumn(
        "ultimate_parent_id",
        F.coalesce(
            F.col("hp.host_parent_id"),
            F.col("np.name_parent_id"),
            F.col("hp.golden_entity_id"),
        ),
    )
    .select(
        F.col("hp.golden_entity_id").alias("golden_entity_id"),
        F.col("ultimate_parent_id"),
        F.col("hp.name_norm").alias("name_norm"),
        F.col("hp.host").alias("host"),
        F.col("hp.country_norm").alias("country_norm"),
    )
)

hierarchy_df = (
    golden_df.alias("g")
    .join(
        rolled.alias("r"),
        F.col("g.golden_entity_id") == F.col("r.golden_entity_id"),
        "left",
    )
    .select(
        F.col("g.record_id").alias("record_id"),
        F.col("g.golden_entity_id").alias("golden_entity_id"),
        F.col("r.ultimate_parent_id").alias("ultimate_parent_id"),
    )
)

if hierarchy_df.filter(F.col("ultimate_parent_id").isNull()).limit(1).count():
    raise ValueError("ultimate_parent_id contains nulls")

parent_n = hierarchy_df.select("ultimate_parent_id").distinct().count()
record_n = hierarchy_df.count()
print(f"record_n={record_n} golden_n={golden_entity_n} ultimate_parent_n={parent_n}")
if record_n > 0:
    golden_reduction = 1.0 - (golden_entity_n / record_n)
    parent_reduction = 1.0 - (parent_n / golden_entity_n) if golden_entity_n else 0.0
    overall_reduction = 1.0 - (parent_n / record_n)
    print(f"reduction_records_to_golden={golden_reduction:.4f}")
    print(f"reduction_golden_to_parent={parent_reduction:.4f}")
    print(f"reduction_records_to_parent={overall_reduction:.4f}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}")
(
    hierarchy_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(hierarchy_table)
)
print(f"wrote {hierarchy_table} rows={spark.table(hierarchy_table).count()}")

# COMMAND ----------

review_base = (
    rolled.withColumn("parent_size", F.count("*").over(Window.partitionBy("ultimate_parent_id")))
    .filter(F.col("parent_size") >= 2)
    .withColumn("name_hash", masked_hash_udf(F.col("name_norm")))
    .withColumn("host_hash", masked_hash_udf(F.col("host")))
)
multi_parent_golden_n = review_base.count()
print(f"goldens_in_multi_parent_groups={multi_parent_golden_n}")

review_cols = [
    "ultimate_parent_id",
    "parent_size",
    "golden_entity_id",
    "country_norm",
]
if unmask_review:
    display(
        review_base.orderBy(F.col("parent_size").desc(), "ultimate_parent_id", "golden_entity_id")
        .select(*review_cols, "name_norm", "host")
        .limit(50)
    )
else:
    display(
        review_base.orderBy(F.col("parent_size").desc(), "ultimate_parent_id", "golden_entity_id")
        .select(*review_cols, "name_hash", "host_hash")
        .limit(50)
    )

exit_msg = (
    f"ok record_n={record_n} golden_n={golden_entity_n} parent_n={parent_n} "
    f"name_prefix_links={name_link_n} multi_parent_goldens={multi_parent_golden_n} "
    f"source_table={source_table} golden_table={golden_table} "
    f"hierarchy_table={hierarchy_table}"
)
dbutils.notebook.exit(exit_msg)
