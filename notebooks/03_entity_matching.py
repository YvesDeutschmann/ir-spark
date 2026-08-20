# Databricks notebook source
# Phase 3: Splink dedupe on the duplicate-enriched company sample → golden entity IDs.
#
# Mirrors src/ir_spark/matching.py (canonical settings; pasted inline because this
# runtime cannot import the uv package).
#
# Free Edition serverless is Spark Connect — no driver JVM — so Splink cannot
# register its similarity JAR. Comparisons use native Spark SQL levenshtein()
# instead of JAR-accelerated Jaro-Winkler (sacrifices some typo nuance, avoids
# the platform dependency). Serverless also rejects DataFrame.persist(); Splink
# breaks lineage with delta_lake_table writes in workspace.ir_spark.
#
# Input:  workspace.ir_spark.companies_match_sample  (duplicate-enriched, ≤50k)
#         Random companies_sample is the sparsity baseline, not the matching eval set.
# Output: workspace.ir_spark.companies_golden   (record_id, golden_entity_id)

# COMMAND ----------

# MAGIC %pip install splink==4.0.16

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import hashlib
import os
import re

import splink.comparison_level_library as cll
import splink.comparison_library as cl
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from splink import Linker, SettingsCreator, SparkAPI, block_on
from splink.blocking_analysis import count_comparisons_from_blocking_rule

# --- Constants (keep in sync with src/ir_spark/matching.py) ---

MAX_SOURCE_ROWS = 50_000
MAX_PAIR_COUNT = 2_000_000
EM_CONVERGENCE = 0.01
LAMBDA_RECALL = 0.6

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

BANNED_SQL_FRAGMENTS = (
    "jaro_winkler",
    "jaro_winkler_sim",
    "jaro_sim",
    "jaro(",
    "damerau",
    "damerau_levenshtein",
)

TABLE_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){0,2}$")
FORBIDDEN_SOURCE_SUFFIXES = frozenset(
    {"companies_raw", "companies_golden", "companies_hierarchy"}
)
FORBIDDEN_GOLDEN_SUFFIXES = frozenset(
    {"companies_raw", "companies_sample", "companies_match_sample"}
)

prepped_df: DataFrame | None = None
training_mode = "em"


def _denylist_sql_literal(hosts: tuple[str, ...] = GENERIC_HOST_DENYLIST) -> str:
    return ", ".join(f"'{host}'" for host in hosts)


def host_blocking_rule_sql(hosts: tuple[str, ...] = GENERIC_HOST_DENYLIST) -> str:
    denied = _denylist_sql_literal(hosts)
    return (
        "l.host = r.host "
        "AND l.host IS NOT NULL "
        f"AND l.host NOT IN ({denied})"
    )


def name_comparison() -> cl.CustomComparison:
    levels = [
        cll.NullLevel("name_norm"),
        cll.ExactMatchLevel("name_norm"),
        cll.CustomLevel(
            "levenshtein(name_norm_l, name_norm_r) <= 1 "
            "AND least(length(name_norm_l), length(name_norm_r)) >= 6",
            label_for_charts="Levenshtein <= 1 (len>=6)",
        ),
        cll.CustomLevel(
            "levenshtein(name_norm_l, name_norm_r) <= 2 "
            "AND least(length(name_norm_l), length(name_norm_r)) >= 8",
            label_for_charts="Levenshtein <= 2 (len>=8)",
        ),
        cll.ElseLevel(),
    ]
    return cl.CustomComparison(output_column_name="name_norm", comparison_levels=levels)


def blocking_rules_for_prediction(name_prefix_col: str = "name_first3") -> list:
    return [
        host_blocking_rule_sql(),
        block_on(name_prefix_col, "country_norm"),
    ]


def deterministic_training_rules() -> list[str]:
    denied = _denylist_sql_literal()
    host_and_name = (
        "l.host = r.host "
        "AND l.host IS NOT NULL "
        f"AND l.host NOT IN ({denied}) "
        "AND levenshtein(l.name_norm, r.name_norm) <= 2 "
        "AND least(length(l.name_norm), length(r.name_norm)) >= 6"
    )
    exact_name_country = (
        "l.name_norm = r.name_norm "
        "AND l.country_norm = r.country_norm "
        "AND l.name_norm IS NOT NULL "
        "AND l.country_norm IS NOT NULL"
    )
    return [host_and_name, exact_name_country]


def em_training_blocking_rules() -> list[str]:
    return [
        host_blocking_rule_sql(),
        "l.name_norm = r.name_norm AND l.country_norm = r.country_norm",
    ]


def build_settings(name_prefix_col: str = "name_first3") -> SettingsCreator:
    settings = SettingsCreator(
        link_type="dedupe_only",
        unique_id_column_name="record_id",
        comparisons=[
            name_comparison(),
            cl.ExactMatch("host"),
            cl.ExactMatch("country_norm"),
            cl.ExactMatch("city_norm"),
        ],
        blocking_rules_to_generate_predictions=blocking_rules_for_prediction(name_prefix_col),
        retain_intermediate_calculation_columns=False,
        em_convergence=EM_CONVERGENCE,
    )
    assert_native_spark_comparisons(settings)
    return settings


def assert_native_spark_comparisons(settings: SettingsCreator) -> None:
    for comparison in settings._as_creator_dict()["comparisons"]:
        comparison_name = type(comparison).__name__
        if comparison_name in {"NameComparison", "EmailComparison"}:
            raise ValueError(f"Banned comparison type for serverless Spark: {comparison_name}")
        for level in comparison.create_comparison_levels():
            sql = level.get_comparison_level("spark").sql_condition.lower()
            for fragment in BANNED_SQL_FRAGMENTS:
                if fragment in sql:
                    raise ValueError(
                        f"Comparison SQL contains banned fragment {fragment!r}: {sql!r}"
                    )


def _table_suffix(table_name: str) -> str:
    if not TABLE_NAME_PATTERN.fullmatch(table_name):
        raise ValueError(f"Invalid table identifier: {table_name!r}")
    return table_name.rsplit(".", 1)[-1]


def assert_distinct_source_and_golden(source_table: str, golden_table: str) -> None:
    source_suffix = _table_suffix(source_table)
    golden_suffix = _table_suffix(golden_table)
    if source_suffix in FORBIDDEN_SOURCE_SUFFIXES:
        raise ValueError(f"Refusing source table: {source_table!r}")
    if golden_suffix in FORBIDDEN_GOLDEN_SUFFIXES:
        raise ValueError(f"Refusing golden output table: {golden_table!r}")
    if source_table == golden_table:
        raise ValueError("source_table and golden_table must differ")


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


masked_hash_udf = F.udf(masked_hash)

# COMMAND ----------

dbutils.widgets.text(
    "source_table",
    "workspace.ir_spark.companies_match_sample",
    "Source Delta table",
)
dbutils.widgets.text("golden_table", "workspace.ir_spark.companies_golden", "Golden entity ID output table")
dbutils.widgets.text("match_threshold", "0.9", "Cluster threshold (match_probability)")
dbutils.widgets.dropdown("unmask_review", "false", ["false", "true"], "Show unmasked review rows")
dbutils.widgets.text("name_prefix_col", "name_first3", "Name prefix column for blocking")

source_table = dbutils.widgets.get("source_table").strip()
golden_table = dbutils.widgets.get("golden_table").strip()
match_threshold = float(dbutils.widgets.get("match_threshold"))
unmask_review = dbutils.widgets.get("unmask_review").strip().lower() == "true"
name_prefix_col = dbutils.widgets.get("name_prefix_col").strip()

assert_distinct_source_and_golden(source_table, golden_table)

catalog = spark.catalog.currentCatalog()
schema_name = golden_table.split(".")[-2] if golden_table.count(".") == 2 else "ir_spark"

print(f"source_table={source_table}")
print(f"golden_table={golden_table}")
print(f"match_threshold={match_threshold}")
print(f"name_prefix_col={name_prefix_col}")

# COMMAND ----------

def prepare_matching_frame(df: DataFrame, prefix_col: str) -> DataFrame:
    name_expr = F.regexp_replace(F.lower(F.trim(F.col("name"))), r"\s+", " ")
    name_expr = F.when(name_expr == "", F.lit(None)).otherwise(name_expr)

    host_expr = F.regexp_replace(
        F.regexp_replace(F.lower(F.trim(F.col("website"))), r"^https?://", ""),
        r"^www\.",
        "",
    )
    host_expr = F.regexp_extract(host_expr, r"^([^/]+)", 1)
    host_expr = F.when((host_expr == "") | host_expr.isNull(), F.lit(None)).otherwise(host_expr)

    country_expr = F.lower(nullify_blank("country_code"))
    city_expr = F.lower(nullify_blank("city"))

    prefix_len = int(re.sub(r"^name_first", "", prefix_col) or "3")
    return (
        df.select("record_id", "name", "website", "city", "country_code")
        .withColumn("name_norm", name_expr)
        .withColumn("host", host_expr)
        .withColumn("country_norm", country_expr)
        .withColumn("city_norm", city_expr)
        .withColumn(prefix_col, F.substring(F.col("name_norm"), 1, prefix_len))
    )


source_df = spark.table(source_table)
sample_n = source_df.count()
distinct_ids = source_df.select("record_id").distinct().count()

if sample_n > MAX_SOURCE_ROWS:
    raise ValueError(f"Refusing source with {sample_n} rows (max {MAX_SOURCE_ROWS})")
if sample_n != distinct_ids:
    raise ValueError(f"record_id must be unique: count={sample_n} distinct={distinct_ids}")
if source_df.filter(F.col("record_id").isNull()).limit(1).count():
    raise ValueError("record_id contains nulls")

# Serverless rejects persist()/PERSIST TABLE; 30k prep is cheap to recompute.
prepped_df = prepare_matching_frame(source_df, name_prefix_col)
prepped_n = prepped_df.count()
print(f"prepped_n={prepped_n}")

# Host group sanity check (denylist applied at blocking time).
host_group_sizes = (
    prepped_df.filter(F.col("host").isNotNull() & ~F.col("host").isin(*GENERIC_HOST_DENYLIST))
    .groupBy("host")
    .count()
    .agg(F.max("count").alias("max_host_group"))
    .collect()[0]["max_host_group"]
)
print(f"max_non_denylisted_host_group={host_group_sizes}")
if host_group_sizes is not None and host_group_sizes > 80:
    raise ValueError(
        f"Largest non-denylisted host group is {host_group_sizes} (>80); "
        "tighten GENERIC_HOST_DENYLIST before predicting"
    )

# COMMAND ----------

settings = build_settings(name_prefix_col)
assert_native_spark_comparisons(settings)

dbr_version = os.environ.pop("DATABRICKS_RUNTIME_VERSION", None)
try:
    db_api = SparkAPI(
        spark_session=spark,
        catalog=catalog,
        database=schema_name,
        num_partitions_on_repartition=2,
        register_udfs_automatically=False,
        break_lineage_method="delta_lake_table",
    )
finally:
    if dbr_version is not None:
        os.environ["DATABRICKS_RUNTIME_VERSION"] = dbr_version

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}")
spark.sql(f"USE {catalog}.{schema_name}")

linker = Linker(prepped_df, settings, db_api=db_api)

# Pair-count guard per blocking rule.
pair_counts: dict[str, int] = {}
host_pair_n = 0
prefix_pair_n = 0
total_pairs = 0
for rule_i, rule in enumerate(blocking_rules_for_prediction(name_prefix_col)):
    result = count_comparisons_from_blocking_rule(
        table_or_tables=[prepped_df],
        blocking_rule=rule,
        link_type="dedupe_only",
        db_api=db_api,
        unique_id_column_name="record_id",
        compute_post_filter_count=True,
    )
    rule_key = rule if isinstance(rule, str) else getattr(rule, "blocking_rule_sql", str(rule))
    count = int(result.get("number_of_comparisons_to_be_scored_post_filter_conditions", 0) or 0)
    pair_counts[rule_key] = count
    total_pairs += count
    label = "host" if rule_i == 0 else "name_prefix_country"
    if rule_i == 0:
        host_pair_n = count
    else:
        prefix_pair_n = count
    print(f"blocking_rule_pairs label={label} count={count} sql={rule_key!r}")

print(f"total_blocking_pairs={total_pairs}")
if total_pairs > MAX_PAIR_COUNT:
    raise ValueError(
        f"Blocking would generate {total_pairs} pairs (max {MAX_PAIR_COUNT}). "
        f"Try name_prefix_col=name_first4 and re-run."
    )

# COMMAND ----------

lambda_hat = None
try:
    linker.training.estimate_probability_two_random_records_match(
        deterministic_training_rules(),
        recall=LAMBDA_RECALL,
    )
    linker.training.estimate_u_using_random_sampling(max_pairs=1_000_000)
    for em_rule in em_training_blocking_rules():
        linker.training.estimate_parameters_using_expectation_maximisation(em_rule)
except Exception as exc:  # noqa: BLE001 - notebook fallback path
    training_mode = "priors"
    print(f"EM training skipped: {type(exc).__name__}: {exc}")

try:
    lambda_hat = float(linker._settings_obj._probability_two_random_records_match)
    print(f"lambda_probability_two_random_records_match={lambda_hat:.6g}")
except Exception as lambda_exc:  # noqa: BLE001
    print(f"lambda unavailable: {type(lambda_exc).__name__}: {lambda_exc}")

# COMMAND ----------

predictions = linker.inference.predict()
pred_df = predictions.as_spark_dataframe()
pair_n = pred_df.count()
print(f"predicted_pairs={pair_n}")

hist_row = pred_df.agg(
    F.sum(F.when(F.col("match_probability") < 0.2, 1).otherwise(0)).alias("p_lt_02"),
    F.sum(
        F.when(
            (F.col("match_probability") >= 0.2) & (F.col("match_probability") < 0.5),
            1,
        ).otherwise(0)
    ).alias("p_02_05"),
    F.sum(
        F.when(
            (F.col("match_probability") >= 0.5) & (F.col("match_probability") < 0.9),
            1,
        ).otherwise(0)
    ).alias("p_05_09"),
    F.sum(F.when(F.col("match_probability") >= 0.9, 1).otherwise(0)).alias("p_ge_09"),
    F.min("match_probability").alias("min_p"),
    F.max("match_probability").alias("max_p"),
).collect()[0]
p_lt_02 = int(hist_row["p_lt_02"] or 0)
p_02_05 = int(hist_row["p_02_05"] or 0)
p_05_09 = int(hist_row["p_05_09"] or 0)
p_ge_09 = int(hist_row["p_ge_09"] or 0)
min_p = hist_row["min_p"]
max_p = hist_row["max_p"]
print(
    f"match_probability_hist p_lt_02={p_lt_02} p_02_05={p_02_05} "
    f"p_05_09={p_05_09} p_ge_09={p_ge_09} min_p={min_p} max_p={max_p}"
)

clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
    predictions,
    threshold_match_probability=match_threshold,
)
clustered = clusters.as_spark_dataframe()

cluster_sizes = clustered.groupBy("cluster_id").agg(F.count("*").alias("cluster_size"))
cluster_stats = (
    cluster_sizes.agg(
        F.count("*").alias("cluster_n"),
        F.sum(F.when(F.col("cluster_size") == 1, 1).otherwise(0)).alias("singleton_n"),
        F.max("cluster_size").alias("max_cluster_size"),
    )
    .collect()[0]
)
cluster_n = int(cluster_stats["cluster_n"])
singleton_n = int(cluster_stats["singleton_n"] or 0)
max_cluster_size = int(cluster_stats["max_cluster_size"] or 1)

golden_df = clustered.select(
    F.col("record_id"),
    F.concat(F.lit("g"), F.col("cluster_id").cast("string")).alias("golden_entity_id"),
)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}")
(
    golden_df.select("record_id", "golden_entity_id")
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(golden_table)
)
print(f"wrote {golden_table} rows={golden_df.count()}")

# COMMAND ----------

# Masked cluster review (default). Unmasked display is opt-in via widget.
# Select only cluster keys from Splink output — clustered frames already
# include input columns, so joining name_norm/host by name is ambiguous.
review_base = (
    clustered.select("record_id", "cluster_id")
    .join(
        prepped_df.select(
            "record_id",
            F.col("name_norm").alias("review_name_norm"),
            F.col("host").alias("review_host"),
            F.col("country_norm").alias("review_country_norm"),
        ),
        "record_id",
    )
    .withColumn("cluster_size", F.count("*").over(Window.partitionBy("cluster_id")))
)
multi_record_rows = review_base.filter(F.col("cluster_size") >= 2).count()
print(f"multi_record_cluster_rows={multi_record_rows}")

if unmask_review:
    review_df = review_base.filter(F.col("cluster_size") >= 2).orderBy(
        F.col("cluster_size").desc(), "cluster_id", "record_id"
    )
    display(
        review_df.select(
            "cluster_id",
            "cluster_size",
            "record_id",
            "review_name_norm",
            "review_host",
            "review_country_norm",
        ).limit(100)
    )
else:
    review_df = (
        review_base.filter(F.col("cluster_size") >= 2)
        .withColumn("name_hash", masked_hash_udf(F.col("review_name_norm")))
        .withColumn("host_hash", masked_hash_udf(F.col("review_host")))
        .orderBy(F.col("cluster_size").desc(), "cluster_id", "record_id")
    )
    display(
        review_df.select(
            "cluster_id",
            "cluster_size",
            "record_id",
            "name_hash",
            "host_hash",
            "review_country_norm",
        ).limit(100)
    )

# COMMAND ----------

# Best-effort cleanup of Splink delta_lake_table intermediates.
try:
    for table in spark.catalog.listTables(schema_name):
        if table.name.startswith("__splink__"):
            spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema_name}.{table.name}")
except Exception as cleanup_exc:  # noqa: BLE001
    print(f"cleanup warning: {type(cleanup_exc).__name__}: {cleanup_exc}")

lambda_txt = f"{lambda_hat:.6g}" if lambda_hat is not None else "na"
exit_msg = (
    f"ok sample_n={prepped_n} pair_n={pair_n} host_pair_n={host_pair_n} "
    f"prefix_pair_n={prefix_pair_n} cluster_n={cluster_n} "
    f"singleton_n={singleton_n} max_cluster_size={max_cluster_size} "
    f"training={training_mode} lambda={lambda_txt} "
    f"p_lt_02={p_lt_02} p_02_05={p_02_05} p_05_09={p_05_09} p_ge_09={p_ge_09} "
    f"max_p={max_p} source_table={source_table} golden_table={golden_table}"
)
dbutils.notebook.exit(exit_msg)
