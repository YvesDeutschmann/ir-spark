# Databricks notebook source
# Phase 5: read-only verification of the eval-set funnel.
#
# Confirms workspace.ir_spark Delta tables still match the frozen numbers in
# PROJECT_BRIEF.md § Evaluation findings. Does not write, drop, or rematch.
#
# Run only when notebooks 03/04 are idle (concurrent overwrites can change
# Delta versions mid-run).
#
# Inputs (widgets, suffix allowlist only):
#   companies_match_sample, companies_golden, companies_hierarchy
# Output: printed aggregates + dbutils.notebook.exit (no Delta writes)

# COMMAND ----------

import re

from pyspark.sql import functions as F

TABLE_NAME_PATTERN = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){0,2}$"
)

ALLOWED_SUFFIXES = frozenset(
    {
        "companies_match_sample",
        "companies_golden",
        "companies_hierarchy",
    }
)

FORBIDDEN_SUFFIXES = frozenset(
    {
        "companies_raw",
        "companies_sample",
    }
)

# Frozen expected funnel (PROJECT_BRIEF.md § Evaluation findings).
EXPECTED_RECORD_N = 40_100
EXPECTED_GOLDEN_N = 33_697
EXPECTED_PARENT_N = 27_156


def _table_suffix(table_name: str) -> str:
    if not TABLE_NAME_PATTERN.fullmatch(table_name):
        raise ValueError(f"Invalid table identifier: {table_name!r}")
    return table_name.rsplit(".", 1)[-1]


def _validate_table_name(table_name: str, role: str) -> str:
    suffix = _table_suffix(table_name)
    if suffix in FORBIDDEN_SUFFIXES:
        raise ValueError(f"Refusing {role} table {table_name!r}: suffix {suffix!r} is forbidden")
    if suffix not in ALLOWED_SUFFIXES:
        raise ValueError(
            f"Refusing {role} table {table_name!r}: suffix {suffix!r} not in allowlist"
        )
    return suffix


def _delta_version(table_name: str) -> int:
    history = spark.sql(f"DESCRIBE HISTORY {table_name}")
    if history.count() == 0:
        raise ValueError(f"No Delta history for {table_name!r}")
    return int(history.select(F.max("version").alias("v")).collect()[0]["v"])


def _assert_versions_unchanged(
    table_versions: dict[str, int],
    *,
    label: str,
) -> None:
    for table_name, start_version in table_versions.items():
        current = _delta_version(table_name)
        if current != start_version:
            raise ValueError(
                f"Delta version moved for {table_name!r} during {label}: "
                f"start={start_version} current={current}"
            )


def _require_columns(df, required: tuple[str, ...], table_name: str) -> None:
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"{table_name!r} missing columns: {missing}")


def _assert_no_nulls(df, column: str, table_name: str) -> None:
    if df.filter(F.col(column).isNull()).limit(1).count():
        raise ValueError(f"{table_name!r} has null {column}")


def _assert_no_duplicate_record_ids(df, table_name: str) -> None:
    if (
        df.groupBy("record_id")
        .agg(F.count("*").alias("n"))
        .filter(F.col("n") > 1)
        .limit(1)
        .count()
    ):
        raise ValueError(f"{table_name!r} has duplicate record_id values")


def _assert_record_id_sets_match(
    left_df,
    right_df,
    *,
    left_name: str,
    right_name: str,
) -> None:
    left_only = (
        left_df.select("record_id")
        .distinct()
        .join(right_df.select("record_id").distinct(), on="record_id", how="left_anti")
    )
    if left_only.limit(1).count():
        raise ValueError(f"record_id present in {left_name!r} but absent in {right_name!r}")

    right_only = (
        right_df.select("record_id")
        .distinct()
        .join(left_df.select("record_id").distinct(), on="record_id", how="left_anti")
    )
    if right_only.limit(1).count():
        raise ValueError(f"record_id present in {right_name!r} but absent in {left_name!r}")


def _pct_reduction(numerator: int, denominator: int) -> float:
    if denominator == 0:
        raise ValueError("Cannot compute reduction with zero denominator")
    return 1.0 - (numerator / denominator)


# COMMAND ----------

dbutils.widgets.text(
    "match_sample_table",
    "workspace.ir_spark.companies_match_sample",
    "Eval input (companies_match_sample)",
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

match_sample_table = dbutils.widgets.get("match_sample_table").strip()
golden_table = dbutils.widgets.get("golden_table").strip()
hierarchy_table = dbutils.widgets.get("hierarchy_table").strip()

_validate_table_name(match_sample_table, "match_sample")
_validate_table_name(golden_table, "golden")
_validate_table_name(hierarchy_table, "hierarchy")

if len({match_sample_table, golden_table, hierarchy_table}) < 3:
    raise ValueError("match_sample_table, golden_table, and hierarchy_table must differ")

print(f"match_sample_table={match_sample_table}")
print(f"golden_table={golden_table}")
print(f"hierarchy_table={hierarchy_table}")

# COMMAND ----------

start_versions = {
    match_sample_table: _delta_version(match_sample_table),
    golden_table: _delta_version(golden_table),
    hierarchy_table: _delta_version(hierarchy_table),
}
print(
    "delta_versions_start "
    f"match_sample={start_versions[match_sample_table]} "
    f"golden={start_versions[golden_table]} "
    f"hierarchy={start_versions[hierarchy_table]}"
)

match_sample_df = spark.table(match_sample_table)
golden_df = spark.table(golden_table)
hierarchy_df = spark.table(hierarchy_table)

_require_columns(match_sample_df, ("record_id",), match_sample_table)
_require_columns(golden_df, ("record_id", "golden_entity_id"), golden_table)
_require_columns(
    hierarchy_df,
    ("record_id", "golden_entity_id", "ultimate_parent_id"),
    hierarchy_table,
)

for df, name, cols in (
    (match_sample_df, match_sample_table, ("record_id",)),
    (golden_df, golden_table, ("record_id", "golden_entity_id")),
    (hierarchy_df, hierarchy_table, ("record_id", "golden_entity_id", "ultimate_parent_id")),
):
    for col in cols:
        _assert_no_nulls(df, col, name)
    _assert_no_duplicate_record_ids(df, name)

_assert_record_id_sets_match(
    match_sample_df,
    golden_df,
    left_name=match_sample_table,
    right_name=golden_table,
)
_assert_record_id_sets_match(
    golden_df,
    hierarchy_df,
    left_name=golden_table,
    right_name=hierarchy_table,
)

record_n = match_sample_df.count()
golden_row_n = golden_df.count()
hierarchy_row_n = hierarchy_df.count()
golden_n = golden_df.select("golden_entity_id").distinct().count()
parent_n = hierarchy_df.select("ultimate_parent_id").distinct().count()

if record_n != golden_row_n or record_n != hierarchy_row_n:
    raise ValueError(
        "Row counts disagree across tables: "
        f"match_sample={record_n} golden={golden_row_n} hierarchy={hierarchy_row_n}"
    )

if record_n == 0 or golden_n == 0 or parent_n == 0:
    raise ValueError(
        f"Zero count in funnel: record_n={record_n} golden_n={golden_n} parent_n={parent_n}"
    )

golden_reduction = _pct_reduction(golden_n, record_n)
parent_reduction = _pct_reduction(parent_n, golden_n)
overall_reduction = _pct_reduction(parent_n, record_n)

print(f"record_n={record_n} golden_n={golden_n} parent_n={parent_n}")
print(f"reduction_records_to_golden={golden_reduction:.4f}")
print(f"reduction_golden_to_parent={parent_reduction:.4f}")
print(f"reduction_records_to_parent={overall_reduction:.4f}")

_assert_versions_unchanged(start_versions, label="counts")

if (record_n, golden_n, parent_n) != (EXPECTED_RECORD_N, EXPECTED_GOLDEN_N, EXPECTED_PARENT_N):
    raise ValueError(
        "Live funnel does not match frozen PROJECT_BRIEF findings: "
        f"live=({record_n}, {golden_n}, {parent_n}) "
        f"expected=({EXPECTED_RECORD_N}, {EXPECTED_GOLDEN_N}, {EXPECTED_PARENT_N})"
    )

# COMMAND ----------

# Documented constants — from PROJECT_BRIEF / notebook 03 exit; not recomputed.
print("--- documented constants (not recomputed) ---")
print(
    "sparsity_baseline: companies_sample n=30000 all_singletons_at_0.9 "
    "pair_n=71551 max_p≈0.20"
)
print(
    "eval_sample_composition: host_collision_rows=18486 "
    "name_country_rows=11644 filler_rows=10000 unique_handles=true"
)
print(
    "enriched_disposition: pair_n=236353 p_ge_09=7033 p_05_09=857 "
    "p_02_05=5581 p_lt_02=222882 lambda=1.86e-05 "
    "max_cluster_size=8 singleton_n=27763 training=em"
)
print(
    "hierarchy_note: name_prefix_links=5; shared_host_not_etld1; "
    "precision_of_ge_09_pairs_not_labeled"
)

# COMMAND ----------

end_versions = {
    match_sample_table: _delta_version(match_sample_table),
    golden_table: _delta_version(golden_table),
    hierarchy_table: _delta_version(hierarchy_table),
}
_assert_versions_unchanged(start_versions, label="exit")

exit_msg = (
    f"ok record_n={record_n} golden_n={golden_n} parent_n={parent_n} "
    f"reduction_records_to_golden={golden_reduction:.4f} "
    f"reduction_golden_to_parent={parent_reduction:.4f} "
    f"reduction_records_to_parent={overall_reduction:.4f} "
    f"match_sample_version={end_versions[match_sample_table]} "
    f"golden_version={end_versions[golden_table]} "
    f"hierarchy_version={end_versions[hierarchy_table]} "
    f"match_sample_table={match_sample_table} "
    f"golden_table={golden_table} hierarchy_table={hierarchy_table}"
)
dbutils.notebook.exit(exit_msg)
