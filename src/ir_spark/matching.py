"""Splink settings for Phase 3 company dedupe (Spark-native comparisons only).

Canonical source for comparison/blocking logic. The Databricks notebook
``notebooks/03_entity_matching.py`` mirrors ``build_settings()`` inline because
the workspace cannot import this uv package.

Free Edition serverless has no driver JVM, so comparisons must not depend on
Splink's JAR-backed Jaro-Winkler / Damerau UDFs.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

import splink.comparison_level_library as cll
import splink.comparison_library as cl
from splink import SettingsCreator, block_on

if TYPE_CHECKING:
    from collections.abc import Sequence

# Limits enforced by the Phase 3 notebook before predict().
MAX_SOURCE_ROWS = 50_000
MAX_PAIR_COUNT = 2_000_000
DEFAULT_MATCH_THRESHOLD = 0.9
EM_CONVERGENCE = 0.01
LAMBDA_RECALL = 0.6

# Blocking-only denylist (original to this repo; not a production ruleset).
GENERIC_HOST_DENYLIST: tuple[str, ...] = (
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

BANNED_SQL_FRAGMENTS: tuple[str, ...] = (
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


def _denylist_sql_literal(hosts: Sequence[str] = GENERIC_HOST_DENYLIST) -> str:
    return ", ".join(f"'{host}'" for host in hosts)


def host_blocking_rule_sql(hosts: Sequence[str] = GENERIC_HOST_DENYLIST) -> str:
    """Same registrable host, excluding generic social / link-aggregator hosts."""
    denied = _denylist_sql_literal(hosts)
    return (
        "l.host = r.host "
        "AND l.host IS NOT NULL "
        f"AND l.host NOT IN ({denied})"
    )


def name_comparison() -> cl.CustomComparison:
    """Fuzzy company name via native Spark ``levenshtein`` with length gates."""
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
    """Two OR-rules: non-denylisted host, or name prefix + country."""
    return [
        host_blocking_rule_sql(),
        block_on(name_prefix_col, "country_norm"),
    ]


def deterministic_training_rules(hosts: Sequence[str] = GENERIC_HOST_DENYLIST) -> list[str]:
    """High-precision rules for lambda — never same-host alone on generic domains."""
    denied = _denylist_sql_literal(hosts)
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


def em_training_blocking_rules(hosts: Sequence[str] = GENERIC_HOST_DENYLIST) -> list[str]:
    """Complementary EM blocks so host and name/country comparisons both get m-estimates."""
    return [
        host_blocking_rule_sql(hosts),
        "l.name_norm = r.name_norm AND l.country_norm = r.country_norm",
    ]


def build_settings(name_prefix_col: str = "name_first3") -> SettingsCreator:
    """Assemble the locked Phase 3 Splink model (dedupe on ``record_id``)."""
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
    """Fail fast if any comparison level would call a JAR-backed UDF."""
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
