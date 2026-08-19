"""Aggregate data-quality report for the local company sample."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd

from ir_spark.constants import (
    DEFAULT_DQ_SUMMARY,
    DEFAULT_SAMPLE_CSV,
    SOURCE_COLUMNS,
)

NULL_CHECK_COLUMNS = ("name", "website", "handle", "city", "state", "country_code")
NAME_SUFFIXES = ("inc", "llc", "ltd", "corp", "gmbh", "sa", "ag")
WHITESPACE_RE = re.compile(r"\s+")


def is_null_or_empty(series: pd.Series) -> pd.Series:
    return series.isna() | series.astype(str).str.strip().eq("")


def null_rates(df: pd.DataFrame) -> dict[str, float]:
    rates: dict[str, float] = {}
    for column in NULL_CHECK_COLUMNS:
        if column not in df.columns:
            continue
        rates[column] = float(is_null_or_empty(df[column]).mean())
    return rates


def exact_duplicate_count(df: pd.DataFrame) -> int:
    compare_cols = [col for col in df.columns if col != "record_id"]
    duplicated = df.duplicated(subset=compare_cols, keep=False)
    return int(duplicated.sum())


def normalize_host(value: object) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip().lower()
    if not text:
        return None
    if "://" not in text:
        text = f"http://{text}"
    parsed = urlparse(text)
    host = parsed.netloc or parsed.path.split("/")[0]
    host = host.removeprefix("www.")
    return host or None


def collision_group_stats(keys: pd.Series) -> dict[str, int]:
    valid = keys.dropna()
    if valid.empty:
        return {"groups_ge_2": 0, "records_in_groups_ge_2": 0}
    counts = valid.value_counts()
    multi = counts[counts >= 2]
    return {
        "groups_ge_2": int(multi.shape[0]),
        "records_in_groups_ge_2": int(multi.sum()),
    }


def normalize_name(value: object) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = WHITESPACE_RE.sub(" ", str(value).strip().lower())
    if not text:
        return None
    tokens = text.split(" ")
    while tokens and tokens[-1].rstrip(".,") in NAME_SUFFIXES:
        tokens.pop()
    return " ".join(tokens) or None


def coarse_name_key(row: pd.Series) -> str | None:
    normalized = normalize_name(row.get("name"))
    if not normalized:
        return None
    prefix = normalized[:20]
    country = row.get("country_code")
    if country is None or (isinstance(country, float) and pd.isna(country)):
        country = ""
    return f"{prefix}|{str(country).strip().lower()}"


def masked_example(value: str) -> str:
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:8]
    return f"hash:{digest}"


def build_summary(df: pd.DataFrame) -> dict[str, object]:
    empty = pd.Series(dtype=object)
    hosts = df["website"].map(normalize_host) if "website" in df.columns else empty
    normalized_names = df["name"].map(normalize_name) if "name" in df.columns else empty
    coarse_keys = df.apply(coarse_name_key, axis=1) if "name" in df.columns else empty

    host_stats = collision_group_stats(hosts)
    name_stats = collision_group_stats(normalized_names)
    coarse_stats = collision_group_stats(coarse_keys)

    country_counts: dict[str, int] = {}
    if "country_code" in df.columns:
        counts = df["country_code"].fillna("").astype(str).str.strip().value_counts()
        country_counts = {str(key): int(value) for key, value in counts.head(20).items()}

    examples: dict[str, str] = {}
    for label, series in (
        ("host_collision_example", hosts),
        ("name_collision_example", normalized_names),
    ):
        counts = series.dropna().value_counts()
        multi = counts[counts >= 2]
        if not multi.empty:
            examples[label] = masked_example(str(multi.index[0]))

    return {
        "row_count": int(len(df)),
        "columns": list(df.columns),
        "null_rates": null_rates(df),
        "exact_duplicate_rows": exact_duplicate_count(df),
        "same_host_collisions": host_stats,
        "normalized_name_collisions": name_stats,
        "coarse_name_country_collisions": coarse_stats,
        "country_code_top_20": country_counts,
        "masked_examples": examples,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Aggregate data-quality report for the local company sample."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_SAMPLE_CSV,
        help=f"Sample CSV path (default: {DEFAULT_SAMPLE_CSV})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_DQ_SUMMARY,
        help=f"JSON summary path (default: {DEFAULT_DQ_SUMMARY})",
    )
    args = parser.parse_args(argv)

    input_path = args.input.expanduser().resolve()
    output_path = args.output.expanduser().resolve()

    if not input_path.is_file():
        print(
            f"Sample file not found: {input_path}\nRun `uv run ir-spark-sample` first.",
            file=sys.stderr,
        )
        return 1

    df = pd.read_csv(input_path, dtype=str, keep_default_na=False)
    missing = [col for col in SOURCE_COLUMNS if col not in df.columns]
    if missing:
        print(f"Sample CSV missing expected columns: {missing}", file=sys.stderr)
        return 1

    summary = build_summary(df)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    print(f"row_count={summary['row_count']}")
    print(f"null_rates={summary['null_rates']}")
    print(f"exact_duplicate_rows={summary['exact_duplicate_rows']}")
    print(f"same_host_collisions={summary['same_host_collisions']}")
    print(f"normalized_name_collisions={summary['normalized_name_collisions']}")
    print(f"Wrote {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
