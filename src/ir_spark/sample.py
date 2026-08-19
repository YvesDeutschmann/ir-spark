"""Reservoir-sample the public company snapshot into a local CSV for iteration."""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import random
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from ir_spark.constants import (
    DEFAULT_RAW_PATH,
    DEFAULT_SAMPLE_CSV,
    DEFAULT_SAMPLE_META,
    DEFAULT_SAMPLE_N,
    DEFAULT_SAMPLE_SEED,
    EXPECTED_SHA256,
    SOURCE_COLUMNS,
)
from ir_spark.download import file_is_complete, sha256_file

if TYPE_CHECKING:
    from collections.abc import Iterator


def iter_csv_rows(path: Path) -> Iterator[dict[str, str]]:
    with gzip.open(path, mode="rt", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError(f"No header row found in {path}")
        missing = [col for col in SOURCE_COLUMNS if col not in reader.fieldnames]
        if missing:
            raise ValueError(f"Missing expected columns in {path}: {missing}")
        for row in reader:
            yield {col: (row.get(col) or "") for col in SOURCE_COLUMNS}


def reservoir_sample(rows: Iterator[dict[str, str]], n: int, seed: int) -> list[dict[str, str]]:
    if n <= 0:
        raise ValueError("--n must be positive")

    rng = random.Random(seed)
    reservoir: list[dict[str, str]] = []
    for index, row in enumerate(rows, start=1):
        if index <= n:
            reservoir.append(row)
            continue
        replace_at = rng.randrange(index)
        if replace_at < n:
            reservoir[replace_at] = row
    return reservoir


def write_sample_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["record_id", *SOURCE_COLUMNS]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for index, row in enumerate(rows, start=1):
            writer.writerow({"record_id": f"r{index:06d}", **row})


def write_meta(path: Path, *, n: int, seed: int, source_path: Path, row_count: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "n_requested": n,
        "n_written": row_count,
        "seed": seed,
        "source_path": str(source_path),
        "source_sha256": sha256_file(source_path),
        "columns": ["record_id", *SOURCE_COLUMNS],
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Reservoir-sample the public company snapshot into data/interim/."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_RAW_PATH,
        help=f"Source gzip (default: {DEFAULT_RAW_PATH})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_SAMPLE_CSV,
        help=f"Sample CSV destination (default: {DEFAULT_SAMPLE_CSV})",
    )
    parser.add_argument(
        "--meta",
        type=Path,
        default=DEFAULT_SAMPLE_META,
        help=f"Metadata sidecar path (default: {DEFAULT_SAMPLE_META})",
    )
    parser.add_argument(
        "--n",
        type=int,
        default=DEFAULT_SAMPLE_N,
        help=f"Sample size (default: {DEFAULT_SAMPLE_N})",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SAMPLE_SEED,
        help=f"Random seed (default: {DEFAULT_SAMPLE_SEED})",
    )
    args = parser.parse_args(argv)

    input_path = args.input.expanduser().resolve()
    output_path = args.output.expanduser().resolve()
    meta_path = args.meta.expanduser().resolve()

    if not input_path.is_file():
        print(f"Source file not found: {input_path}", file=sys.stderr)
        return 1
    if input_path == DEFAULT_RAW_PATH.expanduser().resolve() and not file_is_complete(input_path):
        print(
            f"Source file missing or checksum mismatch: {input_path}\n"
            "Run `uv run ir-spark-download` first.",
            file=sys.stderr,
        )
        return 1

    print(f"Sampling n={args.n} seed={args.seed} from {input_path}")
    sampled = reservoir_sample(iter_csv_rows(input_path), args.n, args.seed)
    write_sample_csv(output_path, sampled)
    write_meta(meta_path, n=args.n, seed=args.seed, source_path=input_path, row_count=len(sampled))

    print(f"Wrote {len(sampled)} rows -> {output_path}")
    print(f"Metadata -> {meta_path}")
    if input_path == DEFAULT_RAW_PATH.expanduser().resolve():
        print(f"source_sha256={EXPECTED_SHA256}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
