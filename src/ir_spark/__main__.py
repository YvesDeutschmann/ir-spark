"""``uv run ir-spark <subcommand>`` dispatcher."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="ir-spark",
        description="Local helpers for the Splink-on-Spark company-entity proof of concept.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    download_parser = sub.add_parser(
        "download",
        help="Download the public company snapshot into data/raw/.",
    )
    download_parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download even if the checksum already matches.",
    )
    download_parser.add_argument(
        "--output",
        type=Path,
        help="Destination path (default: data/raw/companies-2023-q4-sm.csv.gz).",
    )

    sample_parser = sub.add_parser(
        "sample",
        help="Reservoir-sample the gzip into data/interim/companies_sample.csv.",
    )
    sample_parser.add_argument("--input", type=Path)
    sample_parser.add_argument("--output", type=Path)
    sample_parser.add_argument("--meta", type=Path)
    sample_parser.add_argument("--n", type=int)
    sample_parser.add_argument("--seed", type=int)

    profile_parser = sub.add_parser(
        "profile",
        help="Aggregate data-quality report for the local sample CSV.",
    )
    profile_parser.add_argument("--input", type=Path)
    profile_parser.add_argument("--output", type=Path)

    sub.add_parser("smoke", help="Local Phase 1: Spark + Splink SparkAPI smoke test.")

    args = parser.parse_args(sys.argv[1:] if argv is None else argv)

    if args.command == "download":
        extra: list[str] = []
        if args.force:
            extra.append("--force")
        if args.output is not None:
            extra.extend(["--output", str(args.output)])
        from ir_spark.download import main as download_main

        return download_main(extra)

    if args.command == "sample":
        extra = _optional_args(args, ("input", "output", "meta", "n", "seed"))
        from ir_spark.sample import main as sample_main

        return sample_main(extra)

    if args.command == "profile":
        extra = _optional_args(args, ("input", "output"))
        from ir_spark.profile import main as profile_main

        return profile_main(extra)

    from ir_spark.smoke import main as smoke_main

    return smoke_main()


def _optional_args(args: argparse.Namespace, names: tuple[str, ...]) -> list[str]:
    extra: list[str] = []
    for name in names:
        value = getattr(args, name, None)
        if value is not None:
            extra.extend([f"--{name.replace('_', '-')}", str(value)])
    return extra


if __name__ == "__main__":
    raise SystemExit(main())
