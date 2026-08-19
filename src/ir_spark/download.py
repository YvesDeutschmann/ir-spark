"""Download the public company snapshot used by this project.

This script only fetches the raw file into ``data/raw/``. It does not sample
rows, profile quality, load Spark, or write Delta tables (those are Phase 2).
"""

from __future__ import annotations

import argparse
import hashlib
import sys
from pathlib import Path

from ir_spark.constants import (
    DEFAULT_RAW_PATH,
    EXPECTED_SHA256,
    EXPECTED_SIZE_BYTES,
    HF_FILENAME,
    HF_REPO_ID,
)

DEFAULT_OUTPUT = DEFAULT_RAW_PATH


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def file_is_complete(path: Path) -> bool:
    return (
        path.is_file()
        and path.stat().st_size == EXPECTED_SIZE_BYTES
        and sha256_file(path) == EXPECTED_SHA256
    )


def download_from_huggingface(output_path: Path) -> Path:
    from huggingface_hub import hf_hub_download

    output_path.parent.mkdir(parents=True, exist_ok=True)
    downloaded = hf_hub_download(
        repo_id=HF_REPO_ID,
        filename=HF_FILENAME,
        repo_type="dataset",
        local_dir=str(output_path.parent),
    )
    downloaded_path = Path(downloaded)
    if downloaded_path.resolve() != output_path.resolve():
        downloaded_path.replace(output_path)
    return output_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Download the public 17M+ company snapshot into data/raw/."
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download even if the checksum already matches.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Destination path (default: {DEFAULT_OUTPUT})",
    )
    args = parser.parse_args(argv)
    output_path = args.output.expanduser().resolve()

    if not args.force and file_is_complete(output_path):
        print(f"Already present and verified: {output_path}")
        print(f"sha256={EXPECTED_SHA256}")
        return 0

    print(f"Downloading {HF_REPO_ID}/{HF_FILENAME}")
    print(f" -> {output_path}")
    download_from_huggingface(output_path)

    if not file_is_complete(output_path):
        actual = sha256_file(output_path) if output_path.is_file() else "missing"
        print(
            "Checksum mismatch after download.\n"
            f" expected sha256={EXPECTED_SHA256} size={EXPECTED_SIZE_BYTES}\n"
            f" actual sha256={actual} size="
            f"{output_path.stat().st_size if output_path.is_file() else 0}",
            file=sys.stderr,
        )
        return 1

    print(f"Verified {output_path}")
    print(f"sha256={EXPECTED_SHA256}")
    print("Next: `uv run ir-spark-sample` and upload the gzip to a Databricks Volume.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
