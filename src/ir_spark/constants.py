"""Shared dataset metadata for the public company snapshot."""

from __future__ import annotations

from pathlib import Path

HF_REPO_ID = "bigpictureio/companies-2023-q4-sm"
HF_FILENAME = "companies-2023-q4-sm.csv.gz"
EXPECTED_SHA256 = "c673f1c6936c8806f0284aac352c5a765d3ea429dc9bd26ee34b3118954e4466"
EXPECTED_SIZE_BYTES = 629_293_547

SOURCE_COLUMNS = (
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
)

DEFAULT_SAMPLE_N = 30_000
DEFAULT_SAMPLE_SEED = 42


def find_repo_root() -> Path:
    for start in (Path.cwd(), Path(__file__).resolve().parent):
        for path in [start, *start.parents]:
            if (path / "pyproject.toml").is_file() and (path / "src" / "ir_spark").is_dir():
                return path
    return Path.cwd()


REPO_ROOT = find_repo_root()
DEFAULT_RAW_PATH = REPO_ROOT / "data" / "raw" / HF_FILENAME
DEFAULT_SAMPLE_CSV = REPO_ROOT / "data" / "interim" / "companies_sample.csv"
DEFAULT_SAMPLE_META = REPO_ROOT / "data" / "interim" / "companies_sample.meta.json"
DEFAULT_DQ_SUMMARY = REPO_ROOT / "data" / "processed" / "dq_summary.json"
