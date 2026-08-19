# data/

Local-only copies of the public company snapshot. Nothing under `raw/`, `interim/`, or `processed/` is committed except `.gitkeep` files.

## Download

From the repo root (after `uv sync`):

```bash
uv run ir-spark-download
```

This fetches Hugging Face dataset `bigpictureio/companies-2023-q4-sm`, file `companies-2023-q4-sm.csv.gz` (the same BigPicture / PDL free company snapshot mirrored on Kaggle as `mfrye0/bigpicture-company-dataset`). No Kaggle API token is required for the default path.

Expected size: 629,293,547 bytes. Checksum is pinned in `src/ir_spark/download.py`.

## Columns (observed in the gzip header)

`handle`, `name`, `website`, `industry`, `size`, `type`, `founded`, `city`, `state`, `country_code`

Company pages only (`handle` is a LinkedIn *company* slug, not a person). No sampling or quality profiling is done here — that is Phase 2.

## Phase 2 (not in this checkout)

Upload to a personal Databricks volume, sample ~20–50k rows, write Delta, and capture null/dupe rates in a notebook.
