# data/

Local-only copies of the public company snapshot. Nothing under `raw/`, `interim/`, or `processed/` is committed except `.gitkeep` files.

## Download

From the repo root (after `uv sync`):

```bash
uv run ir-spark-download
```

This fetches Hugging Face dataset `bigpictureio/companies-2023-q4-sm`, file `companies-2023-q4-sm.csv.gz` (the same BigPicture / PDL free company snapshot mirrored on Kaggle as `mfrye0/bigpicture-company-dataset`). No Kaggle API token is required for the default path.

Expected size: 629,293,547 bytes. Checksum is pinned in `src/ir_spark/constants.py`.

## Columns (observed in the gzip header)

`handle`, `name`, `website`, `industry`, `size`, `type`, `founded`, `city`, `state`, `country_code`

Company pages only (`handle` is a LinkedIn *company* slug, not a person).

## Phase 2 local artifacts

```bash
uv run ir-spark-sample      # data/interim/companies_sample.csv + .meta.json
uv run ir-spark-profile     # data/processed/dq_summary.json
```

Defaults: `n=30000`, `seed=42`. Adds a surrogate `record_id` column for later Splink use.

## Databricks landing zone

The **full gzip** (~600 MiB) is on Unity Catalog Volume `/Volumes/workspace/default/ir_spark/companies-2023-q4-sm.csv.gz` — not workspace files (500 MB cap), not DBFS root (disabled on Free Edition).

```bash
databricks fs cp data/raw/companies-2023-q4-sm.csv.gz \
  dbfs:/Volumes/workspace/default/ir_spark/companies-2023-q4-sm.csv.gz \
  --profile yves.deutschmann
```

Spark ingestion wrote Delta tables `workspace.ir_spark.companies_raw` (17,154,017 rows) and `workspace.ir_spark.companies_sample` (30,000). See `notebooks/02_data_ingestion.py`.
