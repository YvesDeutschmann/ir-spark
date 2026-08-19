# notebooks/

Databricks notebooks for this proof of concept.

| File | Phase | Status |
| --- | --- | --- |
| `01_environment_smoke.py` | 1 | Imported to `/Users/yves.deutschmann@gmail.com/ir-spark/01_environment_smoke` and run on Free Edition serverless: Spark range + Splink `SparkAPI` init. Does **not** load company data. |
| `02_data_ingestion.py` | 2 | Imported to `/Users/yves.deutschmann@gmail.com/ir-spark/02_data_ingestion` and run on Free Edition serverless. Writes `workspace.ir_spark.companies_raw` (17,154,017 rows) and `workspace.ir_spark.companies_sample` (30,000). |
| *(matching / rollup)* | 3–4 | Not started. |

Re-import from these files after local edits. Serverless compute is the default; do not add cluster-tuning notes as if they were practiced here.

## Phase 2 upload (done)

Gzip is on Volume `/Volumes/workspace/default/ir_spark/companies-2023-q4-sm.csv.gz`.

```bash
databricks fs cp data/raw/companies-2023-q4-sm.csv.gz \
  dbfs:/Volumes/workspace/default/ir_spark/companies-2023-q4-sm.csv.gz \
  --profile yves.deutschmann
```

Widget default `gzip_path` in the notebook is that Volume path (catalog `workspace`).

### Masked DQ (local 30k sample, seed 42)

| Metric | Value |
| --- | --- |
| Rows | 30,000 |
| Null rate `name` | 0.03% |
| Null rate `website` | 20.1% |
| Null rate `handle` | 0% |
| Null rate `city` | 20.8% |
| Null rate `state` | 28.8% |
| Null rate `country_code` | 18.1% |
| Exact duplicate rows | 0 |
| Same-host groups size ≥ 2 | 94 |
| Normalized-name groups size ≥ 2 | 7 |
| Coarse name-prefix+country groups size ≥ 2 | 14 |

Databricks notebook exit: `full_count=17154017 sample_count=30000`.

Local equivalents:

- Phase 1: `uv run ir-spark-smoke`
- Phase 2 sample/profile: `uv run ir-spark-sample`, `uv run ir-spark-profile`
