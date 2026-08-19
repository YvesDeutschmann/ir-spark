# notebooks/

Databricks notebooks for this proof of concept.

| File | Phase | Status |
| --- | --- | --- |
| `01_environment_smoke.py` | 1 | Starter: Spark range + Splink `SparkAPI` import. Does **not** load company data. |
| *(ingestion / matching / rollup)* | 2–4 | Not started. Do not add them unless asked. |

Import `01_environment_smoke.py` into a personal Databricks Free Edition workspace (Workspace → Import) or paste the cells. Serverless compute is enough; do not add cluster-tuning notes as if they were practiced here.

Local equivalent: `uv run ir-spark-smoke`.
