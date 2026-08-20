# notebooks/

Databricks notebooks for this proof of concept.

| File | Phase | Status |
| --- | --- | --- |
| `01_environment_smoke.py` | 1 | Imported to `/Users/yves.deutschmann@gmail.com/ir-spark/01_environment_smoke` and run on Free Edition serverless: Spark range + Splink `SparkAPI` init. Does **not** load company data. |
| `02_data_ingestion.py` | 2 | Imported to `/Users/yves.deutschmann@gmail.com/ir-spark/02_data_ingestion` and run on Free Edition serverless. Writes `workspace.ir_spark.companies_raw` (17,154,017 rows) and `workspace.ir_spark.companies_sample` (30,000). |
| `02b_match_sample.py` | 3 close-out | Imported to `/Users/yves.deutschmann@gmail.com/ir-spark/02b_match_sample` and run on serverless. Writes `workspace.ir_spark.companies_match_sample` (40,100; host and name+country groups size 2–10 plus filler). |
| `03_entity_matching.py` | 3 | Imported to `/Users/yves.deutschmann@gmail.com/ir-spark/03_entity_matching` and run on Free Edition serverless. Eval input is `companies_match_sample`. Writes `workspace.ir_spark.companies_golden` (40,100 rows, 33,697 goldens). Native Spark SQL Levenshtein (no JAR Jaro-Winkler); lineage via `delta_lake_table`. |
| `04_hierarchy_rollup.py` | 4 | Imported to `/Users/yves.deutschmann@gmail.com/ir-spark/04_hierarchy_rollup` and run on serverless. Writes `workspace.ir_spark.companies_hierarchy` (27,156 ultimate parents). |

Re-import from these files after local edits. Serverless compute is the default; do not add cluster-tuning notes as if they were practiced here.

Phase 3 blocks on non-denylisted host or `name_first3` + `country_norm`, aborts if blocking pair count exceeds 2M, and never uses `block_on("name_first3")` alone. Do not add Jaro-Winkler / `NameComparison` on this workspace.

## Phase 4 hierarchy (done)

- Inputs: `workspace.ir_spark.companies_match_sample` + `companies_golden`
- Output: `workspace.ir_spark.companies_hierarchy` (`record_id`, `golden_entity_id`, `ultimate_parent_id`)
- Rules: shared non-denylisted host, plus one-hop name prefix (same country, parent name length ≥ 8). Almost all rollup is shared host (`name_prefix_links=5`); the eval sample was itself drawn from same-host groups.
- Notebook exit: `ok record_n=40100 golden_n=33697 parent_n=27156 name_prefix_links=5 multi_parent_goldens=11845`
- Masked review: multi-golden parent groups (unmask widget off)

## Phase 3 matching (done)

- Sparsity baseline input: `workspace.ir_spark.companies_sample` (30,000; all singletons at 0.9)
- Eval input: `workspace.ir_spark.companies_match_sample` (40,100)
- Output: `workspace.ir_spark.companies_golden` (`record_id`, `golden_entity_id`; 40,100 rows / 33,697 goldens)
- Settings canonical source: `src/ir_spark/matching.py` (mirrored inline in the notebook)
- Enriched-run exit: `ok sample_n=40100 pair_n=236353 host_pair_n=16372 prefix_pair_n=226967 cluster_n=33697 singleton_n=27763 max_cluster_size=8 training=em lambda=1.86467e-05 p_lt_02=222882 p_02_05=5581 p_05_09=857 p_ge_09=7033 max_p≈1.0`
- Disposition on scored pairs: 7,033 match (≥0.9), 857 review (0.5–0.9), rest non-match
- Do not treat this funnel as a random 17M result. Random `companies_sample` matching was all singletons (max p ≈ 0.20). Findings: [`PROJECT_BRIEF.md`](../PROJECT_BRIEF.md) § Evaluation findings.

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
