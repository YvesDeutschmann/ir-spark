# ir-spark

Personal proof of concept: **golden reference matching** for company records with Splink on Spark, then a light **ultimate-parent rollup**. Built for hands-on Databricks/Spark practice and one honest before/after metric — not to ship a production matcher.

## Problem

Take messy-looking company records, cluster them into **golden entity IDs** (dedupe / golden reference matching), then roll related goldens into an **ultimate-parent** group (hierarchy management). Use **disposition logic** — match, review, non-match — on scored pairs before clustering.

This repo does that on a public company snapshot in a personal Databricks Free Edition workspace. Company-level data only; no people, no PII, no employer extracts.

## What the data actually is

The source file is closer to an **already-keyed company census** (one LinkedIn company page per row) than to a dirty CRM extract. Handles are unique: 30,000 / 30,000 on a random 30k slice; 17,154,016 / 17,154,017 on the full 17M table.

On that random 30k, Splink produced **all singletons** at match threshold 0.9 (`pair_n=71,551`, `max_cluster_size=1`, max match probability ≈ 0.20). That is the expected outcome on unique handles with almost no name+country collisions — a sparsity / prior-calibration result, not a matcher bug.

The 17M file still has many small same-host and same-name+country collision groups. I built a **collision-enriched eval set** (`companies_match_sample`, 40,100 rows: 18,486 host-collision rows, 11,644 name+country rows, 10,000 filler) so Splink could train and cluster. That is a valid evaluation design; it is **not** “what happens if you match a random 40k of the 17M.”

## Approach

1. Ingest the public gzip into Delta on Databricks serverless Spark.
2. Run Splink (`SparkAPI`) with from-scratch comparisons on normalized name (Levenshtein with length gates), website host, and geography. Free Edition serverless has no driver JVM, so comparisons use native Spark SQL `levenshtein()` instead of JAR Jaro-Winkler; Splink lineage uses `delta_lake_table` because serverless rejects `persist()`.
3. Block on non-denylisted shared host or `name_first3` + `country_norm`; abort if pair count exceeds 2M.
4. Cluster matches at probability ≥ 0.9 into golden entity IDs.
5. Roll goldens up with shared non-denylisted host (plus a one-hop subsidiary name-prefix rule). Same host + similar name → one golden; same host + different name → sibling goldens under one parent.

Canonical matching settings: [`src/ir_spark/matching.py`](src/ir_spark/matching.py). Notebooks: [`notebooks/`](notebooks/).

## Metric (eval set only)

On `companies_match_sample` only — **do not** read this as the reduction of the 17M file or of a uniform random sample:

| Stage | Count | Reduction vs previous | Reduction vs records |
| --- | --- | --- | --- |
| Records | 40,100 | — | — |
| Golden entities | 33,697 | −16% | −16% |
| Ultimate parents | 27,156 | −19% | −32% |

**Disposition** on scored pairs (enriched run, not labeled for precision): 7,033 match (≥0.9), 857 review (0.5–0.9), 5,581 in 0.2–0.5, 222,882 non-match (&lt;0.2); `pair_n=236,353`; λ ≈ 1.86×10⁻⁵; `max_cluster_size=8`.

Phase 4 rollup is almost entirely shared host (`name_prefix_links=5`). Shared host is not eTLD+1 / public-suffix root domain. There is no parent/subsidiary column in the source — ultimate parent is inferred only.

Read-only verification: [`notebooks/05_eval_metrics.py`](notebooks/05_eval_metrics.py) asserts these counts against Delta. Canonical findings: [`PROJECT_BRIEF.md`](PROJECT_BRIEF.md) § Evaluation findings.

## What would change at ZoomInfo-like scale

A 40k batch on Free Edition is enough to walk through golden matching, disposition, and hierarchy mechanics. At **500M+ profiles** you would not re-match the world every night: you would need **incremental / streaming matching**, a **candidate-generation service**, and much more aggressive blocking — plus cluster sizing, job orchestration, and cost management this project does not pretend to have practiced.

## What this is not

Not production software, not a portfolio flagship, not a claim of Databricks seniority. Cluster tuning, Unity Catalog, jobs, and cost stay named as open gaps.

## Appendix

**Clone and local smoke test**

```bash
uv sync --frozen
uv run ir-spark-smoke          # Spark + Splink SparkAPI (no company data)
uv run ir-spark-download       # public gzip → data/raw/ (gitignored)
uv run ir-spark-sample         # local 30k CSV for DQ profiling
uv run ir-spark-profile        # aggregate DQ JSON only
```

**Databricks notebooks** (import to personal workspace; serverless default): `01_environment_smoke` → `02_data_ingestion` → `02b_match_sample` → `03_entity_matching` → `04_hierarchy_rollup` → `05_eval_metrics` (read-only verifier). Details: [`notebooks/README.md`](notebooks/README.md).

**License.** Code here is for personal interview prep. The company snapshot is ODC-By (BigPicture); this repo does not redistribute it.
