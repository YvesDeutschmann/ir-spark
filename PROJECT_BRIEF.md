# Project brief: entity resolution on Databricks (Splink + Spark)

Personal, self-contained proof of concept. Interview evidence for a Forward Deployed Engineer conversation — not a portfolio flagship, not production software, not employer work.

**Current status (this checkout):** Phases 1–2 are done locally and on Databricks Free Edition. Phase 2 Delta tables: `workspace.ir_spark.companies_raw` (17,154,017 rows) and `workspace.ir_spark.companies_sample` (30,000 rows). Phases 3–6 are not started.

## Objective

Build a small entity-resolution pipeline that runs Splink's Spark backend on Databricks, deduplicating messy **company** records into golden entity IDs and rolling them into a parent-child hierarchy.

Purpose: hands-on Databricks/Spark practice, plus one clean before/after metric and a short README as interview evidence.

## Hard constraints (do not violate)

- **No PII.** Company-level public data only — no person records, no data or code from any employer system.
- **No employer code or configs.** All Splink comparison/blocking logic must be written from scratch for this project. Do not port or reference any production ruleset, comparison config, or schema from prior work.
- **Personal environment only.** Databricks Free Edition signed up with a personal email, not tied to any employer domain or billing.
- **Public dataset only**, sourced below.

## Dataset

**17M+ Company Dataset** (People Data Labs "Free Company Dataset" snapshot, mirrored on Kaggle)

- Kaggle: `kaggle.com/datasets/mfrye0/bigpicture-company-dataset`
- Hugging Face mirror used for unauthenticated download: `bigpictureio/companies-2023-q4-sm` (`companies-2023-q4-sm.csv.gz`)
- Company-level only — no individuals, no PII
- Sample down to ~20–50K rows for iteration speed before scaling up if time allows

Observed columns in the gzip snapshot:

`handle`, `name`, `website`, `industry`, `size`, `type`, `founded`, `city`, `state`, `country_code`

The brief's "domain" / "LinkedIn URL" / "locality" map onto `website`, `handle`, and `city`/`state`/`country_code` in this file.

## Environment

- Databricks Free Edition (`free-edition.cloud.databricks.com` signup; no cloud account or credit card required). Personal workspace is live; CLI profile `yves.deutschmann` is authenticated.
- PySpark on the provided serverless compute (Spark 4.1.0). No classic cluster was created.
- `splink` 4.0.16 in the notebook environment; Spark backend is `from splink import SparkAPI`. Serverless is Spark Connect (no driver JVM), so Phase 1 init skips Splink’s JAR/`sparkContext` hook (`register_udfs_automatically=False`).
- Delta tables for input/output storage within the workspace (Phase 2: `workspace.ir_spark.companies_raw` and `companies_sample`)

Local development (this repo) uses **uv** for Python deps and a local Spark smoke test. That complements the workspace; it does not load company data into Delta.

## Build phases

### Phase 1 — Environment setup (done)

- [x] Sign up for Databricks Free Edition with personal email
- [x] Create workspace, confirm a Spark session runs (`spark.range(10).show()` or equivalent smoke test)
- [x] Install `splink` in the notebook environment, confirm import and Spark backend initialize without error
- **Acceptance (met):** a notebook cell on Free Edition default serverless runs Spark + Splink imports with no manual cluster config. Proven output: `spark=4.1.0 range10=10 splink=4.0.16 SparkAPI=SparkAPI` on `/Users/yves.deutschmann@gmail.com/ir-spark/01_environment_smoke`

Local check in this repo (does not load company data):

- [x] `uv run ir-spark-smoke` — `spark.range(10)` plus Splink `SparkAPI` initialize without error
- [x] `notebooks/01_environment_smoke.py` — source for the workspace notebook; imported and executed on Free Edition serverless

### Phase 2 — Data ingestion (done)

- [x] Local download helper: `uv run ir-spark-download` → `data/raw/companies-2023-q4-sm.csv.gz`
- [x] Local sample + aggregate DQ report: `uv run ir-spark-sample`, `uv run ir-spark-profile`
- [x] Databricks notebook source: `notebooks/02_data_ingestion.py`
- [x] Upload the full gzip to UC Volume `/Volumes/workspace/default/ir_spark/` (not workspace files — 500 MB cap; not DBFS root — disabled on Free Edition)
- [x] Run the notebook on Free Edition serverless: gzip → Delta `workspace.ir_spark.companies_raw` (17,154,017 rows) + `workspace.ir_spark.companies_sample` (30,000 rows)
- **Acceptance (met):** notebook exit `ok catalog=workspace full_count=17154017 sample_count=30000 raw_table=workspace.ir_spark.companies_raw sample_table=workspace.ir_spark.companies_sample` on `/Users/yves.deutschmann@gmail.com/ir-spark/02_data_ingestion`

Local commands (no Spark/Delta):

```bash
uv run ir-spark-download
uv run ir-spark-sample      # default n=30000 seed=42 → data/interim/
uv run ir-spark-profile     # aggregate JSON → data/processed/dq_summary.json
```

Volume path used:

```bash
databricks fs cp data/raw/companies-2023-q4-sm.csv.gz \
  dbfs:/Volumes/workspace/default/ir_spark/companies-2023-q4-sm.csv.gz \
  --profile yves.deutschmann
```

### Phase 3 — Entity matching (Splink on Spark) (not started)

Implementation plan (no code yet): [`PHASE3_PLAN.md`](PHASE3_PLAN.md). Native Spark SQL Levenshtein for names (JAR-free on Free Edition serverless); exact host after Phase 2 normalization; exact city/country.

- [ ] Define comparison logic from scratch: fuzzy company name (Levenshtein, not Jaro-Winkler), exact domain match, locality/country match
- [ ] Define blocking rules to keep the comparison space tractable (e.g. block on first token of domain, or country + first letter of name)
- [ ] Train/estimate the Splink model (u-probabilities via random sampling, m-probabilities via EM if time allows, otherwise reasonable manual priors)
- [ ] Generate match predictions and cluster into golden entity IDs
- **Acceptance:** a table of `record_id -> golden_entity_id`, with a sanity-checked sample of ~10–20 clusters manually reviewed for plausibility

### Phase 4 — Hierarchy rollup (not started)

- [ ] Add a simple parent-child rule on top of golden entities — e.g. shared root domain, or subsidiary-name-contains-parent-name pattern
- [ ] Roll golden entities up to an "ultimate parent" grouping
- **Acceptance:** a table showing golden entity count vs. ultimate-parent count, with a few example rollups spot-checked manually

### Phase 5 — Quantify and document (not started)

- [ ] Compute one clear metric: raw records → golden entities → ultimate-parent count (with % reduction at each stage)
- [ ] Write a one-page README: problem statement, approach, the metric, 2–3 sentences on what would change running this at ZoomInfo's actual scale (500M+ profiles) — e.g. incremental/streaming matching vs. batch, blocking strategy at scale, cluster sizing
- **Acceptance:** README is readable standalone, in your own words, no dataset-provider boilerplate copied in

### Phase 6 — Interview prep pass (not started)

- [ ] Re-read the JD's specific language ("golden reference matching," "disposition logic," "ultimate-parent rollup," "hierarchy management") and confirm README/talking points use matching terminology
- [ ] Prepare one tight spoken answer: what's the same as prior Splink/OFM experience, what's different about Spark/Databricks specifically, what you'd want to learn next
- **Acceptance:** you can explain the project end-to-end out loud in under 2 minutes without notes

## Explicit non-goals

- Not building a production-grade or scalable system — this is a scoped proof of concept
- Not claiming Databricks seniority (cluster tuning, Unity Catalog, job orchestration, cost management) — those stay named as open gaps
- Not polishing this into a public portfolio piece or blog post — it exists to support interview conversation

## Deliverable checklist

- [ ] One or more Databricks notebooks covering ingestion → matching → rollup
- [ ] One before/after metric, clearly stated
- [ ] One README (markdown, in the workspace or exported)
- [ ] One rehearsed 2-minute verbal summary
