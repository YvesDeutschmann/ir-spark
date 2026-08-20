# Project brief: entity resolution on Databricks (Splink + Spark)

Personal, self-contained proof of concept. Interview evidence for a Forward Deployed Engineer conversation — not a portfolio flagship, not production software, not employer work.

**Current status (this checkout):** Phases 1–4 are done locally and on Databricks Free Edition. Phase 2: `workspace.ir_spark.companies_raw` (17,154,017) and random `companies_sample` (30,000). Phase 3 close-out wrote duplicate-enriched `companies_match_sample` (40,100) and `companies_golden` (40,100 `record_id` → 33,697 `golden_entity_id`). Phase 4 wrote `companies_hierarchy` (27,156 ultimate parents). Evaluation findings are recorded below — Phase 5–6 must use that framing. Phases 5–6 are not started.

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

This snapshot is closer to an **already-keyed company census** (one LinkedIn company page per row) than to a messy CRM extract. See **Evaluation findings** before treating any reduction % as a property of the 17M file.

## Environment

- Databricks Free Edition (`free-edition.cloud.databricks.com` signup; no cloud account or credit card required). Personal workspace is live; CLI profile `yves.deutschmann` is authenticated.
- PySpark on the provided serverless compute (Spark 4.1.0). No classic cluster was created.
- `splink` 4.0.16 in the notebook environment; Spark backend is `from splink import SparkAPI`. Serverless is Spark Connect (no driver JVM), so Phase 1 init skips Splink’s JAR/`sparkContext` hook (`register_udfs_automatically=False`).
- Delta tables for input/output storage within the workspace (Phase 2: `workspace.ir_spark.companies_raw` and `companies_sample`; Phase 3: `companies_match_sample`, `companies_golden`; Phase 4: `companies_hierarchy`)

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

### Phase 3 — Entity matching (Splink on Spark) (done)

Free Edition's serverless compute doesn't expose a driver JVM, so Splink's JAR-accelerated Jaro-Winkler comparison is unavailable. Phase 3 uses native Spark SQL `levenshtein()` instead, which sacrifices a bit of match nuance on typos but avoids the platform dependency entirely (`register_udfs_automatically=False`). Serverless also rejects `DataFrame.persist()`; Splink breaks lineage with `break_lineage_method="delta_lake_table"`.

- [x] Define comparison logic from scratch: fuzzy company name via Levenshtein (with length gates), exact host/domain, exact country and city
- [x] Define blocking rules to keep the comparison space tractable (non-denylisted host OR `name_first3` + `country_norm`; abort if pair count > 2M)
- [x] Train/estimate the Splink model (u-probabilities via random sampling; EM with manual-prior fallback)
- [x] Generate match predictions and cluster into golden entity IDs (`notebooks/03_entity_matching.py` → `workspace.ir_spark.companies_golden`)
- **Acceptance (met, two runs):** Random `companies_sample` (30,000) produced all singletons at 0.9 (`pair_n=71551`, `max_cluster_size=1`) — handles are unique and name+country collisions in that slice are almost absent. Masked diagnostics on `companies_raw` showed the 17M file *does* have size 2–10 same-host (680,392 groups) and name+country (125,401 groups) collisions, so `notebooks/02b_match_sample.py` built `companies_match_sample` (40,100). Re-run exit: `ok sample_n=40100 pair_n=236353 host_pair_n=16372 prefix_pair_n=226967 cluster_n=33697 singleton_n=27763 max_cluster_size=8 training=em lambda=1.86467e-05 p_lt_02=222882 p_02_05=5581 p_05_09=857 p_ge_09=7033 max_p≈1.0`. Golden table is 40,100 rows / 33,697 distinct `golden_entity_id`.

### Phase 4 — Hierarchy rollup (done)

- [x] Add a simple parent-child rule on top of golden entities — shared non-denylisted host, plus a one-hop subsidiary-name-prefix pattern (`notebooks/04_hierarchy_rollup.py`)
- [x] Roll golden entities up to an "ultimate parent" grouping
- **Acceptance (met):** `workspace.ir_spark.companies_hierarchy` is `record_id`, `golden_entity_id`, `ultimate_parent_id`. Notebook exit: `ok record_n=40100 golden_n=33697 parent_n=27156 name_prefix_links=5 multi_parent_goldens=11845`. Masked review of multi-golden parent groups ran in the workspace (unmask widget off).

## Evaluation findings (record before Phase 5)

These are the conclusions from the Phase 3 close-out diagnostics and the enriched matching / rollup runs. Phase 5–6 must not contradict them.

**The brief is still satisfied if the write-up is honest.** The purpose is a Databricks/Splink PoC, one before/after metric, and interview evidence — not a realistic model of dirty CRM dedupe. This public census can still deliver that. It cannot support “we cleaned a messy company file” unless evaluation design stays in the frame. Do **not** switch datasets or match all 17M to chase a larger reduction.

### What the data actually is

- Handles are unique: 30,000 / 30,000 on random `companies_sample`; 17,154,016 / 17,154,017 on `companies_raw`. Rows are distinct LinkedIn company pages, not duplicate CRM rows.
- Local 30k DQ (reservoir seed 42, **not** the same rows as the Databricks `orderBy(rand(42))` sample): 0 exact duplicate rows; handle null 0%.
- There is **no parent / subsidiary column**. Ultimate-parent is inferred only.

Masked Databricks SQL (matching `name_norm` / host / `country_norm`; host counts exclude `GENERIC_HOST_DENYLIST`):

| | `companies_sample` (30k random) | `companies_raw` (17.2M) |
| --- | --- | --- |
| Distinct handles | 30,000 / 30,000 | 17,154,016 / 17,154,017 |
| Name groups size 2–10 | 12 groups (29 rows) | 427,649 groups (1,010,657 rows) |
| Name+country groups size 2–10 | 1 group (2 rows) | 125,401 groups (267,664 rows) |
| Non-denylist host groups size 2–10 | 68 groups (197 rows) | 680,392 groups (1,531,981 rows) |
| Max non-denylist host group | 10 | 6,373 (630 hosts with n > 80) |

### Random sample vs eval sample

- Uniform 30k matching: `pair_n=71551`, **all singletons**, `max_cluster_size=1`. Leftover predict table from that run had **max match probability ≈ 0.20**, so lowering the 0.9 threshold to 0.5 would not have created clusters. Fellegi–Sunter λ estimated from almost no deterministic-rule pairs stays tiny; that is expected on a unique-handle census, not a comparator failure (Levenshtein vs Jaro-Winkler is a sideshow here).
- The 17M file **does** contain small same-host and same-name+country collisions. `02b_match_sample` pulled groups of size 2–10 plus random filler → `companies_match_sample` (40,100; 18,486 host-collision rows, 11,644 name+country rows, 10,000 filler; still 40,100 distinct handles).
- That enrichment is a **valid evaluation design**. It is not “what happens if you match a random 40k of the 17M.”

### Metric (eval set only)

On `companies_match_sample` only:

| Stage | Count | Reduction vs previous | Reduction vs records |
| --- | --- | --- | --- |
| Records | 40,100 | — | — |
| Golden entities | 33,697 | −16% | −16% |
| Ultimate parents | 27,156 | −19% | −32% |

Matching calibration (enriched run): `training=em`, λ = 1.86×10⁻⁵, `pair_n=236353` (`host_pair_n=16372`, `prefix_pair_n=226967`), `max_cluster_size=8`, `singleton_n=27763`. Disposition on scored pairs: **7,033 match** (≥0.9), **857 review** (0.5–0.9), **222,882 non-match** (<0.2) plus 5,581 in 0.2–0.5. Precision of the ≥0.9 pairs was **not** labeled against ground truth.

### Matching vs hierarchy

- Same host + similar name → one golden (matching).
- Same host + different name → sibling goldens under one parent (hierarchy).
- Phase 4 is almost entirely **shared non-denylisted host** (`name_prefix_links=5`). The eval sample was itself drawn from same-host groups, so some parent reduction is “we oversampled same-host rows, then grouped remaining same-host goldens.” That split is coherent; it is a weak stand-in for named corporate families (no parent column to check). Shared host is also not eTLD+1 / public-suffix root domain.

### What would overclaim (do not write this)

- Presenting 40,100 → 27,156 as the reduction of “the 17M company dataset” or of a random slice.
- Calling the random-sample all-singleton run a matching failure rather than a sampling / prior-calibration result.
- Claiming labeled golden-reference quality, Unilever-style hierarchy, or ZoomInfo-scale messy-profile matching from this census.

### Phase 5–6 must say

1. Random sample: unique handles; matcher correctly did nothing.
2. 17M still has small same-host / same-name+country collisions; we built a ≤50k eval set from those.
3. On that set: 40,100 → 33,697 → 27,156, with match / review / non-match counts.
4. At ZoomInfo-like scale (500M+ profiles) this would be incremental matching and a candidate-generation service, not a 40k batch — not practiced here.

## Build phases (continued)

### Phase 5 — Quantify and document (not started)

- [ ] Compute one clear metric: raw records → golden entities → ultimate-parent count (with % reduction at each stage) — **on `companies_match_sample`, with the random 30k as the sparsity baseline**, per Evaluation findings
- [ ] Write a one-page README: problem statement, approach, the metric, 2–3 sentences on what would change running this at ZoomInfo's actual scale (500M+ profiles) — e.g. incremental/streaming matching vs. batch, blocking strategy at scale, cluster sizing. Use the four bullets above; do not present the enriched funnel as a random-sample result.
- **Acceptance:** README is readable standalone, in your own words, no dataset-provider boilerplate copied in; evaluation framing matches this section

### Phase 6 — Interview prep pass (not started)

- [ ] Re-read the JD's specific language ("golden reference matching," "disposition logic," "ultimate-parent rollup," "hierarchy management") and confirm README/talking points use matching terminology
- [ ] Prepare one tight spoken answer: what's the same as prior Splink/OFM experience, what's different about Spark/Databricks specifically, what you'd want to learn next
- **Acceptance:** you can explain the project end-to-end out loud in under 2 minutes without notes

## Explicit non-goals

- Not building a production-grade or scalable system — this is a scoped proof of concept
- Not claiming Databricks seniority (cluster tuning, Unity Catalog, job orchestration, cost management) — those stay named as open gaps
- Not polishing this into a public portfolio piece or blog post — it exists to support interview conversation

## Deliverable checklist

- [x] One or more Databricks notebooks covering ingestion → matching → rollup
- [ ] One before/after metric, clearly stated
- [ ] One README (markdown, in the workspace or exported)
- [ ] One rehearsed 2-minute verbal summary
