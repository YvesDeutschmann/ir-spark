# ir-spark

Personal proof of concept: **golden reference matching** for messy company records with Splink on Spark, then a light **ultimate-parent rollup**. Built to get real Databricks/Spark time on a laptop-and-Free-Edition setup — not to ship a production matcher.

The canonical spec is [`PROJECT_BRIEF.md`](PROJECT_BRIEF.md). This README is the working map of the repo.

## What this is (and is not)

Company-level public data only. No people, no PII, no employer extracts, no ported production comparison/blocking configs.

It is **not** a claim of Databricks seniority. Cluster tuning, Unity Catalog, job orchestration, and cost management stay named as gaps. It is also not a flagship public portfolio piece.

**Phases 1–4 are done** (local tooling + Databricks Delta tables through `companies_hierarchy`). Evaluation findings live in [`PROJECT_BRIEF.md`](PROJECT_BRIEF.md) — the −16% / −32% funnel is on the **enriched eval slice**, not a random 17M draw. The interview one-pager is Phase 5.

## Approach (planned)

1. Sample the public company snapshot to a tractable slice (~20–50k rows).
2. On Databricks serverless Spark, run Splink (`SparkAPI`) with from-scratch comparisons on name / website / geography and tight blocking. Free Edition's serverless compute doesn't expose a driver JVM, so JAR-accelerated Jaro-Winkler is unavailable — Phase 3 uses native Spark SQL Levenshtein instead, which sacrifices a bit of match nuance on typos but avoids the platform dependency entirely.
3. Cluster matches into **golden entity IDs**.
4. Apply a simple parent-child rule (shared registrable domain, or subsidiary name containing parent name) for **hierarchy management** / ultimate-parent grouping.
5. Report one metric: raw records → golden entities → ultimate parents, with percent reduction at each stage.

Target interview language: golden reference matching, disposition logic (match / non-match / review), ultimate-parent rollup, hierarchy management.

## Repo layout

| Path | Role |
| --- | --- |
| `PROJECT_BRIEF.md` | Full phase spec and constraints |
| `src/ir_spark/` | Local helpers: download, sample, profile, matching settings, Spark/Splink smoke |
| `data/raw/` | Gitignored gzip snapshot (see `data/README.md`) |
| `data/interim/` | Gitignored local sample CSV + metadata |
| `data/processed/` | Gitignored aggregate DQ JSON |
| `notebooks/` | Databricks notebooks (Phase 1 smoke, Phase 2 ingestion, 02b match sample, Phase 3 matching, Phase 4 rollup) |
| `.cursor/rules/` | Project rules for later agents |
| `.cursor/environment.json` | Cloud Agent install (`uv sync`) |

## Setup (uv)

This project uses [uv](https://docs.astral.sh/uv/) for Python 3.12.

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
./scripts/install.sh          # or: uv sync --frozen
uv run ir-spark-smoke         # local Phase 1: Spark + Splink SparkAPI (+ similarity JAR)
```

Useful commands:

```bash
uv add <package>              # runtime dependency
uv add --dev <package>        # dev dependency
uv lock                       # refresh uv.lock after editing pyproject.toml
uv run ruff check
```

Do not use Poetry or a loose global `pip install` as the project workflow.

## Dataset and Phase 2 (local)

Public 17M+ company snapshot (Kaggle `mfrye0/bigpicture-company-dataset`; Hugging Face gzip used here so no Kaggle token is required):

```bash
uv run ir-spark-download      # → data/raw/companies-2023-q4-sm.csv.gz (~601 MiB, pinned sha256)
uv run ir-spark-sample        # → data/interim/companies_sample.csv (default n=30000, seed=42)
uv run ir-spark-profile       # → data/processed/dq_summary.json (aggregates only)
```

The gzip and derived CSVs are **not** committed. Profiling never prints full company rows.

Masked local 30k-sample DQ (`uv run ir-spark-profile`, seed 42): name null 0.03%; website 20.1%; handle 0%; city 20.8%; state 28.8%; country_code 18.1%. Exact duplicate rows: 0. Same-host collision groups (≥2): 94. Normalized-name collision groups (≥2): 7.

## Databricks

Personal Free Edition workspace, default serverless compute (no cluster config). CLI profile: `yves.deutschmann`.

| Notebook source | Workspace path (after import) | Purpose |
| --- | --- | --- |
| `notebooks/01_environment_smoke.py` | `.../01_environment_smoke` | Phase 1: Spark + Splink import |
| `notebooks/02_data_ingestion.py` | `.../02_data_ingestion` | Phase 2: gzip → Delta + DQ summary |
| `notebooks/02b_match_sample.py` | `.../02b_match_sample` | Phase 3 close-out: duplicate-enriched sample |
| `notebooks/03_entity_matching.py` | `.../03_entity_matching` | Phase 3: Splink dedupe → `companies_golden` |
| `notebooks/04_hierarchy_rollup.py` | `.../04_hierarchy_rollup` | Phase 4: golden → ultimate parent |

### Upload the full gzip to a UC Volume

Do **not** use workspace files (500 MB per-file cap; the gzip is ~600 MB) or DBFS root (disabled on Free Edition). This workspace uses catalog `workspace`. Volume already created and uploaded:

`/Volumes/workspace/default/ir_spark/companies-2023-q4-sm.csv.gz`

```bash
databricks fs cp data/raw/companies-2023-q4-sm.csv.gz \
  dbfs:/Volumes/workspace/default/ir_spark/companies-2023-q4-sm.csv.gz \
  --profile yves.deutschmann
```

`02_data_ingestion.py` is imported at `/Users/yves.deutschmann@gmail.com/ir-spark/02_data_ingestion` and has been run on serverless. `02b_match_sample.py`, `03_entity_matching.py`, and `04_hierarchy_rollup.py` have been imported and run on serverless. Proven tables:

- `workspace.ir_spark.companies_raw` — 17,154,017 rows
- `workspace.ir_spark.companies_sample` — 30,000 rows (random slice; matching sparsity baseline)
- `workspace.ir_spark.companies_match_sample` — 40,100 rows (host / name+country groups size 2–10 plus filler)
- `workspace.ir_spark.companies_golden` — 40,100 rows (`record_id`, `golden_entity_id`; 33,697 goldens at match threshold 0.9)
- `workspace.ir_spark.companies_hierarchy` — 40,100 rows (`record_id`, `golden_entity_id`, `ultimate_parent_id`; 27,156 parents)

If a full Delta write hits Free Edition fair-usage limits, the notebook still writes `companies_sample` and records the full row `count()` when that scan completes.

Serverless is Spark Connect, so Phase 1 skips Splink's JAR/`sparkContext` hook. Phase 3 comparisons use native Spark SQL `levenshtein()` — not JAR Jaro-Winkler — and Splink lineage uses `delta_lake_table` because serverless rejects `persist()`. Canonical settings: `src/ir_spark/matching.py`.

Random-sample matching exit (baseline): `ok sample_n=30000 pair_n=71551 cluster_n=30000 singleton_n=30000 max_cluster_size=1 training=em`. Enriched matching exit: `ok sample_n=40100 pair_n=236353 cluster_n=33697 singleton_n=27763 max_cluster_size=8 training=em lambda=1.86467e-05 p_ge_09=7033`. Hierarchy exit: `ok record_n=40100 golden_n=33697 parent_n=27156`. Funnel on the **eval set only**: 40,100 records → 33,697 goldens (−16%) → 27,156 ultimate parents (−19% from goldens, −32% overall).

## Evaluation findings

Canonical write-up: [`PROJECT_BRIEF.md`](PROJECT_BRIEF.md) § Evaluation findings. Short version:

- This snapshot is an already-keyed company census (unique LinkedIn `handle`s), not a messy CRM. A random 30k match run produced all singletons; leftover scores peaked at ≈0.20.
- The 17M table still has many size 2–10 same-host and name+country groups. `companies_match_sample` (40,100) oversamples those groups plus filler so Splink can train. Handles remain unique on that slice too.
- Do **not** present 40,100 → 27,156 as the reduction of the 17M file or of a uniform sample. Matching vs hierarchy: same host + similar name → one golden; same host + different name → sibling goldens under one parent. Phase 4 is almost entirely shared host (5 name-prefix links). ≥0.9 pairs were not labeled for precision.
- Phase 5 must say: (1) random sample correctly did nothing, (2) eval set was collision-enriched from the 17M, (3) funnel + match/review/non-match on that set, (4) ZoomInfo-scale would be incremental matching, not a 40k batch.

## What would change at ZoomInfo-like scale (preview)

A 20–50k batch on Free Edition is enough to talk through the mechanics. At 500M+ profiles you would not re-match the world every night: you would need incremental / streaming matching, much more aggressive blocking (and probably a candidate-generation service), and cluster sizing that this project will not pretend to have practiced. That paragraph belongs in the Phase 5 write-up once there is a metric to hang it on.

## License and attribution

Code in this repository is for personal interview prep. The company snapshot is published under the Open Data Commons Attribution License (ODC-By) by BigPicture; this repo does not redistribute that file.
