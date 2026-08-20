# ir-spark

Personal proof of concept: **golden reference matching** for messy company records with Splink on Spark, then a light **ultimate-parent rollup**. Built to get real Databricks/Spark time on a laptop-and-Free-Edition setup — not to ship a production matcher.

The canonical spec is [`PROJECT_BRIEF.md`](PROJECT_BRIEF.md). This README is the working map of the repo.

## What this is (and is not)

Company-level public data only. No people, no PII, no employer extracts, no ported production comparison/blocking configs.

It is **not** a claim of Databricks seniority. Cluster tuning, Unity Catalog, job orchestration, and cost management stay named as gaps. It is also not a flagship public portfolio piece.

**Phase 2 ingestion is done** (local sample/profile + Databricks Delta tables). Splink matching, hierarchy rollup, and the interview metric are Phases 3–6. Phase 3 is planned in [`PHASE3_PLAN.md`](PHASE3_PLAN.md) (not implemented).

## Approach (planned)

1. Sample the public company snapshot to a tractable slice (~20–50k rows).
2. On Databricks serverless Spark, run Splink (`SparkAPI`) with from-scratch comparisons on name / website / geography and tight blocking.
3. Cluster matches into **golden entity IDs**.
4. Apply a simple parent-child rule (shared registrable domain, or subsidiary name containing parent name) for **hierarchy management** / ultimate-parent grouping.
5. Report one metric: raw records → golden entities → ultimate parents, with percent reduction at each stage.

Target interview language: golden reference matching, disposition logic (match / non-match / review), ultimate-parent rollup, hierarchy management.

## Repo layout

| Path | Role |
| --- | --- |
| `PROJECT_BRIEF.md` | Full phase spec and constraints |
| `src/ir_spark/` | Local helpers: download, sample, profile, Spark/Splink smoke |
| `data/raw/` | Gitignored gzip snapshot (see `data/README.md`) |
| `data/interim/` | Gitignored local sample CSV + metadata |
| `data/processed/` | Gitignored aggregate DQ JSON |
| `notebooks/` | Databricks notebooks (Phase 1 smoke + Phase 2 ingestion) |
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

### Upload the full gzip to a UC Volume

Do **not** use workspace files (500 MB per-file cap; the gzip is ~600 MB) or DBFS root (disabled on Free Edition). This workspace uses catalog `workspace`. Volume already created and uploaded:

`/Volumes/workspace/default/ir_spark/companies-2023-q4-sm.csv.gz`

```bash
databricks fs cp data/raw/companies-2023-q4-sm.csv.gz \
  dbfs:/Volumes/workspace/default/ir_spark/companies-2023-q4-sm.csv.gz \
  --profile yves.deutschmann
```

`02_data_ingestion.py` is imported at `/Users/yves.deutschmann@gmail.com/ir-spark/02_data_ingestion` and has been run on serverless. Proven tables:

- `workspace.ir_spark.companies_raw` — 17,154,017 rows
- `workspace.ir_spark.companies_sample` — 30,000 rows (Phase 3 input)

If a full Delta write hits Free Edition fair-usage limits, the notebook still writes `companies_sample` and records the full row `count()` when that scan completes.

Serverless is Spark Connect, so the Phase 1 notebook skips Splink’s JAR/`sparkContext` hook. Fuzzy JAR comparisons stay a Phase 3 concern.

## What would change at ZoomInfo-like scale (preview)

A 20–50k batch on Free Edition is enough to talk through the mechanics. At 500M+ profiles you would not re-match the world every night: you would need incremental / streaming matching, much more aggressive blocking (and probably a candidate-generation service), and cluster sizing that this project will not pretend to have practiced. That paragraph belongs in the Phase 5 write-up once there is a metric to hang it on.

## License and attribution

Code in this repository is for personal interview prep. The company snapshot is published under the Open Data Commons Attribution License (ODC-By) by BigPicture; this repo does not redistribute that file.
