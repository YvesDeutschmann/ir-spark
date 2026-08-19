# ir-spark

Personal proof of concept: **golden reference matching** for messy company records with Splink on Spark, then a light **ultimate-parent rollup**. Built to get real Databricks/Spark time on a laptop-and-Free-Edition setup — not to ship a production matcher.

The canonical spec is [`PROJECT_BRIEF.md`](PROJECT_BRIEF.md). This README is the working map of the repo.

## What this is (and is not)

Company-level public data only. No people, no PII, no employer extracts, no ported production comparison/blocking configs.

It is **not** a claim of Databricks seniority. Cluster tuning, Unity Catalog, job orchestration, and cost management stay named as gaps. It is also not a flagship public portfolio piece.

**This checkout stops at infrastructure.** Phases 2–6 (Spark/Delta ingestion, Splink matching, hierarchy, the interview metric) are specified in the brief and not implemented yet.

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
| `src/ir_spark/` | Local helpers: dataset download + Spark/Splink smoke |
| `data/raw/` | Gitignored gzip snapshot (see `data/README.md`) |
| `notebooks/` | Databricks notebooks (Phase 1+; matching notebooks not started) |
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

## Dataset

Public 17M+ company snapshot (Kaggle `mfrye0/bigpicture-company-dataset`; Hugging Face gzip used here so no Kaggle token is required):

```bash
uv run ir-spark-download
```

That writes `data/raw/companies-2023-q4-sm.csv.gz` (~601 MiB) and verifies a pinned sha256. The file is **not** committed. Sampling, profiling, and Delta load are Phase 2.

## Databricks (Phase 1, manual)

1. Sign up at [Databricks Free Edition](https://www.databricks.com/learn/free-edition) with a **personal** email.
2. In a serverless notebook, run `spark.range(10).show()`, then `%pip install splink` and `from splink import SparkAPI`.
3. A starter notebook lives at `notebooks/01_environment_smoke.py` (imports only — no company data).

## What would change at ZoomInfo-like scale (preview)

A 20–50k batch on Free Edition is enough to talk through the mechanics. At 500M+ profiles you would not re-match the world every night: you would need incremental / streaming matching, much more aggressive blocking (and probably a candidate-generation service), and cluster sizing that this project will not pretend to have practiced. That paragraph belongs in the Phase 5 write-up once there is a metric to hang it on.

## License and attribution

Code in this repository is for personal interview prep. The company snapshot is published under the Open Data Commons Attribution License (ODC-By) by BigPicture; this repo does not redistribute that file.
