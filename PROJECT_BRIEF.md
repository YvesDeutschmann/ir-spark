# Project brief: entity resolution on Databricks (Splink + Spark)

Personal, self-contained proof of concept. Interview evidence for a Forward Deployed Engineer conversation — not a portfolio flagship, not production software, not employer work.

**Current status (this checkout):** infrastructure only. The brief below is the spec. Phases 2–6 are intentionally unimplemented.

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
- Action (Phase 2, not this checkout): sample down to ~20–50K rows for iteration speed before scaling up if time allows

Observed columns in the gzip snapshot:

`handle`, `name`, `website`, `industry`, `size`, `type`, `founded`, `city`, `state`, `country_code`

The brief's "domain" / "LinkedIn URL" / "locality" map onto `website`, `handle`, and `city`/`state`/`country_code` in this file.

## Environment

- Databricks Free Edition (`free-edition.cloud.databricks.com` signup; no cloud account or credit card required)
- PySpark on the provided serverless compute
- `splink` Python package, Spark backend (`splink.SparkAPI` in current Splink 4 — verify against installed docs; the API has changed across versions)
- Delta tables for input/output storage within the workspace

Local development (this repo) uses **uv** for Python deps and a local Spark smoke test. That does not replace the Databricks workspace.

## Build phases

### Phase 1 — Environment setup

- [ ] Sign up for Databricks Free Edition with personal email
- [ ] Create workspace, confirm a Spark session runs (`spark.range(10).show()` or equivalent smoke test)
- [ ] Install `splink` in the notebook environment, confirm import and Spark backend initialize without error
- **Acceptance:** a notebook cell runs Spark + Splink imports cleanly with no manual cluster config beyond defaults

Local stand-in in this repo: `uv run ir-spark-smoke`.

### Phase 2 — Data ingestion (not started)

- [ ] Download the Kaggle dataset, upload to Databricks (DBFS or workspace volume)
- [ ] Load into a Spark DataFrame, write to a Delta table
- [ ] Profile the data: row count, null rates on name/domain/address fields, obvious duplicate patterns (same domain, near-identical names)
- **Acceptance:** a Delta table exists with a documented row count and a short data-quality summary (nulls, dupes) captured in the notebook

Local raw file only (no Spark/Delta): `uv run ir-spark-download` → `data/raw/companies-2023-q4-sm.csv.gz`.

### Phase 3 — Entity matching (Splink on Spark) (not started)

- [ ] Define comparison logic from scratch: fuzzy company name (e.g. Jaro-Winkler or Levenshtein), exact/fuzzy domain match, locality/country match
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
