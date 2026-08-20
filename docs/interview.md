# Interview crib (personal — not for sharing)

Numbers: copy from [PROJECT_BRIEF.md](../PROJECT_BRIEF.md) § Evaluation findings only — never from Delta, notebook 05, or memory.

JD terms: map only where this PoC earned them per [zoominfo-senior-fde.md](zoominfo-senior-fde.md). No PII, no employer artifacts, no posting salary or req ID.

---

## Practiced vs not

**Earned (may say)**

- **Golden reference matching** → `golden_entity_id` on one public company snapshot (not a cross-system persistent ID).
- **Disposition logic** → unlabeled pair-score bins: match ≥0.9, review 0.5–0.9, non-match (not orphaned-account or inactive-entity codes).
- **Hierarchy management** → **ultimate-parent rollup** via shared non-denylisted host (`name_prefix_links=5`); weak stand-in for legal-entity families.
- Entity resolution on a collision-enriched ≤50k eval set; Databricks Free Edition serverless Spark as the compute that ran.

**Not earned (must not say)**

- Legal-entity / D&B / tax ID / company-house crosswalks; location-level vs HQ.
- Orphaned-account or inactive-entity disposition; marketability classification; domain-validation pipeline.
- Labeled 98–99% accuracy; TAM/SAM/SOM, white space, buying-group filters, fit scoring, intent.
- Cross-system persistent IDs; CRM field locking; Snowflake/BigQuery customer deploy.
- Contact / professional-profile matching; no-human-in-the-loop production orchestration.

---

## 2-minute script (spoken)

I built a small Databricks proof of concept for **golden reference matching** on public company records — company-level only, no people.

The source is closer to an already-keyed census than a dirty CRM: handles are unique, so on a random 30k slice Splink correctly produced all singletons. That is sparsity, not a matcher failure.

The 17M file still has same-host and name-plus-country collisions, so I built a collision-enriched eval set — 40,100 rows — to train and test. I ran Splink on Spark with blocking on shared host and name prefix plus country, then applied **disposition logic** on scored pairs: 7,033 match at 0.9 or above, 857 review, the rest non-match — unlabeled, not a precision claim. Clustering at 0.9 gave 33,697 golden entities.

For **hierarchy management**, I rolled those goldens up with an **ultimate-parent rollup** on shared host — almost entirely host, not legal-entity structure. On that eval set only: 40,100 records to 33,697 goldens to 27,156 ultimate parents.

At the scale in the JD — 500M-plus profiles, warehouse as the operating layer — this would be incremental matching and candidate generation in a customer environment, not a 40k batch. I practiced the mechanics on Free Edition serverless; jobs, Unity Catalog, and cost are open gaps.

---

## Same / different / next (follow-up only)

- **Same:** Splink-style Fellegi–Sunter, blocking, comparisons, EM, pair-score disposition, cluster-to-golden.
- **Different here:** Spark backend on Databricks serverless — no JAR Jaro-Winkler; Delta lineage instead of `persist()`.
- **Next (unpracticed):** incremental matching / candidate generation at 500M+ profiles; legal-entity hierarchy vs shared host; labeled disposition quality; warehouse-as-operating-layer in customer Snowflake or BigQuery (jobs / UC / cost stay gaps).

---

## Do not say (not spoken)

From PROJECT_BRIEF overclaim list:

- 40,100 → 27,156 as the reduction of the 17M file or a random slice.
- Random-sample all-singleton run as a matching failure.
- Labeled golden-reference quality, Unilever-style hierarchy, or ZoomInfo-scale messy-profile matching from this census.

JD conflation — do not analogize this PoC to posting case studies:

- 7,444 / 98–99% / 10K priority sample (7,033 here is unlabeled pairs on a different eval design).
- 1.8M domain-validation / coverage-gap reframing.
- Disney 5,600 → 31 buying-group filtering.
