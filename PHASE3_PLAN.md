# Phase 3 plan: entity matching (Splink on Spark)

Planning only. Do not implement until this plan is accepted.

Phase 3 is the first Splink matching pass: company **dedupe** on Databricks Free Edition serverless, producing `record_id → golden_entity_id`. Input is the existing 30k slice `workspace.ir_spark.companies_sample`. The 17M table stays unused.

Canonical constraints still apply: public company records only, no PII, no employer comparison/blocking configs, no raw-data commits, masked QA only.

## Why this design (interview framing)

Free Edition’s serverless compute is Spark Connect. There is no driver JVM, so Splink cannot attach its similarity JAR (`spark.udf.registerJavaFunction` / `sparkContext`). Phase 1 already initializes `SparkAPI(..., register_udfs_automatically=False)` for that reason.

The JAR exists specifically for string similarities Spark SQL does not ship: Jaro, Jaro-Winkler, Jaccard, Damerau-Levenshtein, cosine. **Levenshtein distance is native Spark SQL** (`levenshtein(str1, str2)` / `F.levenshtein`). Exact equality and the Phase 2 regex host-normalization are also native.

So Phase 3 treats the substitution as a platform-aware comparison choice, not a workaround:

> Free Edition's serverless compute doesn't expose a driver JVM, so I couldn't use Splink's JAR-accelerated Jaro-Winkler comparison — I substituted a native Spark SQL Levenshtein comparison instead, which sacrifices a bit of match nuance on typos but avoids the platform dependency entirely.

Trade-off to be able to say out loud: Jaro-Winkler rewards shared prefixes (useful for “Acme Corp” / “Acme Corporation”); Levenshtein counts every edit equally, so legal-suffix noise is expensive unless we strip it first. That suffix strip is ordinary company-name hygiene, and it is what makes default Levenshtein bands `[1, 2]` usable.

Stay on Free Edition. Do not create a classic cluster just to load the JAR.

## Confirmed Splink 4.0.16 APIs

Installed notebook version is `splink==4.0.16`. Comparison imports were re-exported at the package root in 4.x; **do not use the Splink 3 `splink.spark.comparison_library` path.**

```python
import splink.comparison_library as cl
from splink import Linker, SettingsCreator, SparkAPI, block_on
from splink.blocking_analysis import (
    count_comparisons_from_blocking_rule,
    cumulative_comparisons_to_be_scored_from_blocking_rules_data,
    n_largest_blocks,
)
```

`splink/comparison_library.py` in v4.0.16 re-exports `LevenshteinAtThresholds`, `ExactMatch`, `JaroWinklerAtThresholds`, `NameComparison`, etc. from `splink.internals.comparison_library`.

Spark dialect function names (from Splink’s `SparkDialect`):

| Splink comparison | SQL function emitted | Spark Connect? |
| --- | --- | --- |
| `LevenshteinAtThresholds` / `LevenshteinLevel` | `levenshtein` | Native — **use** |
| `ExactMatch` | `=` | Native — **use** |
| `JaroWinklerAtThresholds`, `NameComparison` | `jaro_winkler` | JAR — **do not use** |
| `JaroAtThresholds` | `jaro_sim` | JAR — **do not use** |
| `JaccardAtThresholds` | `jaccard` | JAR — **do not use** |
| `DamerauLevenshteinAtThresholds` | `damerau_levenshtein` | JAR — **do not use** |
| `CosineSimilarityAtThresholds` | cosine UDF | JAR — **do not use** |

`NameComparison` is not a generic name helper; its fuzzy bands are Jaro-Winkler (`[0.92, 0.88, 0.7]`). Using it on serverless would fail at SQL execution even if `SparkAPI` init succeeded.

`LevenshteinAtThresholds("name")` levels (defaults `distance_threshold_or_thresholds=[1, 2]`):

1. Null
2. Exact match
3. `levenshtein <= 1`
4. `levenshtein <= 2`
5. Else

Training / inference / clustering (4.x linker components, not the old `linker.estimate_*` free functions):

- `linker.training.estimate_probability_two_random_records_match(...)`
- `linker.training.estimate_u_using_random_sampling(max_pairs=...)`
- `linker.training.estimate_parameters_using_expectation_maximisation(...)`
- `linker.inference.predict(...)`
- `linker.clustering.cluster_pairwise_predictions_at_threshold(...)`

## Inputs and derived columns

Read `workspace.ir_spark.companies_sample` (30,000 rows, seed 42). Unique id is already `record_id`.

Materialize JAR-free features **before** the Linker, reusing Phase 2 Spark expressions rather than Splink `ColumnExpression` so blocking and QA can see the same columns.

### `host` (domain)

Copy the Phase 2 notebook expression from `notebooks/02_data_ingestion.py`:

1. `lower(trim(website))`
2. Strip `^https?://`
3. Strip `^www.`
4. `regexp_extract(..., "^([^/]+)", 1)`
5. Empty → null

Then `cl.ExactMatch("host")`. No fuzzy domain. Website is ~20.1% null on the local 30k profile; `NullLevel` inside `ExactMatch` covers that.

Do not parse registrable / eTLD+1 here. Shared-root-domain rollup is Phase 4 hierarchy, not Phase 3 matching.

### `name_norm`

Levenshtein on raw `name` will spend its entire `[1, 2]` budget on case and “Inc” / “LLC”. Normalize first:

1. `lower(trim(name))`, collapse internal whitespace (already in the Phase 2 Spark notebook)
2. Strip a small, original-to-this-repo legal-suffix list already used by the local profiler (`inc`, `llc`, `ltd`, `corp`, `gmbh`, `sa`, `ag`) — Spark `regexp_replace` on the trailing token, not a ported production list
3. Empty → null

Splink comparison: `cl.LevenshteinAtThresholds("name_norm")` (default `[1, 2]`).

If a first Databricks run shows obvious suffix/punctuation misses, the only comparison knob to turn is thresholds (e.g. `[1, 3]`). Do not silently switch to Jaro-Winkler.

Also derive `name_prefix3 = substr(name_norm, 1, 3)` for blocking only.

### Locality

- `city_norm`: `lower(trim(city))`, collapse whitespace, empty → null. Then `cl.ExactMatch("city_norm")`.
- `country_code`: trim; keep the ISO2 string as stored. Then `cl.ExactMatch("country_code")`.

Two separate comparisons, not one concatenated geo key — independent `m`/`u` estimates. Enable term-frequency adjustments on both exact matches (common cities / `US` should not dominate).

`state` stays out of Phase 3 (~28.8% null; not requested).

### `handle`

LinkedIn company slug, 0% null on the local profile. **Not a Splink comparison in the first cut** (the requested comparison set is name / host / city / country). During prep, print *aggregates only*: distinct-handle count vs row count.

If duplicate handles exist, use `l.handle = r.handle` as a deterministic rule for λ estimation and optionally as one blocking rule. If handle is 1:1 with rows, same-handle blocking finds nothing and can be dropped.

## Comparisons (settings)

```text
link_type = "dedupe_only"
unique_id_column_name = "record_id"

comparisons = [
  cl.LevenshteinAtThresholds("name_norm"),                    # native levenshtein
  cl.ExactMatch("host"),                                      # Phase 2 host-norm
  cl.ExactMatch("city_norm").configure(term_frequency_adjustments=True),
  cl.ExactMatch("country_code").configure(term_frequency_adjustments=True),
]
```

All four are JAR-free. Write these from scratch in this repo; do not paste or paraphrase any employer ruleset.

## Blocking

Unrestricted 30k pairs ≈ 450M. Blocking is the Spark-scale lever.

Union (a pair is compared if **any** rule fires):

1. `block_on("host")` — same website. Local profile: 94 same-host groups of size ≥ 2. Misses the ~20% with null website.
2. `block_on("country_code", "name_prefix3")` — geography + name prefix. Tighter than country + first letter (the brief’s example), which can explode if `US` dominates.
3. `block_on("name_norm")` — exact normalized name even when website/country is missing. Local profile: 7 groups of size ≥ 2; cheap, high precision.

Optional fourth, only if handle is not unique: `block_on("handle")`.

**Required gate before `predict()`:** `cumulative_comparisons_to_be_scored_from_blocking_rules_data` and `n_largest_blocks`. Aim for low millions of pairs on 30k, not tens of millions. If rule 2 dominates, tighten to `name_prefix4` or add `city_norm` to that compound rule. Log pair counts only, not block keys.

Do not use `levenshtein(...)` inside a blocking predicate. Splink will accept it; Spark will not execute it as an efficient equijoin.

## Training

Fellegi–Sunter, Splink’s usual hybrid:

1. **λ** — `estimate_probability_two_random_records_match` with JAR-free deterministic rules, e.g.
   - `l.host = r.host`
   - `l.name_norm = r.name_norm and l.country_code = r.country_code`
   - `levenshtein(l.name_norm, r.name_norm) <= 1 and l.city_norm = r.city_norm`
   - Recall starting guess `0.7`; tune only if the prior is absurd relative to same-host collision counts.
2. **u** — `estimate_u_using_random_sampling(max_pairs=1e6)`. Random pairs on 30k are almost all non-matches.
3. **m** — two EM passes so each comparison is estimated at least once (EM cannot estimate the column you blocked on):
   - `block_on("host")` → trains name / city / country
   - `block_on("name_norm")` → trains host / city / country
   Leave `fix_u_probabilities=True` (default).

If EM is slow or unstable on serverless, fall back to explicit `.configure(m_probabilities=..., u_probabilities=...)` on the comparison objects (the brief already allows manual priors). Do not spend Phase 3 on EM diagnostics beyond “did it converge / are m > u on exact-match levels.”

## Predict, cluster, write

- `linker.inference.predict()` (optionally a low floor such as `threshold_match_probability=0.2` to shrink the edge table).
- `linker.clustering.cluster_pairwise_predictions_at_threshold(..., threshold_match_probability=0.9)` as a first cut; inspect the cluster-size histogram and move the threshold rather than adding new comparisons.
- Map Splink’s `cluster_id` → `golden_entity_id`.
- Write Delta `workspace.ir_spark.companies_golden` with `record_id`, `golden_entity_id` only (plus maybe `cluster_size` as an aggregate join). That is the Phase 3 acceptance table.

Do **not** persist the full pairwise prediction table as a UC table unless it is needed for a masked QA cell; it can be large even with blocking.

Interview language for the threshold: **disposition logic** — above cluster threshold = match, between predict floor and cluster threshold = review, below = non-match. Phase 3 only materializes the match clustering; review is a talking point plus a count, not a third table.

## SparkAPI on serverless (copy Phase 1, do not “improve”)

```text
SparkAPI(
  spark_session=spark,
  num_partitions_on_repartition=<small explicit int>,
  register_udfs_automatically=False,
  break_lineage_method="persist",
)
```

Keep the Phase 1 `DATABRICKS_RUNTIME_VERSION` pop around construction. Otherwise Splink’s Databricks branch calls `enable_splink()` (driver JVM) and defaults `break_lineage_method` to `delta_lake_table`. `checkpoint` / parquet lineage-break go through `spark._jsc`, which Spark Connect does not expose. `persist` is the Connect-safe choice already proven in `notebooks/01_environment_smoke.py`.

Shuffle partitions on serverless may be the string `"auto"`; passing an explicit `num_partitions_on_repartition` avoids Splink’s `int()` on that conf.

## Notebook shape

New Databricks source: `notebooks/03_entity_matching.py`.

Suggested cells:

1. Widgets: source table, cluster threshold, output table name.
2. `%pip install splink` + `restartPython` (same as Phase 1; notebook env ≠ uv).
3. SparkAPI init (pattern above) + version print.
4. Load sample, derive `host` / `name_norm` / `name_prefix3` / `city_norm`.
5. Blocking pair counts (aggregates only).
6. `SettingsCreator` + `Linker`.
7. Train (λ, u, two EM passes).
8. Predict + cluster + write `companies_golden`.
9. Masked QA (below).
10. `dbutils.notebook.exit(...)` with `ok` plus row counts, pair counts, cluster count — no sample rows.

Local uv helpers stay download / sample / profile / smoke. Do not add a local JAR-on matching CLI; matching is a Databricks notebook job. A later optional DuckDB dry-run on the gitignored CSV is out of scope unless implementation gets stuck on serverless iteration time.

## Acceptance and masked QA

Acceptance from `PROJECT_BRIEF.md`: a `record_id → golden_entity_id` table, plus a sanity-checked sample of ~10–20 clusters reviewed for plausibility.

Review **in the notebook only**, and **masked**:

- Hash `name_norm` / `host` (same `sha256[:8]` style as `src/ir_spark/profile.py`).
- Show cluster size, comparison-vector columns (`gamma_*`) if `retain_intermediate_calculation_columns=True`, and match probability — not raw company strings in anything that might be committed or pasted into the README.
- Pick a mix: likely true matches (same host, small Levenshtein on name), likely over-merges (same prefix + same country, different host), singletons.
- Print cluster-size histogram and percent of rows in clusters of size ≥ 2.

Never `show()` unmasked company rows. Never commit prediction dumps.

## Out of scope (do not sneak in)

- Phase 4 parent-child / ultimate-parent rollup (shared eTLD+1, “name contains parent”).
- Phase 5 reduction metric and interview one-pager, beyond a one-line README status bump.
- 17M-row matching.
- Classic clusters, JAR install, Unity Catalog design, Jobs orchestration, cost management.
- Jaro-Winkler “just for local Spark” in the Databricks notebook — that would fork the model from the interview artifact.

## Implementation order (when coding starts)

1. `notebooks/03_entity_matching.py` end-to-end on the 30k table.
2. Import + run on Free Edition serverless; iterate thresholds / blocking using pair counts and the masked cluster sample.
3. Docs only after a successful exit: `PROJECT_BRIEF.md` Phase 3 checkboxes, `README.md` approach line, `notebooks/README.md` row, `.cursor/rules/scope.mdc` (Phase 3 notebook now exists; still no Phase 4+).
4. Optional: extract host/name Spark SQL strings into `src/ir_spark/` with a tiny unit test against invented strings (not dataset rows) so local `uv run ruff check` / tests cover normalization parity. Not required for acceptance.

## Testing plan (when coding starts)

- **Automated:** ruff on any new `src/` helpers; if normalization is extracted, unit-test host stripping (`https://www.example.com/path` → `example.com`) and suffix strip (`acme inc` → `acme`) on synthetic strings.
- **Manual (required for acceptance):** run `03_entity_matching` on Free Edition; capture the notebook exit string, cluster-count aggregate, and a masked 10–20 cluster sample. No unmasked screenshots of company names.

## Talking points this phase should earn

- Golden reference matching on Spark, not a local pandas demo.
- Spark Connect / no driver JVM → native `levenshtein` instead of JAR Jaro-Winkler.
- Blocking, not the comparison library, is what makes 30k tractable and is what would have to change at 500M+ profiles.
- Disposition as probability bands; clustering as connected components at a match threshold.
- Named gaps unchanged: cluster tuning, UC, jobs, cost, incremental matching.
