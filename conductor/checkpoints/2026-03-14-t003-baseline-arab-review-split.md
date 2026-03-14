# T003 Checkpoint: Baseline vs Arab-Extension Review Split

- Timestamp: `2026-03-14T20:00:00+03:00`
- Agent: `Codex`
- Scope: `T003 / 4.3`

## What changed
- Restored the historical `legacy` BRCA review surfaces as the default `Pre-GME` and `Final` pages in the supervisor UI:
  - `supervisor_variant_registry_brca_pre_gme_v1`
  - `supervisor_variant_registry_brca_v1`
- Moved the new Arab-aware checkpoints into a separate review page:
  - `supervisor_variant_registry_brca_arab_pre_gme_v2`
  - `supervisor_variant_registry_brca_arab_v2`
- Added a structured `Data Downloads` page that lists:
  - raw source references only
  - per-source normalized artifacts
  - legacy checkpoint CSVs
  - Arab-extension checkpoint CSVs
- Kept the runtime static:
  - no live BigQuery query paths were added
  - downloads resolve to frozen GCS objects only

## Validation outcome
- Canonical key validation now runs against the frozen Parquet artifacts themselves.
- Validated artifacts:
  - `clinvar_normalized_brca`
  - `gnomad_genomes_normalized_brca`
  - `gnomad_exomes_normalized_brca`
  - `shgp_normalized_brca`
  - `gme_normalized_brca`
  - legacy pre-GME checkpoint
  - legacy final checkpoint
  - Arab pre-GME checkpoint
  - Arab final checkpoint
- Result:
  - all checkpoint artifacts are unique on their canonical key
  - all current normalized artifacts have canonical-key consistency
  - `GME` retains `5` exact duplicate canonical keys that differ only in source-level locator fields; this is preserved and documented rather than silently collapsed

## Supervisor-facing interpretation
- The legacy final table is still the stable baseline.
- The Arab extension is now clearly reviewable as a separate proposal layer.
- The supervisor can compare both without losing the original baseline context.

## Remaining open work in T003
- `5.1` Great Expectations suites/checkpoints for harmonized artifacts
- `5.2` Publish GE Data Docs for harmonized validation runs to GCS
