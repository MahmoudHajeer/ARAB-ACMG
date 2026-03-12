# T002 Checkpoint: Static Review Freeze (2026-03-12)

## Goal
Stop BigQuery runtime spend from the supervisor dashboard while preserving all review evidence and the current BRCA checkpoint outputs.

## What changed
- Exported the processed checkpoint outputs from BigQuery to GCS:
  - `gs://mahmoud-arab-acmg-research-data/frozen/supervisor_review/snapshot_date=2026-03-12/pre_gme/supervisor_variant_registry_brca_pre_gme_v1.parquet`
  - `gs://mahmoud-arab-acmg-research-data/frozen/supervisor_review/snapshot_date=2026-03-12/pre_gme/supervisor_variant_registry_brca_pre_gme_v1.xlsx`
  - `gs://mahmoud-arab-acmg-research-data/frozen/supervisor_review/snapshot_date=2026-03-12/final/supervisor_variant_registry_brca_v1.parquet`
  - `gs://mahmoud-arab-acmg-research-data/frozen/supervisor_review/snapshot_date=2026-03-12/final/supervisor_variant_registry_brca_v1.csv`
- Made the final CSV artifact publicly downloadable for the review UI:
  - `https://storage.googleapis.com/mahmoud-arab-acmg-research-data/frozen/supervisor_review/snapshot_date=2026-03-12/final/supervisor_variant_registry_brca_v1.csv`
- Wrote a frozen UI review bundle at:
  - local: `ui/review_bundle.json`
  - GCS: `gs://mahmoud-arab-acmg-research-data/frozen/supervisor_review/snapshot_date=2026-03-12/review_bundle.json`
- Removed non-raw BigQuery outputs from active use:
  - deleted all tables from `arab_acmg_harmonized`
  - deleted all tables from `arab_acmg_results`

## New cost posture
- BigQuery remains only for the raw source-of-truth datasets in `arab_acmg_raw`.
- The deployed supervisor UI now serves frozen JSON and GCS artifacts instead of issuing BigQuery queries at runtime.
- The only download left in the UI is the frozen final-registry CSV.

## Verification
- `python3 scripts/freeze_supervisor_review_bundle.py` -> pass
- `python3 scripts/decommission_supervisor_bigquery_outputs.py` -> pass
- `python3 scripts/verify_supervisor_registry.py` -> pass
- `python3 scripts/verify_bq_health.py` -> pass
- `python3 scripts/verify_gcp.py` -> pass
- `python3 -m pytest -q tests` -> `54 passed`
- Playwright local UI check on `http://127.0.0.1:8082/` -> pass

## Notes
- The raw BigQuery tables were intentionally left untouched.
- Future BRCA work should read the frozen checkpoint artifacts from GCS/DuckDB unless a new raw extraction from `arab_acmg_raw` is explicitly required.
