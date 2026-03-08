# Checkpoint: 2026-03-08 T002 Raw gnomAD Load Complete + UI Samples

## Scope
- Closed the raw gnomAD BigQuery ingestion path for the required chromosomes only.
- Added a fixed `10`-row sample view for each raw dataset in the supervisor UI.

## Confirmed Raw Tables
- `genome-services-platform.arab_acmg_raw.clinvar_raw_vcf` -> `4,388,226` rows
- `genome-services-platform.arab_acmg_raw.gnomad_v4_1_genomes_chr13_raw` -> `24,993,778` rows
- `genome-services-platform.arab_acmg_raw.gnomad_v4_1_genomes_chr17_raw` -> `21,944,455` rows
- `genome-services-platform.arab_acmg_raw.gnomad_v4_1_exomes_chr13_raw` -> `3,549,140` rows
- `genome-services-platform.arab_acmg_raw.gnomad_v4_1_exomes_chr17_raw` -> `10,744,751` rows

## Cloud Build Jobs
- `920873bb-683d-4aeb-823e-d6cb9915b2ae` -> `genomes chr13` -> `SUCCESS`
- `48af02b6-1919-4862-bfff-61a7b5dd67bf` -> `genomes chr17` -> `SUCCESS`
- `e2664760-d483-42ea-88e0-404e831690f5` -> `exomes chr13` -> `SUCCESS`
- `1922927b-c868-4551-a28a-007f6d281a46` -> `exomes chr17` -> `SUCCESS`

## Verification
- `pytest -q tests` -> pass (`8 passed`)
- `python3 scripts/verify_gcp.py` -> pass
- `python3 scripts/verify_bq_health.py` -> pass
- `node --check ui/app.js` -> pass
- `python3 scripts/update_status_snapshot.py` -> pass

## UI Outcome
- `ui/status_snapshot.json` now includes:
  - live row counts for all raw tables
  - fixed `10`-row samples for each dataset
  - refreshed runtime verification status

## Status Decision
- `T002/3.3` can be marked complete.
- Next execution should stay inside `T002` for QC/staging closure before opening `T003`.
