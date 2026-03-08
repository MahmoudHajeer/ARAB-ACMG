# Checkpoint: 2026-03-08 T002 GME Raw Onboarding

## Scope
- Registered the locally provided GME hg38 file as an Arab/Middle Eastern raw source.
- Persisted the untouched gzip file to the GCS raw vault and loaded a dedicated raw BigQuery table.

## Source
- Local file: `/Users/macbookpro/Desktop/storage/raw/gme/hg38_gme.txt.gz`
- Archive timestamp from gzip header: `2016-10-25T03:27:34Z`
- Snapshot date: `2026-03-08`

## Raw Vault
- `gs://mahmoud-arab-acmg-research-data/raw/sources/gme/release=20161025-hg38/build=hg38/snapshot_date=2026-03-08/hg38_gme.txt.gz`
- `gs://mahmoud-arab-acmg-research-data/raw/sources/gme/release=20161025-hg38/build=hg38/snapshot_date=2026-03-08/manifest.json`

## BigQuery Output
- `genome-services-platform.arab_acmg_raw.gme_hg38_raw`
- Rows loaded: `699,496`
- Schema:
  - `chrom`, `start`, `end`, `ref`, `alt`
  - `gme_af`, `gme_nwa`, `gme_nea`, `gme_ap`, `gme_israel`, `gme_sd`, `gme_tp`, `gme_ca`

## Verification
- `pytest -q tests` -> pass (`11 passed`)
- `python3 scripts/ingest_gme_cloud.py` -> pass
- `python3 scripts/load_gme_to_bq.py` -> pass
- `python3 scripts/verify_bq_health.py` -> pass
- `python3 scripts/update_status_snapshot.py` -> pass

## Notes
- This checkpoint confirms GME is currently the accessible Arab/Middle Eastern source in the workspace.
- Qatar Genome is still not onboarded because no raw source artifact is present in the current environment.
