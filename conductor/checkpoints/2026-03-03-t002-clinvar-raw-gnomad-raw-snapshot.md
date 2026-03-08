# Checkpoint: 2026-03-03 T002 ClinVar Raw + gnomAD Raw Snapshot

## Scope
- Followed stepwise T002 execution with `gnomad_mid_eastern_v4.1` deferred.
- Enforced gnomAD scope to required chromosomes only: `chr13`, `chr17` for `genomes` + `exomes`.

## Executed Commands
- `python3 scripts/verify_gcp.py` -> pass
- `python3 scripts/ingest_clinvar_cloud.py` -> pass
- `python3 scripts/load_clinvar_to_bq.py` -> pass (`4,388,226` rows)
- `python3 scripts/ingest_gnomad_parquet.py` -> pass after large-copy fix (`copy_blob` -> `gsutil cp`)
- `python3 scripts/load_gnomad_to_bq.py` -> fail (platform constraint)

## Raw Snapshot Outputs
- ClinVar raw vault:
  - `gs://mahmoud-arab-acmg-research-data/raw/sources/clinvar/lastmod-20260302/snapshot_date=2026-03-03/`
- gnomAD raw vault:
  - `gs://mahmoud-arab-acmg-research-data/raw/sources/gnomad_v4.1/release=4.1/cohort=genomes/chrom=chr13/snapshot_date=2026-03-03/`
  - `gs://mahmoud-arab-acmg-research-data/raw/sources/gnomad_v4.1/release=4.1/cohort=genomes/chrom=chr17/snapshot_date=2026-03-03/`
  - `gs://mahmoud-arab-acmg-research-data/raw/sources/gnomad_v4.1/release=4.1/cohort=exomes/chrom=chr13/snapshot_date=2026-03-03/`
  - `gs://mahmoud-arab-acmg-research-data/raw/sources/gnomad_v4.1/release=4.1/cohort=exomes/chrom=chr17/snapshot_date=2026-03-03/`

## Confirmed BigQuery Outputs
- `genome-services-platform.arab_acmg_raw.clinvar_raw_vcf`

## Blocker
- BigQuery rejects loading three gnomAD `.vcf.bgz` files because compressed non-splittable CSV input exceeds `4GB`:
  - error: `Input CSV files are not splittable ... Max allowed size is: 4294967296`
- A direct local streaming workaround (`gzip -dc | gsutil cp -`) was tested but is too slow for full-scale execution.

## Status Decision
- Keep `T002/3.3` unchecked.
- Keep later gnomAD QC/staging tasks (`3.4`, `3.5`) unchecked.
- Continue with a chunked/cloud-side ingestion path for gnomAD BQ in the next checkpoint.
