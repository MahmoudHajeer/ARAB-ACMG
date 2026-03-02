# Data Contracts, Provenance, and Cloud-First Policy

This document defines the shared data contracts for all tracks (T002-T005). It is the source of truth for schemas, keys, and provenance requirements.

## Cloud-First Storage Policy
- Large datasets must not be stored locally.
- Every dataset must be snapshotted to **GCS** first (versioned path + checksum).
- A read-only "raw copy" must be loaded into **BigQuery** (`arab_acmg_raw`) before any transformations.
- Transformations create new tables in `arab_acmg_harmonized` and `arab_acmg_results` without overwriting raw.

## Dataset Layers (BigQuery)
- `arab_acmg_raw`: raw parsed tables per source (no normalization, no liftover).
- `arab_acmg_harmonized`: GRCh38-aligned and normalized tables with canonical variant keys.
- `arab_acmg_results`: master dataset, ACMG criteria outputs, and statistics-ready marts.

## Canonical Variant Contract

### Minimum Required Fields (Raw Layer)
All raw tables must include these fields (even if source-specific tables also contain additional columns):
- `source` (STRING): short identifier (example: `clinvar`, `gnomad_v4`, `gme`, `qgp`).
- `source_version` (STRING): upstream version string (or `unknown` if not discoverable).
- `snapshot_date` (DATE): date the raw snapshot was captured.
- `chrom` (STRING): as provided by the source (do not normalize in raw).
- `pos` (INT64): 1-based position as provided.
- `ref` (STRING)
- `alt` (STRING)
- `ingest_run_id` (STRING): identifier of the ingestion run (timestamp or UUID).

### Canonical Fields (Harmonized Layer)
All harmonized tables must provide:
- `chrom38` (STRING): normalized to `chr1..chr22, chrX, chrY, chrMT`.
- `pos38` (INT64)
- `ref_norm` (STRING)
- `alt_norm` (STRING)
- `variant_key` (STRING): `chrom38:pos38:ref_norm:alt_norm`
- `liftover_status` (STRING): `not_needed` | `success` | `failed`
- `norm_status` (STRING): `success` | `failed`
- `source_build` (STRING): `GRCh37` | `GRCh38` | `unknown`

### Key Rules
- Raw uniqueness key (per source snapshot): `(source, snapshot_date, chrom, pos, ref, alt)`
- Harmonized canonical key: `(chrom38, pos38, ref_norm, alt_norm)` and `variant_key`

## Normalization Standard (T003)
- Split multiallelic records to biallelic.
- Left-align and parsimoniously trim indels.
- Preserve an audit trail:
  - original raw fields (or a link back to the raw table row id)
  - tool versions and parameters used

## Provenance Manifest Contract (GCS)
Each raw snapshot must have a manifest file stored alongside the data in GCS (JSON or CSV). Minimum fields:
- `source`
- `source_version`
- `snapshot_date`
- `upstream_url` (or `bq_public_table` if sourced from a public BigQuery dataset)
- `license_notes`
- `sha256`
- `gcs_uri`
- `row_count` (if known at snapshot time)
- `notes`

## Data Quality Gates

### Great Expectations (GE)
GE is used for gatekeeping datasets before moving to the next layer:
- Raw layer suites:
  - required columns present
  - `pos` is positive
  - `ref/alt` are non-empty
  - frequency fields are within range when present (`0 <= AF <= 1`)
  - `AN` present and positive for any record where AF is used downstream
- Harmonized layer suites:
  - `variant_key` not null
  - canonical key uniqueness (within each harmonized table)
  - liftover failure rate reported (not hidden)

GE Data Docs should be published to a stable GCS path per run.

### dbt Tests
dbt tests enforce invariants in BigQuery:
- `unique` + `not_null` for canonical keys in harmonized/results tables
- `accepted_values` for enums (`liftover_status`, `norm_status`)
- `relationships` between master dataset and per-source harmonized tables (where applicable)

## Intermediate Storage (GCS Parquet)
Use Parquet (PyArrow) for:
- immutable intermediate snapshots (especially for large joins and extracts)
- efficient reload into BigQuery
- reproducible inputs for validation and re-runs

Store Parquet under versioned GCS paths and reference it from manifests.
