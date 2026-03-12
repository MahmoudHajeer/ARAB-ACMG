# Data Contracts, Provenance, and Cloud-First Policy

This document defines the shared data contracts for all tracks (T002-T005). It is the source of truth for schemas, keys, and provenance requirements.

## Cloud-First Storage Policy
- Large datasets must not be stored locally.
- Every dataset must be snapshotted to **GCS** first (versioned path + checksum).
- A read-only "raw-as-is copy" must exist before any parsing/filtering:
  - GCS raw vault path: `raw/sources/<source>/<version>/snapshot_date=<YYYY-MM-DD>/...`
  - Files in this vault are byte-for-byte upstream snapshots (no column selection, no record filtering).
- The active downstream workflow no longer creates new BigQuery tables. Existing raw BigQuery mirrors are treated as legacy archival copies only and must not be extended unless no lower-cost alternative exists.
- Transformations now produce GCS-hosted Parquet/CSV artifacts and manifests without overwriting the raw vault.

## Dataset Layers (Active Architecture)
- `raw/sources/...` in GCS: untouched upstream artifacts preserved exactly as received.
- `frozen/arab_variant_evidence/...` in GCS: de-identified source-specific extracts prepared for harmonization.
- `frozen/harmonized/...` in GCS: GRCh38-aligned and normalized Parquet snapshots with canonical keys.
- `frozen/results/...` in GCS: master dataset, ACMG criteria outputs, and statistics-ready marts.
- Legacy BigQuery raw tables may remain for audit/reference, but they are not part of the default execution path.

## Raw Vault (GCS, Untouched)
- Purpose: preserve upstream data exactly as received before any transformation logic.
- Location pattern:
  - `raw/sources/clinvar/<version>/snapshot_date=<YYYY-MM-DD>/clinvar.vcf.gz`
  - `raw/sources/gnomad_v4.1/<version>/snapshot_date=<YYYY-MM-DD>/gnomad.genomes.v4.1.sites.chr17.vcf.bgz`
- Rule: transforms, subsets, and curated extracts must never overwrite or mutate this vault.

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
- `liftover_status` (STRING): `not_applicable` | `not_needed` | `success` | `failed`
- `norm_status` (STRING): `not_applicable` | `success` | `failed`
- `source_build` (STRING): `GRCh37` | `GRCh38` | `unknown`

## Transformation Metadata Contract (T003+)
Every harmonized artifact must carry these provenance fields so future Arab datasets can join the pipeline without ad-hoc rules:
- `source_artifact_uri` (STRING): exact raw artifact URI in GCS.
- `source_artifact_sha256` (STRING): checksum of the raw artifact used for this record set.
- `source_sheet_name` (STRING, nullable): worksheet or subtable name when the raw artifact is a workbook or multi-table file.
- `source_row_number` (INT64, nullable): original 1-based row number within the selected sheet/table after header rows.
- `source_record_locator` (STRING): stable human-readable locator such as `sheet=Table S5;row=14`.
- `parse_status` (STRING): `parsed` | `missing_coordinates` | `missing_allele` | `unsupported_layout` | `excluded_non_variant_row`.
- `source_build` (STRING): `GRCh37` | `GRCh38` | `unknown` | `not_applicable`.
- `liftover_status` (STRING): `not_applicable` | `not_needed` | `success` | `failed`.
- `liftover_tool` (STRING, nullable): example `CrossMap`.
- `liftover_tool_version` (STRING, nullable).
- `liftover_notes` (STRING, nullable): short failure or special-case explanation.
- `norm_status` (STRING): `not_applicable` | `success` | `failed`.
- `norm_tool` (STRING, nullable): example `bcftools norm`.
- `norm_tool_version` (STRING, nullable).
- `norm_notes` (STRING, nullable): normalization warnings, collision notes, or why normalization was skipped.
- `transform_run_id` (STRING): unique run identifier for the extraction/harmonization pass.
- `transform_timestamp_utc` (TIMESTAMP or ISO8601 string): when the harmonized record was written.

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
  - `parse_status`, `liftover_status`, and `norm_status` restricted to accepted enums

GE Data Docs should be published to a stable GCS path per run.

### Validation Tests
Validation may be implemented with `pytest`, Great Expectations, and/or DuckDB SQL checks over Parquet snapshots:
- `unique` + `not_null` for canonical keys in harmonized/results artifacts
- `accepted_values` for enums (`parse_status`, `liftover_status`, `norm_status`)
- relationships between master dataset artifacts and per-source harmonized extracts where applicable

## Intermediate Storage (GCS Parquet)
Use Parquet (PyArrow) for:
- immutable intermediate snapshots (especially for large joins and extracts)
- efficient DuckDB scans and low-cost local/cloud re-use
- reproducible inputs for validation and re-runs

Store Parquet under versioned GCS paths and reference it from manifests.
