# Specification: T002-Data Ingestion & QC (Cloud-First)

## Goal
Ingest all upstream datasets required for BRCA1/BRCA2 misclassification analysis into a reproducible, cloud-first raw layer, with explicit provenance and quality gates.

## References
- Roadmap narrative: [Data collection.MD](<../../../Data collection.MD>)
- Shared contracts: [conductor/data-contracts.md](../../data-contracts.md)
- Research standards: [conductor/product-guidelines.md](../../product-guidelines.md)

## Inputs (Primary Sources)
- ClinVar:
  - Clinical significance, review status (stars), submitter counts, and conflict indicators.
- gnomAD (GRCh38 preferred):
  - Current ingestion scope: required chromosomes only (`chr13`, `chr17`) from both genomes and exomes raw files.
  - Global AF + ancestry AF, AN/AC, hom counts, and relevant QC/coverage fields are extracted in later parsing steps.
- Arab / Middle Eastern frequency sources (license-permitting and accessible):
  - GME Variome (summary), Qatar Genome Program (summary tables), and/or gnomAD ME subset.
  - This stream is intentionally deferred until ClinVar and gnomAD raw freeze is finalized.
- Public cloud datasets (when available):
  - If a dataset is already published in BigQuery, do not copy the full dataset; snapshot only the BRCA1/2 subset into `arab_acmg_raw` and record the upstream table reference and snapshot date.

## Cloud-First Storage Requirements
- **Mandatory order of operations**:
  1. Persist upstream files **as-is** to GCS raw vault (`raw/sources/...`) with manifests.
  2. Only after raw vault success, generate parsed/subset artifacts.
  3. Load parsed working raw tables into BigQuery `arab_acmg_raw`.
- Raw snapshots must be stored in GCS first (versioned paths + checksums + manifest).
- Avoid large local files; local disk is reserved for small test fixtures only.

## Data Contracts
All tables and artifacts must comply with:
- Canonical variant contract and keys (raw vs harmonized).
- Provenance manifest requirements.
- Validation gates (GE + dbt tests).

See: [conductor/data-contracts.md](../../data-contracts.md).

## QC & Validation Tooling
- Great Expectations:
  - expectation suites per raw table and per staged Parquet snapshot
  - checkpoints that gate promotion to the next track
  - GE Data Docs published to a stable GCS path per run
- dbt (BigQuery):
  - `sources` definitions for all raw tables
  - baseline `stg_*` models for type normalization and naming consistency (raw -> staging)
  - dbt tests: `unique`, `not_null`, `accepted_values`, and `relationships` where applicable

## Intermediate Storage
- Use Parquet (PyArrow) in GCS for large intermediate extracts and reproducible snapshots.
- Every Parquet snapshot must have a manifest entry linking it to its BigQuery tables and upstream raw artifacts.

## Deliverables
- Versioned raw artifacts in GCS with manifests/checksums.
- BigQuery `arab_acmg_raw` tables for each source (and snapshot subsets when sourcing from public BQ).
- GE expectation suites + checkpoints and published Data Docs.
- dbt sources + staging models + dbt tests for raw/staging layers.
- Data dictionary and provenance table covering all ingested sources.
- Frozen supervisor-review artifacts in GCS for the processed BRCA checkpoint outputs (`pre-GME` and `final`) so downstream review does not spend BigQuery query quota.

## Success Criteria
- [x] Raw snapshots exist in GCS for the current source set with version/access date and checksums recorded.
- [x] `arab_acmg_raw` tables exist with stable schemas and documented provenance for ClinVar, gnomAD (`chr13`,`chr17`, genomes/exomes), and the accessible GME source.
- [x] GE checkpoints and dbt source/test scaffolding exist for the current raw-source milestone.
- [x] A frozen low-cost supervisor review bundle exists so downstream work can continue from GCS artifacts instead of live BigQuery query scans.

## Current Milestone Boundary
- T002 is considered complete for the current BRCA milestone once the raw sources are frozen and the supervisor review artifacts are exported.
- Additional Arab/Middle Eastern source onboarding beyond the currently available GME artifact is intentionally deferred until a new licensed raw artifact is available.
- Optional refreshes such as raw-layer Data Docs publication, data-dictionary regeneration, and inventory refresh remain backlog work; they do not gate T003.
