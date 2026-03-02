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
  - Global AF + ancestry AF (including Middle Eastern subset when available), AN/AC, hom counts, and relevant QC/coverage fields.
- Arab / Middle Eastern frequency sources (license-permitting and accessible):
  - GME Variome (summary), Qatar Genome Program (summary tables), and/or gnomAD ME subset.
- Public cloud datasets (when available):
  - If a dataset is already published in BigQuery, do not copy the full dataset; snapshot only the BRCA1/2 subset into `arab_acmg_raw` and record the upstream table reference and snapshot date.

## Cloud-First Storage Requirements
- Raw snapshots must be stored in GCS first (versioned paths + checksums + manifest).
- Raw tables must be loaded into BigQuery `arab_acmg_raw` before any transformations.
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

## Success Criteria
- [ ] Raw snapshots exist in GCS for all sources with version/access date and checksums recorded.
- [ ] `arab_acmg_raw` tables exist with stable schemas and documented provenance.
- [ ] GE checkpoints pass for all ingested sources (or failures are documented and triaged).
- [ ] dbt source definitions + baseline tests are in place for raw tables.
