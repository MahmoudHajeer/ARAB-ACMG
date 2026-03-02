# Implementation Plan: T002-Data Ingestion & QC (Cloud-First)

## Phase 1: Contracts, Layout, and Tooling
- [x] 1.1 Confirm BigQuery datasets: `arab_acmg_raw`, `arab_acmg_harmonized`, `arab_acmg_results`.
- [x] 1.2 Adopt the canonical data contract in [conductor/data-contracts.md](../../data-contracts.md).
- [x] 1.3 Define a GCS raw layout: `raw/<source>/<version>/snapshot_date=<YYYY-MM-DD>/...`.
- [x] 1.4 Define provenance manifests (minimum fields + checksum policy) for every raw artifact.
- [x] 1.5 Create Great Expectations project scaffolding (suites + checkpoints + Data Docs path on GCS).
- [x] 1.6 Create dbt project scaffolding for BigQuery (sources + staging models + baseline tests).

## Phase 2: ClinVar (Classification Layer)
- [x] 2.1 Decide ingestion route (NCBI bulk VCF) and record `source_version`/`snapshot_date`.
- [x] 2.2 Create a raw snapshot in GCS (or a manifest pointing to the upstream public dataset) with checksum/provenance.
- [x] 2.3 Create BigQuery raw table(s) for the BRCA1/2 subset in `arab_acmg_raw`.
- [x] 2.4 GE suite + checkpoint for ClinVar raw tables (required fields, key sanity, counts).
- [x] 2.5 dbt `source` + `stg_clinvar_*` models + dbt tests for ClinVar.

## Phase 3: gnomAD (Global + Ancestry Frequencies)
- [x] 3.1 Freeze the exact gnomAD version (v4.1) and dataset choice (genomes).
- [x] 3.2 Extract BRCA1/2 subsets and store as Parquet in GCS (manifested).
- [x] 3.3 Load BRCA subsets into `arab_acmg_raw` with AF/AN/AC and ancestry fields (when available).
- [x] 3.4 GE suite + checkpoint for gnomAD raw tables (AF range, AN sanity, duplicates).
- [x] 3.5 dbt `source` + `stg_gnomad_*` models + dbt tests for gnomAD.

## Phase 4: Arab / Middle Eastern Frequency Sources
- [x] 4.1 Enumerate accessible sources (GME, Qatar Genome) and document license/access constraints.
- [x] 4.2 Snapshot each dataset to GCS with manifest/checksum and load to `arab_acmg_raw`.
- [x] 4.3 GE suite + checkpoint per source (required fields, AF/AN sanity, missingness).
- [x] 4.4 dbt `source` + `stg_*` models + dbt tests per Arab/ME source.

## Phase 5: QC Gate and Handoff Package
- [x] 5.1 Publish GE Data Docs for this track to GCS.
- [x] 5.2 Produce a data dictionary and provenance table for all `arab_acmg_raw` tables.
- [x] 5.3 Create a raw-layer inventory report (row counts, null rates, duplicates).

---
**Track Status**: `[x]`
**Checkpoint SHA**: `[checkpoint: 6c8752e]`
