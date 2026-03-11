# Implementation Plan: T002-Data Ingestion & QC (Cloud-First)

## Phase 1: Contracts, Layout, and Tooling
- [x] 1.1 Confirm BigQuery datasets: `arab_acmg_raw`, `arab_acmg_harmonized`, `arab_acmg_results`.
- [x] 1.2 Adopt the canonical data contract in [conductor/data-contracts.md](../../data-contracts.md).
- [x] 1.3 Define GCS raw-as-is vault layout: `raw/sources/<source>/<version>/snapshot_date=<YYYY-MM-DD>/...`.
- [x] 1.4 Define provenance manifests (minimum fields + checksum policy) for every raw artifact.
- [x] 1.5 Create Great Expectations project scaffolding (suites + checkpoints + Data Docs path on GCS).
- [x] 1.6 Create dbt project scaffolding for BigQuery (sources + staging models + baseline tests). `e040d66`

## Phase 2: ClinVar (Classification Layer)
- [x] 2.1 Decide ingestion route (NCBI bulk VCF) and record `source_version`/`snapshot_date`.
- [x] 2.2 Persist untouched ClinVar files in raw-as-is GCS vault with checksum/provenance.
- [x] 2.3 Create BigQuery raw table(s) for raw-as-is ClinVar in `arab_acmg_raw`.
- [x] 2.4 GE suite + checkpoint for ClinVar raw tables (required fields, key sanity, counts). `e040d66`
- [x] 2.5 dbt `source` + `stg_clinvar_*` models + dbt tests for ClinVar. `e040d66`

## Phase 3: gnomAD (Global + Ancestry Frequencies)
- [x] 3.1 Freeze the exact gnomAD version (v4.1) and dataset choice (genomes + exomes).
- [x] 3.2 Persist untouched gnomAD source files in raw-as-is GCS vault for required chromosomes only (`chr13`,`chr17`) before any subset extraction.
- [x] 3.3 Load raw-as-is required chromosome datasets to `arab_acmg_raw` (genomes/exomes, chr13/chr17), then perform subset extraction in later steps. `f8b5351`
- [x] 3.4 GE suite + checkpoint for gnomAD raw tables (AF range, AN sanity, duplicates). `e040d66` *(Checkpoint runs against raw-derived dbt staging views because direct Great Expectations query-asset reflection on the largest raw tables exceeds BigQuery response-size limits.)*
- [x] 3.5 dbt `source` + `stg_gnomad_*` models + dbt tests for gnomAD. `e040d66`

## Phase 4: Arab / Middle Eastern Frequency Sources
- [x] 4.1 Enumerate accessible sources (GME, Qatar Genome) and document license/access constraints. `3a5a856` *(Accessible now: local GME hg38 raw file. Not yet accessible in workspace: Qatar Genome raw artifact.)*
- [ ] 4.2 Persist untouched source snapshots to raw-as-is GCS vault, then load parsed working tables to `arab_acmg_raw`. (Deferred.)
- [ ] 4.3 GE suite + checkpoint per source (required fields, AF/AN sanity, missingness).
- [ ] 4.4 dbt `source` + `stg_*` models + dbt tests per Arab/ME source.

## Phase 5: QC Gate and Handoff Package
- [ ] 5.1 Publish GE Data Docs for this track to GCS.
- [ ] 5.2 Produce a data dictionary and provenance table for all `arab_acmg_raw` tables.
- [ ] 5.3 Create a raw-layer inventory report (row counts, null rates, duplicates).
- [x] 5.4 Build a supervisor-facing query explorer with live 50-row sample fetches per dataset and a first registry table preview that can absorb new sources incrementally. `34ed035` *(Enhanced on 2026-03-11 with workflow pages and a pre-GME XLSX review export modeled on `example.xlsx`; live deployment stabilized with `f72ad8a` to fix reserved-column sampling in `ui/status_snapshot.json`.)*

---
**Track Status**: `[~]`
**Checkpoint SHA**: `[checkpoint: 2026-03-08-t002-raw-load-ui-samples]`
