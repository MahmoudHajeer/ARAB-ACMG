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
- [x] 4.2 Persist the currently accessible Arab/ME source snapshot (GME) to the raw-as-is GCS vault and load its raw table to `arab_acmg_raw`. `3a5a856`
- [x] 4.3 Record the Arab/ME expansion gate explicitly: no further source-specific QC/modeling work proceeds until an additional licensed raw artifact is present in the workspace. `97ff663`
- [x] 4.4 Close the current Arab/ME phase at the GME-only boundary and move further source onboarding to deferred expansion backlog. `97ff663`

## Phase 5: QC Gate and Handoff Package
- [x] 5.1 Freeze the processed BRCA checkpoint outputs to GCS (`Parquet`, `XLSX`, `CSV`) and publish a static review bundle for the supervisor UI. `f1c2974`
- [x] 5.2 Keep the raw-layer provenance in source-freeze/manifests and carry the processed-artifact manifest into GCS for the review bundle handoff. `f1c2974`
- [x] 5.3 Verify the raw-layer inventory and health at milestone close (`verify_bq_health`, frozen artifact verification, row counts). `f1c2974`
- [x] 5.4 Build a supervisor-facing review surface that starts from the frozen raw evidence and evolves into the BRCA checkpoint preview. `34ed035` *(Enhanced on 2026-03-11 with workflow pages and a pre-GME XLSX review export modeled on `example.xlsx`; later frozen into static artifacts by `f1c2974`.)*
- [x] 5.5 Converge review/export behavior to the current low-cost policy: only the final frozen registry CSV remains downloadable at runtime. `f1c2974`
- [x] 5.6 Harden the supervisor UI and then complete the cost-control pivot to a static GCS-backed runtime. `f1c2974`
- [x] 5.7 Replace live BigQuery-backed supervisor interactions with frozen static review artifacts, keep BigQuery only for raw source-of-truth tables, and move the final harmonized deliverable to GCS-first distribution. `f1c2974` *(Explicit user reprioritization on 2026-03-12 to minimize BigQuery spend; allowed in parallel with `4.2-5.3` because it only restructures access to already frozen outputs.)*
- [x] 5.8 Finalize the T002 milestone boundary, move non-gating backlog items out of the active task queue, and prepare T003 as the next active stage after the cost-control pivot. `88d11ff`

## Deferred Expansion Backlog (Not Gating T002 Closure)
- Future Arab/Middle Eastern raw sources beyond GME:
  - persist the untouched snapshot to `raw/sources/...`
  - add source-specific GE expectations
  - add source-specific dbt models/tests
- Optional raw-layer refresh package:
  - republish GE Data Docs to GCS after the next raw-source expansion
  - regenerate the raw data dictionary/inventory reports when the source set changes
  - expose refreshed review snapshots only after a new approved freeze

---
**Track Status**: `[x]`
**Checkpoint SHA**: `[checkpoint: 88d11ff]`
