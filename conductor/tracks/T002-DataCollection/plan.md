# Implementation Plan: T002-Data Ingestion & QC

## Phase 1: ClinVar Ingestion
- [x] 1.1 Create automated download script for ClinVar (bulk VCF).
- [x] 1.2 Implement BRCA1/2-specific gene filtering logic.
- [x] 1.3 Upload ClinVar BRCA subset to `gs://mahmoud-arab-acmg-research-data/raw/clinvar/`.

## Phase 2: gnomAD & Arab Frequency Ingestion
- [x] 2.1 Create ingestion script for gnomAD v4 (BRCA1/2 subset).
- [x] 2.2 Identify and fetch GME Variome, Qatar Genome Program, and gnomAD Middle Eastern datasets.
- [x] 2.3 Upload frequency subsets to `gs://mahmoud-arab-acmg-research-data/raw/frequencies/`.

## Phase 3: Automated QC & Verification
- [x] 3.1 Write an automated QC script to verify data types and integrity in GCS.
- [x] 3.2 Implement a basic validation unit test for the ingestion pipeline.
- [x] 3.3 Generate a master inventory report of the ingested raw data.

---
**Track Status**: `[x]`
**Checkpoint SHA**: `[checkpoint: f7d1b36]`
