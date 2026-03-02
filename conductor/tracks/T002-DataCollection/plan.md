# Implementation Plan: T002-Data Ingestion & QC

## Phase 1: ClinVar Ingestion
- [x] 1.1 Create automated download script for ClinVar (bulk VCF).
- [x] 1.2 Implement BRCA1/2-specific gene filtering logic.
- [~] 1.3 Upload ClinVar BRCA subset to `gs://mahmoud-arab-acmg-research-data/raw/clinvar/`.

## Phase 2: gnomAD & Arab Frequency Ingestion
- [ ] 2.1 Create ingestion script for gnomAD v4 (BRCA1/2 subset).
- [ ] 2.2 Identify and fetch GME Variome, Qatar Genome Program, and gnomAD Middle Eastern datasets.
- [ ] 2.3 Upload frequency subsets to `gs://mahmoud-arab-acmg-research-data/raw/frequencies/`.

## Phase 3: Automated QC & Verification
- [ ] 3.1 Write an automated QC script to verify data types and integrity in GCS.
- [ ] 3.2 Implement a basic validation unit test for the ingestion pipeline.
- [ ] 3.3 Generate a master inventory report of the ingested raw data.

---
**Track Status**: `[ ]`
**Checkpoint SHA**: `[checkpoint: TBA]`
