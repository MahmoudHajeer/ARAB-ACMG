# Implementation Plan: T003-Data Harmonization & Normalization

## Phase 1: Variant Normalization Pipeline
- [ ] 1.1 Create automated normalization script using `bcftools` and `vt`.
- [ ] 1.2 Implement multiallelic splitting and indel left-alignment logic.
- [ ] 1.3 Apply normalization to ClinVar and gnomAD raw subsets.

## Phase 2: Genome Build Standardization (LiftOver)
- [ ] 2.1 Identify any datasets requiring LiftOver to GRCh38 (e.g., legacy GME).
- [ ] 2.2 Create automated LiftOver script for coordinate conversion.
- [ ] 2.3 Verify GRCh38 alignment of the converted datasets.

## Phase 3: Harmonization Verification & Staging
- [ ] 3.1 Write an automated script to verify Ref/Alt consistency in the harmonized data.
- [ ] 3.2 Implement a basic validation unit test for the normalization pipeline.
- [ ] 3.3 Stage the final harmonized datasets in `gs://mahmoud-arab-acmg-research-data/harmonized/`.

---
**Track Status**: `[ ]`
**Checkpoint SHA**: `[checkpoint: TBA]`