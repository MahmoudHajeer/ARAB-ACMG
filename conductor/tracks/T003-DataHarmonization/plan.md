# Implementation Plan: T003-Data Harmonization & Normalization

## Phase 1: Canonical Key + Transform Contracts
- [ ] 1.1 Define the harmonized variant key (GRCh38 chr/pos/ref/alt) and validation rules.
- [ ] 1.2 Define transformation metadata fields (source_build, liftover_status, norm_status).

## Phase 2: Build Alignment (If Needed)
- [ ] 2.1 Identify sources not in GRCh38 and document build per source.
- [ ] 2.2 Implement liftover pipeline + failure tracking for GRCh37 inputs.
- [ ] 2.3 QC: liftover success rates and failure examples.

## Phase 3: Variant Normalization
- [ ] 3.1 Normalize each source (split multiallelics, left-align indels, trim bases).
- [ ] 3.2 QC: duplicates after normalization, ref/alt validity checks, position bounds.
- [ ] 3.3 Persist normalized outputs to BigQuery `harmonized_*` tables.

## Phase 4: Cross-Source Consistency Checks
- [ ] 4.1 Verify that the same biological variant maps to a single canonical key.
- [ ] 4.2 Produce a harmonization report (counts per source, merges, collisions).

---
**Track Status**: `[ ]`
**Checkpoint SHA**: `[checkpoint: TBA]`
