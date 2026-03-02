# Implementation Plan: T004-Master Dataset & ACMG Analysis Engine

## Phase 1: Definitions & Thresholds
- [ ] 1.1 Define PM2/BS1/BA1 operational thresholds and document them.
- [ ] 1.2 Define ClinVar inclusion policy (stars/conflicts) and document it.

## Phase 2: Master Dataset Build
- [ ] 2.1 Implement joins across harmonized sources into a single master table.
- [ ] 2.2 QC: row counts, duplicate keys, null rates, per-source coverage.
- [ ] 2.3 Produce a data dictionary for the master dataset.

## Phase 3: ACMG Evaluation Engine (Frequency Rules)
- [ ] 3.1 Implement PM2 evaluation under Scenario A and Scenario B.
- [ ] 3.2 Implement BS1/BA1 evaluation under Scenario A and Scenario B.
- [ ] 3.3 Add unit tests covering boundary conditions and missing-data handling.

## Phase 4: Misclassification Outputs
- [ ] 4.1 Generate classification-shift tables (direction, magnitude, by gene).
- [ ] 4.2 Export analysis-ready tables for statistics/plots (T005).

---
**Track Status**: `[ ]`
**Checkpoint SHA**: `[checkpoint: TBA]`
