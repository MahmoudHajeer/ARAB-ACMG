# Implementation Plan: T004-Master Dataset & ACMG Analysis Engine

## Phase 1: Thresholds and Policies (Pre-Registration)
- [ ] 1.1 Define PM2/BS1/BA1 operational thresholds and document rationale.
- [ ] 1.2 Define ClinVar inclusion policy (stars/conflicts) and missing-data rules.
- [ ] 1.3 Implement a threshold/policy registry (dbt seed or BigQuery table) with versioning.

## Phase 2: Master Dataset Build (BigQuery + dbt)
- [ ] 2.1 Build `master_variants` join model in `arab_acmg_results`.
- [ ] 2.2 Add dbt tests: unique canonical key, not-null key fields, basic relationships.
- [ ] 2.3 Add GE suite/checkpoint for master dataset sanity (row counts, null rates, AF ranges).

## Phase 3: ACMG Frequency Evaluation (Scenarios A/B)
- [ ] 3.1 Implement PM2 evaluation under Scenario A (global) and Scenario B (Arab-enriched).
- [ ] 3.2 Implement BS1/BA1 evaluation under Scenario A and Scenario B.
- [ ] 3.3 Add dbt tests and GE checks for evaluation outputs (flag distributions, invariants).

## Phase 4: Misclassification Outputs
- [ ] 4.1 Generate `classification_shifts` tables (direction, magnitude, by gene).
- [ ] 4.2 Export analysis-ready marts for statistics/plots (T005 inputs).

---
**Track Status**: `[ ]`
**Checkpoint SHA**: `[checkpoint: TBA]`
