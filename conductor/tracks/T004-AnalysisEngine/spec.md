# Specification: T004-Master Dataset & ACMG Analysis Engine

## Goal
Build the harmonized master dataset in BigQuery and implement the frequency-driven ACMG evaluation logic to quantify classification shifts between a global model and an Arab-enriched model.

## References
- Shared contracts: [conductor/data-contracts.md](../../data-contracts.md)
- Roadmap narrative: [Data collection.MD](<../../../Data collection.MD>)

## Inputs
- BigQuery `arab_acmg_harmonized` tables (T003 outputs).

## Outputs
- BigQuery `arab_acmg_results` tables:
  - `master_variants`: one row per canonical variant key with joined ClinVar and frequency fields
  - `acmg_frequency_eval`: PM2/BS1/BA1 flags under Scenario A and Scenario B
  - `classification_shifts`: summary of scenario differences (direction, magnitude, stratifications)
- Parameter registry for thresholds and policies (dbt seed or BigQuery parameters table).

## Scenarios (Pre-Declared)
- Scenario A (Global):
  - Evaluate frequency rules using global AF thresholds (gnomAD global).
- Scenario B (Arab-enriched):
  - Evaluate frequency rules using Arab/ME AF thresholds (Arab sources and/or gnomAD ME subset).

## Key Decisions to Freeze Before Implementation
- Operational thresholds for PM2/BS1/BA1 (including any gene-specific or disease-specific adjustments).
- Minimum AN policy (when AF is considered too unstable/uncertain to use).
- ClinVar inclusion policy (review stars threshold, handling conflicts).
- Missing-data handling rules (when one frequency source is absent).

## Quality Gates
- dbt models for master joins and evaluation outputs, with dbt tests for:
  - canonical key uniqueness
  - not-null requirements for required fields
  - accepted values for scenario labels and flags
- Great Expectations validation for critical result tables (sanity checks and distribution checks).

## Success Criteria
- [ ] Master dataset joins deterministically on canonical `variant_key`.
- [ ] PM2/BS1/BA1 evaluation outputs are reproducible with thresholds recorded and versioned.
- [ ] Scenario A vs B shift tables are generated and validated.
