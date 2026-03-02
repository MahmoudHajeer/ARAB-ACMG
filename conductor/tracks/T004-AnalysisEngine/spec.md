# Specification: T004-Master Dataset & ACMG Analysis Engine

## Goal
Build the harmonized master dataset (join of ClinVar classification + population frequencies) and implement the ACMG frequency-related evaluation logic to quantify misclassification shifts between global and Arab-enriched models.

## Inputs
- Harmonized GRCh38 tables from T003

## Requirements
- Master dataset:
  - single joined table keyed by canonical variant (chr38/pos38/ref/alt)
  - columns for ClinVar class + review/conflicts + all frequency sources (AF/AN)
  - explicit handling of missing values per source
- Scenario definitions (must be pre-declared):
  - Scenario A: global model thresholds (gnomAD global)
  - Scenario B: Arab-enriched thresholds (Arab-frequency layer / gnomAD ME subset)
- ACMG logic scope (initial):
  - frequency-driven criteria: PM2, BS1, BA1 (if used), with documented thresholds
  - conflict handling policy for ClinVar submissions (filtering, weighting, or exclusion)
- Reproducibility:
  - thresholds and inclusion criteria must be written down before running analysis
  - unit tests for rule evaluation functions

## Decisions To Freeze Early
- AF threshold(s) for BS1/BA1 and the definition of "absent from controls" for PM2
- Whether to weight by AN and how to treat small AN (uncertainty)
- Whether to restrict ClinVar to >=2-star review status
- How to handle conflicting ClinVar submissions (exclude vs model separately)

## Success Criteria
- [ ] Master dataset table exists (BigQuery + local test fixture).
- [ ] ACMG evaluation functions produce deterministic outputs and are unit-tested.
- [ ] Scenario A vs B classification shift table is generated for BRCA1/2.
