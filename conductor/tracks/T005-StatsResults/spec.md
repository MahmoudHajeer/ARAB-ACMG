# Specification: T005-Statistical Evaluation & Results

## Goal
Quantify and communicate classification shifts between Scenario A (global) and Scenario B (Arab-enriched) frequency models, with rigorous statistical analysis and publication-quality figures/tables.

## Inputs
- Analysis-ready outputs from T004 (master dataset + shift tables)

## Requirements
- Core metrics:
  - percent of variants shifting class
  - direction of shift (toward pathogenic vs toward benign)
  - stratification by gene (BRCA1 vs BRCA2), ClinVar review status, and variant type
- Statistical rigor:
  - confidence intervals / uncertainty estimates
  - sensitivity analyses (threshold variation, star filtering, AN weighting)
- Deliverables:
  - reproducible notebooks/scripts (Vertex AI optional)
  - figures and tables suitable for manuscript/proposal

## Success Criteria
- [ ] Statistical summary tables and plots are generated reproducibly from T004 outputs.
- [ ] Sensitivity analyses are documented and reproducible.
- [ ] Final results package is produced (figures + tables + narrative summary).
