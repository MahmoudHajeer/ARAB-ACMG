# Specification: T005-Statistical Evaluation & Results

## Goal
Quantify and communicate classification shifts between Scenario A (global) and Scenario B (Arab-enriched) with rigorous statistical evaluation, sensitivity analyses, and publication-quality outputs.

## References
- Roadmap narrative: [Data collection.MD](<../../../Data collection.MD>)

## Inputs
- BigQuery `arab_acmg_results` tables from T004 (master dataset + evaluation + shift tables).

## Outputs
- Statistics-ready marts in `arab_acmg_results` (via dbt):
  - summary tables of shift rates and directionality
  - stratifications by gene, ClinVar review confidence, variant type
- Reproducible figures and tables for manuscript/proposal.
- Sensitivity analysis results (threshold sweeps, AN policies, ClinVar filters).

## Statistical Requirements
- Primary metrics:
  - percent of variants that shift classification
  - direction of shift (toward pathogenic vs toward benign)
  - stratification by gene (BRCA1 vs BRCA2) and data confidence indicators
- Uncertainty reporting:
  - confidence intervals or appropriate uncertainty estimates
- Sensitivity analyses:
  - thresholds variation (PM2/BS1/BA1)
  - ClinVar star/conflict filtering
  - minimum AN handling policy

## Success Criteria
- [ ] Reproducible tables/plots are generated from T004 outputs without manual steps.
- [ ] Sensitivity analyses are documented and reproducible.
- [ ] Final results package is produced (figures + tables + narrative summary).
