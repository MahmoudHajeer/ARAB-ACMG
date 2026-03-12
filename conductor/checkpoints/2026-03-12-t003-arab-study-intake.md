# T003 Checkpoint: Arab Study Intake and Low-Cost Rescope

## Summary
- Removed future BigQuery dependence from the active T003-T005 architecture.
- Defined the transformation metadata contract needed for harmonized Parquet artifacts.
- Onboarded two additional Arab/Gulf study source packages without using BigQuery:
  - Saudi Arab breast-cancer supplementary workbook (`PMC10474689`)
  - UAE BRCA supplementary workbook (`PMC12011969`)

## Scientific Notes
- `PMC10474689` is a Saudi Arab breast-cancer study. The downstream-ready extract is restricted to `Table S5`, which is the workbook sheet carrying variant-carrier rows.
- `PMC12011969` is a UAE screening/cancer cohort workbook. The raw workbook includes patient-level columns, so only de-identified mutation-positive extracts were frozen for downstream harmonization.
- No values were guessed or backfilled. The extracted columns are source-backed only.

## GCS Artifacts
- Intake report:
  - `gs://mahmoud-arab-acmg-research-data/frozen/arab_variant_evidence/snapshot_date=2026-03-12/intake_report.json`
- Saudi raw workbook:
  - `gs://mahmoud-arab-acmg-research-data/raw/sources/saudi_breast_cancer_pmc10474689/version=moesm1/snapshot_date=2026-03-12/saudi_breast_cancer_pmc10474689_moesm1.xls`
- Saudi de-identified extract:
  - `gs://mahmoud-arab-acmg-research-data/frozen/arab_variant_evidence/source=saudi_breast_cancer_pmc10474689/version=moesm1/snapshot_date=2026-03-12/variant_carriers.parquet`
- UAE raw workbook:
  - `gs://mahmoud-arab-acmg-research-data/raw/sources/uae_brca_pmc12011969/version=moesm1/snapshot_date=2026-03-12/uae_brca_pmc12011969_moesm1.xlsx`
- UAE de-identified extracts:
  - `gs://mahmoud-arab-acmg-research-data/frozen/arab_variant_evidence/source=uae_brca_pmc12011969/version=moesm1/snapshot_date=2026-03-12/family_screening_variant_rows.parquet`
  - `gs://mahmoud-arab-acmg-research-data/frozen/arab_variant_evidence/source=uae_brca_pmc12011969/version=moesm1/snapshot_date=2026-03-12/cancer_cohort_variant_rows.parquet`

## Extract Counts
- Saudi `Table S5` de-identified variant rows: `38`
- UAE `Family Screening` mutation-positive de-identified rows: `18`
- UAE `Cancer Cohort` mutation-positive de-identified rows: `65`

## Verification
- `python3 -m pytest -q tests` -> `57 passed`
- `python3 scripts/verify_arab_study_sources.py` -> pass
- `python3 scripts/update_ui_overview_state.py` -> pass

## Next Exact Action
- Start `T003 / 2.1` and document `source_build` plus coordinate readiness for the newly frozen Arab extracts before liftover/normalization work begins.
