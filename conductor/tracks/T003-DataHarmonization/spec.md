# Specification: T003-Data Harmonization & Normalization

## Goal
Convert all ingested raw datasets into a consistent GRCh38-aligned and normalized representation with canonical variant keys, enabling deterministic joins for downstream ACMG evaluation.

## References
- Shared contracts: [conductor/data-contracts.md](../../data-contracts.md)
- Roadmap narrative: [Data collection.MD](<../../../Data collection.MD>)

## Inputs
- BigQuery `arab_acmg_raw` tables (T002 outputs).
- GCS raw artifacts and manifests (T002 outputs).

## Outputs
- BigQuery `arab_acmg_harmonized` tables per source with:
  - GRCh38 coordinates
  - normalized REF/ALT
  - canonical `variant_key`
  - transformation metadata (`liftover_status`, `norm_status`, tool versions)
- Parquet snapshots in GCS for harmonized tables and key mapping tables.

## Requirements
- Build standardization:
  - Prefer GRCh38 upstream sources.
  - If any source is GRCh37, liftover to GRCh38 with explicit failure tracking and reporting.
- Variant normalization:
  - split multiallelics into biallelic records
  - left-align and parsimoniously normalize indels
  - trim common bases consistently
- Auditability:
  - preserve source identifiers (ClinVar VariationID, gnomAD identifiers where available)
  - keep a mapping table from raw key -> canonical key
- Quality gates:
  - Great Expectations suites + checkpoints for harmonized tables
  - dbt models + tests to enforce canonical key invariants

## Success Criteria
- [ ] All harmonized tables use the canonical GRCh38 `variant_key`.
- [ ] Liftover failures are explicitly logged and summarized (not silently dropped).
- [ ] Normalization collisions/duplicates are detected and reported.
- [ ] GE + dbt tests pass for harmonized invariants (or failures are documented with remediation).
