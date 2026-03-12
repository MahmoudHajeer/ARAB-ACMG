# Specification: T003-Data Harmonization & Normalization

## Goal
Convert all ingested raw datasets into a consistent GRCh38-aligned and normalized representation with canonical variant keys, enabling deterministic joins for downstream ACMG evaluation.

## References
- Shared contracts: [conductor/data-contracts.md](../../data-contracts.md)
- Roadmap narrative: [Data collection.MD](<../../../Data collection.MD>)

## Inputs
- BigQuery `arab_acmg_raw` tables (T002 outputs).
- GCS raw artifacts and manifests (T002 outputs).
- Frozen BRCA checkpoint artifacts exported from T002 step `5.7`:
  - GCS Parquet/XLSX/CSV review artifacts
  - `ui/review_bundle.json` and its matching GCS manifest

## Outputs
- GCS-first harmonized artifacts:
  - one BRCA pre-GME review Parquet snapshot
  - one final BRCA Parquet/CSV snapshot after GME integration
  - manifests/reports that describe liftover, normalization, collisions, and validation state
- BigQuery may be used only ephemerally for raw-source extraction when absolutely required; durable harmonized outputs are not part of the current cost-controlled architecture.
- Both checkpoint tables must expose, at minimum, the user-mandated publication-facing header:
  - `CHROM, POS, END, ID, REF, ALT, VARTYPE, Repeat, Segdup, Blacklist, GENE, EFFECT, HGVS_C, HGVS_P, PHENOTYPES_OMIM, PHENOTYPES_OMIM_ID, INHERITANCE_PATTERN, ALLELEID, CLNSIG, TOPMED_AF, TOPMed_Hom, ALFA_AF, ALFA_Hom, GNOMAD_ALL_AF, gnomAD_all_Hom, GNOMAD_MID_AF, gnomAD_mid_Hom, ONEKGP_AF, REGENERON_AF, TGP_AF, QATARI, JGP_AF, JGP_MAF, JGP_Hom, JGP_Het, JGP_AC_Hemi, SIFT_PRED, POLYPHEN2_HDIV_PRED, POLYPHEN2_HVAR_PRED, PROVEAN_PRE`
- Additional columns are allowed only when they are clearly marked as pipeline extras in the UI/export layer.
- Missing source-backed values must remain `NULL`; they must not be guessed or hard-coded.

## Requirements
- Harmonized-layer scope:
  - do not keep per-source `h_*` or staging-derived BRCA tables/views as long-lived outputs
  - do not keep durable harmonized BigQuery tables during the current cost-controlled phase
  - retain the checkpoint artifacts in GCS as the default downstream handoff surface
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
  - Great Expectations suites + checkpoints for harmonized artifacts
  - dbt and/or DuckDB-backed validation checks to enforce canonical key invariants without restoring durable BigQuery harmonized tables

## Success Criteria
- [ ] All harmonized artifacts use the canonical GRCh38 `variant_key`.
- [ ] Liftover failures are explicitly logged and summarized (not silently dropped).
- [ ] Normalization collisions/duplicates are detected and reported.
- [ ] GE + validation tests pass for harmonized invariants (or failures are documented with remediation).
