# Implementation Plan: T003-Data Harmonization & Normalization

## Phase 1: Canonical Keys and Transform Contracts
- [ ] 1.1 Confirm harmonized schema and `variant_key` definition per [conductor/data-contracts.md](../../data-contracts.md).
- [ ] 1.2 Define standardized chromosome naming for GRCh38: `chr1..chr22, chrX, chrY, chrMT`.
- [ ] 1.3 Define required transformation metadata fields (`liftover_status`, `norm_status`, tool versions).

## Phase 2: Genome Build Standardization (LiftOver)
- [ ] 2.1 Identify any sources not already GRCh38 and document their `source_build`.
- [ ] 2.2 Implement liftover workflow (cloud-first) and stage results as Parquet in GCS.
- [ ] 2.3 Produce a liftover report (success count, failure count, failure examples).

## Phase 3: Variant Normalization
- [ ] 3.1 Normalize each source (split multiallelics, left-align indels, trim bases).
- [ ] 3.2 Produce a normalization report (before/after counts, duplicates/collisions).
- [ ] 3.3 Persist harmonized Parquet snapshots to GCS with manifests.

## Phase 4: BigQuery Harmonized Layer + Modeling
- [ ] 4.1 Load harmonized tables into BigQuery `arab_acmg_harmonized`.
- [ ] 4.2 Create dbt models for harmonized tables (`h_*`) and mapping tables (`map_*`).
- [ ] 4.3 Add dbt tests for canonical key uniqueness and enum accepted values.

## Phase 5: GE Quality Gates
- [ ] 5.1 Create GE suites/checkpoints for harmonized tables (canonical key, duplicates, status enums).
- [ ] 5.2 Publish GE Data Docs for harmonized validation runs to GCS.

---
**Track Status**: `[ ]`
**Checkpoint SHA**: `[checkpoint: TBA]`
