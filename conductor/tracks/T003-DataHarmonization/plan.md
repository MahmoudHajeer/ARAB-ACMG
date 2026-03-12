# Implementation Plan: T003-Data Harmonization & Normalization

## Phase 1: Canonical Keys and Transform Contracts
- [x] 1.1 Confirm harmonized schema and `variant_key` definition per [conductor/data-contracts.md](../../data-contracts.md). (`e7c50e8`)
- [x] 1.2 Redefine the harmonized checkpoint schema around the mandated publication-facing header and a single-table-per-checkpoint model; extras may exist only as clearly marked additions. (`600bd3e`)
- [~] 1.3 Define required transformation metadata fields (`parse_status`, `source_artifact_*`, `liftover_status`, `norm_status`, tool versions) and remove any remaining downstream BigQuery dependency from active plans/specs.

## Phase 2: Genome Build Standardization (LiftOver)
- [ ] 2.1 Identify any sources not already GRCh38 and document their `source_build`.
- [ ] 2.2 Implement liftover workflow and stage results as Parquet in GCS.
- [ ] 2.3 Produce a liftover report (success count, failure count, failure examples).

## Phase 3: Variant Normalization
- [ ] 3.1 Normalize each source (split multiallelics, left-align indels, trim bases).
- [ ] 3.2 Produce a normalization report (before/after counts, duplicates/collisions).
- [ ] 3.3 Persist harmonized Parquet snapshots to GCS with manifests.

## Phase 4: Frozen Artifact Layer + Modeling
- [x] 4.1 Adopt frozen GCS checkpoint artifacts as the default harmonized handoff surface for downstream work. (`f1c2974`, implemented via T002 step 5.7 closeout)
- [x] 4.2 Remove durable harmonized BigQuery outputs and keep active BigQuery usage raw-only. (`f1c2974`, implemented via T002 step 5.7 closeout)
- [ ] 4.3 Add canonical-key validation tests against the frozen harmonized artifacts (DuckDB/pytest, whichever is lowest cost and reproducible).

## Phase 5: GE Quality Gates
- [ ] 5.1 Create GE suites/checkpoints for harmonized artifacts (canonical key, duplicates, status enums).
- [ ] 5.2 Publish GE Data Docs for harmonized validation runs to GCS.

---
**Track Status**: `[~]`
**Checkpoint SHA**: `[checkpoint: f1c2974]`
