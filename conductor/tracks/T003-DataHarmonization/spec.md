# Specification: T003-Data Harmonization & Normalization

## Goal
Transform all ingested raw variant datasets into a single consistent, GRCh38-aligned, normalized representation suitable for deterministic joins and downstream ACMG evaluation.

## Inputs
- BigQuery raw-layer tables produced in T002 (ClinVar, gnomAD, Arab-frequency sources)
- Raw snapshots in GCS with provenance manifests

## Requirements
- Coordinate system:
  - All harmonized outputs must be GRCh38
  - If any raw source is GRCh37, perform liftover with explicit tracking of failures
- Variant normalization:
  - split multiallelics into biallelic records
  - left-align and normalize indels
  - trim common bases and enforce consistent REF/ALT representation
- Canonical variant key:
  - stable key used across all harmonized tables (chr38/pos38/ref/alt)
- Auditability:
  - preserve source identifiers (ClinVar VariationID, gnomAD ID where available)
  - record transformation metadata (tool versions, params, counts)

## Tooling (Current Stack)
- `bcftools norm` and `vt normalize` for normalization
- Liftover via CrossMap/LiftOver (GRCh37 -> GRCh38) when needed
- Python (pandas/dask) for tabular transforms and validation

## Success Criteria
- [ ] Harmonized BigQuery dataset exists with per-source normalized tables and a shared variant key.
- [ ] Liftover + normalization reports exist (input count, output count, dropped/failed records).
- [ ] Joins across sources are deterministic and reproducible from raw snapshots.
