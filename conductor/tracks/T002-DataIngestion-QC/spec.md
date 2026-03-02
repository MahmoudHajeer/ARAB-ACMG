# Specification: T002-Data Ingestion & QC

## Goal
Acquire, version, and quality-control all upstream datasets needed to study BRCA1/BRCA2 variant misclassification under ACMG rules, storing raw snapshots in GCS and queryable "raw layer" tables in BigQuery.

## Scope
- Genes: BRCA1, BRCA2
- Variant types: SNVs and small indels
- Build policy:
  - Prefer GRCh38 sources
  - If a source is GRCh37, ingest as-is with explicit provenance; liftover happens in T003

## Primary Sources
- ClinVar (classification baseline + review/conflict metadata)
- gnomAD (global AF + ancestry stratified AF, AN, hom counts, coverage metrics)
- Arab / Middle Eastern frequency sources (as available and license-permitted):
  - GME Variome (summary frequencies)
  - Qatar Genome Program (summary AF tables if accessible)
  - gnomAD Middle Eastern ancestry subset

## Requirements
- Provenance for every source:
  - source name, version, access date, license notes, and a checksum for each raw file
- Storage layout:
  - Raw files in GCS under `raw/<source>/<version>/...`
  - BigQuery "raw layer" tables per source with stable schemas
- QC gate:
  - basic schema validation (required columns present)
  - null-rate thresholds on critical columns (chr/pos/ref/alt, AF, AN)
  - duplicate detection on the canonical variant key
- Deliverables:
  - data dictionary for all raw tables
  - a small reproducible sample snapshot under `data/` for local tests

## References
- Extended narrative roadmap: [Data collection.MD](<../../../Data collection.MD>)

## Success Criteria
- [ ] Raw snapshots exist in GCS for each source, with checksums and provenance recorded.
- [ ] BigQuery raw tables exist with stable schemas for each source.
- [ ] Automated QC checks run and produce a report (counts, null rates, duplicates).
- [ ] A documented data dictionary and provenance table exist for downstream tracks.
